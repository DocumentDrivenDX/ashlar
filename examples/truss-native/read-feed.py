#!/usr/bin/env python3
"""Register a local consumer and durably spool complete public Truss feed pages."""
import argparse,base64,json,os,sys
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from ashlar_host.lifecycle import finish


def carrier(value):
    if type(value)is bytes:return {'encoding':'base64','content':base64.b64encode(value).decode('ascii')}
    if type(value)is dict:return {key:carrier(item)for key,item in value.items()}
    if type(value)in (tuple,list):return [carrier(item)for item in value]
    return value


def durable(path,value):
    with path.open('x',encoding='utf8')as stream:
        json.dump(value,stream,ensure_ascii=False,allow_nan=False);stream.write('\n');stream.flush();os.fsync(stream.fileno())
    descriptor=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def run(configuration,output):
    import psycopg
    from truss import Ok,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host
    from truss.feed import FeedConsumer,FeedPage,FeedAcknowledged
    with Path(configuration).open('rb')as stream:raw=stream.read(65537)
    if len(raw)>65536:raise ValueError('Configuration bound exceeded')
    config=json.loads(raw)
    if type(config)is not dict or set(config)!={'administrator_uri','consumer_uri','consumer_id','reader_role','maximum_pages'}:raise ValueError('Exact explicit feed configuration required')
    if type(config['maximum_pages'])is not int or not 1<=config['maximum_pages']<=16:raise ValueError('Finite 1..16 pages required')
    if any(type(config[key])is not str or not config[key]for key in ('administrator_uri','consumer_uri','consumer_id','reader_role')):raise ValueError('Explicit existing roles and connections required')
    destination=Path(output)
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or destination.exists():raise ValueError('Fresh canonical private spool directory required')
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public feed operation refused')
        return result.value
    @contextmanager
    def interval(connection,readonly=False):
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        if readonly:connection.execute('SET TRANSACTION READ ONLY')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction(access_mode='read_only'if readonly else'read_write'));assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host));yield assembly,tx
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('Host detach refused; inspect durable outcome')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    destination.mkdir(mode=0o700)
    parent_descriptor=os.open(destination.parent,os.O_RDONLY)
    try:os.fsync(parent_descriptor)
    finally:os.close(parent_descriptor)
    with psycopg.connect(config['administrator_uri'],connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            registration=value(assembly.register_feed_consumer_in_transaction(tx,consumer_id=config['consumer_id'],reader_role=config['reader_role']))
            if not isinstance(registration,FeedConsumer):raise RuntimeError('Actual consumer registration required')
        connection.commit()
    durable(destination/'registration.json',{'original':carrier(asdict(registration)),'caller_commit_confirmed':True})
    pages=[];checkpoint=registration.checkpoint
    with psycopg.connect(config['consumer_uri'],connect_timeout=5)as connection:
        for ordinal in range(config['maximum_pages']):
            with interval(connection,readonly=True)as (assembly,tx):
                page=value(assembly.read_feed_in_transaction(tx,consumer_id=config['consumer_id'],maximum_facts=4096,maximum_bytes=8388608))
                if not isinstance(page,FeedPage)or page.registration_id!=registration.registration_id or page.prior!=checkpoint:raise RuntimeError('Complete original registration/checkpoint correspondence required')
            connection.rollback()
            # Source ACK asserts this complete file's local durable delivery only.
            durable(destination/('page-'+str(ordinal)+'.json'),carrier(asdict(page)))
            with interval(connection)as (assembly,tx):
                acknowledged=value(assembly.acknowledge_feed_in_transaction(tx,page))
                if not isinstance(acknowledged,FeedAcknowledged)or acknowledged.checkpoint!=page.next:raise RuntimeError('Actual source ACK correspondence required')
            connection.commit()
            with interval(connection)as (assembly,tx):
                replay=value(assembly.acknowledge_feed_in_transaction(tx,page))
                if not isinstance(replay,FeedAcknowledged)or replay.outcome!='equal'or replay.checkpoint!=page.next:raise RuntimeError('Committed source checkpoint replay differs')
            connection.commit()
            checkpoint=page.next
            receipt={'page':ordinal,'outcome':page.outcome,'sha256':page.sha256,'writer_xid':page.writer_xid,'ack_original':carrier(asdict(acknowledged)),'ack_committed_replay':carrier(asdict(replay)),'caller_commits_confirmed':True}
            durable(destination/('ack-'+str(ordinal)+'.json'),receipt);pages.append(receipt)
            if page.outcome=='empty_interval':break
    result={'pages':pages,'checkpoint':carrier(asdict(checkpoint)),'drained_to_safe_empty_interval':bool(pages and pages[-1]['outcome']=='empty_interval'),
        'scope':'Original complete Truss committed feed, locally durable file delivery and caller-committed source ACK only; no Delta publication or retention release.'}
    durable(destination/'result.json',result)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Feed delivery refused; inspect retained pages and durable checkpoints before retrying.',file=sys.stderr);raise SystemExit(2)from None
