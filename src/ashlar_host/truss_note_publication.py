"""Fresh local Note Delta publication from actual ordinary public Truss pages."""
import base64,datetime,hashlib,json,os,time
from dataclasses import asdict
from pathlib import Path
from ashlar.apply import empty_state
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.manifest import FIELDS
from ashlar.truss_publication import publish_truss_transaction
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.staging import batch_row
from ashlar.stored_publisher import StoredPublisherBackend
from .truss_backend import TrussPublisherBackend
from .delta_custody import LocalDeltaTransport,DeltaTarget,encoded
from .driver import NativeDriver,PrivatePolicy,AttemptExecutor,CarrierPolicy,ManifestPort,progress_union
from .graph_sql import graph_sql_plan
from .lifecycle import finish
from .publication_planning import local_effect_plan,truss_request_for
from .resources import RESOURCE_ROOT
from .schema_rows import fixture_columns
from .supply_chain_native import PrivateFiniteLease
from .truss_feed_sessions import TrussFeedSessions
from .truss_note_admission import TrussNoteAdmission



def bind_ordinary_connection(connection_factory,context):
    """Adapt the selected ordinary zero-argument factory to session context calls."""
    def ordinary(supplied):
        if supplied is not context:raise PermissionError('Original Note publication context required')
        return connection_factory()
    return ordinary


def register_note_consumer(connection_factory,consumer_id,reader_role):
    """Administrator registers the selected distinct Delta consumer; caller commits."""
    from truss import Ok,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host
    from truss.feed import FeedConsumer
    with connection_factory()as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None;registration=None
        def value(result):
            if not isinstance(result,Ok):raise ValueError('Public Truss registration refused')
            return result.value
        try:
            tx=value(host.adopt_transaction());assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host))
            registration=value(assembly.register_feed_consumer_in_transaction(tx,consumer_id=consumer_id,reader_role=reader_role))
            if not isinstance(registration,FeedConsumer):raise ValueError('Actual complete registration required')
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise ValueError('Registration host detach refused')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
        connection.commit()
    return registration


def read_page(connection_factory,consumer_id):
    from truss import Ok,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host
    from truss.feed import FeedPage
    with connection_factory()as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None;page=None
        def value(result):
            if not isinstance(result,Ok):raise ValueError('Public Truss read refused')
            return result.value
        try:
            tx=value(host.adopt_transaction(access_mode='read_only'));assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host))
            page=value(assembly.read_feed_in_transaction(tx,consumer_id=consumer_id,maximum_facts=4096,maximum_bytes=1048576))
            if not isinstance(page,FeedPage):raise ValueError('Actual complete Truss page required')
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise ValueError('Source host detach refused')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
        connection.rollback()
    return page




def read_next_page(connection_factory,consumer_id,wait_seconds=0,*,deadline=None):
    """Poll ordinary public pages within one bounded wait; never ACK empty pages."""
    if type(wait_seconds)is not int or not 0<=wait_seconds<=60:raise ValueError('Finite wait_seconds integer 0..60 required')
    limit=time.monotonic()+wait_seconds
    if deadline is not None:limit=min(limit,deadline)
    while True:
        page=read_page(connection_factory,consumer_id)
        if page.outcome=='transaction':return page
        remaining=limit-time.monotonic()
        if remaining<=0:return page
        time.sleep(min(1,remaining))


def validate_graph_edge_orders(previous,changes,event_bytes):
    """Validate original edge before-order against published rows before any effects."""
    events={}
    for raw in event_bytes:
        event=json.loads(raw)
        if event['operation']!='property':events[event['xid']+':'+event['seq']]=event
    known={(row['source_system'],row['rel_type_id'],row['id']):row['order_key']for row in previous['edge_current']}
    delivered={(row['feed'],row['epoch'],row['delivery_id'])for row in previous['whole_source_history']}
    def order(image):
        selected=image.get('orderKey')
        if selected=={'state':'null'}:return None
        if type(selected)is dict and set(selected)=={'state','text'}and selected['state']=='text'and type(selected['text'])is str:return selected['text']
        raise ValueError('Original closed edge order-key state required')
    for change in changes:
        if change.state.key.kind!='edge'or (change.feed,change.epoch,change.delivery_id)in delivered:continue
        key=change.state.key;identity=(key.source,str(key.type_id),str(key.id));event=events[change.delivery_id]
        if change.operation=='create':
            if identity in known:raise ValueError('Original edge creation already has published state')
            known[identity]=order(event['after'])
        else:
            if identity not in known or order(event['before'])!=known[identity]:raise ValueError('Original edge before-order differs from published current row')
            if change.operation=='delete':del known[identity]
            else:known[identity]=order(event['after'])


def graph_expected_rows(previous,state,changes,batch,columns,materialized_at,event_bytes,edge_order_keys=()):
    """Independent full source-image projection; never copy proposed SQL rows."""
    validate_graph_edge_orders(previous,changes,event_bytes)
    result=json.loads(encoded(previous));fresh=[change for change in changes if not any(row['delivery_id']==change.delivery_id and row['feed']==change.feed and row['epoch']==change.epoch for row in previous['whole_source_history'])]
    originals={}
    for raw in event_bytes:
        event=json.loads(raw);originals[event['xid']+':'+event['seq']]=raw
    delta=datetime.datetime.fromisoformat(materialized_at)-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)
    stamp=str((delta.days*86400+delta.seconds)*1000000+delta.microseconds)
    cursor=json.dumps({'profile':batch.profile,'cursorBase64':batch.cursor_after},separators=(',',':'))
    touched={change.state.key for change in fresh}
    orders={(key,version):order for key,version,order in edge_order_keys}
    for kind,role,typed in (('object','object_current','type_id'),('edge','edge_current','rel_type_id')):
        selected={key for key in touched if key.kind==kind}
        result[role]=[row for row in result[role]if not any((row['source_system'],row[typed],row['id'])==(key.source,str(key.type_id),str(key.id))for key in selected)]
        for key in sorted(selected):
            entity=state.current.get(key)
            if entity is None:continue
            change=next(item for item in reversed(fresh)if item.state.key==key)
            identity={'source_system':key.source,typed:key.type_id,'id':key.id}
            row={name:None for name,_ in columns[role]}
            row.update({name:str(value)for name,value in identity.items()})
            row.update(schema_revision=entity.schema_revision,entity_version=str(entity.version),props_json=entity.props_json,retained_json=entity.retained_json,source_feed=change.feed,source_epoch=change.epoch,source_cursor_json=cursor,source_delivery_id=change.delivery_id,published_at=stamp,lookup_hash=hashlib.sha256(json.dumps(identity,ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),apply_batch_id=batch.batch_id)
            if kind=='object':row['logical_key_json']='[]'
            else:
                source,target=entity.endpoints
                row.update(source_type=str(source.type_id),source_id=str(source.id),target_type=str(target.type_id),target_id=str(target.id),order_key=orders[key,entity.version])
            result[role].append(row)
    for change in fresh:
        result['whole_source_history'].append({'feed':change.feed,'epoch':change.epoch,'delivery_id':change.delivery_id,'digest':change.raw_digest,'change_json':json.dumps(asdict(change),separators=(',',':')),'raw_base64':base64.b64encode(originals[change.delivery_id]).decode()})
        if change.operation=='delete':
            key=change.state.key;row={name:None for name,_ in columns['tombstone']}
            row.update(source_system=key.source,entity_kind=key.kind,type_id=str(key.type_id),id=str(key.id),entity_version=str(change.state.version),source_feed=change.feed,source_epoch=change.epoch,source_cursor_json=cursor,source_delivery_id=change.delivery_id)
            result['tombstone'].append(row)
    return result



def note_expected_rows(previous,state,changes,batch,columns,materialized_at,event_bytes):
    return graph_expected_rows(previous,state,changes,batch,columns,materialized_at,event_bytes)


def publish_graph_pages(spark,output,*,connection_factory,consumer_id,feed,maximum_pages=8,wait_seconds=0,archived_acceptance_files=(),export_graph=False):
    """Publish accepted scalar objects/edges with real historical catalog admission."""
    return publish_note_pages(spark,output,connection_factory=connection_factory,consumer_id=consumer_id,feed=feed,maximum_pages=maximum_pages,graph=True,wait_seconds=wait_seconds,archived_acceptance_files=archived_acceptance_files,export_graph=export_graph)


def publish_note_pages(spark,output,*,connection_factory,consumer_id,feed,maximum_pages=8,graph=False,wait_seconds=0,archived_acceptance_files=(),export_graph=False):
    """Read, publish and ACK up to eight pages in a fresh owned local destination.

    Registration must already exist at genesis. Partial effects remain on failure;
    this fresh-run example never resets/replaces or blindly resumes them.
    """
    output=Path(output);release=None;custody=None
    if type(export_graph)is not bool or export_graph and not graph:raise ValueError('Graph export requires graph publication and a boolean selection')
    if type(wait_seconds)is not int or not 0<=wait_seconds<=60:raise ValueError('Finite wait_seconds integer 0..60 required')
    if archived_acceptance_files and not graph:raise ValueError('Archived catalogs are selected for graph publication only')
    if type(graph)is not bool:raise ValueError('Explicit scalar graph selection required')
    if not output.is_absolute()or output.parent.resolve()!=output.parent or output.exists()or any(character in str(output)for character in ('`',"'",'\\','\0','\n','\r'))or type(maximum_pages)is not int or not 1<=maximum_pages<=8 or not callable(connection_factory):raise ValueError('Fresh canonical local output and finite ordinary source selection required')
    if any(type(value)is not str or not value for value in (consumer_id,feed)):raise ValueError('Explicit original consumer/feed required')
    # Source is observed before creating destination resources.
    selected=read_page(connection_factory,consumer_id)
    if selected.outcome!='transaction'or json.loads(selected.prior.content).get('nextXid')!='0':raise ValueError('Distinct consumer with pending actual transaction at genesis required')
    context=object();ordinary=bind_ordinary_connection(connection_factory,context);output.mkdir(mode=0o700);lease=PrivateFiniteLease(output,create=True)
    transport=None;primary=None;results=[];state=empty_state();progress={};predecessor='explicit-truss-genesis';expected=None;wait_deadline=None
    try:
        graph_columns=fixture_columns(RESOURCE_ROOT)
        columns={**graph_columns,'attempts':tuple((name,'STRING')for name in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),'manifest':tuple((name,'TIMESTAMP'if name=='recorded_at'else'STRING')for name in FIELDS)}
        namespace='trussgraph'if graph else'trussnote'
        tables={role:'local.'+namespace+'.'+role for role in columns};targets=[]
        for role,schema in columns.items():
            path=output/role;spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(name+' '+kind for name,kind in schema)+') USING DELTA')
            targets.append(DeltaTarget(tables[role],path,spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first()['id']))
        policy=PrivatePolicy(context,tuple(targets))
        transport=LocalDeltaTransport.initialize(spark,output/'operations.sqlite','truss-graph-local'if graph else'truss-note-local',tuple(targets),policy,context=context);policy.initializing=False
        expected={role:[] for role in graph_columns}
        for ordinal in range(maximum_pages):
            if ordinal==0:page=selected
            else:
                if wait_deadline is None:wait_deadline=time.monotonic()+wait_seconds
                page=read_next_page(connection_factory,consumer_id,wait_seconds,deadline=wait_deadline)
            if page.outcome=='empty_interval':break
            lease.renew();sessions=TrussFeedSessions(page,feed,ordinary,context);batch=sessions.batch()
            from .truss_page_archive import encode_truss_page
            original_page=encode_truss_page(page,feed);page_path=output/('source-page-'+str(ordinal+1)+'.json')
            with page_path.open('xb')as stream:
                os.fchmod(stream.fileno(),0o600)
                if stream.write(original_page)!=len(original_page):raise OSError('Complete original source page write required')
                stream.flush();os.fsync(stream.fileno())
            if graph:
                from .truss_catalog_reader import read_historical_catalog_views
                from .truss_graph_admission import TrussGraphAdmission
                admission=TrussGraphAdmission(batch,read_historical_catalog_views(connection_factory,batch,archived_acceptance_files))
            else:admission=TrussNoteAdmission(batch)
            driver=NativeDriver(transport,policy,context,tables,{},admission.changes,graph_columns,source_admission=admission,source_sessions=sessions,ack_sessions=sessions)
            materialized_at=datetime.datetime.now(datetime.timezone.utc).isoformat()
            graph_tables={role:tables[role]for role in graph_columns}
            validate_graph_edge_orders(expected,admission.changes,admission.parsed.event_bytes)
            after,generated=graph_sql_plan(state,batch,graph_tables,materialized_at=materialized_at,schema_policy=admission.admit,schema_transition_policy=admission.admit_transition)
            following=graph_expected_rows(expected,after,admission.changes,batch,graph_columns,materialized_at,admission.parsed.event_bytes,admission.parsed.edge_order_keys)
            steps,elisions=local_effect_plan(generated,expected,graph_tables)
            revisions={json.loads(page.prior.content)['installationId']:str(max(int(value)for value in admission.revisions))}if admission.revisions else {json.loads(page.prior.content)['installationId']:'0'}
            request=truss_request_for(feed,batch,predecessor,revisions);checkpoint=request['source_checkpoint_json']
            publication_id=('truss-graph-'if graph else'truss-note-')+str(ordinal+1)
            intent_path=output/('publication-intent-'+str(ordinal+1)+'.json')
            intent=encoded({'request_digest':request['request_digest'],'publication_id':publication_id,'source_page_path':str(page_path),'source_page_sha256':hashlib.sha256(original_page).hexdigest(),'feed':feed})+'\n'
            with intent_path.open('xb')as stream:
                os.fchmod(stream.fileno(),0o600)
                raw_intent=intent.encode()
                if stream.write(raw_intent)!=len(raw_intent):raise OSError('Complete publication selection write required')
                stream.flush();os.fsync(stream.fileno())
            policy.active={'request':request,'steps':steps,'expected':following,'publication_id':publication_id,'previous_progress':progress,'progress':progress_union(progress,json.loads(checkpoint)),'previous_expected':expected,'generated_steps':generated,'elisions':elisions,'recorded_at':str((datetime.datetime.fromisoformat(materialized_at)-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc))//datetime.timedelta(microseconds=1))}
            target=transport.targets[tables['attempts']]
            attempts=DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid)
            manifest_factory=lambda original,supplied:ManifestPort(driver,original)
            backend=(TrussPublisherBackend(attempts,driver,manifest_factory,context=context)
                if graph else StoredPublisherBackend(attempts,driver,manifest_factory))
            # Concrete backend validates actual native effects/manifest/pins before its
            # TrussFeedSessions performs public ACK and caller commit.
            descriptor=publish_truss_transaction(backend,feed,batch,predecessor=predecessor,schema_revisions_json=request['schema_revisions_json'],context=context)
            results.append({'source_page':{'path':str(page_path),'sha256':hashlib.sha256(original_page).hexdigest()},'writer_xid':page.writer_xid,'page_sha256':page.sha256,'manifest':dict(descriptor.raw)})
            state=after;expected=following;progress=policy.active['progress'];predecessor=publication_id;lease.renew()
        if export_graph and results:
            from .truss_graph_export import read_original_truss_graph_export
            release,custody=read_original_truss_graph_export(driver,with_custody=True)
    except BaseException as error:primary=error
    finish(primary,([transport.close]if transport is not None else [])+[lease.renew,lease.close])
    result={'publications':results,'scope':('Fresh private local Delta scalar graph publication'if graph else'Fresh private local Delta Note publication')+' with actual public Truss source ACK; no Unity Catalog or restart recovery qualification.'}
    if release is not None:
        path=output/'graph-release.json'
        with path.open('xb')as stream:
            if stream.write(release.payload)!=len(release.payload):raise OSError('Complete graph release write required')
        custody_path=output/'graph-custody.json'
        with custody_path.open('xb')as stream:
            if stream.write(custody)!=len(custody):raise OSError('Complete graph custody write required')
        result['graph_release']={'path':str(path),'sha256':release.sha256,'custody_path':str(custody_path),'custody_sha256':hashlib.sha256(custody).hexdigest(),'qualification':'Canonical immutable graph captured with the original live Truss source owner; no protected outbox custody or external engine qualification.'}
    with (output/'result.json').open('x')as stream:json.dump(result,stream,ensure_ascii=False)
    return result
