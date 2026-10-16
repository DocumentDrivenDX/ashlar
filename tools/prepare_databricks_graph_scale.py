"""Prepare a deterministic managed-layout probe. Never connects or starts compute.

Direct SQL fixture loading measures physical layout only, not publication/source
authority. Runtime owners must independently authorize compute, spend and effects.
"""
from __future__ import annotations
import argparse
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

SHARED_WAREHOUSE = '2439e1f2e37ac563'
PROFILE = 'ashlar-managed-graph-layout-probe/0.1'
CLOCK = '2026-10-15T00:00:00.000Z'
SOURCE = 'layout-probe'


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def strict_json(raw):
    if type(raw) is not bytes or len(raw) > 1_048_576:
        raise ValueError('Bounded exact configuration bytes required')
    def pairs(items):
        result = {}
        for k, v in items:
            if k in result: raise ValueError('Duplicate member')
            result[k] = v
        return result
    def reject(_): raise ValueError('Floating/nonfinite atoms refused')
    return json.loads(raw, object_pairs_hook=pairs, parse_float=reject, parse_constant=reject)


def closed(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ValueError('Closed explicit configuration required')


def bounded(value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError('Exact positive bounded integer required')
    return value


@dataclass(frozen=True)
class ScaleConfig:
    catalog: str
    schema: str
    compute_kind: str
    compute_id: str
    nodes: int
    edges: int
    seed: int
    statement_seconds: int
    whole_seconds: int
    maximum_rows: int
    maximum_bytes: int
    maximum_cost_usd: str
    layout_sha256: str


def parse_configuration(raw):
    v = strict_json(raw)
    closed(v, ('profile','catalog','schema','compute','scale','seed','limits','cache_mode','layout_sha256'))
    if v['profile'] != PROFILE or v['cache_mode'] != 'observe-without-changing-settings':
        raise ValueError('Exact probe/cache profile required')
    for k in ('catalog','schema'):
        if type(v[k]) is not str or re.fullmatch('[a-z][a-z0-9_]{0,62}',v[k]) is None:
            raise ValueError('Explicit safe fresh namespace required')
    c=v['compute'];closed(c,('kind','id','dedicated'))
    if (c['kind'] not in ('sql-warehouse','spark-cluster') or type(c['kind']) is not str
            or type(c['id']) is not str or re.fullmatch('[A-Za-z0-9-]{1,128}',c['id']) is None
            or c['id']==SHARED_WAREHOUSE or c['dedicated'] is not True):
        raise ValueError('Explicit dedicated compute required; shared warehouse refused')
    s=v['scale'];closed(s,('stage','nodes','edges'))
    if s['stage']=='initial': maximum=(100_000,1_000_000)
    elif s['stage']=='expanded-explicit': maximum=(1_000_000,10_000_000)
    else: raise ValueError('Explicit supported scale stage required')
    n=bounded(s['nodes'],maximum[0]);e=bounded(s['edges'],maximum[1])
    if n<100 or e!=10*n: raise ValueError('At least100 nodes and exactly10 edges/node required')
    seed=bounded(v['seed'],2**31-1)
    l=v['limits'];closed(l,('statement_seconds','whole_seconds','maximum_rows','maximum_bytes','maximum_cost_usd'))
    statement=bounded(l['statement_seconds'],900);whole=bounded(l['whole_seconds'],3600)
    rows=bounded(l['maximum_rows'],1024);size=bounded(l['maximum_bytes'],1_048_576)
    if whole <= statement: raise ValueError('Whole deadline must exceed statement deadline')
    cost=l['maximum_cost_usd']
    if type(cost) is not str or re.fullmatch('[0-9]{1,5}(?:\.[0-9]{1,2})?',cost) is None:
        raise ValueError('Explicit decimal spend ceiling required')
    try: valid=Decimal(cost)>0
    except InvalidOperation: valid=False
    if not valid: raise ValueError('Positive spend ceiling required')
    digest=v['layout_sha256']
    if type(digest) is not str or re.fullmatch('[0-9a-f]{64}',digest) is None:
        raise ValueError('Original generated layout digest required')
    return ScaleConfig(v['catalog'],v['schema'],c['kind'],c['id'],n,e,seed,
        statement,whole,rows,size,cost,digest)


def outgoing(config, node):
    """Independent tiny root oracle; never materializes the graph on the host."""
    active=config.nodes-config.nodes//100
    if node>=active: return ()
    result=[]
    for base in range(node*10,config.edges,active*10):
        for edge in range(base,min(base+10,config.edges)):
            result.append((edge,(node+(edge%10)//2)%active))
    return tuple(result)


def singleton(config,node):
    identity={'source_system':SOURCE,'type_id':1,'id':node}
    return dict(source_system=SOURCE,type_id='1',id=str(node),logical_key_json='[]',
        schema_revision='layout-probe-r1',entity_version='1',
        props_json=json.dumps({'label':f'node-{node}','seed':config.seed},separators=(',',':')),
        retained_json='{}',root_id=None,source_feed='direct-layout-fixture',source_epoch='1',
        source_position=str(node),published_at=CLOCK,
        lookup_hash=hashlib.sha256(encoded(identity)).hexdigest(),apply_batch_id='layout-probe',
        source_cursor_json='{}',source_delivery_id=str(node))


def prepare_plan(config, layout_bytes):
    if type(config) is not ScaleConfig or type(layout_bytes) is not bytes or len(layout_bytes)>1_048_576:
        raise ValueError('Typed config and bounded original generated layout required')
    if hashlib.sha256(layout_bytes).hexdigest()!=config.layout_sha256:
        raise ValueError('Original generated layout changed')
    layout=strict_json(layout_bytes)
    if layout.get('profile')!='ashlar-generated-delta-carriers/0.1':
        raise ValueError('Canonical UMF-generated carrier profile required')
    q=lambda name:'`'+name+'`'
    namespace=q(config.catalog)+'.'+q(config.schema)
    tables={role:namespace+'.'+q(role) for role in ('object_current','edge_current')}
    ddl=[]
    for role in tables:
        carrier=layout['carriers'][role];original=carrier['sql'];prefix='CREATE TABLE '+q(role)
        if type(original) is not str or not original.startswith(prefix+' ('):
            raise ValueError('Exact generated managed CREATE required')
        ddl.append(original.replace(prefix,'CREATE TABLE '+tables[role],1))
    active=config.nodes-config.nodes//100
    identity="sha2(to_json(named_struct('source_system','layout-probe','type_id',CAST(1 AS BIGINT),'id',id)),256)"
    node_values={
        'source_system':"'layout-probe'",'type_id':'CAST(1 AS BIGINT)','id':'id',
        'logical_key_json':"'[]'",'schema_revision':"'layout-probe-r1'",'entity_version':'CAST(1 AS BIGINT)',
        'props_json':f"to_json(named_struct('label',concat('node-',CAST(id AS STRING)),'seed',{config.seed}))",
        'retained_json':"'{}'",'root_id':'CAST(NULL AS BIGINT)',
        'source_feed':"'direct-layout-fixture'",'source_epoch':"'1'",'source_position':'id',
        'published_at':f"TIMESTAMP '{CLOCK}'",'lookup_hash':identity,
        'apply_batch_id':"'layout-probe'",'source_cursor_json':"'{}'",'source_delivery_id':'CAST(id AS STRING)'}
    edge_values=dict(node_values)
    edge_values.pop('type_id');edge_values.pop('logical_key_json');edge_values.pop('root_id')
    edge_values.update(rel_type_id='CAST(1 AS BIGINT)',source_type='CAST(1 AS BIGINT)',
        source_id=f'pmod(CAST(floor(id/10) AS BIGINT),{active})',target_type='CAST(1 AS BIGINT)',
        target_id=f'pmod(pmod(CAST(floor(id/10) AS BIGINT),{active})+CAST(floor(pmod(id,10)/2) AS BIGINT),{active})',
        props_json="'{}'",order_key='CAST(NULL AS STRING)',
        lookup_hash="sha2(to_json(named_struct('source_system','layout-probe','rel_type_id',CAST(1 AS BIGINT),'id',id)),256)")
    ingest=[]
    for role,count,values in [('object_current',config.nodes,node_values),('edge_current',config.edges,edge_values)]:
        columns=[v['name'] for v in layout['carriers'][role]['columns']]
        if set(columns)!=set(values): raise ValueError('Complete canonical carrier columns differ')
        ingest.append('INSERT INTO '+tables[role]+' ('+','.join(map(q,columns))+') SELECT '+
            ','.join(values[k] for k in columns)+f' FROM range({count})')
    columns=[v['name'] for v in layout['carriers']['object_current']['columns']]
    selected=','.join(("date_format(published_at,\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")" if k=='published_at'
                      else 'CAST('+q(k)+' AS STRING)')+' AS '+q(k) for k in columns)
    key=singleton(config,0)['lookup_hash']
    queries=[dict(name='singleton',sql=f"SELECT {selected} FROM {tables['object_current']} WHERE lookup_hash='{key}' AND source_system='layout-probe' AND type_id=1 AND id=0",
                  expected=[singleton(config,0)])]
    edges=tables['edge_current']
    for root,label in [(0,'connected'),(config.nodes-1,'isolate')]:
        one=[{'edge':str(e),'target':str(t)}for e,t in outgoing(config,root)]
        two=[{'edge1':str(e),'edge2':str(e2),'target':str(t2)}for e,t in outgoing(config,root)for e2,t2 in outgoing(config,t)]
        if max(len(one),len(two))>config.maximum_rows: raise ValueError('Independent path bag exceeds selected output bound')
        scope="source_system='layout-probe' AND rel_type_id=1 AND source_type=1"
        queries.extend([
            dict(name=label+'-one-hop',sql=f"SELECT CAST(id AS STRING) AS edge,CAST(target_id AS STRING) AS target FROM {edges} WHERE {scope} AND source_id={root}",expected=one),
            dict(name=label+'-two-hop',sql=f"SELECT CAST(a.id AS STRING) AS edge1,CAST(b.id AS STRING) AS edge2,CAST(b.target_id AS STRING) AS target FROM {edges} a JOIN {edges} b ON a.source_system=b.source_system AND a.target_type=b.source_type AND a.target_id=b.source_id WHERE a.source_system='layout-probe' AND a.rel_type_id=1 AND b.rel_type_id=1 AND a.source_type=1 AND a.source_id={root}",expected=two),
            dict(name=label+'-distinct-terminal',sql=f"SELECT CAST(COUNT(DISTINCT b.target_id) AS STRING) AS count FROM {edges} a JOIN {edges} b ON a.source_system=b.source_system AND a.target_type=b.source_type AND a.target_id=b.source_id WHERE a.source_system='layout-probe' AND a.rel_type_id=1 AND b.rel_type_id=1 AND a.source_type=1 AND a.source_id={root}",expected=[{'count':str(len({r['target']for r in two}))}])])
    return dict(profile=PROFILE,state='prepared-not-executed',scope='Direct-load canonical managed-layout performance probe only; no publication/source/ACK/security authority',
        config=config.__dict__,generator=layout['generator'],layout_sha256=config.layout_sha256,
        topology=dict(nodes=config.nodes,edges=config.edges,isolates=config.nodes-active,
            parallel_edges=True,self_loops=True,degree_profile='bounded near-uniform; no hub/skew qualification',
            hash_profile='synthetic paired SQL/host formula; not publisher authority',formula='source=floor(edge/10)%active; target=(source+floor((edge%10)/2))%active'),
        setup=['CREATE SCHEMA '+namespace,*ddl],ingest=ingest,queries=queries,
        validation_sql=[f'SELECT CAST(COUNT(*) AS STRING) AS count FROM {tables[r]}'for r in tables],
        expected_counts=[str(config.nodes),str(config.edges)],
        observations=[f'DESCRIBE DETAIL {t}'for t in tables.values()]+[f'DESCRIBE HISTORY {t} LIMIT 1'for t in tables.values()]+[f'SHOW TBLPROPERTIES {t}'for t in tables.values()],
        measurement=dict(startup='separate externally observed phase',ingest='separate statement phase',warmup_runs=1,warm_runs=5,
            cache_mode='observe-without-changing-settings',execution_fetch='combined unless native timing supplied; never infer split',
            validation='separate host phase',query_profile='unavailable unless captured by native owner',
            dbu_cost='unavailable unless independently attributable native usage is supplied',
            predictive_optimization='observe inherited/effective state; never change or claim maintenance-free timing'),
        prerequisites=['Independent dedicated-compute and spend authorization; configuration is not permission',
            'Fresh schema only; no overwrite/reuse/drop or compute creation/start by this preparation tool',
            'Bounded complete results and original UUID/version/schema/layout checks before and after measured reads',
            'Record native queue/execution/fetch/cache/scan statistics when available; unavailable is not zero'])


def measure_query(executor, query, *, maximum_rows, maximum_bytes):
    """Existing SQL Executor port, read-only query only; execute/fetch is combined.

    No runtime is acquired here. Native statement deadlines and output bounds
    before materialization remain mandatory executor responsibilities.
    """
    bounded(maximum_rows,1024);bounded(maximum_bytes,1_048_576)
    if not query['sql'].startswith('SELECT '): raise ValueError('Prepared read query required')
    start=time.perf_counter_ns();result=executor.query(query['sql'],{})
    captured=time.perf_counter_ns();rows=list(result.rows)
    if len(rows)>maximum_rows or len(encoded(rows))>maximum_bytes:
        raise ValueError('Complete measurement output capacity exceeded')
    def bag(values): return Counter(tuple(sorted(dict(row).items())) for row in values)
    if bag(rows)!=bag(query['expected']): raise ValueError('Independent complete result bag differs')
    end=time.perf_counter_ns()
    return dict(execution_fetch_ns=captured-start,validation_ns=end-captured,
        queue_ns=None,execution_ns=None,fetch_ns=None,dbu=None,query_profile=None,
        cache='not-observed',rows=len(rows),passed=True)


class SparkSQLExecutor:
    """Explicit dedicated Spark driver adapter; caller owns session and cleanup."""
    def __init__(self,spark,*,maximum_rows,maximum_bytes):
        self.spark=spark;self.maximum_rows=bounded(maximum_rows,1024);self.maximum_bytes=bounded(maximum_bytes,1_048_576)
    def query(self,sql,parameters):
        from ashlar.native import SQLResult
        if parameters or not sql.startswith('SELECT '): raise ValueError('Fixed prepared read query required')
        frame=self.spark.sql(sql);names=frame.columns
        if len(names)!=len(set(names)): raise ValueError('Unique probe column names required')
        rows=[];size=0;iterator=frame.limit(self.maximum_rows+1).toLocalIterator(prefetchPartitions=False)
        from ashlar_host.lifecycle import finish
        primary=None
        try:
            for row in iterator:
                item=dict(zip(names,row))
                if len(rows)>=self.maximum_rows or any(v is not None and type(v)is not str for v in item.values()):
                    raise ValueError('Bounded native String probe result required')
                size+=len(encoded(item))
                if size>self.maximum_bytes: raise ValueError('Probe byte capacity exceeded')
                rows.append(item)
        except BaseException as error: primary=error
        finish(primary,[iterator.close] if callable(getattr(iterator,'close',None)) else [])
        return SQLResult(rows,tuple((name,'STRING')for name in names))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--configuration',type=Path,required=True);p.add_argument('--layout',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    from ashlar_host.evolution_producer import snapshot
    for path in (a.configuration,a.layout):
        if not path.is_absolute() or path.resolve()!=path: raise ValueError('Canonical explicit input required')
    if (not a.output.is_absolute() or a.output.parent.resolve()!=a.output.parent or a.output.exists()
            or a.output.parent.stat().st_uid!=os.getuid() or stat.S_IMODE(a.output.parent.stat().st_mode)!=0o700):
        raise ValueError('Fresh output under owned private canonical parent required')
    config_raw=snapshot(a.configuration,1_048_576);layout_raw=snapshot(a.layout,1_048_576)
    plan=prepare_plan(parse_configuration(config_raw),layout_raw)
    if snapshot(a.configuration,1_048_576)!=config_raw or snapshot(a.layout,1_048_576)!=layout_raw:
        raise ValueError('Original input changed')
    raw=encoded(plan)+b'\n'
    fd=os.open(a.output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    from ashlar_host.lifecycle import finish
    primary=None
    try:
        if os.write(fd,raw)!=len(raw):raise OSError('Preparation output write refused')
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)])


if __name__=='__main__':main()
