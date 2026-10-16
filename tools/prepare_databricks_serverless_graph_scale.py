"""Generate one explicit serverless notebook job; never contacts Databricks.

The output is a direct-load physical layout experiment, not an Ashlar publication
or a security qualification. Job timeout limits runtime, not a dollar hard cap.
"""
from __future__ import annotations
import argparse
import hashlib
import inspect
import json
from pathlib import Path
import re
from prepare_databricks_graph_scale import ScaleConfig,prepare_plan,encoded,strict_json,closed

PROFILE='ashlar-serverless-graph-layout-probe/0.1'


def run_probe(spark,plan,layout,*,deadline_seconds,progress):
    """Standalone notebook runtime. All native calls remain emitted plan SQL."""
    import collections,datetime,hashlib,json,time
    start=time.monotonic();stages=[]
    def text(value):return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)
    def remaining():
        if time.monotonic()-start>=deadline_seconds:raise TimeoutError('Graph probe runtime deadline')
    def phase(name,action):
        remaining();progress(dict(stage=name,state='begin'));before=time.perf_counter_ns()
        value=action();remaining()
        stages.append(dict(stage=name,elapsed_ns=time.perf_counter_ns()-before))
        progress(dict(stage=name,state='completed'))
        return value
    def cells(sql,maximum_rows=1024,maximum_bytes=1048576):
        remaining();frame=spark.sql(sql);names=frame.columns
        if len(names)>64 or len(names)!=len(set(names)):raise ValueError('Bounded unique result columns required')
        result=[];size=0;iterator=frame.limit(maximum_rows+1).toLocalIterator()
        primary=None
        try:
            for row in iterator:
                remaining()
                if len(result)>=maximum_rows:raise ValueError('Complete row capacity exceeded')
                item=dict(zip(names,row))
                if any(value is not None and type(value)is not str for value in item.values()):raise ValueError('Exact String result required')
                raw=text(item).encode();size+=len(raw)
                if size>maximum_bytes:raise ValueError('Complete byte capacity exceeded')
                result.append(item)
        except BaseException as error:primary=error
        try:
            close=getattr(iterator,'close',None)
            if callable(close):close()
        except BaseException as error:
            if primary is None or isinstance(primary,Exception) and not isinstance(error,Exception):primary=error
            elif primary is not None:
                try:primary.cleanup_failed=True
                except BaseException:pass
        if primary is not None:raise primary
        return result
    def effect(sql):
        # Fixed CREATE/INSERT statements have no unbounded client row output.
        if not sql.startswith(('CREATE SCHEMA ','CREATE TABLE ','INSERT INTO ')):raise ValueError('Closed setup/ingest only')
        rows=spark.sql(sql).take(2)
        if len(rows)>1:raise ValueError('Bounded command result required')
        return None
    def bag(rows):return collections.Counter(tuple(sorted(row.items())) for row in rows)
    def read_case(query):
        before=time.perf_counter_ns();rows=cells(query['sql'],plan['config']['maximum_rows'],plan['config']['maximum_bytes'])
        captured=time.perf_counter_ns()
        if bag(rows)!=bag(query['expected']):raise ValueError('Independent complete result bag differs')
        return dict(execution_fetch_ns=captured-before,validation_ns=time.perf_counter_ns()-captured,rows=len(rows),passed=True)
    namespace='`'+plan['config']['catalog']+'`.`'+plan['config']['schema']+'`'
    def observation():
        result=[]
        for role in ('object_current','edge_current'):
            table=namespace+'.`'+role+'`';frame=spark.table(table)
            fields=[(f.name,f.dataType.simpleString().upper(),f.nullable) for f in frame.schema.fields]
            expected=layout['carriers'][role]['columns']
            native={'long':'BIGINT','string':'STRING','timestamp':'TIMESTAMP','boolean':'BOOLEAN'}
            if fields!=[(c['name'],native[c['deltaType']],c['nullable']) for c in expected]:raise ValueError('Complete generated native schema differs')
            detail=spark.sql('DESCRIBE DETAIL '+table).select('id','format','location','properties','clusteringColumns').take(2)
            history=spark.sql('DESCRIBE HISTORY '+table+' LIMIT 1').select('version').take(2)
            if len(detail)!=1 or len(history)!=1 or detail[0]['format']!='delta':raise ValueError('Actual Delta identity required')
            if type(history[0]['version']) is not int:raise ValueError('Native integral Delta version required')
            properties=dict(detail[0]['properties'])
            selected_layout=layout['carriers'][role]['layout']
            clustering=list(detail[0]['clusteringColumns'])
            if clustering!=selected_layout['clusterBy']:raise ValueError('Generated clustering columns differ')
            if any(properties.get(key)!=value for key,value in selected_layout['properties'].items()):raise ValueError('Required generated table properties differ')
            if len(text(properties).encode())>65536:raise ValueError('Native property capacity exceeded')
            if spark.catalog.getTable(table).tableType!='MANAGED':raise ValueError('Managed table required')
            result.append(dict(role=role,uuid=detail[0]['id'],version=str(history[0]['version']),location=detail[0]['location'],
                schema_sha256=hashlib.sha256(frame.schema.json().encode()).hexdigest(),properties=properties,clustering_columns=clustering))
        return result
    # Explicit fixture semantics selected before any CREATE or INSERT.
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.conf.set('spark.databricks.execution.timeout','90')
    session_configuration={key:spark.conf.get(key) for key in ('spark.sql.session.timeZone','spark.databricks.execution.timeout')}
    if session_configuration!={'spark.sql.session.timeZone':'UTC','spark.databricks.execution.timeout':'90'}:raise ValueError('Explicit serverless fixture session configuration required')
    for i,sql in enumerate(plan['setup']):phase('setup-'+str(i),lambda sql=sql:effect(sql))
    for i,sql in enumerate(plan['ingest']):phase('ingest-'+str(i),lambda sql=sql:effect(sql))
    for i,sql in enumerate(plan['validation_sql']):
        rows=phase('count-'+str(i),lambda sql=sql:cells(sql,1,1024))
        if rows!=[{'count':plan['expected_counts'][i]}]:raise ValueError('Complete fixture count differs')
    opening=phase('opening-native',observation);queries=[]
    for query in plan['queries']:
        warmup=phase(query['name']+'-warmup',lambda query=query:read_case(query))
        samples=[phase(query['name']+'-sample-'+str(i),lambda query=query:read_case(query)) for i in range(1)]
        queries.append(dict(name=query['name'],expected=query['expected'],warmup=warmup,samples=samples))
    closing=phase('closing-native',observation)
    if closing!=opening:raise ValueError('Native table identity/version/schema/properties changed')
    return dict(profile='ashlar-serverless-graph-layout-probe/0.1',state='completed',scope=plan['scope'],
        nodes=plan['config']['nodes'],edges=plan['config']['edges'],layout_sha256=plan['layout_sha256'],
        warmup_runs=1,warm_runs=1,stages=stages,queries=queries,native_opening=opening,native_closing=closing,
        elapsed_seconds=time.monotonic()-start,session_configuration=session_configuration,predictive_optimization='effective inherited state unknown; no settings changed',
        cost_usd=None,cost_scope='No dollar hard cap or attributed usage measurement; external owner budget gate required',
        runtime=dict(spark=spark.version),queue_ns=None,execution_ns=None,fetch_ns=None,cache='not controlled')


def prepare_serverless(raw,layout_bytes):
    value=strict_json(raw)
    if type(value)is not dict:raise ValueError('Closed explicit configuration required')
    value=dict(value)
    performance=value.pop('performance_target','STANDARD')
    if type(performance)is not str or performance not in ('STANDARD','PERFORMANCE_OPTIMIZED'):raise ValueError('Closed explicit serverless performance target required')
    closed(value,('profile','catalog','schema','stage','seed','deadline_seconds','whole_seconds','notebook_path','job_name','maximum_cost_usd','layout_sha256'))
    if value['profile']!=PROFILE or value['catalog']!='client_dev':raise ValueError('Explicit client_dev serverless profile required')
    if type(value['schema'])is not str or re.fullmatch(r'ashlar_scale_[a-z0-9_]{1,40}',value['schema'])is None:raise ValueError('Fresh probe namespace required')
    if type(value['stage'])is not str or value['stage'] not in ('initial','expanded-explicit'):raise ValueError('Explicit separately authorized scale stage required')
    nodes,edges=(100000,1000000) if value['stage']=='initial' else (1000000,10000000)
    if type(value['whole_seconds'])is not int or value['whole_seconds'] not in (360,480):raise ValueError('Explicit360 or480 second outer run budget required')
    if type(value['seed'])is not int or not 1<=value['seed']<2**31:raise ValueError('Exact positive seed required')
    deadline=value['deadline_seconds']
    if type(deadline)is not int or not 120<=deadline<=180:raise ValueError('Explicit120–180 second runtime ceiling required')
    if type(value['notebook_path'])is not str or not value['notebook_path'].startswith('/Users/') or '..' in value['notebook_path'].split('/') or len(value['notebook_path'])>512:raise ValueError('Explicit owned workspace notebook path required')
    if type(value['job_name'])is not str or re.fullmatch(r'ashlar-scale-[a-z0-9-]{1,48}',value['job_name'])is None:raise ValueError('Fresh isolated job name required')
    if value['maximum_cost_usd']!='25.00':raise ValueError('Explicit25 dollar owner budget required')
    config=ScaleConfig('client_dev',value['schema'],'serverless-job','new-single-task',nodes,edges,value['seed'],90,value['whole_seconds'],1024,1048576,'25.00',value['layout_sha256'])
    plan=prepare_plan(config,layout_bytes)
    plan['measurement'].update(warm_runs=1,override='Explicit serverless pilot1warmup/1measured instead of prepared5')
    source='# Databricks notebook source\nimport json\nimport time\nPLAN=json.loads('+repr(encoded(plan).decode())+')\nLAYOUT=json.loads('+repr(layout_bytes.decode())+')\n'+inspect.getsource(run_probe)+'\nRESULT=run_probe(spark,PLAN,LAYOUT,deadline_seconds='+str(deadline)+',progress=lambda row:print(json.dumps(row)))\nRETAINED=json.dumps(RESULT,ensure_ascii=False,separators=(\",\",\":\"))\nif len(RETAINED.encode())>1048576:raise ValueError(\"Bounded final result required\")\ndbutils.notebook.exit(RETAINED)\n'
    settings=dict(name=value['job_name'],max_concurrent_runs=1,performance_target=performance,timeout_seconds=value['whole_seconds'],queue=dict(enabled=False),
        tasks=[dict(task_key='graph-layout',notebook_task=dict(notebook_path=value['notebook_path'],source='WORKSPACE'),timeout_seconds=value['whole_seconds'],max_retries=0,retry_on_timeout=False,disable_auto_optimization=True)])
    return dict(profile=PROFILE,plan=plan,job_settings=settings,notebook=source,
        limitations=['No cloud calls, upload, compute acquisition, grants or cleanup performed by generator',
        'External runtime timeout controls blocking Spark calls; cooperative checks occur only between calls',
        'Task/job native timeouts include service-defined setup accounting; notebook clock starts at run_probe; no dollar hard cap',
        'No existing cluster or SQL warehouse is referenced; explicit serverless performance mode single task only',
        'No predictive optimization/table maintenance settings changed; effective inherited predictive optimization remains unknown',
        'Expanded stage is explicit selection only; no automatic second run or expansion'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('configuration','layout','output'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args()
    result=prepare_serverless(args.configuration.read_bytes(),args.layout.read_bytes())
    args.output.mkdir(mode=0o700)
    for name,payload in (('notebook.py',result.pop('notebook').encode()),('preparation.json',encoded(result))):
        with (args.output/name).open('xb') as stream:
            if stream.write(payload)!=len(payload):raise OSError('Complete preparation write required')

if __name__=='__main__':main()
