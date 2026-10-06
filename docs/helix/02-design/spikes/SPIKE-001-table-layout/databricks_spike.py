# Databricks notebook source
# Disposable sandbox only. No resources provisioned or executed by this artifact.
# Uses existing notebook Spark session; configure selected cloud/runtime separately.
import json, math, time
spark = globals().get('spark')
if spark is None:
    raise RuntimeError('Run in an authorized Databricks notebook with Spark')
dbutils.widgets.text('catalog', '')
dbutils.widgets.text('schema', '')
dbutils.widgets.text('rows_per_type', '10000')
dbutils.widgets.text('repeats', '30')
catalog, schema = dbutils.widgets.get('catalog'), dbutils.widgets.get('schema')
if not catalog or not schema or not all(c.isalnum() or c=='_' for c in catalog+schema):
    raise ValueError('Explicit disposable catalog/schema identifiers required')
n, repeats = int(dbutils.widgets.get('rows_per_type')), int(dbutils.widgets.get('repeats'))
if n < 10000 or repeats < 20: raise ValueError('At least 10000 rows/type and 20 repetitions')
spark.sql(f'USE CATALOG `{catalog}`'); spark.sql(f'USE SCHEMA `{schema}`')
# Engine timing excludes network and warehouse queueing: measure those separately
# with the intended Databricks SQL caller before accepting lookup latency.
report={'scope':'native Spark/Delta experiment; not warehouse end-to-end latency',
        'spark':spark.version, 'rows_per_type':n,'repeats':repeats,'results':[]}
report['runtime_version']=[r.asDict() for r in spark.sql('SELECT current_version()').collect()]
base=f"""SELECT t type_id, i id,
 to_json(named_struct('101',concat('g',cast(i%100 as string)),
 '102',i%1000,'103',repeat('x',512))) props_json,
 '{{"future":{{"preserve":true}}}}' retained_json
 FROM range(1,4) tt(t) CROSS JOIN range(1,{n+1}) ii(i)"""
for name,layout in [('spike_bag_l','CLUSTER BY (type_id,id)'),
                    ('spike_bag_z','PARTITIONED BY (type_id)')]:
    spark.sql(f'CREATE TABLE {name} USING DELTA {layout} AS {base}')
    stats='id' if name.endswith('_z') else 'type_id,id'
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.dataSkippingStatsColumns'='{stats}')")
    spark.sql(f'OPTIMIZE {name}' + (' ZORDER BY (id)' if name.endswith('_z') else ''))
spark.sql("""CREATE TABLE spike_promoted_l USING DELTA CLUSTER BY (type_id,id)
 AS SELECT *,get_json_object(props_json,'$.101') group_value,
 cast(get_json_object(props_json,'$.102') AS BIGINT) rank_value FROM spike_bag_l""")
spark.sql("OPTIMIZE spike_promoted_l")
spark.sql("""CREATE TABLE spike_type_a_l USING DELTA CLUSTER BY (id,group_value)
 AS SELECT * FROM spike_promoted_l WHERE type_id=1""")
spark.sql('OPTIMIZE spike_type_a_l')
spark.sql(f"""CREATE TABLE spike_edges_l USING DELTA
 CLUSTER BY (rel_type_id,source_id,target_id) AS
 SELECT rel rel_type_id,(rel-1)*{n*4}+i*4+k id,rel source_type,i source_id,
 rel+1 target_type,CASE WHEN i%100=0 THEN 1 ELSE ((i+k-1)%{n})+1 END target_id,
 k/10.0 score FROM range(1,3) r(rel) CROSS JOIN range(1,{n+1}) a(i)
 CROSS JOIN range(0,4) b(k)""")
spark.sql(f"INSERT INTO spike_edges_l SELECT 1,{n*20}+i,1,1,2,i,0.9 FROM range(1,{n+1}) t(i)")
spark.sql('OPTIMIZE spike_edges_l')
spark.sql('CREATE TABLE spike_edge_ab_l USING DELTA CLUSTER BY (source_id,target_id) AS SELECT * FROM spike_edges_l WHERE rel_type_id=1')
spark.sql('CREATE TABLE spike_edge_bc_l USING DELTA CLUSTER BY (source_id,target_id) AS SELECT * FROM spike_edges_l WHERE rel_type_id=2')
spark.sql('CREATE TABLE spike_edge_ab_reverse_l USING DELTA CLUSTER BY (target_id) AS SELECT id,source_id,target_id FROM spike_edge_ab_l')
for table in ['spike_edge_ab_l','spike_edge_bc_l','spike_edge_ab_reverse_l']:spark.sql(f'OPTIMIZE {table}')
# Disable SQL result caching at session scope where supported. Record any refusal.
try: spark.sql('SET use_cached_result=false')
except Exception as exc: report['cache_setting_error']=str(exc)
queries=[]
for table in ['spike_bag_l','spike_bag_z','spike_promoted_l','spike_type_a_l']:
    grp="get_json_object(props_json,'$.101')" if 'bag' in table else 'group_value'
    queries.extend([(table,'lookup',f'SELECT id,props_json,retained_json FROM {table} WHERE type_id=1 AND id={{key}}'),
                    (table,'list',f"SELECT id FROM {table} WHERE type_id=1 AND {grp}='g42' ORDER BY id LIMIT 100"),
                    (table,'count',f'SELECT {grp} grp,count(*) n FROM {table} WHERE type_id=1 GROUP BY {grp} ORDER BY grp')])
for table in ['spike_edges_l','spike_edge_ab_l','spike_edge_ab_reverse_l']:
    rel_filter=' AND rel_type_id=1' if table=='spike_edges_l' else ''
    queries.append((table,'reverse',f'SELECT id,source_id FROM {table} WHERE target_id=1{rel_filter} ORDER BY id LIMIT 100'))
queries.append(('spike_edges_l','two_hop',"SELECT e.id first_edge,f.id second_edge FROM spike_edges_l e JOIN spike_edges_l f ON e.target_id=f.source_id AND e.target_type=f.source_type WHERE e.rel_type_id=1 AND e.source_type=1 AND e.source_id={key} AND f.rel_type_id=2 AND e.score>=0.1 AND f.score>=0.1 ORDER BY first_edge,second_edge LIMIT 100"))
queries.append(('spike_edge_ab_l','two_hop',"SELECT e.id first_edge,f.id second_edge FROM spike_edge_ab_l e JOIN spike_edge_bc_l f ON e.target_id=f.source_id AND e.target_type=f.source_type WHERE e.source_id={key} AND e.score>=0.1 AND f.score>=0.1 ORDER BY first_edge,second_edge LIMIT 100"))
expected_results={}
for table,shape,template in queries:
    samples=[];fingerprints=[]
    for rep in range(repeats+1):
        # Randomized deterministic identity positions avoid repeated singleton hits.
        key=2+(rep*7919)%(n-1);sql=template.format(key=key)
        start=time.perf_counter();rows=spark.sql(sql).collect();elapsed=(time.perf_counter()-start)*1000
        if rep:samples.append(elapsed)
        result=[r.asDict() for r in rows]
        baseline=expected_results.setdefault((shape,key),result)
        if result!=baseline:raise AssertionError(f'Differential mismatch: {table}/{shape}/{key}')
        fingerprints.append({'key':key,'rows':result})
    samples.sort();report['results'].append({'table':table,'shape':shape,'p50_ms':samples[len(samples)//2],
        'p95_ms':samples[math.ceil(.95*len(samples))-1],'samples_ms':samples,
        'query_plan':'\n'.join(r[0] for r in spark.sql('EXPLAIN FORMATTED '+sql).collect()),
        'result_samples':fingerprints})
report['tables']={}
for row in spark.sql('SHOW TABLES').collect():
    name=row['tableName']
    if name.startswith('spike_'):
        report['tables'][name]=[r.asDict() for r in spark.sql(f'DESCRIBE DETAIL {name}').collect()]
print(json.dumps(report,default=str,indent=2))
# Retain report plus query profiles (bytes/files scanned, pruning, shuffle, spill,
# queued time) from the actual SQL warehouse run. Cleanup only named spike tables
# after evidence is saved; no automatic DROP or existing-table replacement here.
