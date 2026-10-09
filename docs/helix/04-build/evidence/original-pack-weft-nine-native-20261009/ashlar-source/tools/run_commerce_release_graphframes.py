"""Seven original commerce common queries on a separately admitted immutable export.

Private local Delta/GraphFrames compatibility evidence only; canonical scalar
carriers stay exact text. No scalar promotion or ongoing source retention claim.
"""
import argparse,base64,hashlib,importlib.metadata,json,sys
from collections import Counter
from pathlib import Path
from ashlar.graph_release import _carrier
from commerce_source_transaction import build_transaction
from fixture_oracle import fixture_columns
from run_commerce_outbox_publication import original_commerce_oracle,SOURCE_SYSTEM
from run_graph_release_graphframes import load_release,columns,graph_rows,row_bag,JARS,VERSIONS
from private_graph_custody import PROFILE

def original(value):
    admission=json.loads(value['publication']['validation_report_json'])['source_admission']
    model=base64.b64decode(admission['original_model_base64'],validate=True)
    graph_bytes=base64.b64decode(admission['original_graph_base64'],validate=True)
    binding_bytes=base64.b64decode(admission['original_bindings_base64'],validate=True)
    if hashlib.sha256(binding_bytes).hexdigest()!=admission['development_bindings_sha256']:raise ValueError('Original binding bytes differ')
    bindings=json.loads(binding_bytes);graph=json.loads(graph_bytes)
    batch,rebuilt=build_transaction(model,graph_bytes,source_system=SOURCE_SYSTEM,binding_profile=bindings['profile'])
    if rebuilt!=bindings:raise ValueError('Original full development bindings differ')
    expected=original_commerce_oracle(model,graph_bytes,bindings,batch,fixture_columns(Path(__file__).resolve().parents[1]))
    for kind,role,source in [('node','nodes','object_current'),('edge','edges','edge_current')]:
        if row_bag(value[role])!=row_bag(_carrier(r,kind) for r in expected[source]):raise ValueError('Independent original complete carrier bag differs')
    entities={(e['kind'],e['originalKey']):e for e in bindings['entities']}
    node_map={r['graph_id']:next(o['key'] for o in graph['objects'] if entities['object',o['key']]['id']==r['id'] and entities['object',o['key']]['type_id']==r['type_id']) for r in value['nodes']}
    edge_map={r['graph_id']:next(e['key'] for e in graph['edges'] if entities['edge',e['key']]['id']==r['id'] and entities['edge',e['key']]['type_id']==r['rel_type_id']) for r in value['edges']}
    return graph,node_map,edge_map,admission

def query_oracle(graph):
    edges=graph['edges'];objects=graph['objects'];singleton=min(o['key'] for o in objects)
    selected=next(o for o in objects if o['key']==singleton)['type']
    filtered=sorted(o['key'] for o in objects if o['type']==selected)
    groups={}
    for e in edges:
        key=(e['source'],json.dumps(e['relationship'],sort_keys=True,separators=(',',':')))
        groups.setdefault(key,[]).append(e['target'])
    return {'one-hop':[[e['source'],e['key'],e['target']] for e in edges],
            'two-hop':[[a['source'],a['key'],a['target'],b['key'],b['target']] for a in edges for b in edges if a['target']==b['source']],
            'grouped-count':[[s,t,len(targets),len(set(targets))] for (s,t),targets in groups.items()],
            'singleton':[singleton],'filtered-limit':filtered[:2],'filtered-total':len(filtered)}

def bag(rows):return Counter(json.dumps(r,ensure_ascii=False,sort_keys=True,separators=(',',':')) for r in rows)

def run(release,custody,release_sha,custody_sha,output,jars):
    value=load_release(release,release_sha,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=custody_sha)
    graph,nodes,edges,admission=original(value);expected=query_oracle(graph)
    versions={p:importlib.metadata.version(p) for p in VERSIONS}
    if versions!=VERSIONS:raise ValueError('Explicit qualified existing runtime required')
    paths=[Path(jars)/p for p in JARS]
    if not all(p.is_file() for p in paths):raise ValueError('Existing jars required')
    output=Path(output)
    if output.exists():raise ValueError('Fresh output required')
    output.mkdir(parents=True)
    from pyspark.sql import SparkSession,functions as F
    from pyspark.sql.types import StructType,StructField,StringType
    from graphframes import GraphFrame
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original commerce complete bags')
      .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false')
      .config('spark.jars',','.join(map(str,paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
      .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    try:
        frames={};snapshots={}
        for kind,role in [('node','nodes'),('edge','edges')]:
            schema=StructType([StructField(c,StringType(),True) for c in columns(kind)])
            spark.createDataFrame(value[role],schema).write.format('delta').save(str(output/role))
            frame=spark.read.format('delta').option('versionAsOf',0).load(str(output/role))
            snapshots[role]={'uuid':spark.sql("DESCRIBE DETAIL delta.`"+str(output/role)+"`").first()['id'],'version':0}
            frames[role]=frame.withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id')
        g=GraphFrame(frames['nodes'],frames['edges']);actual={}
        for role,frame in frames.items():
            rows=[r.asDict() for r in frame.collect()]
            if row_bag(rows)!=row_bag(graph_rows(value[role])):raise AssertionError('Full original carrier parity')
            actual['all-objects' if role=='nodes' else 'all-edges']=rows
        actual['one-hop']=[[nodes[r.a],edges[r.e],nodes[r.b]] for r in g.find('(a)-[e]->(b)').select('a.id','e.id','b.id').toDF('a','e','b').collect()]
        actual['two-hop']=[[nodes[r.a],edges[r.e],nodes[r.b],edges[r.f],nodes[r.c]] for r in g.find('(a)-[e]->(b); (b)-[f]->(c)').select('a.id','e.id','b.id','f.id','c.id').toDF('a','e','b','f','c').collect()]
        types={e['key']:json.dumps(e['relationship'],sort_keys=True,separators=(',',':')) for e in graph['edges']}
        reltypes={r['rel_type_id']:types[edges[r['graph_id']]] for r in value['edges']}
        actual['grouped-count']=[[nodes[r.src],reltypes[r.rel_type_id],r.occurrences,r.destinations] for r in frames['edges'].groupBy('src','rel_type_id').agg(F.count('*').alias('occurrences'),F.countDistinct('dst').alias('destinations')).collect()]
        metadata=[(gid,key,json.dumps(next(o['type'] for o in graph['objects'] if o['key']==key),sort_keys=True,separators=(',',':'))) for gid,key in nodes.items()]
        selected=next(t for gid,key,t in metadata if key==expected['singleton'][0])
        joined=frames['nodes'].join(spark.createDataFrame(metadata,['id','original_key','original_type']),'id')
        singleton=joined.filter(F.col('original_key')==expected['singleton'][0]).collect()
        filtered=joined.filter(F.col('original_type')==selected)
        limited=filtered.orderBy('original_key').limit(2).collect()
        actual['singleton']=[r.original_key for r in singleton];actual['filtered-limit']=[r.original_key for r in limited];actual['filtered-total']=filtered.count()
        native_by_key={nodes[r['id']]:r for r in actual['all-objects']}
        for r in singleton+limited:
            if {k:v for k,v in r.asDict().items() if k not in ('original_key','original_type')}!=native_by_key[r.original_key]:raise AssertionError('Complete selected carrier differs')
        for name,rows in expected.items():
            if (actual[name]!=rows if name=='filtered-total' else bag(actual[name])!=bag(rows)):raise AssertionError('Original common query mismatch '+name)
        report={'format':'ashlar-commerce-graphframes-common/0.1','scope':'private immutable export rematerialization; exact raw carriers; no scalar promotion, UC, native Databricks or continuing source retention',
          'release_sha256':release_sha,'custody_sha256':custody_sha,'source_admission':admission,'original_graph':graph,'original_snapshots':value['snapshots'],'local_snapshots':snapshots,'versions':versions,'jars':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
          'expected':expected,'actual':actual,'required_cases':['QUERY/commerce/'+n for n in ['all-objects','all-edges','one-hop','two-hop','grouped-count','singleton','filtered-limit']],
          'limitations':['Commerce scenario and shared scalar/presence/topology fixtures remain separate required work.','Grouping uses occurrence counts and distinct destinations for nonempty source/relationship groups; zero groups are not declared.']}
    finally:spark.stop()
    (output/'report.json').write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    return {'report':str(output/'report.json'),'cases':7,'nodes':len(nodes),'edges':len(edges)}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','output','jars'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--release-sha256',required=True);p.add_argument('--custody-sha256',required=True)
    a=p.parse_args();print(json.dumps(run(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,a.output,a.jars)))
