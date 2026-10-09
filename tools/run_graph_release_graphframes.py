"""Bounded local Delta rematerialization of exact release carriers, not UC access."""
import argparse
from collections import Counter
import hashlib
import importlib.metadata
import json
from pathlib import Path
import re
import sys
from ashlar.graph_release import NODE_INTS, NODE_TEXT, EDGE_INTS, EDGE_TEXT, _carrier

FORMAT='ashlar-graph-release/0.1'
VERSIONS={'pyspark':'3.5.3','delta-spark':'3.2.1','graphframes-py':'0.12.3'}
JARS=('io.delta_delta-spark_2.12-3.2.1.jar','io.delta_delta-storage-3.2.1.jar',
      'io.graphframes_graphframes-spark3_2.12-0.12.3.jar',
      'io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar')

def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('Duplicate JSON member')
        result[key]=value
    return result

def _lineage(value):
    if type(value) is not dict or set(value)!={'format','publication','snapshots','roles','nodes','edges','mapping'}:
        raise ValueError('Closed complete release envelope required')
    publication=value['publication'];snapshots=value['snapshots'];roles=value['roles'];mapping=value['mapping']
    required={'publication_id','profile_version','recorded_at','table_versions_json','schema_revisions_json','source_progress_json','validation_report_json'}
    if type(publication) is not dict or not required.issubset(publication):raise ValueError('Original manifest fields required')
    for name in required:
        text=publication[name]
        if type(text) is not str or not text or '\x00' in text:raise ValueError('Exact manifest text required')
        text.encode('utf-8')
    if publication['profile_version']!='ashlar-delta/0.3':raise ValueError('Qualified canonical source profile required')
    decoded={}
    for name in ['table_versions_json','schema_revisions_json','source_progress_json','validation_report_json']:
        decoded[name]=json.loads(publication[name],object_pairs_hook=_object)
        if type(decoded[name]) is not dict:raise ValueError('Original manifest object carrier required')
    versions=decoded['table_versions_json']
    if type(snapshots) is not dict or not snapshots or set(snapshots)!=set(versions):
        raise ValueError('Complete original manifest snapshot inventory required')
    for table,snapshot in snapshots.items():
        if type(table) is not str or not table or type(snapshot) is not dict or set(snapshot)!={'uuid','version'}:
            raise ValueError('Original table snapshot identity required')
        if type(snapshot['uuid']) is not str or not re.fullmatch('[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}',snapshot['uuid']):
            raise ValueError('Native table UUID required')
        if type(snapshot['version']) is not int or not 0<=snapshot['version']<2**63 or type(versions[table]) is not int or versions[table]!=snapshot['version']:
            raise ValueError('Original manifest snapshot version mismatch')
    retention=decoded['validation_report_json'].get('retention',{}).get('targets')
    if type(retention) is not dict or set(retention)!=set(snapshots):
        raise ValueError('Original manifest UUID/version custody inventory required')
    for table,snapshot in snapshots.items():
        target=retention[table]
        if type(target) is not dict or target.get('uuid')!=snapshot['uuid'] or type(target.get('version')) is not int or target['version']!=snapshot['version']:
            raise ValueError('Original manifest UUID/version custody differs')
    if type(roles) is not dict or set(roles)!={'nodes','edges'} or any(type(t) is not str or t not in snapshots for t in roles.values()) or roles['nodes']==roles['edges']:
        raise ValueError('Distinct original snapshot graph roles required')
    expected={'reversibleIdentity':True,'independentEdges':True,'isolatedNodes':True,'exactCanonicalText':True,'selectedScalarPromotion':False,'nativeReleaseMaterialization':False,'engineExecution':False}
    if type(mapping) is not dict or set(mapping)!={'identity','properties','residuals','capabilities','losses','engineSupport'}:
        raise ValueError('Complete mapping capability/loss inventory required')
    if mapping['identity']!='ashlar-key/1' or type(mapping['properties']) is not str or not mapping['properties'] or type(mapping['residuals']) is not list or not mapping['residuals'] or any(type(t) is not str or not t for t in mapping['residuals']):
        raise ValueError('Named exact carrier mapping required')
    capabilities=mapping['capabilities']
    if type(capabilities) is not dict or capabilities!=expected or any(type(v) is not bool for v in capabilities.values()) or mapping['losses']!=[] or mapping['engineSupport']!=[]:
        raise ValueError('Supported lossless unexecuted release capability inventory required')

def load_release(payload, trusted_sha256):
    if not re.fullmatch('[0-9a-f]{64}',trusted_sha256) or hashlib.sha256(payload).hexdigest()!=trusted_sha256:
        raise ValueError('Trusted release byte digest mismatch')
    value=json.loads(payload.decode('utf-8'),object_pairs_hook=_object,
                    parse_constant=lambda x: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
    _lineage(value)
    if value.get('format')!=FORMAT or value.get('mapping',{}).get('identity')!='ashlar-key/1':
        raise ValueError('Unsupported named release/identity encoding')
    if (json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()!=payload:
        raise ValueError('Noncanonical exact release encoding')
    for kind,role in [('node','nodes'),('edge','edges')]:
        rows=value.get(role)
        if type(rows) is not list or len(rows)>10000:raise ValueError('Bounded row inventory required')
        names=(NODE_INTS+NODE_TEXT if kind=='node' else EDGE_INTS+EDGE_TEXT)+('published_at',)
        for row in rows:
            if type(row) is not dict:raise ValueError('Object carrier required')
            original={name:row[name] for name in names if name in row}
            if _carrier(original,kind)!=row:raise ValueError('Closed exact graph carrier mismatch')
        if len({row['graph_id'] for row in rows})!=len(rows):raise ValueError('Duplicate graph identity')
    nodes={row['graph_id'] for row in value['nodes']}
    if any(row['src'] not in nodes or row['dst'] not in nodes for row in value['edges']):
        raise ValueError('Dangling endpoint')
    return value

def columns(kind):
    return (NODE_INTS+NODE_TEXT if kind=='node' else EDGE_INTS+EDGE_TEXT)+('published_at','graph_id')+(('src','dst') if kind=='edge' else ())

def graph_rows(rows):
    return [{('carrier_id' if k=='id' else 'id' if k=='graph_id' else k):v for k,v in row.items()} for row in rows]

def oracle(value):
    edges=value['edges'];nodes=value['nodes']
    incoming=Counter(e['dst'] for e in edges);outgoing=Counter(e['src'] for e in edges)
    endpoints=set(incoming)|set(outgoing)
    return {'nodes':len(nodes),'edges':len(edges),'one_hop':len(edges),
            'two_hop':sum(count*outgoing[key] for key,count in incoming.items()),
            'isolates':sum(n['graph_id'] not in endpoints for n in nodes)}

def row_bag(rows):
    return Counter(json.dumps(dict(row),ensure_ascii=False,sort_keys=True,separators=(',',':')) for row in rows)

def run(payload, trusted_sha256, output, jars):
    value=load_release(payload,trusted_sha256)
    output=Path(output)
    if output.exists():raise ValueError('Fresh output directory required')
    versions={p:importlib.metadata.version(p) for p in VERSIONS}
    if versions!=VERSIONS:raise ValueError('Qualified dependency versions required')
    paths=[Path(jars)/name for name in JARS]
    if not all(p.is_file() for p in paths):raise ValueError('Existing local jars required')
    output.mkdir(parents=True)
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import StructType,StructField,StringType
    from graphframes import GraphFrame
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar release small GraphFrames')
           .config('spark.pyspark.python',sys.executable).config('spark.pyspark.driver.python',sys.executable)
           .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1')
           .config('spark.databricks.delta.snapshotPartitions','1')
           .config('spark.ui.enabled','false').config('spark.jars',','.join(str(p) for p in paths))
           .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
           .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
           .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    try:
        pinned={};schemas={}
        for kind,role in [('node','nodes'),('edge','edges')]:
            schema=StructType([StructField(name,StringType(),True) for name in columns(kind)])
            schemas[role]=schema
            spark.createDataFrame(value[role],schema).write.format('delta').save(str(output/role))
            pinned[role]=spark.read.format('delta').option('versionAsOf',0).load(str(output/role))
        vertices=pinned['nodes'].withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id')
        edges=pinned['edges'].withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id')
        graph=GraphFrame(vertices,edges)
        actual={'nodes':graph.vertices.count(),'edges':graph.edges.count(),
                'one_hop':graph.find('(a)-[e]->(b)').count(),
                'two_hop':graph.find('(a)-[e]->(b); (b)-[f]->(c)').count(),
                'isolates':vertices.join(edges.select(F.col('src').alias('id')).union(edges.select(F.col('dst').alias('id'))).distinct(),'id','left_anti').count()}
        expected=oracle(value)
        if actual!=expected:raise AssertionError('Independent directed topology oracle mismatch')
        for role,frame in [('nodes',vertices),('edges',edges)]:
            if row_bag(r.asDict() for r in frame.collect())!=row_bag(graph_rows(value[role])):
                raise AssertionError('Independent exact full-row parity mismatch')
        # Append one unpublished row in the private sandbox only; R1 remains Delta0.
        role='nodes' if value['nodes'] else 'edges' if value['edges'] else None
        if role:
            spark.createDataFrame([value[role][0]],schemas[role]).write.format('delta').mode('append').save(str(output/role))
            latest=spark.read.format('delta').load(str(output/role)).count()
            old=spark.read.format('delta').option('versionAsOf',0).load(str(output/role))
            if latest!=len(value[role])+1 or row_bag(r.asDict() for r in old.collect())!=row_bag(value[role]):
                raise AssertionError('Unpublished append changed pinned R1')
        report={'format':'ashlar-graphframes-local-check/0.1','release_sha256':trusted_sha256,
                'scope':'Local exact release rematerialization; no direct Unity Catalog access or source semantic admission',
                'versions':versions,'local_delta_versions':{'nodes':0,'edges':0},
                'original_roles':value['roles'],'original_snapshots':value['snapshots'],
                'counts':actual,'full_row_parity':True,'unpublished_append':{'role':role,'pinned_r1_unchanged':True} if role else {'role':None,'status':'no rows available; append control not executed'},
                'jar_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}
        (output/'report.json').write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
        return report
    finally:spark.stop()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--release',required=True);p.add_argument('--sha256',required=True)
    p.add_argument('--output',required=True);p.add_argument('--jars',required=True)
    a=p.parse_args();print(json.dumps(run(Path(a.release).read_bytes(),a.sha256,a.output,a.jars),sort_keys=True))
