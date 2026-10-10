"""Seven common graph queries per original pack on immutable local releases.

Exact raw String carrier rematerialization; no scalar promotion, clinical
inference, UC/Truss authority or continuing canonical retention. These 21 cases
are separate from the 17 original authored SQL scenarios and topology controls.
"""
import argparse,base64,hashlib,importlib,importlib.metadata,json
from pathlib import Path
from ashlar.graph_release import _carrier
from fixture_oracle import fixture_columns
from private_graph_custody import PROFILE
from run_graph_release_graphframes import load_release,columns,graph_rows,row_bag,JARS,VERSIONS
from run_commerce_release_graphframes import bag

PACKS=('archaeology','ecology','medical')
QUALIFIED_JAR_SHA={'io.delta_delta-spark_2.12-3.2.1.jar': '088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499', 'io.delta_delta-storage-3.2.1.jar': '4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7', 'io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar': '8bbd2bbb4c7a3b137e51b4f32f49a30a2ccf70eeb07b452cffa20090dd481b96', 'io.graphframes_graphframes-spark3_2.12-0.12.3.jar': '69d7909628caf42bcbe1c7e6e5a5a0901040d00583738d931af41bd72af1d3dc'}
CASES=('all-objects','all-edges','one-hop','two-hop','grouped-count','singleton','filtered-limit')

def original(pack,value,publication):
    if pack not in PACKS:raise ValueError('Explicit original pack required')
    publication=Path(publication)
    admission=json.loads(value['publication']['validation_report_json'])['source_admission']
    source={name:base64.b64decode(admission['original_'+name+'_base64'],validate=True) for name in ('model','graph','bindings')}
    for name,filename in [('model','original-ontology.json'),('graph','original-graph.json'),('bindings','development-bindings.json')]:
        if source[name]!=(publication/filename).read_bytes():raise ValueError('Exact original source custody differs')
    if hashlib.sha256(source['bindings']).hexdigest()!=admission['development_bindings_sha256']:raise ValueError('Original development binding hash differs')
    converter=importlib.import_module(pack+'_source_transaction');native=importlib.import_module('run_'+pack+'_outbox_publication')
    public=(publication/'public-dataset.json').read_bytes()
    batch,bindings=(converter.build_transaction(source['model'],source['graph'],public,source_system=native.SOURCE_SYSTEM) if pack=='medical' else converter.build_transaction(source['model'],source['graph'],source_system=native.SOURCE_SYSTEM))
    if bindings!=json.loads(source['bindings']) or batch.begin+b''.join(r.raw for r in batch.records)+batch.commit!=(publication/'source.jsonl').read_bytes():raise ValueError('Original complete source/binding transaction differs')
    admitted=getattr(native,pack.title()+'Admission')(batch,bindings,publication/'original-ontology.json',publication/'original-graph.json',publication/'development-bindings.json',publication/'public-dataset.json')
    if admitted.metadata()!=admission:raise ValueError('Original full public source admission differs')
    expected=getattr(native,'original_'+pack+'_oracle')(source['model'],source['graph'],bindings,batch,fixture_columns(Path(__file__).resolve().parents[1]))
    for kind,role,source_role in [('node','nodes','object_current'),('edge','edges','edge_current')]:
        if row_bag(value[role])!=row_bag(_carrier(row,kind)for row in expected[source_role]):raise ValueError('Independent original complete carrier bag differs')
    graph=json.loads(source['graph']);assigned={(e['kind'],e['originalKey']):e for e in bindings['entities']}
    def map_rows(role,kind,original_rows,type_column):
        by_native={(assigned[kind,r['key']]['type_id'],assigned[kind,r['key']]['id']):r['key'] for r in original_rows}
        mapped={}
        for row in value[role]:
            key=(row[type_column],row['id'])
            if row['source_system']!=batch.feed or key not in by_native or row['graph_id']in mapped:raise ValueError('Injective source-qualified original identity required')
            mapped[row['graph_id']]=by_native[key]
        if len(mapped)!=len(original_rows) or set(mapped.values())!={r['key']for r in original_rows}:raise ValueError('Complete original graph identities required')
        return mapped
    nodes=map_rows('nodes','object',graph['objects'],'type_id');edges=map_rows('edges','edge',graph['edges'],'rel_type_id')
    return graph,nodes,edges,admission

def query_oracle(graph):
    objects,edges=graph['objects'],graph['edges']
    if not objects:raise ValueError('This finite corpus requires original objects')
    types={};groups={}
    for o in objects:types.setdefault(json.dumps(o['type'],sort_keys=True,separators=(',',':')),[]).append(o['key'])
    selected=min(types,key=lambda t:(-len(types[t]),t));filtered=sorted(types[selected])
    if len(filtered)<2:raise ValueError('Nonvacuous original filtered-limit control required')
    for e in edges:groups.setdefault((e['source'],json.dumps(e['relationship'],sort_keys=True,separators=(',',':'))),[]).append(e['target'])
    endpoints={e[k]for e in edges for k in ('source','target')}
    return {'one-hop':[[e['source'],e['key'],e['target']]for e in edges],
      'two-hop':[[a['source'],a['key'],a['target'],b['key'],b['target']]for a in edges for b in edges if a['target']==b['source']],
      'grouped-count':[[s,t,len(destinations),len(set(destinations))]for(s,t),destinations in groups.items()],
      'singleton':[min(o['key']for o in objects)],'filtered-type':selected,'filtered-limit':filtered[:1],'filtered-total':len(filtered),'isolates':sorted(o['key']for o in objects if o['key']not in endpoints)}

def validate_vector(spark,paths,snapshots):
    for role,path in paths.items():
        detail=spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first().asDict()
        if detail['id']!=snapshots[role]['uuid'] or detail['location'].removeprefix('file:').rstrip('/')!=str(path):raise ValueError('Complete original local table UUID/location vector changed')
        versions=spark.sql('DESCRIBE HISTORY delta.`'+str(path)+'`').select('version').collect()
        if [r['version']for r in versions]!=[0]:raise ValueError('Immutable local Delta0 version changed')

def closing_parity(spark,paths,snapshots,frames,value):
    validate_vector(spark,paths,snapshots)
    for role,frame in frames.items():
        if row_bag([r.asDict()for r in frame.collect()])!=row_bag(graph_rows(value[role])):raise ValueError('Complete original local pinned carrier parity changed')
    validate_vector(spark,paths,snapshots)

def native_filter_and_isolates(nodes,edges,functions,source_system,type_id):
    filtered=nodes.filter((functions.col('source_system')==source_system)&(functions.col('type_id')==type_id))
    endpoints=edges.select(functions.col('src').alias('id')).union(edges.select(functions.col('dst').alias('id'))).distinct()
    return filtered,nodes.join(endpoints,'id','left_anti')

def finish_native(spark,output,report):
    spark.stop()
    (Path(output)/'report.json').write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n')

def admit_runtime(jars,versions):
    if versions!=VERSIONS:raise ValueError('Exact qualified existing GraphFrames runtime required')
    paths=[Path(jars)/name for name in JARS]
    if not all(p.is_file()and hashlib.sha256(p.read_bytes()).hexdigest()==QUALIFIED_JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing qualified jar fingerprints required')
    return paths

def run(candidates,output,jars):
    if type(candidates)is not list or [c['pack']for c in candidates]!=list(PACKS):raise ValueError('All three original packs in explicit order required')
    prepared=[]
    for c in candidates:
        release=Path(c['release']).read_bytes();custody=Path(c['custody']).read_bytes()
        value=load_release(release,c['release_sha256'],custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=c['custody_sha256'])
        graph,nodes,edges,admission=original(c['pack'],value,c['publication']);prepared.append((c,value,graph,nodes,edges,admission,query_oracle(graph)))
    versions={p:importlib.metadata.version(p)for p in VERSIONS}
    jar_paths=admit_runtime(jars,versions)
    output=Path(output)
    if output.exists():raise ValueError('Fresh immutable output required')
    output.mkdir(parents=True)
    from pyspark.sql import SparkSession,functions as F
    from pyspark.sql.types import StructType,StructField,StringType
    from graphframes import GraphFrame
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar three original release common queries').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(map(str,jar_paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    reports=[];complete=False
    try:
        spark.sparkContext.setLogLevel('ERROR')
        for c,value,graph,nodes,edges,admission,expected in prepared:
            frames={};snapshots={};paths={role:output/c['pack']/role for role in ('nodes','edges')}
            for kind,role in [('node','nodes'),('edge','edges')]:
                schema=StructType([StructField(name,StringType(),True)for name in columns(kind)])
                spark.createDataFrame(value[role],schema).write.format('delta').save(str(paths[role]))
                detail=spark.sql('DESCRIBE DETAIL delta.`'+str(paths[role])+'`').first().asDict()
                snapshots[role]={'uuid':detail['id'],'version':0,'minReaderVersion':detail['minReaderVersion'],'minWriterVersion':detail['minWriterVersion'],'tableFeatures':detail.get('tableFeatures')}
                frames[role]=spark.read.format('delta').option('versionAsOf',0).load(str(paths[role])).withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id')
            closing_parity(spark,paths,snapshots,frames,value)
            g=GraphFrame(frames['nodes'],frames['edges']);actual={}
            for role,frame in frames.items():actual['all-objects'if role=='nodes'else'all-edges']=[r.asDict()for r in frame.collect()]
            actual['one-hop']=[[nodes[r.a],edges[r.e],nodes[r.b]]for r in g.find('(a)-[e]->(b)').select('a.id','e.id','b.id').toDF('a','e','b').collect()]
            actual['two-hop']=[[nodes[r.a],edges[r.e],nodes[r.b],edges[r.f],nodes[r.c]]for r in g.find('(a)-[e]->(b); (b)-[f]->(c)').select('a.id','e.id','b.id','f.id','c.id').toDF('a','e','b','f','c').collect()]
            reltypes={r['rel_type_id']:json.dumps(next(e['relationship']for e in graph['edges']if e['key']==edges[r['graph_id']]),sort_keys=True,separators=(',',':'))for r in value['edges']}
            actual['grouped-count']=[[nodes[r.src],reltypes[r.rel_type_id],r.occurrences,r.destinations]for r in frames['edges'].groupBy('src','rel_type_id').agg(F.count('*').alias('occurrences'),F.countDistinct('dst').alias('destinations')).collect()]
            by_key={key:gid for gid,key in nodes.items()};selected_type=json.loads(expected['filtered-type'])
            original_bindings=json.loads(base64.b64decode(admission['original_bindings_base64'],validate=True))
            type_entries=[t for t in original_bindings['types']if t['identity']==['object',selected_type['module'],selected_type['element']]]
            if len(type_entries)!=1:raise ValueError('One admitted original type binding required')
            selected_source=importlib.import_module('run_'+c['pack']+'_outbox_publication').SOURCE_SYSTEM
            filter_binding={'source_system':selected_source,'type_id':type_entries[0]['type_id'],'original_type':selected_type}
            singleton=frames['nodes'].filter(F.col('id')==by_key[expected['singleton'][0]]).collect()
            filtered,isolated_frame=native_filter_and_isolates(frames['nodes'],frames['edges'],F,filter_binding['source_system'],filter_binding['type_id'])
            # All original graph-key metadata is admitted independently. It is
            # used only for deterministic ordering, never native membership.
            ordering_metadata=[(gid,key)for gid,key in nodes.items()]
            order_map=spark.createDataFrame(ordering_metadata,['id','original_key']);limited=filtered.join(order_map,'id').orderBy('original_key').limit(1).collect()
            actual['singleton']=[nodes[r.id]for r in singleton];actual['filtered-limit']=[nodes[r.id]for r in limited];actual['filtered-total']=filtered.count()
            canonical={r['id']:r for r in graph_rows(value['nodes'])}
            for r in singleton+limited:
                if {k:v for k,v in r.asDict().items()if k!='original_key'}!=canonical[r.id]:raise ValueError('Complete selected original carrier differs')
            isolated=isolated_frame.collect()
            actual['isolates']=sorted(nodes[r.id]for r in isolated)
            if any(r.asDict()!=canonical[r.id]for r in isolated):raise ValueError('Complete native isolated carrier differs')
            for name in ('one-hop','two-hop','grouped-count','singleton','filtered-limit','isolates'):
                if bag(actual[name])!=bag(expected[name]):raise AssertionError('Original common query mismatch '+name)
            if actual['filtered-total']!=expected['filtered-total']:raise AssertionError('Original filtered candidate total differs')
            closing_parity(spark,paths,snapshots,frames,value)
            reports.append({'pack':c['pack'],'release_sha256':c['release_sha256'],'custody_sha256':c['custody_sha256'],'original_snapshots':value['snapshots'],'local_snapshots':snapshots,'source_admission':admission,'original_graph':graph,'filter_binding':filter_binding,'ordering_metadata':ordering_metadata,'expected':expected,'actual':actual,'required_cases':['QUERY/'+c['pack']+'/'+name for name in CASES]})
        complete=True
    finally:
        if not complete:spark.stop()
    if not complete:raise ValueError('No report from incomplete engine execution')
    report={'format':'ashlar-original-pack-graphframes-common/0.1','qualification':__doc__,'versions':versions,'jars':{p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in jar_paths},'reports':reports,'cases':21,'original17_authored_scenarios_qualified':False}
    finish_native(spark,output,report)
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--candidates',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--jars',type=Path,required=True)
    a=p.parse_args();print('Completed common cases:',run(json.loads(a.candidates.read_bytes()),a.output,a.jars)['cases'])
