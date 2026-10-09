"""Tiny publicly admitted authored scalar/presence/topology GraphFrames fixture.

Exact public UMF Value/state carriers stay text. No canonical Ashlar publication
or scalar property promotion follows from this source-profile compatibility run.
"""
import argparse,base64,hashlib,importlib.metadata,json,subprocess,tempfile
from collections import Counter
from pathlib import Path
from run_graph_release_graphframes import JARS,VERSIONS,row_bag

def required_cases(fixture):
    augmentation={a['id']:a for a in fixture['originalAugmentations']}
    def encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    topology=augmentation['A-TOPOLOGY'];topology_nodes=[n['key']for n in fixture['nodes']if n['case']=='A-TOPOLOGY']
    if sorted(topology_nodes)!=sorted(topology['nodes'])or sorted([[e['key'],e['source'],e['target']]for e in fixture['edges']])!=sorted(topology['edges']):raise ValueError('Prescribed topology inventory differs')
    scalar=augmentation['A-SCALAR']
    for field,carrier,group in [('text','string','strings'),('boolean','boolean','booleans'),('integer','integerToken','integerTokens'),('decimal','decimalToken','fixedDecimalTokens')]:
        actual=[]
        for n in fixture['nodes']:
            if n['case']!='A-SCALAR':continue
            for m in n['values']:
                if m['field']['element']==field and m['state']=='present':
                    if type(m.get('value'))is not dict or set(m['value'])!={carrier}:raise ValueError('Exact original scalar carrier required')
                    actual.append(m['value'][carrier])
        if Counter(encoded(v)for v in actual)!=Counter(encoded(v)for v in scalar[group]):raise ValueError('Prescribed exact scalar corpus differs')
    presence={n['key'].removeprefix('presence-'):n for n in fixture['nodes']if n['case']=='A-PRESENCE'}
    if set(presence)!=set(augmentation['A-PRESENCE']['states']):raise ValueError('Complete five-state presence corpus required')
    for state,(field,availability,value)in {'absent':('text','absent',None),'null':('text','present',None),'present-empty-string':('text','present',{'string':''}),'present-zero':('integer','present',{'integerToken':'0'}),'present-false':('boolean','present',{'boolean':False})}.items():
        members=[m for m in presence[state]['values']if m['field']['element']==field]
        if len(members)!=1:raise ValueError('Complete original presence member required')
        m=members[0]
        if m['state']!=availability or ('value'in m)!=(availability=='present')or (availability=='present'and encoded(m['value'])!=encoded(value)):raise ValueError('Prescribed valid presence state differs')

def admitted(model,source,receipt,umf):
    value=json.loads(receipt)
    if value['originalModelBase64']!=base64.b64encode(model).decode() or value['originalSourceBase64']!=base64.b64encode(source).decode() or value['modelSha256']!=hashlib.sha256(model).hexdigest() or value['sourceSha256']!=hashlib.sha256(source).hexdigest():raise ValueError('Original source/receipt byte custody differs')
    with tempfile.TemporaryDirectory(prefix='ashlar-augment-public-')as d:
        p=Path(d);(p/'model.json').write_bytes(model);(p/'source.json').write_bytes(source)
        subprocess.run(['bun',str(Path(__file__).with_name('check_graph_augmentations.ts')),str(umf),str(p/'model.json'),str(p/'source.json'),str(p/'receipt.json')],check=True,capture_output=True)
        if (p/'receipt.json').read_bytes()!=receipt:raise ValueError('Actual public receipt recomputation differs')
    fixture=json.loads(source);corpus=json.loads((Path(__file__).resolve().parents[1]/'docs/helix/03-test/fixtures/domain-pack-corpus.json').read_bytes())
    if fixture['corpusSha256']!=hashlib.sha256((Path(__file__).resolve().parents[1]/'docs/helix/03-test/fixtures/domain-pack-corpus.json').read_bytes()).hexdigest() or fixture['originalAugmentations']!=[a for a in corpus['augmentations']if a['id']in ('A-TOPOLOGY','A-SCALAR','A-PRESENCE')]:raise ValueError('Exact prescribed original corpus fixtures required')
    required_cases(fixture)
    return fixture,value

def run(model,source,receipt,umf,output,jars):
    fixture,public=admitted(model,source,receipt,umf)
    versions={p:importlib.metadata.version(p)for p in VERSIONS}
    if versions!=VERSIONS:raise ValueError('Qualified existing local runtime required')
    output=Path(output)
    if output.exists():raise ValueError('Fresh native output required')
    def text(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    nodes=[{'id':n['key'],'case_id':n['case'],'record_json':text(n)}for n in fixture['nodes']]
    edges=[{'id':e['key'],'src':e['source'],'dst':e['target'],'record_json':text(e)}for e in fixture['edges']]
    topology=next(a for a in fixture['originalAugmentations']if a['id']=='A-TOPOLOGY')
    source_edges=[[e['key'],e['source'],e['target']]for e in fixture['edges']]
    expected_one=[[s,e,t]for e,s,t in source_edges if s==topology['queryStart']]
    expected_two=[[s,e,m,f,t]for e,s,m in source_edges for f,m2,t in source_edges if s==topology['queryStart']and m==m2]
    from pyspark.sql import SparkSession,functions as F
    from pyspark.sql.types import StructType,StructField,StringType
    from graphframes import GraphFrame
    paths=[Path(jars)/n for n in JARS]
    if not all(p.is_file()for p in paths):raise ValueError('Existing qualified jars required')
    output.mkdir(parents=True)
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar admitted shared augmentations')
      .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false')
      .config('spark.jars',','.join(map(str,paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
      .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    try:
        frames={};snapshots={};actual={}
        for role,rows in [('nodes',nodes),('edges',edges)]:
            schema=StructType([StructField(n,StringType(),False)for n in rows[0]])
            spark.createDataFrame(rows,schema).write.format('delta').save(str(output/role))
            frames[role]=spark.read.format('delta').option('versionAsOf',0).load(str(output/role))
            actual[role]=[r.asDict()for r in frames[role].collect()]
            if row_bag(actual[role])!=row_bag(rows):raise ValueError('Full original exact Value/state/edge carriers differ')
            snapshots[role]={'uuid':spark.sql('DESCRIBE DETAIL delta.`'+str(output/role)+'`').first()['id'],'version':0}
        gf=GraphFrame(frames['nodes'],frames['edges'])
        one=gf.find('(a)-[e]->(b)').filter(F.col('a.id')==topology['queryStart']).select('a.id','e.id','b.id').collect()
        two=gf.find('(a)-[e]->(b); (b)-[f]->(c)').filter(F.col('a.id')==topology['queryStart']).select('a.id','e.id','b.id','f.id','c.id').collect()
        one_rows=[list(r)for r in one];two_rows=[list(r)for r in two]
        counter=lambda rows:Counter(text(r)for r in rows)
        if counter(one_rows)!=counter(expected_one)or counter(two_rows)!=counter(expected_two):raise ValueError('Complete prescribed parallel/cyclic/self-loop walk bags differ')
        if len(two_rows)!=8 or sorted({r[-1]for r in two_rows})!=topology['twoHopDistinctDestinations']:raise ValueError('Occurrence/distinct destination parity differs')
        isolates=frames['nodes'].filter(F.col('id').isin(topology['nodes'])).join(frames['edges'].select(F.col('src').alias('id')).union(frames['edges'].select(F.col('dst').alias('id'))).distinct(),'id','left_anti').collect()
        if [r.id for r in isolates]!=['A0']:raise ValueError('Original topology isolate lost')
        scalar_extractions=[]
        for n in fixture['nodes']:
            if n['case']!='A-SCALAR':continue
            selected=[(i,m)for i,m in enumerate(n['values'])if m['field']['element']!='id'and m['state']=='present']
            if len(selected)!=1:raise ValueError('One declared scalar value per fixture row required')
            i,m=selected[0];carrier=next(iter(m['value']));expected=m['value'][carrier]
            result=frames['nodes'].filter(F.col('id')==n['key']).select(F.get_json_object('record_json','$.values['+str(i)+'].value.'+carrier).alias('token')).collect()
            lexical=str(expected).lower()if type(expected)is bool else expected
            if len(result)!=1 or type(result[0].token)is not str or result[0].token!=lexical:raise ValueError('Native exact scalar extraction differs')
            scalar_extractions.append({'key':n['key'],'field':m['field'],'carrier':carrier,'native_token':result[0].token,'original_value':expected})
        presence=[]
        for r in frames['nodes'].filter(F.col('case_id')=='A-PRESENCE').collect():
            n=json.loads(r.record_json);members={m['field']['element']:m for m in n['values']};state=n['key'].removeprefix('presence-')
            expected={'absent':('text','absent',None),'null':('text','present',None),'present-empty-string':('text','present',{'string':''}),'present-zero':('integer','present',{'integerToken':'0'}),'present-false':('boolean','present',{'boolean':False})}[state]
            field,availability,value=expected;m=members[field]
            if m['state']!=availability or ('value'in m)!=(availability=='present') or (availability=='present'and m['value']!=value):raise ValueError('Distinct valid native presence carrier lost')
            index=next(i for i,m in enumerate(n['values'])if m['field']['element']==field)
            extracted=frames['nodes'].filter(F.col('id')==n['key']).select(F.get_json_object('record_json','$.values['+str(index)+'].state').alias('availability'),F.get_json_object('record_json','$.values['+str(index)+'].value').alias('value')).collect()
            expected_native=None if value is None else text(value)
            if len(extracted)!=1 or extracted[0].availability!=availability or extracted[0].value!=expected_native:raise ValueError('Native presence state/value extraction differs')
            presence.append({'native_extraction':extracted[0].asDict(),'state':state,'original_field':field,'native_member':m})
        report={'format':'ashlar-public-graph-augmentations-graphframes/0.1','original_model_base64':base64.b64encode(model).decode(),'original_source_base64':base64.b64encode(source).decode(),'public_receipt_sha256':hashlib.sha256(receipt).hexdigest(),'public_receipt':public,'versions':versions,'jars':{p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in paths},'native_snapshots':snapshots,'native_full_carriers':actual,'one_hop':one_rows,'two_hop':two_rows,'two_hop_distinct_destinations':sorted({r[-1]for r in two_rows}),'isolates':[r.id for r in isolates],'scalar_extractions':scalar_extractions,'presence_states':presence,'qualification':'Actual local source-profile GraphFrames preserves exact public Value/state/lexical tokens and full directed multigraph bags. No scalar property promotion/canonical Ashlar publication/ACK/UC/production authority.'}
    finally:spark.stop()
    (output/'report.json').write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n');return report
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('model','source','receipt','umf','output','jars'):p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();run(a.model.read_bytes(),a.source.read_bytes(),a.receipt.read_bytes(),a.umf,a.output,a.jars)
