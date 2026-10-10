"""Original seventeen relational scenarios on explicit GraphFrames vertex carriers.

Native Spark DataFrame operations over a genuine GraphFrame reproduce authored
property-equality semantics. Independent edges remain intact; no traversal
substitution, host row repair, output deduplication or SQL/Weft execution claim.
"""
import json
from check_pack_puppygraph import bag,integer


def expressions(frame,profile):
    from pyspark.sql import functions as F
    fields={(f['record_identity'][2],f['original_field']['name']):f for f in profile['fields']}
    pack=profile['pack']
    import base64
    model=json.loads(base64.b64decode(profile['source_admission']['original_model_base64']))
    def table(name,alias):
        identity=json.dumps({'document':model['id'],'module':'domain','element':name},sort_keys=True,separators=(',',':'))
        return frame.vertices.where(F.col('original_type')==identity).alias(alias)
    def col(alias,record,name,numeric=False):
        field=fields[(record,name)];column=field['signed64_column']if numeric else field['lexical_column']
        if column is None:raise ValueError('Named finite numeric capability required')
        return F.col(alias+'.'+column)
    def selected(data,values,witnesses=()):
        return data.select(*[v.alias('result_'+str(i))for i,v in enumerate(values)],*[v.alias(n)for v,n in witnesses])
    cases={}
    if pack=='archaeology':
        a=table('stratigraphic_assertions','a');b=table('stratigraphic_assertions','b')
        cases['cycle']=selected(a.join(b,(col('a','stratigraphic_assertions','source_context_id')==col('b','stratigraphic_assertions','target_context_id'))&(col('a','stratigraphic_assertions','target_context_id')==col('b','stratigraphic_assertions','source_context_id'))).where(col('a','stratigraphic_assertions','id')<col('b','stratigraphic_assertions','id')),[col('a','stratigraphic_assertions','id'),col('b','stratigraphic_assertions','id')])
        a=table('interpretations','a');b=table('interpretations','b')
        cases['dating']=selected(a.join(b,col('a','interpretations','context_id')==col('b','interpretations','context_id')).where((col('a','interpretations','id')<col('b','interpretations','id'))&(col('a','interpretations','earliest',True)<=col('b','interpretations','latest',True))&(col('b','interpretations','earliest',True)<=col('a','interpretations','latest',True))),[col('a','interpretations','author'),col('b','interpretations','author')])
        s=table('asset_subjects','s');g=s.groupBy(col('s','asset_subjects','asset_id').alias('asset')).agg(F.countDistinct(col('s','asset_subjects','context_id')).alias('contexts'))
        cases['media']=selected(g.where(F.col('contexts')>1),[F.col('asset'),F.col('contexts')])
        s=table('assets','s');cases['missing-media']=selected(s.where(col('s','assets','availability')=='external'),[col('s','assets','id')])
        f=table('fauna_results','f');a=table('analyses','a');s=table('samples','s');l=table('find_lots','l');o=table('objects','o');p=table('pottery_results','p')
        joined=f.join(a,col('a','analyses','id')==col('f','fauna_results','analysis_id')).join(s,col('s','samples','id')==col('a','analyses','sample_id')).join(l,col('l','find_lots','context_id')==col('s','samples','context_id')).join(o,col('o','objects','lot_id')==col('l','find_lots','id')).join(p,col('p','pottery_results','object_id')==col('o','objects','id'))
        cases['specialists']=selected(joined,[col('f','fauna_results','nisp'),col('f','fauna_results','mni'),col('p','pottery_results','sherd_count'),col('p','pottery_results','estimated_vessels')])
        o=table('objects','o');l=table('find_lots','l');c=table('contexts','c')
        cases['lineage']=selected(o.join(l,col('l','find_lots','id')==col('o','objects','lot_id')).join(c,col('c','contexts','id')==col('l','find_lots','context_id')),[col('o','objects','id'),col('c','contexts','native_locus')])
        i=table('interpretations','i');e=table('interpretation_evidence','e');p=table('pottery_results','p');f=table('fauna_results','f')
        joined=i.join(e,col('e','interpretation_evidence','interpretation_id')==col('i','interpretations','id')).join(p,col('p','pottery_results','id')==col('e','interpretation_evidence','pottery_result_id'),'left').join(f,col('f','fauna_results','id')==col('e','interpretation_evidence','fauna_result_id'),'left').orderBy(col('i','interpretations','id'))
        witnesses=[(F.col('i.original_key'),'interpretation_key'),(F.col('e.original_key'),'evidence_key'),(col('i','interpretations','id'),'interpretation_order_token'),(F.col('p.original_key'),'matched_pottery_key'),(F.col('f.original_key'),'matched_fauna_key'),(F.col('p.'+fields[('pottery_results','form')]['presence_column']),'pottery_form_presence'),(F.col('f.'+fields[('fauna_results','taxon')]['presence_column']),'fauna_taxon_presence')]
        cases['evidence-links']=selected(joined,[col('i','interpretations','author'),col('p','pottery_results','form'),col('f','fauna_results','taxon')],witnesses)
        s=table('samples','s');a=table('analyses','a');r=table('soil_results','r')
        cases['sample']=selected(s.join(a,col('a','analyses','sample_id')==col('s','samples','parent_id')).join(r,col('r','soil_results','analysis_id')==col('a','analyses','id')).where(col('s','samples','parent_id').isNotNull()),[col('s','samples','parent_id'),col('r','soil_results','preparation')])
    elif pack=='ecology':
        o=table('occurrences','o');e=table('effort','e');joined=o.join(e,col('e','effort','id')==col('o','occurrences','effort_id'))
        cases['effort-event']=selected(joined.where(col('e','effort','event_id')!=col('o','occurrences','event_id')),[col('o','occurrences','id')])
        cases['effort']=selected(joined.where(col('e','effort','amount').isNull()),[col('o','occurrences','id')])
        cases['zero']=selected(joined.where((col('o','occurrences','count',True)==0)&(col('o','occurrences','detection')=='not-detected')&col('e','effort','amount').isNotNull()),[col('o','occurrences','id')])
        o=table('observations','o');p=table('observed_properties','p');s=table('samples','s');e=table('sampling_events','e');site=table('monitoring_sites','site');r=table('reaches','r');m=table('methods','m')
        joined=o.join(p,col('p','observed_properties','id')==col('o','observations','property_id')).join(s,col('s','samples','id')==col('o','observations','sample_id')).join(e,col('e','sampling_events','id')==col('s','samples','event_id')).join(site,col('site','monitoring_sites','id')==col('e','sampling_events','site_id')).join(r,col('r','reaches','id')==col('site','monitoring_sites','reach_id')).join(m,col('m','methods','id')==col('e','sampling_events','method_id')).where(col('p','observed_properties','name').isin('temperature','dissolved-oxygen')&(col('m','methods','fraction')=='in-situ'))
        grouped=joined.groupBy(col('p','observed_properties','name').alias('property')).agg(F.countDistinct(col('r','reaches','id')).alias('reaches'))
        cases['connected-measurements']=selected(grouped.orderBy(F.col('property')),[F.col('property'),F.col('reaches')])
        cases['censor']=selected(o.where((col('o','observations','result_kind')=='censored')&col('o','observations','value').isNull()),[col('o','observations','id'),col('o','observations','threshold'),col('o','observations','qualifier')])
        s=table('site_matches','s');cases['match']=selected(s.where(col('s','site_matches','resolution')=='unresolved'),[col('s','site_matches','id')])
        n=table('network_links','n');cases['network']=selected(n,[col('n','network_links','upstream_id'),col('n','network_links','downstream_id')])
        o=table('occurrences','o');e=table('sampling_events','e');m=table('methods','m');i=table('identifications','i');t=table('taxa','t')
        joined=o.join(e,col('e','sampling_events','id')==col('o','occurrences','event_id')).join(m,col('m','methods','id')==col('e','sampling_events','method_id')).join(i,col('i','identifications','id')==col('o','occurrences','identification_id')).join(t,col('t','taxa','id')==col('i','identifications','taxon_id'))
        cases['comparability']=selected(joined,[col('m','methods','matrix'),col('m','methods','fraction'),col('t','taxa','rank')]).distinct()
        f=table('fishing_events','f');cases['fishing']=selected(f,[col('f','fishing_events','effort_unit')])
    else:raise ValueError('Closed original scenario pack required')
    if set(cases)!={c['original_scenario']['id']for c in profile['cases']}:raise ValueError('Every original authored scenario required')
    return cases

ARITY={'archaeology':{'cycle':2,'dating':2,'media':2,'missing-media':1,'specialists':4,'lineage':2,'evidence-links':3,'sample':2},'ecology':{'effort-event':1,'connected-measurements':2,'censor':3,'match':1,'effort':1,'zero':1,'network':2,'comparability':3,'fishing':1}}
WITNESSES=['interpretation_key','evidence_key','interpretation_order_token','matched_pottery_key','matched_fauna_key','pottery_form_presence','fauna_taxon_presence']


def validate_case(profile,name,schema,raw):
    count=ARITY[profile['pack']][name];names=['result_'+str(i)for i in range(count)]+(WITNESSES if name=='evidence-links'else [])
    types=['string']*len(names)
    if name in ('media','connected-measurements'):types[1]='bigint'
    if schema!=[{'name':n,'type':t}for n,t in zip(names,types)]:raise ValueError('Actual native ordered result schema differs')
    expected=next(c['original_graph_expected']for c in profile['cases']if c['original_scenario']['id']==name)
    import base64
    original=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64']));objects={o['key']:o for o in original['objects']}
    actual=[]
    for row in raw:
        if type(row)is not list or len(row)!=len(names):raise ValueError('Actual ordered native cells required')
        cells=[]
        for i,v in enumerate(row[:count]):
            if types[i]=='bigint':v=str(integer(v))
            elif v is not None and type(v)is not str:raise ValueError('Exact lexical String/null result required')
            cells.append(v)
        if name=='evidence-links':
            interpretation=objects.get(row[3]);evidence=objects.get(row[4])
            if (not interpretation or interpretation['type']['element']!='interpretations' or not evidence or evidence['type']['element']!='interpretation_evidence'
                    or evidence['values']['interpretation_evidence.interpretation_id']!=interpretation['values']['interpretations.id']
                    or row[5]!=interpretation['values']['interpretations.id'] or cells[0]!=interpretation['values']['interpretations.author']):
                raise ValueError('Exact optional left/evidence identity and ordering witness required')
            for key,presence,result_index,record,field,foreign in [(row[6],row[8],1,'pottery_results','form','pottery_result_id'),(row[7],row[9],2,'fauna_results','taxon','fauna_result_id')]:
                reference=evidence['values']['interpretation_evidence.'+foreign]
                matching=[o for o in objects.values()if o['type']['element']==record and reference is not None and o['values'][record+'.id']==reference]
                if key is None:
                    if matching or presence is not None or cells[result_index]is not None:raise ValueError('Unmatched right row must remain distinct and source-correspondent')
                else:
                    o=objects.get(key)
                    if not o or o not in matching:raise ValueError('Actual optional matched relational source identity differs')
                    token=o['values'][record+'.'+field]
                    if cells[result_index]!=token or presence!=('present-null'if token is None else'present'):raise ValueError('Optional native source presence/value witness differs')
        actual.append(cells)
    if name=='evidence-links':
        expected_witnesses=[]
        for i in objects.values():
            if i['type']['element']!='interpretations':continue
            for e in objects.values():
                if e['type']['element']!='interpretation_evidence' or e['values']['interpretation_evidence.interpretation_id']!=i['values']['interpretations.id']:continue
                matches=[]
                for record,foreign in [('pottery_results','pottery_result_id'),('fauna_results','fauna_result_id')]:
                    reference=e['values']['interpretation_evidence.'+foreign]
                    matches.append([o['key']for o in objects.values()if o['type']['element']==record and reference is not None and o['values'][record+'.id']==reference]or[None])
                expected_witnesses.extend([i['key'],e['key'],p,f]for p in matches[0]for f in matches[1])
        if bag([[r[3],r[4],r[6],r[7]]for r in raw])!=bag(expected_witnesses):raise ValueError('Complete optional source occurrence witness bag differs')
        if any(a[5]>b[5]for a,b in zip(raw,raw[1:])):raise ValueError('Native interpretation ORDER BY differs')
    if name=='connected-measurements'and actual!=expected:raise ValueError('Exact native grouped ORDER BY differs')
    if bag(actual)!=bag(expected):raise ValueError('Original complete authored relational bag differs '+name)
    return actual


def materialize(spark,profile):
    from graphframes import GraphFrame
    from pyspark.sql.types import StructType,StructField,StringType,LongType
    from run_graph_release_graphframes import columns,row_bag
    nodes=profile['nodes'];edges=profile['edges'];names=list(nodes[0]);integers={f['signed64_column']for f in profile['fields']if f['signed64_column']}
    schema=StructType([StructField(n,LongType()if n in integers else StringType(),True)for n in names])
    vertices=spark.createDataFrame([[r[n]for n in names]for r in nodes],schema)
    edge_names=list(columns('edge'));edge_schema=StructType([StructField(n,StringType(),True)for n in edge_names])
    edge_frame=spark.createDataFrame([[r[n]for n in edge_names]for r in edges],edge_schema)
    def parity():
        actual=[dict(zip(names,list(r)))for r in vertices.collect()]
        if row_bag(actual)!=row_bag(nodes):raise ValueError('Every native original lexical/presence/Integer carrier cell must match')
        actual_edges=[dict(zip(edge_names,list(r)))for r in edge_frame.collect()]
        if row_bag(actual_edges)!=row_bag(edges):raise ValueError('Every independent native edge carrier must match')
    parity()
    frame=GraphFrame(vertices.withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id'),edge_frame.withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id'))
    return frame,parity


def execute(spark,profile):
    frame,parity=materialize(spark,profile);plans=expressions(frame,profile);records=[]
    for candidate in profile['cases']:
        scenario=candidate['original_scenario'];name=scenario['id'];data=plans[name]
        native_schema=[{'name':f.name,'type':f.dataType.simpleString()}for f in data.schema.fields]
        raw=[list(r)for r in data.collect()]
        actual=validate_case(profile,name,native_schema,raw)
        records.append({'original_scenario':scenario,'native_schema':native_schema,'ordered_native_rows':raw,'actual_logical_rows':actual,'independent_original_graph_expected':candidate['original_graph_expected'],'executed_native_plan':data._jdf.queryExecution().executedPlan().toString()})
    parity()
    return {'pack':profile['pack'],'records':records,'all_original_cases_passed':len(records),'native_full_carrier_parity_before_after':True,'scope':__doc__}


def run(candidates,umf,output,jars):
    import hashlib,importlib.metadata,os,sys
    from pathlib import Path
    from prepare_pack_graph_scenarios import prepare
    from run_pack_release_graphframes import admit_runtime
    from run_graph_release_graphframes import VERSIONS
    if type(candidates)is not list or [c.get('pack')for c in candidates]!=['archaeology','ecology']:raise ValueError('Both original scenario packs required')
    output=Path(output)
    if output.exists():raise ValueError('Fresh bounded output required')
    if sys.version_info[:2]!=(3,11)or any(os.environ.get(n)!=sys.executable for n in ('PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON')):raise ValueError('Qualified matching Python3.11 driver/workers required')
    versions={n:importlib.metadata.version(n)for n in VERSIONS};paths=admit_runtime(jars,versions)
    watched={}
    for c in candidates:
        for name in ('release','custody'):watched[str(Path(c[name]).resolve())]=c[name+'_sha256']
        for name in ('original-ontology.json','original-graph.json','development-bindings.json','public-dataset.json','source.jsonl','report.json'):
            p=Path(c['publication'])/name;watched[str(p.resolve())]=hashlib.sha256(p.read_bytes()).hexdigest()
    def original_guard():
        if any(hashlib.sha256(Path(p).read_bytes()).hexdigest()!=h for p,h in watched.items()):raise ValueError('Original immutable source/release/custody changed')
    original_guard();profiles=[prepare(c,umf)for c in candidates];original_guard();output.mkdir(mode=0o700)
    for profile in profiles:(output/(profile['pack']+'-profile.json')).write_text(json.dumps(profile,sort_keys=True,ensure_ascii=False,indent=2)+'\n')
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original17 vertex scenarios').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(map(str,paths))).config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    try:
        spark.sparkContext.setLogLevel('ERROR');reports=[execute(spark,p)for p in profiles];original_guard()
        report={'format':'ashlar-original17-graphframes-vertex-relational/0.1','profiles':profiles,'reports':reports,'versions':versions,'jars':{p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in paths},'original_files':watched,'original_files_unchanged':True,'original_authored_cases':17,'scope':__doc__}
    finally:spark.stop()
    # Cleanup/closing failures must leave no complete report.
    original_guard();(output/'report.json').write_text(json.dumps(report,sort_keys=True,ensure_ascii=False,indent=2)+'\n')
    return report

if __name__=='__main__':
    import argparse
    from pathlib import Path
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('candidates','umf','output','jars'):p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();print('Completed original scenarios:',run(json.loads(a.candidates.read_bytes()),a.umf,a.output,a.jars)['original_authored_cases'])
