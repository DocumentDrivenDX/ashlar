"""Original seventeen relational plans over independently admitted vertex carriers.

GraphFrame operations preserve property-equality bags; canonical edges remain
independent. This representation and its oracles confer no source authority.
"""
import base64,json,re
from collections import Counter
from .finite_pack import FinitePackDefinition
from .supply_chain_request import source_snapshot
from .delta_custody import encoded
from .finite_projection import original_finite_pack_oracle
from .schema_rows import fixture_columns
from .resources import RESOURCE_ROOT
from ashlar.graph_release import validate_release_carrier

def bag(rows):return Counter(encoded(row)for row in rows)
def integer(value):
 if type(value)is not int or not -(2**63)<=value<2**63:raise ValueError('Exact signed64 native result required')
 return value
INTEGER_FIELDS={'archaeology':{('interpretations','earliest'),('interpretations','latest')},'ecology':{('occurrences','count')}}
def signed64_token(token,field):
 if field.get('scalarType')!='integer' or type(token)is not str or len(token)>20 or not re.fullmatch(r'-?(0|[1-9][0-9]*)',token):raise ValueError('Named finite Integer token required')
 return integer(int(token))

def prepare_scenarios(definition,model_bytes,graph_bytes,pack_bytes,release):
 """Pure original-byte projection; caller separately holds current source policy."""
 if type(definition)is not FinitePackDefinition or definition.name not in INTEGER_FIELDS:raise ValueError('Closed original17 definition required')
 definition.inputs(model_bytes,graph_bytes,pack_bytes)
 release=source_snapshot(release);model=json.loads(model_bytes);graph=json.loads(graph_bytes)
 batch,bindings=definition.build(model_bytes,graph_bytes)
 expected_rows=original_finite_pack_oracle(definition,model_bytes,graph_bytes,bindings,batch,fixture_columns(RESOURCE_ROOT))
 for kind,role,source_role in (('node','nodes','object_current'),('edge','edges','edge_current')):
  if bag(release[role])!=bag(validate_release_carrier(row,kind)for row in expected_rows[source_role]):raise ValueError('Complete original canonical carrier bag differs')
 elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
 properties={tuple(p['identity']):p['property_id']for p in bindings['properties']}
 fields=[];by_record={}
 for module in model['modules']:
  for record in module['elements']:
   if record['kind']!='record':continue
   entries=[]
   for ref in record['members']:
    field=elements[(ref['module'],ref['element'])];identity=[model['id'],ref['module'],ref['element']];key=properties[tuple(identity)]
    entry={'identity':identity,'record_identity':[model['id'],module['id'],record['id']],'original_field':field,'development_property_id':key,'lexical_column':'field_'+key,'presence_column':'presence_'+key,'signed64_column':'integer_'+key if (record['id'],field['name'])in INTEGER_FIELDS[definition.name]else None}
    if entry not in fields:fields.append(entry)
    entries.append(entry)
   by_record[(module['id'],record['id'])]=entries
 if len({f['development_property_id']for f in fields})!=len(fields):raise ValueError('Injective original Field columns required')
 assigned={(e['kind'],e['originalKey']):e for e in bindings['entities']}
 objects={o['key']:o for o in graph['objects']}
 native={(assigned[('object',k)]['type_id'],assigned[('object',k)]['id']):k for k in objects}
 rows=[];seen=set()
 for raw in release['nodes']:
  if raw['source_system']!=batch.feed:raise ValueError('Source-qualified original carrier required')
  key=native[(raw['type_id'],raw['id'])];source=objects[key];seen.add(key)
  selected=(source['type']['module'],source['type']['element'])
  if source['type']['document']!=model['id'] or set(source['values'])!={f['identity'][2]for f in by_record[selected]}:raise ValueError('Complete exact original member presence required')
  row=dict(raw,original_key=key,original_type=encoded(source['type']))
  for f in fields:
   member=tuple(f['record_identity'][1:])==selected
   token=source['values'][f['identity'][2]]if member else None
   if token is not None and type(token)is not str:raise ValueError('Original lexical/null carrier required')
   row[f['lexical_column']]=token;row[f['presence_column']]='nonmember'if not member else'present-null'if token is None else'present'
   if f['signed64_column']:row[f['signed64_column']]=None if token is None else signed64_token(token,f['original_field'])
  rows.append(row)
 if len(rows)!=len(objects) or seen!=set(objects):raise ValueError('Complete original vertex occurrence bag required')
 cases=dict(definition.cases(model_bytes,graph_bytes,pack_bytes));expected=definition.oracle(model_bytes,graph_bytes)['scenarios']
 return {'pack':definition.name,'fields':fields,'nodes':rows,'edges':release['edges'],'source_admission':{'original_model_base64':base64.b64encode(model_bytes).decode(),'original_graph_base64':base64.b64encode(graph_bytes).decode()},'cases':[{'original_scenario':{'id':key,'sql':sql},'original_graph_expected':expected[key]}for key,sql in cases.items()]}

def expressions(frame,profile):
    from pyspark.sql import functions as F
    fields={(f['record_identity'][1],f['record_identity'][2],f['original_field']['name']):f for f in profile['fields']}
    pack=profile['pack']
    import base64
    model=json.loads(base64.b64decode(profile['source_admission']['original_model_base64']))
    def table(name,alias):
        identity=json.dumps({'document':model['id'],'module':'domain','element':name},sort_keys=True,separators=(',',':'))
        return frame.vertices.where(F.col('original_type')==identity).alias(alias)
    def col(alias,record,name,numeric=False):
        field=fields[('domain',record,name)];column=field['signed64_column']if numeric else field['lexical_column']
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
        witnesses=[(F.col('i.original_key'),'interpretation_key'),(F.col('e.original_key'),'evidence_key'),(col('i','interpretations','id'),'interpretation_order_token'),(F.col('p.original_key'),'matched_pottery_key'),(F.col('f.original_key'),'matched_fauna_key'),(F.col('p.'+fields[('domain','pottery_results','form')]['presence_column']),'pottery_form_presence'),(F.col('f.'+fields[('domain','fauna_results','taxon')]['presence_column']),'fauna_taxon_presence')]
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
    selected={c['original_scenario']['id']for c in profile['cases']}
    if not selected<=set(cases):raise ValueError('Known original authored scenarios required')
    return {name:cases[name]for name in cases if name in selected}

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


def common_oracle(graph):
 """Independent occurrence bags including parallel paths and isolated vertices."""
 objects,edges=graph['objects'],graph['edges'];types={};groups={}
 for obj in objects:types.setdefault(encoded(obj['type']),[]).append(obj['key'])
 selected=min(types,key=lambda t:(-len(types[t]),t));filtered=sorted(types[selected])
 if len(filtered)<2:raise ValueError('Nonvacuous original filtered limit required')
 for edge in edges:groups.setdefault((edge['source'],encoded(edge['relationship'])),[]).append(edge['target'])
 endpoints={edge[k]for edge in edges for k in ('source','target')}
 return {'one-hop':[[e['source'],e['key'],e['target']]for e in edges],'two-hop':[[a['source'],a['key'],a['target'],b['key'],b['target']]for a in edges for b in edges if a['target']==b['source']], 'grouped-count':[[s,t,len(ds),len(set(ds))]for(s,t),ds in groups.items()], 'singleton':[min(o['key']for o in objects)],'filtered-type':selected,'filtered-limit':filtered[:1],'filtered-total':len(filtered),'isolates':sorted(o['key']for o in objects if o['key']not in endpoints)}

def bounded_frame_rows(frame,maximum):
 if type(maximum)is not int or not 0<maximum<=10000:raise ValueError('Finite complete row capacity required')
 rows=[list(r)for r in frame.limit(maximum+1).collect()]
 if len(rows)>maximum:raise ValueError('Complete bag exceeds capacity; no truncated success')
 if len(encoded(rows).encode())>16777216:raise ValueError('Complete cells exceed byte capacity')
 return rows

def execute_scenarios(frame,profile,observe,maximum=10000):
 """Record owned bounded raw native cells before any semantic comparison."""
 if not callable(observe):raise ValueError('Owned raw observation sink required')
 plans=expressions(frame,profile);records=[]
 for candidate in profile['cases']:
  scenario=candidate['original_scenario'];name=scenario['id'];data=plans[name]
  schema=[{'name':f.name,'type':f.dataType.simpleString()}for f in data.schema.fields]
  raw=bounded_frame_rows(data,maximum)
  observed={'case':name,'kind':'original-scenario','original_scenario':scenario,'schema':schema,'rows':raw}
  if observe(source_snapshot(observed))is not None:raise ValueError('Observation must retain without granting authority')
  actual=validate_case(profile,name,schema,raw)
  records.append({'original_scenario':scenario,'schema':schema,'rows':raw,'logical_rows':actual,'independent_expected':candidate['original_graph_expected']})
 return records
