# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact original commerce GraphFrames projections and independent finite bags.

Logical UMF0.8 Integer remains unbounded. Decimal38 arithmetic here is a closed
original-fixture realization with exact-or-refuse guards, never source authority.
"""
import base64,json,re
from fractions import Fraction
from ashlar.graph_release import GraphRelease,validate_release_carrier
from .commerce_admission import CommerceAdmission,original_commerce_oracle
from .commerce_original_scenarios import commerce_original_cases,original_commerce_scenario_oracle
from .graphframes_pack_scenarios import bag,bounded_frame_rows
from .resources import RESOURCE_ROOT
from .schema_rows import fixture_columns
from .supply_chain_request import source_snapshot
from .delta_custody import encoded

PROFILE='original-commerce/0.1'

def exact_number(token,representation):
 if type(token)is not str or len(token)>128 or not re.fullmatch(r'-?(0|[1-9][0-9]*)(\.[0-9]+)?',token):raise ValueError('Exact bounded original numeric token required')
 digits=token.lstrip('-').split('.');fraction=digits[1]if len(digits)==2 else''
 if representation=='decimal(38,0)':
  if fraction or len(digits[0])>38:raise ValueError('Finite exact Integer realization refused')
 elif representation=='decimal(18,2)':
  if len(fraction)>2 or len(digits[0])>16:raise ValueError('Exact authored fixed decimal realization refused')
 else:raise ValueError('Closed original numeric representation required')
 return Fraction((-1 if token.startswith('-')else 1)*int(digits[0]+fraction),10**len(fraction))

def prepare_commerce_graphframes_profile(admission,pack_bytes,release):
 if type(admission)is not CommerceAdmission or type(release)is not GraphRelease:raise ValueError('Exact original commerce source/release required')
 admission.metadata();model,graph,binding,_=[raw for _,raw in admission.originals]
 document=json.loads(model);original=json.loads(graph);bindings=json.loads(binding)
 if document['umf']!='0.8.0':raise ValueError('Declared original UMF0.8 required')
 payload=source_snapshot(json.loads(release.payload))
 expected=original_commerce_oracle(model,graph,bindings,admission.batch,fixture_columns(RESOURCE_ROOT))
 for kind,role,source_role in (('node','nodes','object_current'),('edge','edges','edge_current')):
  if bag(payload[role])!=bag(validate_release_carrier(row,kind)for row in expected[source_role]):raise ValueError('Complete independent original carrier bag differs')
 elements={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
 properties={tuple(p['identity']):p['property_id']for p in bindings['properties']}
 fields=[];members={}
 for module in document['modules']:
  for record in module['elements']:
   if record['kind']!='record':continue
   entries=[]
   for ref in record['members']:
    field=elements[(ref['module'],ref['element'])];identity=[document['id'],ref['module'],ref['element']];key=properties[tuple(identity)]
    entry={'identity':identity,'record_identity':[document['id'],module['id'],record['id']],'original_field':field,'development_property_id':key,'lexical_column':'field_'+key,'presence_column':'presence_'+key,'signed64_column':None}
    if entry not in fields:fields.append(entry)
    entries.append(entry)
   members[(module['id'],record['id'])]=entries
 if len({f['development_property_id']for f in fields})!=len(fields):raise ValueError('Injective original field identities required')
 assigned={(e['kind'],e['originalKey']):e for e in bindings['entities']};objects={o['key']:o for o in original['objects']}
 native={(assigned[('object',key)]['type_id'],assigned[('object',key)]['id']):key for key in objects}
 rows=[];numeric=[]
 for raw in payload['nodes']:
  key=native[(raw['type_id'],raw['id'])];obj=objects[key];selected=(obj['type']['module'],obj['type']['element'])
  if raw['source_system']!=admission.batch.feed or obj['type']['document']!=document['id']or set(obj['values'])!={f['identity'][2]for f in members[selected]}:raise ValueError('Complete original member/source correspondence required')
  row=dict(raw,original_key=key,original_type=encoded(obj['type']))
  for field in fields:
   member=tuple(field['record_identity'][1:])==selected;token=obj['values'][field['identity'][2]]if member else None
   if token is not None and type(token)is not str:raise ValueError('Original exact lexical/null member required')
   row[field['lexical_column']]=token;row[field['presence_column']]='nonmember'if not member else'present-null'if token is None else'present'
   scalar=field['original_field'].get('scalarType')
   if member and scalar in ('integer','decimal'):
    representation='decimal(38,0)'if scalar=='integer'else'decimal(18,2)'
    if scalar=='decimal'and field['original_field'].get('facets')!={'precision':18,'scale':2}:raise ValueError('Original authored decimal facets required')
    exact_number(token,representation)
    numeric.append({'key':key,'graph_id':raw['graph_id'],'field':field['identity'],'column':field['lexical_column'],'token':token,'representation':representation})
  rows.append(row)
 if len(rows)!=len(objects)or{r['original_key']for r in rows}!=set(objects):raise ValueError('Complete original object occurrence bag required')
 cases=[]
 for name,sql in commerce_original_cases(pack_bytes):
  oracle=original_commerce_scenario_oracle(model,graph,{'sql':sql})
  cases.append({'original_scenario':{'id':name,'sql':sql},'original_graph_expected':oracle['rows'],'source_occurrences':oracle['witnesses']['source_occurrences']})
 return {'pack':'commerce','profile':PROFILE,'fields':fields,'nodes':rows,'edges':payload['edges'],'numeric_guards':numeric,'source_admission':{'original_model_base64':base64.b64encode(model).decode(),'original_graph_base64':base64.b64encode(graph).decode()},'cases':cases}

def validate_case(profile,name,schema,raw):
 profile=source_snapshot(profile);schema=source_snapshot(schema);raw=source_snapshot(raw)
 cases=[c for c in profile['cases']if c['original_scenario']['id']==name]
 names=['fulfilled','returned','remaining']if name=='partial-return'else['id']
 if len(cases)!=1 or schema!=[{'name':n,'type':'string'}for n in names]or any(type(row)is not list or len(row)!=len(names)or any(type(v)is not str for v in row)for row in raw):raise ValueError('Exact complete commerce native schema/cells required')
 if name=='partial-return':
  for row in raw:
   for value in row:exact_number(value,'decimal(38,0)')
 if bag(raw)!=bag(cases[0]['original_graph_expected']):raise ValueError('Original complete commerce scenario occurrence bag differs')
 return raw

def commerce_relationship_types(profile):
 """Resolve authored relationship IDs, distinct from UMF Element references."""
 model=json.loads(base64.b64decode(profile['source_admission']['original_model_base64'],validate=True))
 declared={(model['id'],module['id'],relation['id']) for module in model['modules'] for relation in module.get('relationships',[])}
 identities={};result={}
 for row in profile['edges']:
  reference=json.loads(row['retained_json'])['original']['relationship']
  if type(reference)is not dict or set(reference)!={'document','module','id'} or any(type(reference[name])is not str for name in reference):raise ValueError('Exact original relationship reference required')
  identity=tuple(reference[name]for name in ('document','module','id'))
  if identity not in declared:raise ValueError('Original relationship document/module/id differs')
  name=reference['id'];native=row['rel_type_id']
  if name in identities and (identities[name]!=identity or result[name]!=native):raise ValueError('Unambiguous original relationship/native type required')
  identities[name]=identity;result[name]=native
 return result

def execute_commerce_graphframes_scenarios(frame,profile,observe):
 from pyspark.sql import functions as F
 fields={(f['record_identity'][2],f['original_field']['name']):f for f in profile['fields']}
 edges=commerce_relationship_types(profile)
 def lexical(alias,record,name):return F.col(alias+'.'+fields[(record,name)]['lexical_column'])
 def number(alias,record,name):
  field=fields[(record,name)]['original_field'];return lexical(alias,record,name).cast('decimal(38,0)'if field['scalarType']=='integer'else'decimal(18,2)')
 def rel(alias,name):return F.col(alias+'.rel_type_id')==edges[name]
 def capture(kind,name,data):
  raw=bounded_frame_rows(data,10000);schema=[{'name':f.name,'type':f.dataType.simpleString()}for f in data.schema.fields]
  record={'kind':kind,'case':name,'schema':schema,'rows':raw}
  if kind=='original-scenario':record['original_scenario']=next(c['original_scenario']for c in profile['cases']if c['original_scenario']['id']==name)
  if observe(source_snapshot(record))is not None:raise ValueError('Raw commerce observation sink refused')
  return schema,raw
 for item in profile['numeric_guards']:
  data=frame.vertices.where(F.col('id')==item['graph_id']).select(F.col(item['column']).alias('token'),F.col(item['column']).cast(item['representation']).cast('string').alias('exact'))
  _,rows=capture('numeric-guard',item['key']+':'+item['field'][2],data)
  if len(rows)!=1 or rows[0][0]!=item['token']or rows[0][1]is None or exact_number(rows[0][1],item['representation'])!=exact_number(item['token'],item['representation']):raise ValueError('Original numeric token/exact cast differs')
 partial=frame.find('(f)-[fl]->(l); (r)-[rl]->(l)').where(rel('fl','fulfillments.line_id')&rel('rl','returns.line_id'))
 pair=frame.find('(f)-[fl]->(l)').where(rel('fl','fulfillments.line_id'))
 settlement=frame.find('(p)-[pi]->(i)').where(rel('pi','payments.invoice_id'))
 refund=frame.find('(r)-[rr]->(t); (t)-[tl]->(l); (l)-[lp]->(p)').where(rel('rr','refunds.return_id')&rel('tl','returns.line_id')&rel('lp','order_lines.product_id'))
 # Complete unfiltered arithmetic occurrence witnesses precede final predicates.
 intermediates=[('partial-return',partial.select(F.col('l.original_key').alias('line'),F.col('f.original_key').alias('fulfillment'),F.col('r.original_key').alias('return'),(number('l','order_lines','quantity')-number('f','fulfillments','quantity')).cast('string').alias('subtract'),(number('l','order_lines','quantity')-number('f','fulfillments','quantity')+number('r','returns','quantity')).cast('string').alias('add'))),('refund',refund.select(F.col('r.original_key').alias('refund'),F.col('t.original_key').alias('return'),F.col('l.original_key').alias('line'),F.col('p.original_key').alias('product'),(number('t','returns','quantity')*number('p','products','unit_price')).cast('string').alias('multiply')))]
 for name,data in intermediates:
  schema,rows=capture('arithmetic-intermediate',name,data);validate_intermediate(profile,name,schema,rows)
 plans={
 'partial-return':partial.where(number('f','fulfillments','quantity')<number('l','order_lines','quantity')).select(number('f','fulfillments','quantity').cast('string').alias('fulfilled'),number('r','returns','quantity').cast('string').alias('returned'),(number('l','order_lines','quantity')-number('f','fulfillments','quantity')+number('r','returns','quantity')).cast('string').alias('remaining')),
 'fulfillment':pair.where(number('f','fulfillments','quantity')>number('l','order_lines','quantity')).select(lexical('f','fulfillments','id').alias('id')),
 'settlement':settlement.where((number('p','payments','amount')==number('i','invoices','amount'))&(lexical('p','payments','currency')==lexical('i','invoices','currency'))).select(lexical('p','payments','id').alias('id')),
 'refund':refund.where(number('r','refunds','amount')==number('t','returns','quantity')*number('p','products','unit_price')).select(lexical('r','refunds','id').alias('id'))}
 results=[]
 for case in profile['cases']:
  name=case['original_scenario']['id'];schema,rows=capture('original-scenario',name,plans[name])
  # Original SQL metadata is retained alongside the raw native observation.
  logical=validate_case(profile,name,schema,rows)
  results.append({'original_scenario':case['original_scenario'],'schema':schema,'rows':rows,'logical_rows':logical,'independent_expected':case['original_graph_expected']})
 return results

def validate_intermediate(profile,name,schema,rows):
 graph=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64'],validate=True));objects=graph['objects']
 def records(record):return[o for o in objects if o['type']['element']==record]
 def value(obj,record,field):return obj['values'][record+'.'+field]
 expected=[]
 if name=='partial-return':
  names=['line','fulfillment','return','subtract','add']
  for line in records('order_lines'):
   for fulfillment in records('fulfillments'):
    for returned in records('returns'):
     if value(line,'order_lines','id')==value(fulfillment,'fulfillments','line_id')==value(returned,'returns','line_id'):
      a,b,c=[exact_number(value(o,r,'quantity'),'decimal(38,0)')for o,r in ((line,'order_lines'),(fulfillment,'fulfillments'),(returned,'returns'))]
      expected.append([line['key'],fulfillment['key'],returned['key'],a-b,a-b+c])
 elif name=='refund':
  names=['refund','return','line','product','multiply']
  for refund in records('refunds'):
   for returned in records('returns'):
    for line in records('order_lines'):
     for product in records('products'):
      if value(refund,'refunds','return_id')==value(returned,'returns','id')and value(returned,'returns','line_id')==value(line,'order_lines','id')and value(line,'order_lines','product_id')==value(product,'products','id'):
       expected.append([refund['key'],returned['key'],line['key'],product['key'],exact_number(value(returned,'returns','quantity'),'decimal(38,0)')*exact_number(value(product,'products','unit_price'),'decimal(18,2)')])
 else:raise ValueError('Closed original arithmetic intermediate required')
 if schema!=[{'name':n,'type':'string'}for n in names]or any(type(row)is not list or len(row)!=len(names)or any(type(v)is not str for v in row)for row in rows):raise ValueError('Exact arithmetic intermediate schema required')
 # A numeric witness is compared mathematically, with exact finite lexical bounds.
 index=3 if name=='partial-return'else 4
 actual=[row[:index]+[str(Fraction(v))for v in [exact_number(token,'decimal(38,0)'if name=='partial-return'else'decimal(18,2)')for token in row[index:]]]for row in rows]
 independent=[row[:index]+[str(v)for v in row[index:]]for row in expected]
 if bag(actual)!=bag(independent):raise ValueError('Complete original arithmetic intermediate occurrence bag differs')

def validate_arithmetic_evidence(profile,observations):
 profile=source_snapshot(profile);observations=source_snapshot(observations)
 guards=[v for v in observations if v.get('kind')=='numeric-guard']
 if len(guards)!=len(profile['numeric_guards']):raise ValueError('Every original numeric guard required')
 for item in profile['numeric_guards']:
  name=item['key']+':'+item['field'][2];found=[v for v in guards if v.get('case')==name]
  if len(found)!=1 or set(found[0])!={'kind','case','schema','rows'}or found[0]['schema']!=[{'name':'token','type':'string'},{'name':'exact','type':'string'}]:raise ValueError('Closed original numeric guard witness required')
  rows=found[0]['rows']
  if type(rows)is not list or len(rows)!=1 or type(rows[0])is not list or len(rows[0])!=2 or rows[0][0]!=item['token']or exact_number(rows[0][1],item['representation'])!=exact_number(item['token'],item['representation']):raise ValueError('Raw original token/cast witness differs')
 intermediates=[v for v in observations if v.get('kind')=='arithmetic-intermediate']
 if len(intermediates)!=2:raise ValueError('Both complete original arithmetic witness bags required')
 for name in ('partial-return','refund'):
  found=[v for v in intermediates if v.get('case')==name]
  if len(found)!=1 or set(found[0])!={'kind','case','schema','rows'}:raise ValueError('Closed original intermediate witness required')
  validate_intermediate(profile,name,found[0]['schema'],found[0]['rows'])
