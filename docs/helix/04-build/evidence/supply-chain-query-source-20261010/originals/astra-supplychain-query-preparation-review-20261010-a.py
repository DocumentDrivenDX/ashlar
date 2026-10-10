import hashlib, importlib.util, itertools, json, subprocess, sys, unittest
from collections import Counter
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
ROOT=Path('/Users/erik/Projects/ashlar'); BASE=Path('/private/tmp/ashlar-supply-chain-query-preparation-20261010-a')
PREFIX=Path('/private/tmp/astra-supplychain-query-preparation-review-20261010-a')
sys.path[:0]=[str(ROOT/'tools'),str(ROOT/'src')]
def descriptor(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
m=json.loads((BASE/'manifest.json').read_bytes()); opening=[]
for x in m['source_files']:
 for key in ('path','snapshot'):
  d=descriptor(Path(x[key]));assert d['sha256']==x['sha256'] and d['bytes']==x['bytes'];opening.append(d)
spec=importlib.util.spec_from_file_location('review_frozen_supplychain',BASE/'prepare_supply_chain_query_cases.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
p=ROOT/'examples/domain-packs/supply-chain/upstream'; names=['pack.json','ontology.json','graph/fixture.json'];raw=[(p/n).read_bytes() for n in names]
for b,x in zip(raw,m['original_inputs']):assert len(b)==x['bytes'] and hashlib.sha256(b).hexdigest()==x['sha256']
pack,model,graph=map(json.loads,raw);T={}
for obj in graph['objects']:T.setdefault(obj['type']['element'],[]).append(obj['values'])
# Independent direct bag evaluation of the authored inner joins, group keys,
# COUNT DISTINCT, HAVING and DISTINCT. No production graph-oracle helper used.
split={}
for l,i,x,s in itertools.product(T['lots'],T['items'],T['shipment_items'],T['shipments']):
 if i['items.lot_id']==l['lots.id'] and x['shipment_items.item_id']==i['items.id'] and s['shipments.id']==x['shipment_items.shipment_id']:
  split.setdefault(l['lots.lot_code'],set()).add(s['shipments.id'])
counts=Counter(e['events.upstream_event_id'] for e in T['events'])
expected={
 'split-excursion':[[k,str(len(v))] for k,v in split.items()],
 'excursion':[[r['sensor_readings.id'],r['sensor_readings.value'],r['sensor_readings.unit']] for r in T['sensor_readings'] if Decimal(r['sensor_readings.value'])>Decimal(r['sensor_readings.upper_limit'])],
 'replay':[[k,str(v)] for k,v in counts.items() if v>1],
 'lineage':[],
 'sensor':[list(x) for x in set((r['sensor_readings.unit'],r['sensor_readings.method']) for r in T['sensor_readings'])]}
for i,l,pd,a,c in itertools.product(T['items'],T['lots'],T['products'],T['containment'],T['containers']):
 if l['lots.id']==i['items.lot_id'] and pd['products.id']==l['lots.product_id'] and a['containment.item_id']==i['items.id'] and c['containers.id']==a['containment.container_id']:
  expected['lineage'].append([i['items.serial'],l['lots.lot_code'],pd['products.sku'],c['containers.parent_id']])
with patch.object(subprocess,'Popen',side_effect=AssertionError('Pure preparation must not spawn')):
 actual=mod.prepare_supply_chain_cases(*raw)
assert len(actual['cases'])==5
for c,original in zip(actual['cases'],pack['scenario_checks']):
 assert c['original_scenario']==original and c['original_scenario']['sql']==original['sql']
 assert Counter(map(tuple,c['original_graph_rows']))==Counter(map(tuple,expected[c['id']]))
 assert c['comparison']=='multiset; original query has no ORDER BY'
 assert 'ORDER BY' not in original['sql'].upper()
elems={(mo['id'],e['id']):e for mo in model['modules'] for e in mo['elements']}
records={k:e for k,e in elems.items() if e['kind']=='record'}
assert len(records)==len(actual['tables'])==12
seen=[]
for tab in actual['tables']:
 assert tab['identity'][0]==model['id']; rec=records[tuple(tab['identity'][1:])]
 assert [f['identity'] for f in tab['fields']]==[[model['id'],r['module'],r['element']] for r in rec['members']]
 for field in tab['fields']:
  assert field['original_field']==elems[tuple(field['identity'][1:])];seen.append(tuple(field['identity']))
assert len(seen)==len(set(seen))==40
assert actual['original_umf_version']=='0.8.0'
assert len(actual['optional_props_requirements'])==1
op=actual['optional_props_requirements'][0]
assert op['identity']==[model['id'],'domain','containers.parent_id']
assert {k:op[k] for k in ['required_home_encoding','missing_property','present_null']}=={'required_home_encoding':'ashlar-weft-json-native-null/0.1-candidate','missing_property':'refuse','present_null':'state:null'}
assert expected['excursion']==[['[42,0,"M2"]','17','Cel']]
assert sum(r['containers.parent_id'] is None for r in T['containers'])==2
assert sum(row[-1] is None for row in expected['lineage'])==1
for field in (e for e in elems.values() if e.get('scalarType')=='decimal'):assert field['facets']=={'precision':18,'scale':2}
# Return snapshots may be mutated by the caller; future calls must stay original.
actual['tables'][0]['fields'][0]['original_field']['nullability']='corrupted'
actual['cases'][0]['original_scenario']['sql']='corrupted'
again=mod.prepare_supply_chain_cases(*raw)
assert again['cases'][0]['original_scenario']==pack['scenario_checks'][0]
assert again['tables'][0]['fields'][0]['original_field']['nullability']!='corrupted'
for index in range(3):
 for replacement in (raw[index]+b' ',bytearray(raw[index])):
  damaged=list(raw);damaged[index]=replacement
  try:mod.prepare_supply_chain_cases(*damaged)
  except ValueError:pass
  else:raise AssertionError('Tamper/type admitted')
# Run frozen-author tests through original location so fixture path is honest.
suite=unittest.defaultTestLoader.discover(str(ROOT/'tests'),pattern='test_supply_chain_query_cases.py');result=unittest.TextTestRunner(verbosity=2).run(suite);assert result.wasSuccessful() and result.testsRun==3
assert opening==[descriptor(Path(d['path'])) for d in opening]
receipt={'status':'approved-source-only','scope':'Exact finite original five-query preparation and independent graph bags. No compiler, native, publication, source authority or ACK qualification.', 'freeze':descriptor(BASE/'manifest.json'),'source_opening_closing':opening,'original_inputs':[descriptor(p/n) for n in names],'independent_expected_bags':expected,'controls':{'author_tests':3,'independent_inner_join_bags':5,'original_records':12,'original_fields':40,'original_graph_objects':len(graph['objects']),'original_edge_occurrences':len(graph['edges']),'input_tamper_or_type_refusals':6,'returned_nested_snapshot_isolation':True,'subprocess_forbidden_during_preparation':True,'original_graph_decimal_lexemes':['4','17','8'],'declared_decimal_facets':{'precision':18,'scale':2},'present_nulls':2,'lineage_nulls':1},'findings':[],'limits':['Fixed original hashes; not a general SQL evaluator or generalized mutable-data oracle.','COUNT outputs are exact decimal text; original source-qualified identity strings remain opaque.','Original graph decimal lexemes 17/8 are retained rather than rewritten to CSV display scale 17.00/8.00.','Optional native-null home requirements are declared only; execution must independently admit them.','No ORDER BY exists; fixture list order is not a query ordering guarantee.','Finite semantic/source tests; no analyzer proof or engine support claim.'],'referenced_dependency_inputs':[descriptor(ROOT/'tools/supply_chain_graph_oracle.py'),descriptor(ROOT/'tools/supply_chain_source_transaction.py')], 'review_script':descriptor(Path(__file__))}
PREFIX.with_suffix('.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'status':receipt['status'],'cases':5,'records':12,'fields':40,'receipt':str(PREFIX.with_suffix('.json'))}))
