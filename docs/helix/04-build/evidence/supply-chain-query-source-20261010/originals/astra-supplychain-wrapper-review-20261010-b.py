import copy,hashlib,importlib.util,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
R=Path('/Users/erik/Projects/ashlar'); B=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-b');P=Path('/private/tmp/astra-supplychain-wrapper-review-20261010-b')
sys.path[:0]=[str(R/'src'),str(R/'tools'),str(R/'tests')]
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
M=json.loads((B/'manifest.json').read_bytes());selected=[]
for name,live in [('supply_chain_weft_request.py',R/'tools/supply_chain_weft_request.py'),('test_supply_chain_weft_request.py',R/'tests/test_supply_chain_weft_request.py')]:
 x=next(x for x in M['files'] if Path(x['path']).name==name)
 for p in (B/name,live):
  d=desc(p);assert (d['bytes'],d['sha256'])==(x['bytes'],x['sha256']);selected.append(d)
import supply_chain_weft_request as W
from run_pack_publication_weft import compiler_request
from test_supply_chain_weft_request import SupplyChainWeftRequest
x=SupplyChainWeftRequest();x.setUp(); originals=copy.deepcopy([x.raw,x.bindings,x.manifest,x.registry,x.aliases])
scenarios=json.loads(x.raw[0])['scenario_checks'];requests=[]
for s in scenarios:
 base=compiler_request('supply-chain',s['sql'],x.raw[1],x.raw[2],x.bindings,x.manifest,x.registry,x.aliases)
 new=x.request(s['id']);bb=json.loads(base['target']['bindingJson']);nb=json.loads(new['target']['bindingJson'])
 changed=[]
 for r in nb['records']:
  for p in r['properties']:
   if 'encoding' in p['home']:
    changed.append((r['logical']['element'],p['logical']['element'],p['home'].pop('encoding')))
 assert changed==[('containers','containers.parent_id','ashlar-weft-json-native-null/0.1-candidate')]
 assert nb==bb
 assert {k:v for k,v in base.items() if k not in ('interfaceVersion','dialect','target')}=={k:v for k,v in new.items() if k not in ('interfaceVersion','dialect','target')}
 assert new['interfaceVersion']=='weft-compile/0.4.0' and new['dialect']=='weft-sql/0.4.0'
 assert {k:v for k,v in new['target'].items() if k not in ('bindingJson','bindingSha256')}==W.PATHS_KEYS_BACKEND
 assert hashlib.sha256(new['target']['bindingJson'].encode()).hexdigest()==new['target']['bindingSha256']
 candidate=B/(s['id']+'.request.json');assert json.loads(candidate.read_bytes())==new
 assert candidate.read_bytes()==json.dumps(new,ensure_ascii=False,separators=(',',':')).encode()
 requests.append(desc(candidate))
assert originals==[x.raw,x.bindings,x.manifest,x.registry,x.aliases]
# Isolate optional-home admission: complete helper still owns original mapping;
# wrapper rejects absent/duplicated/changed optional mappings, not silent fallback.
for defect in ('missing','duplicate','wronghome'):
 bad=copy.deepcopy(base);binding=json.loads(bad['target']['bindingJson']);record=next(r for r in binding['records'] if r['logical']['element']=='containers');prop=next(p for p in record['properties'] if p['logical']['element']=='containers.parent_id')
 if defect=='missing':record['properties'].remove(prop)
 elif defect=='duplicate':record['properties'].append(copy.deepcopy(prop))
 else:prop['home']['propertyId']='99999'
 bad['target']['bindingJson']=json.dumps(binding)
 with patch.object(W,'compiler_request',return_value=bad):
  try:x.request('sensor')
  except ValueError:pass
  else:raise AssertionError('Optional mapping defect admitted: '+defect)
suite=unittest.defaultTestLoader.discover(str(R/'tests'),pattern='test_supply_chain_weft_request.py');res=unittest.TextTestRunner(verbosity=2).run(suite);assert res.wasSuccessful() and res.testsRun==2
assert selected==[desc(Path(d['path'])) for d in selected]
out={'status':'approved-source-only','scope':'Two-file explicit original supply-chain PathsKeys request wrapper; no compiler execution or current held-publication/native admission.','source_opening_closing':selected,'freeze':desc(B/'manifest.json'),'controls':{'author_tests':2,'full_request_comparisons':5,'sole_optional_home_encoding_addition':True,'input_no_mutation':True,'optional_inventory_negative_controls':3,'candidate_raw_bytes_equal_regenerated_requests':5},'candidate_inputs':requests,'source_inputs':[desc(R/'examples/domain-packs/supply-chain/upstream'/n) for n in ('pack.json','ontology.json','graph/fixture.json')],'historical_publication_reference':desc(R/'docs/helix/04-build/evidence/supply-chain-native-publication-20261009.json'),'findings':[],'limits':['Explicit new target; original model documentJson/SQL/options/membership and full supplied table vector preserved.','The single native-null encoding changes binding bytes and digest deliberately; future native admission must check actual source null/missing behavior.','Historical table UUID/version pins and held_supply aliases are candidate compile inputs only; no current publication or ACK freshness established.','Shared old compiler loop/profile/constants remain unchanged.'],'review_script':desc(Path(__file__))}
P.with_suffix('.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'status':out['status'],'raw_requests':5,'receipt':str(P.with_suffix('.json'))}))
