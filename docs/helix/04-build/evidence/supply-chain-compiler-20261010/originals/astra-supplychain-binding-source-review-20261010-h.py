import copy,hashlib,importlib.util,json,sys,unittest
from pathlib import Path
R=Path('/Users/erik/Projects/ashlar');B=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-h');G=B.with_name('ashlar-supply-chain-compile-candidate-20261010-g');P=Path('/private/tmp/astra-supplychain-binding-source-review-20261010-h')
sys.path[:0]=[str(R/'src'),str(R/'tools'),str(R/'tests')]
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
m=json.loads((B/'manifest.json').read_bytes());source=[]
for n,dir in [('supply_chain_weft_request.py','tools'),('test_supply_chain_weft_request.py','tests')]:
 d=next(x for x in m['files'] if Path(x['path']).name==n);assert desc(B/n)==d
 live=desc(R/dir/n);assert live['sha256']==d['sha256'] and live['bytes']==d['bytes'];source.append(live)
from supply_chain_weft_request import supply_chain_weft_request
from test_supply_chain_weft_request import SupplyChainWeftRequest
fixture=SupplyChainWeftRequest();fixture.setUp();aliases=json.loads((B/'aliases.json').read_bytes());assert aliases==fixture.aliases
assert aliases=={name:name.replace('local.','spark_catalog.',1) for name in json.loads(fixture.manifest['table_versions_json'])}
rows=[]
for s in json.loads(fixture.raw[0])['scenario_checks']:
 p=B/(s['id']+'.request.json');actual=fixture.request(s['id']);assert p.read_bytes()==json.dumps(actual,ensure_ascii=False,separators=(',',':')).encode()
 old=json.loads((G/p.name).read_bytes());ob=json.loads(old['target']['bindingJson']);nb=json.loads(actual['target']['bindingJson'])
 assert all(len(t['name'])==2 for t in ob['publication']['tables']) and all(len(t['name'])==3 for t in nb['publication']['tables'])
 for t in ob['publication']['tables']:t['name']=['spark_catalog','supplychain',t['name'][-1]]
 assert ob==nb
 for t in actual['target']:
  if t not in ('bindingJson','bindingSha256'):assert actual['target'][t]==old['target'][t]
 assert {k:v for k,v in actual.items() if k!='target'}=={k:v for k,v in old.items() if k!='target'}
 assert actual['sql']==s['sql']
 assert hashlib.sha256(actual['target']['bindingJson'].encode()).hexdigest()==actual['target']['bindingSha256']
 rows.append(desc(p))
# Public wrapper refuses malformed physical name shape before binding generation.
checks=0
for bad in ['two.parts','one','four.name.parts.here','a..c','a.b.','a.\x00.c',1,None]:
 changed=dict(aliases);changed[next(iter(changed))]=bad
 try:supply_chain_weft_request('sensor',*fixture.raw,fixture.bindings,fixture.manifest,fixture.registry,changed)
 except ValueError:checks+=1
 else:raise AssertionError('Invalid alias admitted')
suite=unittest.defaultTestLoader.discover(str(R/'tests'),pattern='test_supply_chain_weft_request.py');result=unittest.TextTestRunner(verbosity=2).run(suite);assert result.wasSuccessful() and result.testsRun==3
for d in source:assert desc(Path(d['path']))==d
out={'status':'approved-source-only','resolved_finding':'SUPPLY-BINDING-001','source_files':source,'source_freeze':desc(B/'manifest.json'),'independent_malformed_alias_refusals':checks,'author_tests':3,'regenerated_five_raw_requests':rows,'changed_request_semantics':'Only publication.tables[*].name changes from two-part held_supply.role to the exact original namespace three-part spark_catalog.supplychain.role, plus binding digest; original model/SQL/property mapping/UUID/version/source vectors unchanged.','limits':['No current Spark table resolution/publication authority established by alias construction.','Replay original HAVING SQL remains unchanged and its actual G parse refusal is a distinct upstream investigation.','Nested binding structural source audit and outer schema validation are separate recorded controls; no native or compiler pass claimed.'],'review_script':desc(Path(__file__))}
P.with_suffix('.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'status':out['status'],'alias_negatives':checks,'receipt':str(P.with_suffix('.json'))}))
