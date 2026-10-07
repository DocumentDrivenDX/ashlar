"""Actual CONTRACT-003 (relationship,edge) ordering over pinned hub fixtures."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hub_contract_pages_r112'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86'
s=json.loads((B/'out/native/ashlar_hub_ordering_r111/audited-summary.json').read_text())
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
for source,hub in [('pilot',32),('other',160)]:
 for rel,edge in [(9005,4000000),(9085,14000000),(9155,22000000)]:
  expected=[]
  for relationship in range(rel,9165):
   typ,hop=divmod(relationship-9000,5)
   if not 1<=typ<=32:continue
   lower=hop*4000000+1;upper=(hop+1)*4000000
   ordinal=lower+((typ-1-lower)%32)
   while ordinal<=upper and len(expected)<100:
    identity=4000000+ordinal
    if (relationship,identity)>(rel,edge) and (ordinal%10!=0)==(source=='pilot'):
     expected.append([str(identity),str(relationship),str(typ),str((ordinal-1)%4000000+1)])
    ordinal+=32
   if len(expected)==100:break
  assert len(expected)==100
  for i in range(3):
   for mode in (('endpoint','ordered') if i%2==0 else ('ordered','endpoint')):
    table=s['tables'][mode];version=s['versions'][mode]
    query=f"SELECT edge_id,rel_type_id,target_type,target_id FROM {table} VERSION AS OF {version} WHERE source_system='{source}' AND source_type=1 AND source_id={hub} AND (rel_type_id>{rel} OR (rel_type_id={rel} AND edge_id>{edge})) ORDER BY rel_type_id,edge_id LIMIT 100"
    assert c.sql(f'page-{source}-{rel}-{mode}-{i}',query)==expected
(O/'summary.json').write_text(json.dumps({'state':'36 contract-order page checks passed; final-history audit pending','tables':s['tables'],'versions':s['versions'],'qualification':'Exact lexicographic relation/edge paging independently reconstructed from synthetic arithmetic. Not a canonical published projection or actual producer. No adjacency SLO or billion-scale admission.'},indent=2)+'\n')
c.history();c.close();print('Passed 36 exact contract-order page checks')
