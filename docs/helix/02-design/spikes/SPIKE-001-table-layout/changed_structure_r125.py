"""Read-only changed-set structural differential with fixed trusted membership."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_changed_structure_r125'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles'
s=json.loads((B/'out/native/ashlar_isolation_r123/audited-summary.json').read_text());r=s['batches'][0]
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';S=r['source_stage'];v=r['versions'][E];old=r['old_current_version']
columns=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id']
keys=f'SELECT source_system,rel_type_id,id,lookup_hash FROM {S} VERSION AS OF 0'
def slice(version):return f"SELECT {','.join('b.'+x for x in columns)} FROM {E} VERSION AS OF {version} b JOIN ({keys}) k ON b.lookup_hash=k.lookup_hash AND b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id"
before=slice(old);after=slice(v);stage=f"SELECT {','.join(columns)} FROM {S} VERSION AS OF 0"
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
anchor=c.sql('anchor',f'SELECT min(id) FROM {S} VERSION AS OF 0')[0][0]
def diff(label,a):return int(c.sql(label,f'SELECT count(*) FROM (({before} EXCEPT ALL SELECT * FROM ({a})) UNION ALL (SELECT * FROM ({a}) EXCEPT ALL {before}))')[0][0])
assert c.sql('trusted-membership',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {S} VERSION AS OF 0')==[['100000','100000']]
checks={}
for i in range(3):
 assert diff('stage-exact-'+str(i),stage)==0
 assert diff('output-exact-'+str(i),after)==0
for field in columns:
 expr="concat(source_system,'-corrupt')" if field=='source_system' else field+'+1'
 mutated=f"SELECT {','.join(('CASE WHEN id='+anchor+' THEN '+expr+' ELSE '+x+' END AS '+x) if x==field else x for x in columns)} FROM {S} VERSION AS OF 0"
 checks[field]=diff('reject-'+field,mutated);assert checks[field]>0
checks['deletion']=diff('reject-deletion',stage+' WHERE id<>'+anchor);assert checks['deletion']==1
checks['duplicate']=diff('reject-duplicate',stage+' UNION ALL '+stage+' WHERE id='+anchor);assert checks['duplicate']==1
(O/'summary.json').write_text(json.dumps({'state':'6 exact100k structural differentials passed;9 deliberate corruptions rejected; final metrics pending','old_version':old,'new_version':v,'trusted_stage':S,'trusted_stage_version':0,'corruptions':checks,'qualification':'Fixed original membership independently used for predecessor/output selection; source stage immutable and trusted for this finite synthetic test. Separate finite lineage guard available; source membership/owned SQL and outside-membership mutations remain separate obligations, unknown source effects, concurrent fencing, outside-membership mutations, adjacency baseline binding or full publication replacement qualified.'},indent=2)+'\n');c.history();c.close();print('Exact changed-set structural checks and9 corruptions passed')
