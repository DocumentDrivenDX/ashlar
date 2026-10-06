"""Pinned structural correctness and degree admission controls; no physical scan cap claim."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';out=B/'out/native/ashlar_structural_reads_20261005_r5';c=DriverClient(out)
s=json.loads((B/'out/native/ashlar_structural_descriptor_20261005_r4/summary.json').read_text());assert s['state']=='passed';v=s['versions'];av=v[F+'.adjacency'];dv=v[F+'.out_degree_r1']
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
def paths(root,version):
 return f"SELECT a.id,b.id,b.target_id FROM {F}.adjacency VERSION AS OF {version} a JOIN {F}.adjacency VERSION AS OF {version} b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.target_type=b.source_type AND a.target_id=b.source_id WHERE a.source_system='pilot:0' AND a.rel_type_id=7 AND a.source_type=1 AND a.source_id={root} ORDER BY a.id,b.id"
def expected(root,deleted=False):
 edges=[]
 # Modular inverse maps native source to first-slot native edge identity.
 first=((root-1)*pow(104729,-1,200000))%200000+1
 for slot in range(5):
  id=first+slot*200000
  if deleted and id<=20:continue
  target=((root+(17 if slot<2 else slot*17)-1)%200000)+1
  edges.append((id,target))
 return edges
for root in [1,12345]:
 for version,changed in [(0,False),(av,True)]:
  wanted=[[str(a),str(b),str(t)] for a,target in expected(root,changed) for b,t in expected(target,changed)]
  assert c.sql(f'paths-{root}-v{version}',paths(root,version))==sorted(wanted,key=lambda r:(int(r[0]),int(r[1])))
 # Re-read old snapshot after the published mutation, independent of current shape.
 assert c.sql(f'old-pinned-{root}',paths(root,0))==c.sql(f'old-pinned-repeat-{root}',paths(root,0))
assert c.sql('parallel-counterparts',f"SELECT count(*) FROM {F}.delete_r1 d JOIN {F}.adjacency VERSION AS OF {av} a ON d.source_system=a.source_system AND d.rel_type_id=a.rel_type_id AND d.source_type=a.source_type AND d.source_id=a.source_id AND d.target_type=a.target_type AND d.target_id=a.target_id AND d.id<>a.id")==[['20']]
# Application admission is a separate gate; no path SQL is submitted when refused.
results=[]
for root in [200001,1,12345,999999]:
 r=c.sql(f'admission-{root}',f"SELECT count(*),coalesce(sum(d.out_degree),0) FROM {F}.adjacency VERSION AS OF {av} a LEFT JOIN {F}.out_degree_r1 VERSION AS OF {dv} d ON a.source_system=d.source_system AND a.rel_type_id=d.rel_type_id AND a.target_type=d.source_type AND a.target_id=d.source_id WHERE a.source_system='pilot:0' AND a.rel_type_id=7 AND a.source_type=1 AND a.source_id={root}")
 work=sum(int(x) for x in r[0]);refused=work>100000
 if root==200001:assert work==120003 and refused
 if refused:rows=None
 else:rows=c.sql(f'admitted-paths-{root}',paths(root,av));assert len(rows)==int(r[0][1])
 results.append(dict(root=root,logical_work=work,refused=refused,path_count=None if rows is None else len(rows)))
(out/'summary.json').write_text(json.dumps(dict(state='passed',controls=results,versions=v,scope='application preflight refusal; no path query for hub; no physical scan cap, combined latency or concurrency admission'),indent=2));c.history();c.close();print('Structural read controls passed',flush=True)
