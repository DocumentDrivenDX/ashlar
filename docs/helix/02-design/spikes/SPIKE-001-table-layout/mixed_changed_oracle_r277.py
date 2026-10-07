"""Independent bounded changed-current/forward oracle; no native access."""
import json,hashlib,time,collections
from pathlib import Path
from mixed_changes_r228 import Changes,verify
from mixed_change_queries_r230 import row_hash
B=Path(__file__).resolve().parent

def main():
 start=time.monotonic();c=Changes(8000000,40000000,100000);hashes=collections.defaultdict(list);fields={};seen=set();deleted=[]
 for i in range(c.count):
  x=c.change(i);assert verify(x) and x['ordinal'] not in seen;seen.add(x['ordinal'])
  if x['after'] is None:deleted.append((x['before']['source_system'],x['before']['rel_type_id'],x['before']['id']));continue
  rows={'edge_current':x['after']}
  base=dict(c.w.roles('edge',x['ordinal']))['adjacency_forward'];rows['adjacency_forward']={k:x['after'][k] for k in base}
  for role,row in rows.items():
   fields.setdefault(role,list(row));assert list(row)==fields[role];hashes[role].append(row_hash(row,fields[role]))
 assert len(seen)==100000 and len(deleted)==10000 and all(len(h)==90000 for h in hashes.values())
 a={'format':'ashlar-changed-role-oracle/1','state':'All90k surviving current and forward rows independently qualified','base':{'nodes':8000000,'edges':40000000},'selected':100000,'deletes':10000,'final_current_rows':39990000,'roles':{r:{'rows':len(h),'fields':fields[r],'digest':hashlib.sha256(''.join(sorted(h)).encode()).hexdigest()} for r,h in hashes.items()},'source_sha256':{f:hashlib.sha256((B/f).read_bytes()).hexdigest() for f in ['mixed_changed_oracle_r277.py','mixed_changes_r228.py','scale_mixed_r219.py','mixed_change_queries_r230.py']},'wall_s':time.monotonic()-start,'qualification':'Independent full changed-role digests only; baseline unchanged rows must pass both directions of native EXCEPT ALL and deleted identities must be absent. No full native graph/publication/freshness claim; SHA256 collision resistance assumed.'}
 stage=json.loads((B/'out/ashlar_normalized_batch_files_r271/summary.json').read_text());assert a['roles']['edge_current']['digest']==stage['roles']['current_replacement']['all_known_field_digest']
 (B/'out/mixed-changed-oracle-r277.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'state':a['state'],'wall_s':a['wall_s']},indent=2))
if __name__=='__main__':main()
