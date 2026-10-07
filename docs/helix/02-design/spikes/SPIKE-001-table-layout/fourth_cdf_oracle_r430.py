"""Independent full-field expectations for one synthetic native change interval."""
import json,hashlib,time,collections
from pathlib import Path
from fourth_changes_r427 import FourthChanges,verify_fourth
from mixed_change_queries_r230 import row_hash
B=Path(__file__).resolve().parent

def main():
 assert not (B/'out/fourth-cdf-image-oracle-r430.json').exists();start=time.monotonic();c=FourthChanges();hashes=collections.defaultdict(list);fields={};tomb_hashes=[];tomb_fields=None
 for i in range(100000):
  x=c.change(i);assert verify_fourth(x)
  if x['tombstone'] is not None:
   tomb_fields=[k for k in x['tombstone'] if k!='apply_batch_id'];tomb_hashes.append(row_hash(x['tombstone'],tomb_fields))
  adj=dict(c.original.w.roles('edge',x['ordinal']))['adjacency_forward']
  for role,keys in [('edge_current',list(x['before'])),('adjacency_forward',list(adj))]:
   fields.setdefault(role,keys)
   before={k:x['before'][k] for k in keys};kind='delete' if x['after'] is None else 'update_preimage';hashes[(role,kind)].append(row_hash(before,keys))
   if x['after'] is not None:hashes[(role,'update_postimage')].append(row_hash({k:x['after'][k] for k in keys},keys))
 a={'format':'ashlar-cdf-image-oracle/1','state':'Independent complete190k-image multisets for each mutable role','roles':{},'source_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['fourth_cdf_oracle_r430.py','fourth_changes_r427.py','mixed_changes_r228.py','scale_mixed_r219.py','mixed_change_queries_r230.py']},'qualification':'Local exact190k rows per role:90k preimages90k postimages10k deletion preimages, no inserts. Every native field included; SHA256 collision resistance assumed. Not native CDF compatibility, complete inherited custody, permanent history or freshness evidence.'}
 for role in fields:
  a['roles'][role]={'fields':fields[role],'total_rows':190000,'images':{kind:{'rows':len(hashes[(role,kind)]),'digest':hashlib.sha256(''.join(sorted(hashes[(role,kind)])).encode()).hexdigest()} for kind in ['update_preimage','update_postimage','delete']}}
  assert {k:v['rows'] for k,v in a['roles'][role]['images'].items()}=={'update_preimage':90000,'update_postimage':90000,'delete':10000}
 assert len(tomb_hashes)==10000 and len(tomb_fields)==10
 tomb={'rows':10000,'fields':tomb_fields,'digest':hashlib.sha256(''.join(sorted(tomb_hashes)).encode()).hexdigest(),'source_sha256':a['source_sha256'],'qualification':'Independent complete canonical10field tombstone oracle; input-only apply_batch_id remains preserved separately in original input_json.'};(B/'out/fourth-canonical-tomb-r430.json').write_text(json.dumps(tomb,indent=2)+'\n')
 a['wall_s']=time.monotonic()-start;(B/'out/fourth-cdf-image-oracle-r430.json').write_text(json.dumps(a,indent=2)+'\n');print(a['state'],a['wall_s'])
if __name__=='__main__':main()
