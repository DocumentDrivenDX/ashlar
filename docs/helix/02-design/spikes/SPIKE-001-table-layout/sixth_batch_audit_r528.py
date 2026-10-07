"""Read every generated local part; independently recompute field and input digests."""
import hashlib,json,time
from pathlib import Path
from mixed_change_queries_r230 import row_hash
B=Path(__file__).resolve().parent

def main():
 p=B/'out/ashlar_sixth_batch_files_r527';a=json.loads((p/'summary.json').read_text());oracle=B/'out/sixth-batch-oracle-r526.json';o=json.loads(oracle.read_text());assert hashlib.sha256(oracle.read_bytes()).hexdigest()==a['oracle_sha256'];assert a['processed_changes']==100000 and o['counts']['updates']==90000 and o['counts']['deletes']==10000 and len(o['negative_controls'])==5
 for name,h in a['source_sha256'].items():assert hashlib.sha256((B/name).read_bytes()).hexdigest()==h
 root=Path(a['local_root']);start=time.monotonic();roles={}
 for role,r in a['roles'].items():
  hashes=[];inputs=[];count=0;byte_count=0
  for part in r['parts']:
   h=hashlib.sha256();n=0;b=0
   with (root/part['name']).open('rb') as stream:
    for data in stream:
     assert data.endswith(b'\n') and len(data)<=a['bounds']['row_bytes'];h.update(data);n+=1;b+=len(data);row=json.loads(data);assert list(row)==r['fields'];hashes.append(row_hash(row,r['fields']));inputs.append(hashlib.sha256(data[:-1]).hexdigest())
   assert n==part['rows'] and b==part['bytes'] and b<=a['bounds']['part_bytes'] and h.hexdigest()==part['file_sha256'];count+=n;byte_count+=b
  field=hashlib.sha256(''.join(sorted(hashes)).encode()).hexdigest();original=hashlib.sha256(''.join(sorted(inputs)).encode()).hexdigest();assert count==r['rows']==o['roles'][role]['rows'] and byte_count==r['bytes']==o['roles'][role]['utf8_jsonl_bytes'] and field==r['all_known_field_digest']==o['roles'][role]['all_field_multiset_digest'] and original==r['original_input_multiset_digest'];roles[role]={'rows':count,'bytes':byte_count,'all_known_field_digest':field,'original_input_multiset_digest':original}
 assert sum(r['bytes'] for r in roles.values())==a['source_bytes']<=a['bounds']['total_bytes'];a['audit']={'source_summary_sha256':hashlib.sha256((p/'summary.json').read_bytes()).hexdigest(),'roles':roles,'readback_wall_s':time.monotonic()-start,'qualification':'All local bytes/rows/fields/input hashes checked against independently generated oracle. Files remain mutable and outside Git; rehash before transfer. No native access/apply/publication or source arrival measurement.'};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'bytes':a['source_bytes'],'parts':a['parts'],'readback_wall_s':a['audit']['readback_wall_s']}))
if __name__=='__main__':main()
