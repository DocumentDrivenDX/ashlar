"""Bounded exact-fixture hash occupancy; no runtime speed inference."""
import collections,hashlib,json,time
from pathlib import Path
from fourth_changes_r427 import FourthChanges
B=Path(__file__).resolve().parent
start=time.monotonic();changes=FourthChanges();hashes=[changes.change(i)['before']['lookup_hash'] for i in range(100000)];assert len(set(hashes))==100000
out={'state':'Exact fourth100k hash range occupancy measured','source_sha256':hashlib.sha256((B/'fourth_changes_r427.py').read_bytes()).hexdigest(),'hash_sequence_sha256':hashlib.sha256(''.join(hashes).encode()).hexdigest(),'rows':100000,'ranges':[],'qualification':'Synthetic exact source hashes; uniform byte ranges are not actual Delta file boundaries. Occupancy is evidence about batching, not measured scan bytes, MERGE speed or freshness.'}
for bits in [4,6,8,10,12,16]:
 buckets=[int(h[:8],16)>>(32-bits) for h in hashes];counts=collections.Counter(buckets);entry={'bits':bits,'ranges':2**bits,'occupied':len(counts),'occupied_fraction':len(counts)/2**bits,'min_occupied_rows':min(counts.values()),'max_rows':max(counts.values()),'microbatches':[]}
 for n in [100,1000,10000]:
  occupied=[len(set(buckets[i:i+n])) for i in range(0,len(buckets),n)];entry['microbatches'].append({'size':n,'count':len(occupied),'sum_occupied_ranges':sum(occupied),'mean_occupied_fraction':sum(occupied)/len(occupied)/2**bits,'qualification':'Range scans independently per microbatch can revisit targets; sum measures requests, not actual files.'})
 out['ranges'].append(entry)
out['wall_s']=time.monotonic()-start;assert out['wall_s']<180
p=B/'out/hash-range-occupancy-r449.json';assert not p.exists();p.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
