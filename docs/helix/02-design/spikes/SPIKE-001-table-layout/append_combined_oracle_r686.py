"""Combine two terminal independent local edge ranges without regenerating them."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent
paths=['out/append-edge-oracle-r656.json','out/append-edge-oracle-r680.json']
first,second=[json.loads((B/p).read_text()) for p in paths]
assert first['start']==40000000 and first['end']==second['start']==48000000 and second['end']==56000000
assert len(first['chunks'])==len(second['chunks'])==80
assert first['source_sha256']==second['source_sha256']
for name,h in first['source_sha256'].items():assert hashlib.sha256((B/name).read_bytes()).hexdigest()==h
chunks=first['chunks']+second['chunks'];counts={k:0 for k in ['edge_current','source_record','property_journal','adjacency_forward']}
for i,chunk in enumerate(chunks):
 assert chunk['start']==40000000+i*100000 and chunk['end']==chunk['start']+100000
 assert set(chunk['roles'])==set(counts)
 for role,entry in chunk['roles'].items():
  assert entry['fields']==chunks[0]['roles'][role]['fields']
  assert entry['rows']==(400000 if role=='property_journal' else 100000)
  counts[role]+=entry['rows']
result={'state':'Complete160-chunk independent16M new-edge local oracle coverage','start':40000000,'end':56000000,'chunks':chunks,'counts':counts,'sources':{p:hashlib.sha256((B/p).read_bytes()).hexdigest() for p in paths},'qualification':'Composition of two complete independently generated local ranges with identical source recipes; receipt/coverage validation, not a third regeneration. Every field included in source multiset digests; SHA256 collision assumption. No native16M-edge writes or physical/endpoint/performance admission.'}
(B/'out/combined-edge-oracle-r686.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'state':result['state'],'counts':counts},indent=2))
