"""Full streaming local oracle for next8M new edges and associated retained history."""
import hashlib
import json
import platform
import time
from pathlib import Path
from append_profile_r636 import AppendWorkload, OLD_EDGES, NEW_EDGES
from range_verification_r243 import oracle_chunks

BASE=Path(__file__).resolve().parent
OUT=BASE/'out/append-edge-oracle-r680.json'
PROGRESS=BASE/'out/append-edge-oracle-progress-r680.json'

def run():
    assert not OUT.exists() and not PROGRESS.exists(), 'Inspect existing local run before retry'
    package=json.loads((BASE/'layout-package-candidate.json').read_text())
    for name,digest in package['files'].items():
        assert hashlib.sha256((BASE/name).read_bytes()).hexdigest()==digest,name
    sources=['append_profile_r636.py','scale_mixed_r219.py','mixed_history_r215.py','range_verification_r243.py','mixed_change_queries_r230.py']
    started=time.monotonic()
    result={'state':'Generating independent next8M new-edge extent oracle','start':48000000,'end':56000000,'width':100000,'python':platform.python_version(),'source_sha256':{n:hashlib.sha256((BASE/n).read_bytes()).hexdigest() for n in sources},'chunks':[],
            'bounds':{'wall_s':1800,'max_chunk_entities':100000},'qualification':'Every generated node/raw/property field contributes to a streaming sorted SHA256 multiset per100k entities; cryptographic collision assumption. First8M edge append extent, not existing published-edge revalidation, native physical parity, real Truss authority, full16M/80M graph, or performance admission. Memory bounded by one100k-entity chunk, not a global identity set. No native operations.'}
    for chunk in oracle_chunks(AppendWorkload(),'edge',56000000,start=48000000):
        assert time.monotonic()-started<1800
        assert chunk['roles']['edge_current']['rows']==100000
        assert chunk['roles']['adjacency_forward']['rows']==100000
        assert chunk['roles']['source_record']['rows']==100000
        assert chunk['roles']['property_journal']['rows']==400000
        result['chunks'].append(chunk)
        result['wall_s']=time.monotonic()-started
        PROGRESS.write_text(json.dumps(result,indent=2)+'\n')
        if len(result['chunks'])%10==0:
            print(json.dumps({'complete_edges':len(result['chunks'])*100000,'wall_s':result['wall_s']}),flush=True)
    assert len(result['chunks'])==80
    assert [c['start'] for c in result['chunks']]==list(range(48000000,56000000,100000))
    result['state']='Complete independent next8M new-edge oracle with8M raw records32M property events and8M adjacency records'
    OUT.write_text(json.dumps(result,indent=2)+'\n')
    PROGRESS.unlink()
    print(json.dumps({'state':result['state'],'wall_s':result['wall_s']}),flush=True)
if __name__=='__main__':run()
