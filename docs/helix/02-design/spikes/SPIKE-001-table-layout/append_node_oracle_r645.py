"""Full streaming local oracle for 8M new nodes and associated retained history."""
import hashlib
import json
import platform
import time
from pathlib import Path
from append_profile_r636 import AppendWorkload, OLD_NODES, NEW_NODES
from range_verification_r243 import oracle_chunks

BASE=Path(__file__).resolve().parent
OUT=BASE/'out/append-node-oracle-r645.json'
PROGRESS=BASE/'out/append-node-oracle-progress-r645.json'

def run():
    assert not OUT.exists() and not PROGRESS.exists(), 'Inspect existing local run before retry'
    package=json.loads((BASE/'layout-package-candidate.json').read_text())
    for name,digest in package['files'].items():
        assert hashlib.sha256((BASE/name).read_bytes()).hexdigest()==digest,name
    sources=['append_profile_r636.py','scale_mixed_r219.py','mixed_history_r215.py','range_verification_r243.py','mixed_change_queries_r230.py']
    started=time.monotonic()
    result={'state':'Generating independent full new-node extent oracle','start':OLD_NODES,'end':NEW_NODES,'width':100000,'python':platform.python_version(),'source_sha256':{n:hashlib.sha256((BASE/n).read_bytes()).hexdigest() for n in sources},'chunks':[],
            'bounds':{'wall_s':1800,'max_chunk_entities':100000},'qualification':'Every generated node/raw/property field contributes to a streaming sorted SHA256 multiset per100k entities; cryptographic collision assumption. Full8M append extent, not8M existing published-node revalidation, native physical parity, real Truss authority, full16M/80M graph, or performance admission. Memory bounded by one100k-entity chunk, not a global identity set. No native operations.'}
    for chunk in oracle_chunks(AppendWorkload(),'node',NEW_NODES,start=OLD_NODES):
        assert time.monotonic()-started<1800
        assert chunk['roles']['object_current']['rows']==100000
        assert chunk['roles']['source_record']['rows']==100000
        assert chunk['roles']['property_journal']['rows']==400000
        result['chunks'].append(chunk)
        result['wall_s']=time.monotonic()-started
        PROGRESS.write_text(json.dumps(result,indent=2)+'\n')
        if len(result['chunks'])%10==0:
            print(json.dumps({'complete_nodes':len(result['chunks'])*100000,'wall_s':result['wall_s']}),flush=True)
    assert len(result['chunks'])==80
    assert [c['start'] for c in result['chunks']]==list(range(OLD_NODES,NEW_NODES,100000))
    result['state']='Complete independent8M new-node oracle with8M raw records and32M property events'
    OUT.write_text(json.dumps(result,indent=2)+'\n')
    PROGRESS.unlink()
    print(json.dumps({'state':result['state'],'wall_s':result['wall_s']}),flush=True)
if __name__=='__main__':run()
