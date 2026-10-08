"""Validate complete local oracle receipt coverage and source provenance."""
import hashlib
import json
from append_edge_oracle_r680 import BASE, OUT, PROGRESS

def run():
    s=json.loads(OUT.read_text())
    assert not PROGRESS.exists()
    assert s['state']=='Complete independent next8M new-edge oracle with8M raw records32M property events and8M adjacency records'
    assert s['start']==48000000 and s['end']==56000000 and s['width']==100000
    assert len(s['chunks'])==80 and s['wall_s']<1800
    for name,h in s['source_sha256'].items():
        assert hashlib.sha256((BASE/name).read_bytes()).hexdigest()==h
    totals={'edge_current':0,'source_record':0,'property_journal':0,'adjacency_forward':0}
    for i,chunk in enumerate(s['chunks']):
        assert chunk['start']==48000000+i*100000 and chunk['end']==chunk['start']+100000
        assert set(chunk['roles'])==set(totals)
        for role,entry in chunk['roles'].items():
            assert entry['rows']==(400000 if role=='property_journal' else 100000)
            assert entry['fields']==s['chunks'][0]['roles'][role]['fields']
            assert len(entry['digest'])==64 and all(c in '0123456789abcdef' for c in entry['digest'])
            totals[role]+=entry['rows']
    result={'state':'Complete80-chunk local oracle coverage and source provenance validated','rows':totals,'oracle_sha256':hashlib.sha256(OUT.read_bytes()).hexdigest(),'wall_s':s['wall_s'],'qualification':'Receipt coverage audit, not a second independent regeneration. Every generated field was hashed by the terminal streaming run; native physical comparison remains pending. No native writes, table growth or performance admission.'}
    (BASE/'out/append-edge-oracle-audit-r685.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
if __name__=='__main__':run()
