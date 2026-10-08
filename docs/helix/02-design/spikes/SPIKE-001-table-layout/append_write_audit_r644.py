"""Terminal receipt/cost audit for private physical calibration; no SQL submission."""
import hashlib
import json
from append_write_r643 import BASE, OUT, BOUNDS

def run():
    s=json.loads((OUT/'summary.json').read_text())
    records=[json.loads(x) for x in (OUT/'statements.jsonl').read_text().splitlines()]
    history=json.loads((OUT/'shared-history.json').read_text())
    queries={q['query_id']:q for q in history['queries']}
    assert len(records)==len(queries)==30 and not history['missing_ids']
    assert len({r['statement_id'] for r in records})==30
    assert s['state']=='Complete private append write calibration with all-role multiset parity'
    assert not (OUT/'live-statement.json').exists()
    totals={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
    for r in records:
        q=queries[r['statement_id']]
        assert q['query_text']==r['sql'] and q['is_final'] and q['status']=='FINISHED'
        assert r['response']['status']['state']=='SUCCEEDED'
        for k in totals:totals[k]+=q['metrics'][k]
    assert totals==s['costs'] and s['bounds']==BOUNDS
    assert all(totals[k]<=BOUNDS[k] for k in totals) and s['wall_s']<=BOUNDS['wall_s']
    assert len(s['tables'])==len(s['checks'])==7
    for key,t in s['tables'].items():
        create=next(r for r in records if r['label']=='create-'+key)
        parity=next(r for r in records if r['label']=='parity-'+key)
        assert create['sql'].startswith('CREATE TABLE '+t['table']+' USING DELTA CLUSTER BY ')
        assert "'delta.targetFileSize'='67108864'" in create['sql']
        assert t['version']==0 and int(t['commit']['version'])==0
        assert t['commit']['queryHistoryStatementId']==create['statement_id']
        assert t['detail']['id']==t['id'] and json.loads(t['detail']['partitionColumns'])==[]
        assert parity['response']['result']['data_array']==[[str(s['checks'][key]['rows']),'0','0']]
        assert queries[parity['statement_id']]['metrics']['result_from_cache'] is False
        assert 'EXCEPT ALL' in parity['sql'] and 'VERSION AS OF 0' in parity['sql']
    profiles={key:{'files':int(t['detail']['numFiles']),'bytes':int(t['detail']['sizeInBytes']),'rows':s['checks'][key]['rows']} for key,t in s['tables'].items()}
    a={'state':'All30 exact final native statements and seven private table version0 receipts audited','costs':totals,'profiles':profiles,'active_bytes':sum(p['bytes'] for p in profiles.values()),'wall_s':s['wall_s'],
       'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [OUT/'summary.json',OUT/'statements.jsonl',OUT/'shared-history.json']},
       'qualification':s['qualification']+' This audit verifies recorded statements/results/commit attribution, not independent whole-range generator correctness or a later physical-head observation. Cancellation/budget guards are observations, not hard billing caps.'}
    (OUT/'audited-summary-r644.json').write_text(json.dumps(a,indent=2)+'\n')
    print(json.dumps(a,indent=2))
if __name__=='__main__':run()
