"""Supplemental post-publication lexical qualification of r94's ASCII fixture.

No generic token extractor claim: escaped/pretty source syntax is out of profile.
No writes and no changes to the recorded arrival-to-manifest measurement.
"""
import json
from pathlib import Path
from persistent_sql import Client

O = Path(__file__).resolve().parent/'out/native/ashlar_entropy_wide_journal_20261006_r94/token-qualification'
c = Client(O,observation_timeout=960,cancel_after=900)
F = 'client_dev.ashlar_entropy_20261006_r86'
records_path=O/'statements.jsonl'
previous=[json.loads(line) for line in records_path.read_text().splitlines()] if records_path.exists() else []
c.records=previous
completed=next((r for r in previous if r['label']=='exact-old-token-member-and-changed-value' and r['response']['status']['state']=='SUCCEEDED'),None)
if completed:
    assert completed['response']['result']['data_array']==[['0']]
else:
    assert c.sql('exact-old-token-member-and-changed-value',f'''SELECT count(*) FROM
 (SELECT source_system,rel_type_id,id,props_json FROM {F}.edge_current VERSION AS OF 2
 WHERE entity_version=1 AND apply_batch_id='r89' AND pmod(id,8)>=4) b
 FULL OUTER JOIN {F}.publication_stage_r94 VERSION AS OF 0 s
 ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id
 WHERE b.id IS NULL OR s.id IS NULL OR instr(b.props_json,concat('"105":',s.old_json))=0
 OR s.old_json=concat('"',get_json_object(s.props_json,'$.105'),'"')''')==[['0']]

def lit(value):
    return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"

cases = [('json compact source','{"105":"abc"}',True),
         ('escaped source','{"105":"\\u0061bc"}',False),
         ('pretty source','{"105": "abc"}',False)]
values = ' UNION ALL '.join('SELECT '+lit(label)+' label,'+lit(source)+' source,'+('true' if expected else 'false')+' expected' for label,source,expected in cases)
negative_done=next((r for r in previous if r['label']=='outside-profile-lexical-negative-controls' and r['response']['status']['state']=='SUCCEEDED'),None)
if negative_done:
    assert negative_done['response']['result']['data_array']==[['0']]
else:
    assert c.sql('outside-profile-lexical-negative-controls',f'''SELECT count(*) FROM ({values}) AS cases
 WHERE (instr(source,concat('"105":',{lit('"abc"')}))>0)<>expected''')==[['0']]
history = {q['query_id']:q for q in c.history()}
assert all(history.get(r['statement_id'],{}).get('is_final') for r in c.records)
assert all(not r['cancel_requested'] for r in c.records)
assert all(history[r['statement_id']]['metrics'].get('result_from_cache') is False for r in c.records if r['response']['status']['state']=='SUCCEEDED')
(O/'summary.json').write_text(json.dumps({'state':'passed post-publication exact-token qualification and escaped/pretty syntax negatives',
    'rows':100000,'initial_negative_query':'Inline VALUES rejected computed literals; corrected SELECT UNION ALL; failed read retained','scope':'Known compact ASCII-hex fixture only; outside-profile source syntax rejected, not normalized. Supplement after publication; excluded from 78s arrival measurement.',
    'phases':[{'label':r['label'],'caller_ms':r['wall_ms'],'state':r['response']['status']['state'],'metrics':history[r['statement_id']].get('metrics')} for r in c.records]},indent=2)+'\n')
