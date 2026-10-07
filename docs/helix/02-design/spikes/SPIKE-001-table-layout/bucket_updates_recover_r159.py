"""Recover completed pilot evidence after external canonical maintenance; no write replay."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_screen_r159';records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
assert all(r['response']['status']['state']=='SUCCEEDED' for r in records)
assert len([r for r in records if '-read-' in r['label']])==60
by={r['label']:r for r in records}
def data(label):return by[label]['response']['result']['data_array']
def detail(label):
 r=by[label];names=[x['name'] for x in r['response']['manifest']['schema']['columns']];return dict(zip(names,data(label)[0]))
owned={}
for name in ('lc','part'):
 d=detail(name+'-detail');pd=detail(name+'-post-detail')
 owned[name]={'table':d['name'],'id':d['id'],'version':int(data(name+'-post-version')[0][0]),'old_version':int(data(name+'-version')[0][0]),'detail':d,'post_detail':pd}
F='client_dev.ashlar_entropy_20261006_r86';Q=O/'canonical-lineage';assert not (Q/'statements.jsonl').exists(),'Inspect existing lineage handles'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
rows=c.sql('canonical-history','DESCRIBE HISTORY '+F+'.edge_current LIMIT 5');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,r)) for r in rows]
changes=[r for r in history if int(r['version'])>23];assert {int(r['version']) for r in changes}==set(range(24,int(history[0]['version'])+1))
assert all(r['operation']=='OPTIMIZE' for r in changes),'Other changes: retain outputs and inspect, no replay'
pinned=c.sql('published-vector',f"SELECT table_versions_json FROM {F}.publication_manifest_r89 WHERE publication_id='r139-b1'");assert len(pinned)==1
vector=json.loads(pinned[0][0]);assert vector[F+'.edge_current']==23 and vector[F+'.source_record_r89']==17 and vector[F+'.property_journal_r89']==19
assert c.sql('pinned-edge-count',f'SELECT count(*) FROM {F}.edge_current VERSION AS OF 23')==[['20000000']]
c.history();c.close()
s={'state':'Both matched-rowTracking100k layouts and bucket-aware updates exact;60 lookups passed; canonical head maintenance observed', 'owned':owned,'stage':F+'.bucket_stage_r159','distribution':data('bucket-distribution'),'canonical_history':history,'published_vector':vector,'head_assertion_failure':'Expected23, observed25 OPTIMIZE; original script stopped after60 successful reads before writing summary','qualification':'No writes replayed. Current canonical head advanced through external OPTIMIZE operations; unchanged published E23/R17/J19/r139-b1 verified.100k compact64-character replacements, no full20M layout, wide ingress, generic source authority, graph-engine or scale admission.'}
(O/'summary.json').write_text(json.dumps(s,indent=2)+'\n');print('Completed pilot recovered; published vector stillE23/R17/J19, current headOPTIMIZE lineage recorded')
