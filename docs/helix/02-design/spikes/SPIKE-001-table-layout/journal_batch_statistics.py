"""Bounded r100 journal batch-statistics experiment; no new publication."""
import json
from pathlib import Path
from driver_sql import DriverClient

B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_journal_batch_statistics_20261007_r100'
assert not (O/'statements.jsonl').exists(), 'Inspect known handles and commits; do not blindly repeat metadata writes'
F='client_dev.ashlar_entropy_20261006_r86'
J=F+'.property_journal_r89'
c=DriverClient(O)
report={'state':'running statistics comparison','phases':[],
        'scope':'Existing 900k-row synthetic journal and immutable r99-b3 input; one metadata property change plus statistics recomputation. No new publication or freshness admission.'}
def run(label,statement):
    rows=c.sql(label,statement)
    report['phases'].append({'label':label,'caller_ms':c.records[-1]['wall_ms'],'result':rows})
    (O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    print(label,round(c.records[-1]['wall_ms']),flush=True)
    return rows
def detail(label):
    rows=run(label,'DESCRIBE DETAIL '+J)
    return dict(zip([x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']],rows[0]))
def history(label):
    rows=run(label,'DESCRIBE HISTORY '+J+' LIMIT 30')
    names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
    return [dict(zip(names,row)) for row in rows]
run('timeout-set','SET STATEMENT_TIMEOUT=180')
assert run('timeout-readback','SET STATEMENT_TIMEOUT')[0][-1]=='180'
assert run('cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
lineage=history('before-history')
old=int(lineage[0]['version'])
assert {int(v['version']) for v in lineage if int(v['version'])>=10}==set(range(10,old+1))
assert all(v['operation']=='OPTIMIZE' for v in lineage if int(v['version'])>10)
before=detail('before-detail')
properties=json.loads(before['properties'])
original=properties['delta.dataSkippingStatsColumns']
assert original=='source_feed,source_epoch,source_position,id'
report.update(before_version=old,before_detail=before)
assert run('before-identities',f'''SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_delivery_id,event_ordinal))
 FROM {J} VERSION AS OF {old}''')==[['900000','900000']]
prior=[json.loads(line) for line in (B/'out/native/ashlar_filtered_publication_20261007_r99/statements.jsonl').read_text().splitlines()]
query=next(r['sql'] for r in prior if r['label']=='journal-parity-r99-b3')
marker=J+' VERSION AS OF 10'
assert query.count(marker)==2
def parity(label,version):
    assert run(label,query.replace(marker,J+f' VERSION AS OF {version}'))==[['0']]
parity('baseline-parity',old)
run('add-batch-statistic',f"ALTER TABLE {J} SET TBLPROPERTIES ('delta.dataSkippingStatsColumns'='{original},apply_batch_id')")
run('recompute-delta-statistics',f'ANALYZE TABLE {J} COMPUTE DELTA STATISTICS')
after_lineage=history('after-history')
new=int(after_lineage[0]['version'])
assert new>old
recent=[v for v in after_lineage if int(v['version'])>old]
assert {int(v['version']) for v in recent}==set(range(old+1,new+1))
after=detail('after-detail')
assert json.loads(after['properties'])['delta.dataSkippingStatsColumns']==original+',apply_batch_id'
assert before['clusteringColumns']==after['clusteringColumns']
report.update(after_version=new,after_detail=after,commits=recent)
# Exact all-column equality, including nullable fields and lexical JSON text,
# at both pinned snapshots. EXCEPT ALL retains duplicate multiplicity.
assert run('full-journal-parity',f'''SELECT count(*) FROM
 ((SELECT * FROM {J} VERSION AS OF {old} EXCEPT ALL SELECT * FROM {J} VERSION AS OF {new})
 UNION ALL (SELECT * FROM {J} VERSION AS OF {new} EXCEPT ALL SELECT * FROM {J} VERSION AS OF {old}))''')==[['0']]
for i,versions in enumerate(((new,old),(old,new))):
    for v in versions: parity(f'paired-{i}-'+('new' if v==new else 'old'),v)
report['state']='completed journal batch statistics; exact full-row parity passed; final metrics pending'
(O/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
c.history();c.close()
