"""Plan-only range-input MERGE probe; executes no mutation."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_range_plan_r186';assert not O.exists()
u=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text());T=u['owned']['lc']['table'];S=u['stage'];c=BoundedReads(O)
c.sql('timeout','SET STATEMENT_TIMEOUT=30')
p=f"SELECT /*+ REPARTITION_BY_RANGE(6,lookup_hash) */ {','.join(COLS)} FROM {S} VERSION AS OF 0"
for label,q in [('input','EXPLAIN FORMATTED '+p),('merge',f"EXPLAIN FORMATTED MERGE INTO {T} t USING ({p}) s ON t.lookup_hash=s.lookup_hash AND t.source_system=s.source_system AND t.rel_type_id=s.rel_type_id AND t.id=s.id AND t.entity_version=15 AND t.apply_batch_id='r139-b1' WHEN MATCHED THEN UPDATE SET *")]:
 rows=c.sql(label,q);(O/(label+'-plan.txt')).write_text('\n'.join(str(r[0]) for r in rows)+'\n')
assert c.sql('final-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1'
c.close()
for attempt in range(8):
 h={x['query_id']:x for x in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 time.sleep(2)
assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
(O/'summary.json').write_text(json.dumps({'state':'Plan-only probe completed; inspect plans for actual range exchange','table':T,'version':1,'costs':{k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']},'qualification':'EXPLAIN only, no MERGE execution. Input range partition does not guarantee final Delta writer layout; optimizer/runtime may repartition. Source and old eligibility remain owned profile, not producer authority.'},indent=2)+'\n')
print((O/'input-plan.txt').read_text()[:1600]);print((O/'merge-plan.txt').read_text()[:2000])
