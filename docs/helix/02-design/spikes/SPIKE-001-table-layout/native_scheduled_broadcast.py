"""Four scattered 300k/30s arrivals; overlapping staging, fixed freshness clock."""
import json,re,time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_scattered_20261005_n1'
out=B/'out/native/ashlar_scheduled_broadcast_20261005_n4';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('environment','SELECT current_version()')
before=int(c.sql('before-version',f'DESCRIBE HISTORY {F}.object_current LIMIT 1')[0][0]);c.sql('before-detail',f'DESCRIBE DETAIL {F}.object_current')
records=[json.loads(x) for x in (B/'out/native/ashlar_broadcast_validation_20261005_n3/statements.jsonl').read_text().splitlines()]
template=next(x['sql'] for x in records if x['label']=='atomic-apply')
def atomic(pos):
 s=template.replace('stage_n3',f'stage_n4_{pos}').replace('200000','300000')
 for a,b in [('IS DISTINCT FROM 3',f'IS DISTINCT FROM {pos}'),('entity_version=3',f'entity_version={pos}'),('source_position=3',f'source_position={pos}'),('1,id,103,3,',f'1,id,103,{pos},'),("'S','e',3,id",f"'S','e',{pos},id"),('synthetic-batch:3',f'synthetic-batch:{pos}'),('VALUES (3,',f'VALUES ({pos},')]:s=s.replace(a,b)
 return s
for i,pos in enumerate(range(4,8)):
 lo=600000+i*300000;hi=lo+300000
 payload='concat('+','.join(f"sha2(concat(cast(id AS STRING),':scheduled:{pos}:{k}'),256)" for k in range(32))+')'
 c.sql(f'producer-{pos}',f"CREATE TABLE {F}.producer_n4_{pos} USING DELTA AS SELECT *,to_json(named_struct('101',concat('g',cast(id%100 AS STRING)),'102',id%1000,'103',{payload})) new_props FROM {F}.object_current VERSION AS OF {before} WHERE pmod((id-1)*104729,10000000)>={lo} AND pmod((id-1)*104729,10000000)<{hi}")
 assert c.sql(f'producer-keys-{pos}',f'SELECT count(*),count(DISTINCT id),count(DISTINCT floor((id-1)/100000)) FROM {F}.producer_n4_{pos}')==[['300000','300000','100']]
stager=DriverClient(out/'staging');stager.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
pool=ThreadPoolExecutor(max_workers=1);t0=time.time();results=[]
def stage(i,pos):
 ready=t0+(i+1)*30
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 stager.sql(f'stage-{pos}',f'CREATE TABLE {F}.stage_n4_{pos} USING DELTA AS SELECT * FROM {F}.producer_n4_{pos}')
 return time.time()
futures={pos:pool.submit(stage,i,pos) for i,pos in enumerate(range(4,8))}
(out/'schedule.json').write_text(json.dumps(dict(start_epoch=t0,interval_s=30,batch_entities=300000,batches=4,modeled_entities_s=10000,before_version=before,producer_generation='excluded',staging='one independent driver, overlaps serial publisher',identity_schedule='four disjoint modular 300k sets spanning full 10M-node range',scope='bounded two-minute schedule, not sustained or population p95 admission'),indent=2)+'\n')
for i,pos in enumerate(range(4,8)):
 ready=t0+(i+1)*30;oldest=t0+i*30
 while time.time()<ready:time.sleep(min(.5,ready-time.time()))
 begun=time.time();staged=futures[pos].result();c.sql(f'atomic-apply-{pos}',atomic(pos))
 v={t:int(c.sql(f'version-{t}-{pos}',f'DESCRIBE HISTORY {F}.{t} LIMIT 1')[0][0]) for t in ['object_current','property_journal']}
 c.sql(f'publish-{pos}',f"INSERT INTO {F}.publication_manifest VALUES ('batch-{pos}-scheduled-broadcast','ashlar-delta/0.1-spike','{json.dumps(v,separators=(',',':'))}','{{\"S\":{{\"epoch\":\"e\",\"position\":{pos}}}}}','{{\"pilot\":\"r1\"}}','{{\"transaction_checks\":\"passed\",\"changed_objects\":300000}}',current_timestamp())")
 end=time.time();r=dict(position=pos,entities=300000,versions=v,ready_epoch=ready,start_epoch=begun,end_epoch=end,queue_delay_s=begun-ready,staging_completed_epoch=staged,processing_s=end-begun,oldest_freshness_s=end-oldest,newest_freshness_s=end-ready);results.append(r)
 (out/'summary.json').write_text(json.dumps(dict(state='publishing',before_version=before,batches=results),indent=2)+'\n');print(json.dumps(r),flush=True)
 if end-ready>120:raise RuntimeError('Bounded backlog exceeded; inspect completed publications; no write retry')
pool.shutdown(wait=True);stager.close();c.records.extend(stager.records)
# Independent fixed-vector guard evaluation for each completed publication.
for r in results:
 pos=r['position'];v=r['versions'];queries=re.findall(r'IF \((SELECT.*?)\)(?:<>300000)? THEN SIGNAL',atomic(pos),re.S);assert len(queries)==3
 q=queries[1].replace(F+'.object_current o',f"{F}.object_current VERSION AS OF {v['object_current']} o").replace(F+'.property_journal j',f"{F}.property_journal VERSION AS OF {v['property_journal']} j")
 rows=c.sql(f'fixed-vector-validation-{pos}',q);assert rows[0][0].lower()=='false'
cols=['source_system','type_id','id','logical_key_json','schema_revision','entity_version','props_json','retained_json','root_id','source_feed','source_epoch','source_position','published_at'];diff=' OR '.join(f'o.{x} IS DISTINCT FROM n.{x}' for x in cols)
last=results[-1]['versions']
assert c.sql('final-untouched-all-carriers',f"SELECT count(*),count_if({diff}) FROM {F}.object_current VERSION AS OF {last['object_current']} o JOIN {F}.object_current VERSION AS OF {before} n ON o.source_system=n.source_system AND o.type_id=n.type_id AND o.id=n.id WHERE pmod((o.id-1)*104729,10000000)<600000 OR pmod((o.id-1)*104729,10000000)>=1800000")==[['8800000','0']]
assert c.sql('final-canonical-identities',f"SELECT count(*),count(DISTINCT struct(source_system,type_id,id)) FROM {F}.object_current VERSION AS OF {last['object_current']}")==[['10000000','10000000']]
assert c.sql('all-journal-event-keys',f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {F}.property_journal VERSION AS OF {last['property_journal']} WHERE source_feed='S' AND source_epoch='e' AND source_position BETWEEN 4 AND 7")==[['1200000','1200000']]
c.sql('mutation-history',f'DESCRIBE HISTORY {F}.object_current');c.sql('after-detail',f'DESCRIBE DETAIL {F}.object_current')
(out/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n');c.history();c.close()
(out/'summary.json').write_text(json.dumps(dict(state='completed',before_version=before,batches=results,preservation='all four fixed-vector changed/journal guards; final 8.8M untouched rows all 13 fields; 10M distinct canonical keys; 1.2M distinct journal events',scope='four 300k batches/30s; two-minute modeled 10k/s; no concurrent readers, burst, native feed, replay/recovery or billion-scale admission'),indent=2)+'\n');print('Scheduled broadcast probe completed',flush=True)
