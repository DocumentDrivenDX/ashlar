"""Bounded100k complete-carrier native LC versus64 hash-bucket/Z-order pilot."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,TEXTS,PropertyApply
from bucket_apply_queries import BUCKET_SQL,bucket_apply
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_screen_r161'
assert not (O/'statements.jsonl').exists(),'Inspect prior native handle; no replay'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.schedule_r139_1';L=F+'.bucket_lc_r161';P=F+'.bucket_part_r161'
c=BoundedReads(O)
import time
deadline=time.monotonic()+600
original_sql=c.sql
def bounded_sql(label,statement,parameters=None,tag=True):
 assert time.monotonic()<deadline,'Controller admission deadline reached; inspect saved handles'
 return original_sql(label,statement,parameters=parameters,tag=tag)
c.sql=bounded_sql
c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('absence',f"SHOW TABLES IN {F} LIKE 'bucket_*_r161'")==[]
bucket="cast(pmod(cast(conv(substr(lookup_hash,1,15),16,10) AS BIGINT),64) AS INT)"
columns=','.join(COLS)
base=f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0"
props="'delta.enableRowTracking'='true','delta.parquet.compression.codec'='zstd','delta.targetFileSize'='67108864','delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id,entity_version,apply_batch_id'"
E=F+'.edge_current'
c.sql('lc-create',f'CREATE TABLE {L} SHALLOW CLONE {E} VERSION AS OF 23')
c.sql('lc-settings',f"ALTER TABLE {L} SET TBLPROPERTIES ('delta.targetFileSize'='67108864')")
c.sql('part-create',f'CREATE TABLE {P} USING DELTA PARTITIONED BY (lookup_bucket) TBLPROPERTIES ({props}) AS SELECT {columns},{bucket} lookup_bucket FROM {E} VERSION AS OF 23')
cost_rows=c.sql('part-cost-detail','DESCRIBE DETAIL '+P)
cost_names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
copy_bytes=int(dict(zip(cost_names,cost_rows[0]))['sizeInBytes'])
assert copy_bytes<=36000000000 and 2*copy_bytes+1400000000<=75000000000,'Stop: copy invalidates new-write budget'
c.sql('part-zorder','OPTIMIZE '+P+' ZORDER BY (lookup_hash)')
owned={}
fields=','.join(f"hex(encode({col},'UTF-8')) AS {col}" if col in TEXTS else col for col in COLS)
for name,t in [('lc',L),('part',P)]:
 version=int(c.sql(name+'-version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0])
 assert c.sql(name+'-membership',f'SELECT count(*),count(DISTINCT id) FROM {t} VERSION AS OF {version}')==[['20000000','20000000']]
 x=f'SELECT {fields} FROM {S} VERSION AS OF 0';y=f'SELECT {fields} FROM {t} VERSION AS OF {version}'
 actual_slice=f"SELECT {fields} FROM {t} VERSION AS OF {version} WHERE entity_version=15 AND apply_batch_id='r139-b1'"
 assert c.sql(name+'-exact',f'SELECT count(*) FROM (({x} EXCEPT ALL {actual_slice}) UNION ALL ({actual_slice} EXCEPT ALL {x}))')==[['0']]
 structural='source_system,rel_type_id,id,source_type,source_id,target_type,target_id'
 old=f'SELECT {structural} FROM {E} VERSION AS OF 23';new=f'SELECT {structural} FROM {t} VERSION AS OF {version}'
 assert c.sql(name+'-structural',f'SELECT count(*) FROM (({old} EXCEPT ALL {new}) UNION ALL ({new} EXCEPT ALL {old}))')==[['0']]
 rows=c.sql(name+'-detail','DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 detail=dict(zip(names,rows[0]));owned[name]={'table':t,'id':detail['id'],'version':version,'detail':detail}
# One immutable100k new property stage; all target rows update, full exact outputs checked.
U=F+'.bucket_stage_r161'
assert c.sql('stage-absence',f"SHOW TABLES IN {F} LIKE 'bucket_stage_r161'")==[]
overrides={'entity_version':'entity_version+1','props_json':"replace(props_json,concat(char(34),'105',char(34),':',char(34),old_opaque,char(34)),concat(char(34),'105',char(34),':',char(34),new_opaque,char(34)))",'source_epoch':"'bucket-r161'",'apply_batch_id':"'r161-b1'",'published_at':'current_timestamp()', 'source_delivery_id':"concat('r161-b1:edge:',cast(id AS STRING))",'source_cursor_json':"concat('{',char(34),'xid',char(34),':',char(34),'9007199254741501',char(34),',',char(34),'seq',char(34),':',char(34),cast(id AS STRING),char(34),'}')"}
c.sql('stage-create',f"CREATE TABLE {U} USING DELTA AS SELECT "+','.join(overrides.get(col,col)+' AS '+col for col in COLS)+",concat(char(34),old_opaque,char(34)) old_json FROM (SELECT *,get_json_object(props_json,'$.105') old_opaque,concat_ws('',transform(sequence(0,cast(length(get_json_object(props_json,'$.105'))/64 AS INT)-1),block ->sha2(concat('r161:',cast(id AS STRING),':',cast(block AS STRING)),256))) new_opaque FROM ("+base+'))')
assert c.sql('stage-membership',f'SELECT count(*),count(DISTINCT id) FROM {U} VERSION AS OF 0')==[['100000','100000']]
for name,t in [('lc',L),('part',P)]:
 q=PropertyApply(t,U,owned[name]['version'],15,'r161-b1','r139-b1','bucket-r161',9007199254741501,eligibility_placement='on')
 assert c.sql(name+'-intended',q.intended())==[['0']]
 c.sql(name+'-apply',q.apply() if name=='lc' else bucket_apply(q))
 v=int(c.sql(name+'-post-version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0]);assert v>owned[name]['version']
 assert c.sql(name+'-output',q.output(v))==[['0']]
 assert c.sql(name+'-post-membership',f'SELECT count(*),count(DISTINCT id) FROM {t} VERSION AS OF {v}')==[['20000000','20000000']]
 rows=c.sql(name+'-post-detail','DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 detail=dict(zip(names,rows[0]));assert 'rowTracking' in json.loads(detail['tableFeatures']) and json.loads(detail['properties'])['delta.enableRowTracking']=='true'
 owned[name]['old_version']=owned[name]['version'];owned[name]['version']=v;owned[name]['post_detail']=detail
assert c.sql('bucket-drift',f'SELECT count(*) FROM {P} VERSION AS OF {owned["part"]["version"]} WHERE lookup_bucket IS NULL OR lookup_bucket<>({bucket}) OR lookup_bucket<0 OR lookup_bucket>=64')==[['0']]
distribution=c.sql('bucket-distribution',f'SELECT lookup_bucket,count(*) FROM {P} VERSION AS OF {owned["part"]["version"]} GROUP BY lookup_bucket ORDER BY lookup_bucket');assert len(distribution)==64
# Thirty alternating complete exact lookups per layout, tuple remains identity.
oracle=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {U} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30")
for i,row in enumerate(oracle):
 for name in (('lc','part') if i%2==0 else ('part','lc')):
  q=f"SELECT {','.join(COLS)} FROM {owned[name]['table']} VERSION AS OF {owned[name]['version']} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
  params={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]}
  if name=='part':q+=' AND lookup_bucket=CAST(:bucket AS INT)';params['bucket']=int(row[16][:15],16)%64
  assert c.sql(name+'-read-'+str(i),q,parameters=params)==[row]
head=c.sql('canonical-head',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')
pinned=c.sql('published-vector',f"SELECT table_versions_json FROM {F}.publication_manifest_r89 WHERE publication_id='r139-b1'")
assert len(pinned)==1 and json.loads(pinned[0][0])[F+'.edge_current']==23
assert c.sql('payload-length-drift',f"SELECT count(*) FROM {U} VERSION AS OF 0 WHERE length(old_json)-2<>length(get_json_object(props_json,'$.105')) OR (length(old_json)-2)%64<>0")==[['0']]
(O/'summary.json').write_text(json.dumps({'state':'20M physical layouts and100k exact hot-set updates;60 lookups passed; final native metrics pending','owned':owned,'stage':U,'canonical_head_observed':head[0][0],'published_vector':json.loads(pinned[0][0]),'distribution':distribution,'bucket_derivation':'unsigned first60 SHA256 bits modulo64; signed64 conversion is safe below2^60','qualification':'100k r139 hot-set pilot only; one100k complete hot-set update only; no20M full layout, generic source authority, graph-engine, cold/service/scale admission. Partition count64 is a bounded comparison, not a billion-scale choice. Full native tuple remains identity; derived bucket is metadata.'},indent=2)+'\n')
c.history();c.close();print('64-bucket pilot exact carriers, distribution and60 lookups passed')
