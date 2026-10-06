"""Bounded 10M-node stage of the actual proposed canonical object schema.
Existing compute only; 180s server statement cap, 15min preparation wall bound.
Payload pool is reused 10x and explicitly not billion-node admission.
"""
import json,re,time
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;schema='ashlar_scale_20261005_i1';F='client_dev.'+schema;out=BASE/'out/native'/schema;c=DriverClient(out);deadline=time.monotonic()+900
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar bounded 10M canonical-node schema stage; synthetic'")
c.sql('environment','SELECT current_version()')
# Actual CONTRACT-003 object fields and key clustering; smaller-file intervention.
text='\n'.join(x for x in (BASE/'sql/delta-candidate.sql').read_text().splitlines() if not x.lstrip().startswith('--'))
ddl=text.split(';')[0].strip().replace('CREATE TABLE object_current','CREATE TABLE '+F+'.object_current')
ddl=ddl.replace("'delta.dataSkippingStatsColumns'='source_system,type_id,id'", "'delta.dataSkippingStatsColumns'='source_system,type_id,id','delta.targetFileSize'='16777216'")
c.sql('canonical-ddl',ddl)
c.sql('populate',f"""INSERT INTO {F}.object_current
 SELECT 'pilot',cast(1 AS BIGINT),id+tile*1000000,to_json(array(id+tile*1000000)),
 'r1',cast(0 AS BIGINT),props_json,retained_json,cast(NULL AS BIGINT),'S','e',cast(0 AS BIGINT),current_timestamp()
 FROM client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3 CROSS JOIN range(0,10) t(tile)""")
print('Populated 10M canonical nodes',flush=True)
if time.monotonic()>deadline:raise RuntimeError('Preparation wall bound; stop before optimization')
c.sql('optimize',f'OPTIMIZE {F}.object_current FULL')
assert c.sql('identity-count',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {F}.object_current')==[['10000000','10000000','1','10000000']]
assert c.sql('full-carrier-parity',f"SELECT count(*) FROM {F}.object_current t JOIN client_dev.ashlar_ingest_20261005_a2.serving VERSION AS OF 3 s ON pmod(t.id-1,1000000)+1=s.id WHERE t.props_json<>s.props_json OR t.retained_json<>s.retained_json OR t.logical_key_json<>to_json(array(t.id)) OR t.source_system<>'pilot' OR t.type_id<>1 OR t.entity_version<>0")==[['0']]
c.sql('detail',f'DESCRIBE DETAIL {F}.object_current')
if time.monotonic()>deadline:raise RuntimeError('Preparation wall bound; stop before measured reads')
for rep in range(51):
 key=1+(rep*104729)%10000000
 rows=c.sql(f'lookup-{rep}',f"SELECT id,props_json,retained_json,logical_key_json FROM {F}.object_current WHERE source_system='pilot' AND type_id=1 AND id={key}")
 assert len(rows)==1 and rows[0][0]==str(key) and rows[0][3]=='['+str(key)+']'
 if rep%10==0:print('round',rep,flush=True)
c.history();c.close()
(out/'run.json').write_text(json.dumps({'state':'completed','nodes':10000000,'edges':0,'schema':F,'payload_pool':'fixed 1M source reused 10x; not independent high-entropy per node','scope':'actual canonical schema warm lookup and full stored-carrier equality; no billion-node/edge/concurrent/ingest admission','statement_timeout_s':180,'preparation_bound_s':900},indent=2)+'\n')
