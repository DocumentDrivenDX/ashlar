"""10M mixed-domain fixture; derived hash is pruning only, never identity."""
import hashlib,json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_20261005_p1r1';out=B/'out/native/ashlar_composite_20261005_p1r1';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');c.sql('environment','SELECT current_version()');c.sql('schema',f"CREATE SCHEMA {F} COMMENT 'Ashlar synthetic mixed-domain physical-key experiment'")
expr="sha2(to_json(named_struct('source_system',source_system,'type_id',type_id,'id',id)),256)"
def key(source,type_id,id):return hashlib.sha256(json.dumps(dict(source_system=source,type_id=type_id,id=id),ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
def literal(s):return "decode(unhex('"+s.encode('utf-8').hex()+"'),'UTF-8')"
# Independent native/Python derivation at signed64 and source-text boundaries.
for n,(source,typ,id) in enumerate([('pilot:0',1,1),('pilot0',1,1),('α',1,-1),("O'Reilly",2,1),('',1,0),('a|b',1,9223372036854775807),('a:b',2,-9223372036854775808)]):
 rows=c.sql(f'key-control-{n}',f"SELECT {expr} FROM (SELECT {literal(source)} source_system,cast({typ} AS BIGINT) type_id,cast('{id}' AS BIGINT) id)")
 assert rows==[[key(source,typ,id)]]
# Forced hash collision does not merge or misroute exact source/type identity.
assert c.sql('forced-collision',"SELECT count(*) FROM VALUES ('a',1L,1L,'forced'),('b',1L,1L,'forced'),('a',2L,1L,'forced') AS t(source_system,type_id,id,lookup_hash) WHERE lookup_hash='forced' AND source_system='a' AND type_id=1 AND id=1")==[['1']]
ddl='\n'.join(x for x in (B/'sql/delta-candidate.sql').read_text().splitlines() if not x.lstrip().startswith('--')).split(';')[0].strip()
ddl=ddl.replace('CREATE TABLE object_current','CREATE TABLE '+F+'.object_current').replace('published_at TIMESTAMP NOT NULL','published_at TIMESTAMP NOT NULL,lookup_hash STRING NOT NULL').replace('CLUSTER BY (source_system, type_id, id)','CLUSTER BY (lookup_hash)').replace("'source_system,type_id,id'","'lookup_hash,source_system,type_id,id'")
assert ddl.endswith(')');ddl=ddl[:-1]+",'delta.targetFileSize'='16777216','delta.feature.catalogManaged'='supported')"
c.sql('ddl',ddl)
c.sql('populate',f"""INSERT INTO {F}.object_current SELECT *,{expr} FROM (SELECT concat('pilot:',cast(d.domain DIV 2 AS STRING)) source_system,cast(1+d.domain%2 AS BIGINT) type_id,o.id,o.logical_key_json,o.schema_revision,o.entity_version,o.props_json,o.retained_json,o.root_id,concat('synthetic-domain-',cast(d.domain AS STRING)) source_feed,o.source_epoch,o.source_position,o.published_at FROM client_dev.ashlar_scale_20261005_i1.object_current VERSION AS OF 2 o CROSS JOIN range(10) d(domain) WHERE o.id<=1000000)""")
c.sql('cluster-baseline',f'OPTIMIZE {F}.object_current FULL')
v=int(c.sql('version',f'DESCRIBE HISTORY {F}.object_current LIMIT 1')[0][0]);c.sql('detail',f'DESCRIBE DETAIL {F}.object_current')
assert c.sql('identity-and-key-integrity',f'SELECT count(*),count(DISTINCT struct(source_system,type_id,id)),count(DISTINCT id),count_if(lookup_hash IS DISTINCT FROM {expr}) FROM {F}.object_current VERSION AS OF {v}')==[['10000000','10000000','1000000','0']]
assert c.sql('exact-carriers',f"SELECT count(*),count_if(o.props_json IS DISTINCT FROM b.props_json OR o.retained_json IS DISTINCT FROM b.retained_json OR o.logical_key_json IS DISTINCT FROM b.logical_key_json OR o.schema_revision IS DISTINCT FROM b.schema_revision) FROM {F}.object_current VERSION AS OF {v} o JOIN client_dev.ashlar_scale_20261005_i1.object_current VERSION AS OF 2 b ON o.id=b.id WHERE b.id<=1000000")==[['10000000','0']]
expected={}
for phase in ['prime','repeat','repeat2']:
 for rep in range(51):
  domain=rep%10;source='pilot:'+str(domain//2);typ=1+domain%2;id=1+(rep*196613)%1000000;hash=key(source,typ,id)
  rows=c.sql(f'{phase}-{rep}',f"SELECT source_system,type_id,id,props_json,retained_json,logical_key_json,lookup_hash FROM {F}.object_current VERSION AS OF {v} WHERE lookup_hash='{hash}' AND source_system='{source}' AND type_id={typ} AND id={id}")
  assert len(rows)==1 and rows[0][:3]==[source,str(typ),str(id)] and rows[0][5:]==['['+str(id)+']',hash]
  if phase=='prime':expected[rep]=rows
  else:assert rows==expected[rep]
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps(dict(state='completed',object_version=v,nodes=10000000,sources=5,types_per_source=2,distinct_native_ids=1000000,physical_key='SHA256 of fixed-order exact source/type/id JSON; native tuple remains identity and mandatory lookup filter',preservation='all 10M derived hashes and typed identities; all 10M property/retained/logical-key/schema carriers against fixed source; seven signed64/text derivation controls and forced-collision filtering',scope='synthetic fixed snapshot only; no mutation/ingest/concurrent read/billion-scale or external-engine admission'),indent=2)+'\n');print('Composite physical-key baseline completed',flush=True)
