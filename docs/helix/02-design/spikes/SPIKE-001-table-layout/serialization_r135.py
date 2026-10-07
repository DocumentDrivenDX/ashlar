"""Read-only forced serialization versus immutable stored-wire aggregates."""
import json
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import COLS
from wire_json import encode
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_serialization_r135'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';S=F+'.schedule_r133_1';R=F+'.source_record_r89'
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
payload=encode('named_struct('+','.join("'"+col+"',"+col for col in list(COLS)+['old_json'])+')')
queries={'encoded':f'SELECT {payload} payload FROM {S} VERSION AS OF 0',
 'stored':f"SELECT payload_json payload FROM {R} VERSION AS OF 16 WHERE apply_batch_id='r133-b1'"}
results=[]
for i in range(3):
 for mode in (('encoded','stored') if i%2==0 else ('stored','encoded')):
  # Digest aggregates force consumption of all payload bytes; not an exact equality proof.
  rows=c.sql(f'{mode}-{i}',f"SELECT count(*),sum(octet_length(payload)),sum(cast(xxhash64(payload) AS DECIMAL(38,0))) FROM ({queries[mode]})")
  assert rows[0][0]=='100000';results.append({'mode':mode,'round':i,'result':rows})
assert all(r['result']==results[0]['result'] for r in results)
# Independent exact lexical content proof, not digest equality alone.
assert c.sql('exact-wire',f"SELECT count(*) FROM {S} VERSION AS OF 0 s FULL OUTER JOIN (SELECT * FROM {R} VERSION AS OF 16 WHERE apply_batch_id='r133-b1') r ON s.source_feed=r.source_feed AND s.source_epoch=r.source_epoch AND s.source_delivery_id=r.delivery_id WHERE s.id IS NULL OR r.delivery_id IS NULL OR NOT(hex(encode({encode('named_struct('+','.join(chr(39)+col+chr(39)+',s.'+col for col in list(COLS)+['old_json'])+')')},'UTF-8')) <=> hex(encode(r.payload_json,'UTF-8')))")==[['0']]
(O/'summary.json').write_text(json.dumps({'state':'Six forced-payload aggregates and independent exact wire comparison passed; final metrics pending','results':results,'qualification':'Read-only three balanced serial pairs, immutable100k stage0/raw16, no writes. Same byte-count/digest aggregates plus independent exact lexical comparison. Source layouts and columns read differ, so timing difference does not uniquely attribute serialization CPU or predict append/publication throughput.'},indent=2)+'\n')
c.history();c.close();print('Read-only serialization and exact wire checks passed')
