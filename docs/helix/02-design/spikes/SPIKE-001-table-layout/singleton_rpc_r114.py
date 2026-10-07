"""Exact pinned singleton RPC timing; record method names/durations, never args."""
import json,time,hashlib,csv,base64
from pathlib import Path
from importlib.metadata import distribution
from driver_sql import DriverClient
from isolation_reader import expected_r103,ExactReader
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_singleton_rpc_r114'
assert not (O/'statements.jsonl').exists(),'Inspect prior query IDs; no blind repetition'
versions,rows=expected_r103(B)
c=DriverClient(O);reader=ExactReader(c,versions,rows);reader.preflight()
backend=c.cursor.backend
assert hasattr(backend,'make_request'),'Actual backend is not expected Thrift transport'
original=backend.make_request;current=[]
def traced(method,*args,**kwargs):
 started=time.perf_counter()
 try:return original(method,*args,**kwargs)
 finally:current.append({'method':method.__name__,'wall_ms':(time.perf_counter()-started)*1000})
backend.make_request=traced
calls=[]
try:
 for i in range(30):
  current.clear();reader.read('rpc',i)
  calls.append({'statement_id':c.records[-1]['statement_id'],'label':c.records[-1]['label'],'caller_ms':c.records[-1]['wall_ms'],'rpc':list(current)})
finally:backend.make_request=original
pkg=distribution('databricks-sql-connector');record={r[0]:r[1] for r in csv.reader(pkg.read_text('RECORD').splitlines())}
fingerprints={}
for name in ['client.py','backend/thrift_backend.py','backend/sea/backend.py','session.py']:
 relative='databricks/sql/'+name;p=Path(pkg.locate_file(relative));digest=hashlib.sha256(p.read_bytes()).digest()
 expected='sha256='+base64.urlsafe_b64encode(digest).decode().rstrip('=')
 fingerprints[name]={'sha256':digest.hex(),'matches_installed_record':record.get(relative)==expected}
(O/'summary.json').write_text(json.dumps({'state':'30 exact full-field singleton checks and RPC timings completed; final history audit pending','backend_class':type(backend).__name__,'package_metadata_version':pkg.version,'source_fingerprints':fingerprints,'queries':calls,'qualification':'Method-only instance instrumentation; no credentials or RPC arguments retained. Same fixed warm30-key cohort, no publisher. Current source fingerprint does not retroactively identify historical connector source.'},indent=2)+'\n')
c.history();c.close();print('Completed 30 exact singleton RPC traces')
