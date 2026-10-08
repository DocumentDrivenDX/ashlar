"""Protected full-vector registration and competing native release refusal."""
import hashlib,json,re,selectors,subprocess,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'))
from ashlar.pins import PinVector,PostgresPins
from ashlar.native import SQLResult
OUT=B/'out/native/pin_vector_20261008';OUT.mkdir(parents=True,exist_ok=True)
raw=(B/'out/native/graph_full_parity_20261008/summary.json').read_bytes();proof=json.loads(raw)
uuids=json.loads((B/'out/native/recoverable_graph_20261008/summary.json').read_text())['table_uuids']
v=PinVector('private-development','recovery','recoverable-graph-final',hashlib.sha256(raw).hexdigest(),{t:(uuids[t],version) for t,version in proof['version_vector'].items()})
command=['/usr/local/bin/docker','exec','-i','ashlar-e2e-truss-pg17','psql','-X','-qAt','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1']
def bound(sql,params):
    return re.sub(r'(?<!:):([a-z_]+)',lambda m:"'"+params[m.group(1)].replace("'","''")+"'",sql)
class Collector:
    def __init__(self):self.statements=[]
    def query(self,sql,params):self.statements.append(bound(sql,params)+';');return SQLResult([])
register=Collector();guard=Collector()
for table in sorted(v.targets):
    PostgresPins._call(register,'register',v,table)
    PostgresPins._call(guard,'assert_active',v,table)
r=subprocess.run(command,input=('BEGIN; SET LOCAL ROLE ashlar_pin_writer;\n'+'\n'.join(register.statements)+'\nCOMMIT;\n').encode(),capture_output=True,timeout=20)
(OUT/'register-output.txt').write_bytes(r.stdout+r.stderr)
if r.returncode:raise SystemExit(r.returncode)
reader=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
try:
    reader.stdin.write(('BEGIN; SET LOCAL ROLE ashlar_pin_reader;\n'+'\n'.join(guard.statements)+"\nSELECT 'READ_GUARD_HELD';\n").encode());reader.stdin.flush()
    sel=selectors.DefaultSelector();sel.register(reader.stdout,selectors.EVENT_READ);deadline=time.monotonic()+20;observed=b''
    while b'READ_GUARD_HELD' not in observed:
        if time.monotonic()>deadline:raise RuntimeError('Guard observation deadline; inspect existing reader process')
        if sel.select(timeout=.2):observed+=__import__('os').read(reader.stdout.fileno(),4096)
        if reader.poll() is not None:raise RuntimeError('Guard reader terminated before observation')
    target=sorted(v.targets)[0];p=v.parameters(target)
    sql=bound("SET lock_timeout='100ms'; SET ROLE ashlar_pin_writer; SELECT ashlar_pins.release(:id,:authority,decode(:digest,'hex'));",p)
    release=subprocess.run(command,input=sql.encode(),capture_output=True,timeout=20)
    (OUT/'release-output.txt').write_bytes(release.stdout+release.stderr)
    assert release.returncode!=0 and b'lock timeout' in release.stderr
    reader.stdin.write(b'COMMIT;\n');reader.stdin.flush();reader.stdin.close();reader.wait(timeout=20)
    observed+=reader.stdout.read();error=reader.stderr.read()
    (OUT/'read-guard-output.txt').write_bytes(observed+error)
    assert reader.returncode==0
finally:
    if reader.poll() is None:reader.terminate();reader.wait(timeout=10)
params={'authority':v.authority,'kind':v.scope_kind,'scope':v.scope_id}
query=bound("SELECT coalesce(json_agg(r),'[]'::json) FROM (SELECT table_name,table_uuid,version::text AS version,encode(custody_digest,'hex') AS digest,released FROM ashlar_pins.pin WHERE authority=:authority AND scope_kind=:kind AND scope_id=:scope) r;",params)
r=subprocess.run(command,input=query.encode(),capture_output=True,timeout=20)
assert r.returncode==0
rows=json.loads(r.stdout);assert len(rows)==4
for row in rows:
    uuid,version=v.targets[row['table_name']]
    assert (row['table_uuid'],row['version'],row['digest'],row['released'])==(uuid,str(version),v.custody_digest,False)
(OUT/'retained-vector.json').write_text(json.dumps(rows,indent=2)+'\n')
summary={'state':'passed','scope_id':v.scope_id,'custody_digest':v.custody_digest,'pins':4,'checks':['full exact vector registered transactionally','four active read guards held in ordinary reader transaction','competing writer release hits native lock timeout','read transaction commits and every original pin remains active'],'qualification':'Private recovery-fixture pin custody and read/release exclusion. Complete effect/read receipts observed; retention operator enforcement, native role-table fencing, protocol admission and publication are still unfinished. No automatic expiry or cleanup.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
