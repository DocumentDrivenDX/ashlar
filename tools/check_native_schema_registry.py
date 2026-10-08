"""Small real PostgreSQL/Delta intake custody checks. No catalog acceptance."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
B = ROOT / 'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(B))
from ashlar.schema import SchemaIntake
from persistent_sql import Client

OUT = B / 'out/native/schema_registry_20261008'
OUT.mkdir(parents=True, exist_ok=True)
PIN = '16c35e8d943769ccfa7bb57d16785aa7159abe65'
TABLE = 'client_dev.ashlar_layout_v03_20261006_r73.schema_intake_20261008'
PG = 'ashlar_intake.document'
container = 'ashlar-e2e-truss-pg17'
label = subprocess.run(['docker','inspect',container,'--format','{{index .Config.Labels "ashlar.purpose"}}'],check=True,capture_output=True,text=True).stdout.strip()
if label != 'end-to-end-development':
    raise RuntimeError('Refuse unrelated container')

def pg(sql):
    result = subprocess.run(['docker','exec','-i',container,'psql','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1','-At'],input=sql,capture_output=True,text=True)
    with (OUT/'postgres.jsonl').open('a') as stream:
        stream.write(json.dumps({'sql':sql,'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr})+'\n')
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()

# No IF NOT EXISTS: inspect prior custody rather than quietly reusing/replacing it.
if '--resume-empty-postgres' in sys.argv:
    assert pg('SELECT count(*) FROM '+PG+';')=='0'
    assert pg("SELECT string_agg(column_name,',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema='ashlar_intake' AND table_name='document';")== 'document_id,document_revision,source_base64,artifact_base64,source_sha256,artifact_sha256,validator_revision,complete_interpretation'
else:
    pg('BEGIN; CREATE SCHEMA ashlar_intake; CREATE TABLE '+PG+''' (
document_id text NOT NULL, document_revision text NOT NULL,
source_base64 text NOT NULL, artifact_base64 text NOT NULL,
source_sha256 text NOT NULL, artifact_sha256 text NOT NULL,
validator_revision text NOT NULL, complete_interpretation boolean NOT NULL,
PRIMARY KEY(document_id,document_revision),
CHECK (source_sha256=encode(sha256(decode(source_base64,'base64')),'hex')),
CHECK (artifact_sha256=encode(sha256(decode(artifact_base64,'base64')),'hex'))
); REVOKE ALL ON SCHEMA ashlar_intake FROM PUBLIC;
REVOKE ALL ON ashlar_intake.document FROM PUBLIC; COMMIT;''')
c = Client(OUT/'delta')
c.sql('create-raw-registry','CREATE TABLE '+TABLE+''' (
document_id STRING NOT NULL, document_revision STRING NOT NULL,
source_base64 STRING NOT NULL, artifact_base64 STRING NOT NULL,
source_sha256 STRING NOT NULL, artifact_sha256 STRING NOT NULL,
validator_revision STRING NOT NULL, complete_interpretation BOOLEAN NOT NULL
) USING DELTA''')
c.sql('source-digest-constraint','ALTER TABLE '+TABLE+' ADD CONSTRAINT source_digest CHECK (source_sha256=sha2(unbase64(source_base64),256))')
c.sql('artifact-digest-constraint','ALTER TABLE '+TABLE+' ADD CONSTRAINT artifact_digest CHECK (artifact_sha256=sha2(unbase64(artifact_base64),256))')
items = [SchemaIntake.read((ROOT/('examples/end-to-end/schema-'+name+'.intake.json')).read_bytes(),rev,trusted_validator_revision=PIN) for name,rev in [('v1','1'),('v2','2'),('unknown','retained-unknown')]]
fields = list(items[0].row())
rows = [x.row() for x in items]

def store_pg(row):
    # A UTF8 hex literal is the only embedded value; source-authored text is never SQL.
    encoded = json.dumps(row,separators=(',',':')).encode().hex()
    incoming = "json_populate_record(NULL::"+PG+", convert_from(decode('"+encoded+"','hex'),'UTF8')::json)"
    sql = 'INSERT INTO '+PG+' SELECT * FROM '+incoming+' ON CONFLICT(document_id,document_revision) DO NOTHING;'
    pg('BEGIN; '+sql+' COMMIT;')
    actual = json.loads(pg("SELECT row_to_json(d) FROM "+PG+" d WHERE (document_id,document_revision) IN (SELECT document_id,document_revision FROM "+incoming+");"))
    if actual != row:
        raise RuntimeError('SCHEMA_INTAKE_CONFLICT')

def store_delta(batch):
    # Serialized development writer only; MERGE does not establish concurrent uniqueness.
    schema = 'ARRAY<STRUCT<'+','.join(k+':'+('BOOLEAN' if k=='complete_interpretation' else 'STRING') for k in fields)+'>>'
    same = ' AND '.join('t.'+k+' IS NOT DISTINCT FROM s.'+k for k in fields)
    query = 'MERGE INTO '+TABLE+' t USING (SELECT r.* FROM (SELECT explode(from_json(:payload,\''+schema+'\')) r)) s ON t.document_id=s.document_id AND t.document_revision=s.document_revision WHEN MATCHED AND NOT ('+same+") THEN UPDATE SET artifact_base64=cast(raise_error('SCHEMA_INTAKE_CONFLICT') AS STRING) WHEN NOT MATCHED THEN INSERT *"
    return c.sql('intake-merge',query,parameters=[{'name':'payload','type':'STRING','value':json.dumps(batch)}])

for row in rows:
    store_pg(row)
store_delta(rows)
for row in rows:
    store_pg(row)
store_delta(rows)
# Independently recover source and diagnostic artifact bytes from both native stores.
pgrows = json.loads(pg('SELECT json_agg(d ORDER BY document_revision) FROM '+PG+' d;'))
delta = c.sql('read-raw-custody','SELECT '+','.join(fields)+' FROM '+TABLE+' ORDER BY document_revision')
expected = sorted(rows,key=lambda r:r['document_revision'])
assert pgrows == expected
for actual,row in zip(delta,expected):
    assert actual == [str(row[k]).lower() if type(row[k]) is bool else row[k] for k in fields]
assert len(delta)==3
# Same qualified revision with independently valid, different schema bytes must refuse.
conflict = dict(rows[1],document_revision='1')
try:
    store_pg(conflict)
    raise AssertionError('PG conflict accepted')
except RuntimeError as exc:
    assert 'SCHEMA_INTAKE_CONFLICT' in str(exc)
try:
    store_delta([conflict])
    raise AssertionError('Delta conflict accepted')
except RuntimeError as exc:
    assert 'SCHEMA_INTAKE_CONFLICT' in str(exc)
assert json.loads(pg('SELECT json_agg(d ORDER BY document_revision) FROM '+PG+' d;'))==expected
assert c.sql('conflict-parity','SELECT '+','.join(fields)+' FROM '+TABLE+' ORDER BY document_revision')==delta
assert pg('SELECT rev FROM truss.schema_head WHERE id=1;')=='0'
summary = {'state':'passed','postgres_table':PG,'delta_table':TABLE,'documents':3,'checks':['exact source and diagnostic artifact bytes on both native stores','v1 and additive v2 separately retained','unknown assertions retained with incomplete interpretation','identical replay preserves original rows','same identity/revision different valid bytes refuses with unchanged originals','Truss accepted head remains zero'], 'qualification':'Raw intake custody only. Admin development profile; serialized Delta writer, no concurrent uniqueness/accepted schema/binding/catalog IDs/producer/feed claim. Three small rows; existing warehouse, no resize or benchmark.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
