"""Install one exact source declaration snapshot in the isolated sandbox only."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

CONTAINER = 'ashlar-e2e-truss-pg17'
source, expected_hash, receipt = sys.argv[1:]
raw = Path(source).read_bytes()
if hashlib.sha256(raw).hexdigest() != expected_hash:
    raise SystemExit('Truss source hash mismatch')
check = subprocess.run(['docker', 'inspect', CONTAINER, '--format', '{{index .Config.Labels "ashlar.purpose"}}'], check=True, capture_output=True, text=True)
if check.stdout.strip() != 'end-to-end-development':
    raise SystemExit('Refusing unrelated container')
base = ['docker', 'exec', '-i', CONTAINER, 'psql', '-U', 'postgres', '-d', 'truss_e2e', '-v', 'ON_ERROR_STOP=1', '-At']
present = subprocess.run(base + ['-c', "SELECT count(*) FROM pg_namespace WHERE nspname='truss'"], check=True, capture_output=True, text=True)
if present.stdout.strip() != '0':
    raise SystemExit('Existing Truss namespace: inspect installation before retry; no overwrite')
encoding = subprocess.run(base + ['-c', 'SHOW server_encoding'], check=True, capture_output=True, text=True)
if encoding.stdout.strip() != 'UTF8':
    raise SystemExit('UTF8 compatibility profile requires a UTF8 database')
original = "pg_catalog.convert_to(artifact_identity, 'UTF8')"
source_sql = raw.decode('utf-8')
if source_sql.count(original) != 1:
    raise SystemExit('Unexpected generated-expression source; refuse compatibility rewrite')
helper = """CREATE SCHEMA ashlar_bootstrap;
CREATE FUNCTION ashlar_bootstrap.utf8_bytes(value text) RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path TO pg_catalog, pg_temp
AS $$ SELECT pg_catalog.convert_to(value, 'UTF8') $$;
REVOKE ALL ON FUNCTION ashlar_bootstrap.utf8_bytes(text) FROM PUBLIC;
"""
# Fixed UTF8 source/target conversion is deterministic in this isolated UTF8 profile.
# Keep original source and the explicit derived execution digest separately.
installed_sql = helper + source_sql.replace(original, 'ashlar_bootstrap.utf8_bytes(artifact_identity)').rstrip() + ';\n'
result = subprocess.run(base, input='BEGIN;\n' + installed_sql + '\nCOMMIT;\n', capture_output=True, text=True)
Path(receipt).parent.mkdir(parents=True, exist_ok=True)
if result.returncode:
    Path(receipt).write_text(json.dumps({'state': 'Declaration installation refused; transaction not committed', 'source_sha256': expected_hash, 'error': result.stderr}, indent=2)+'\n')
    raise SystemExit(result.stderr)
query = "SELECT json_build_object('tables',(SELECT count(*) FROM pg_tables WHERE schemaname='truss'),'functions',(SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='truss'),'server',version(),'head',(SELECT rev FROM truss.schema_head WHERE id=1))"
actual = subprocess.run(base + ['-c', query], check=True, capture_output=True, text=True)
observed = json.loads(actual.stdout)
if observed['tables'] != 46 or observed['functions'] != 2 or observed['head'] != 0:
    raise SystemExit('Unexpected installed inventory; inspect committed declaration before further work')
Path(receipt).write_text(json.dumps({'state': 'Truss0.11 declarations installed with explicit UTF8 compatibility profile', 'source_sha256': expected_hash, 'execution_sha256': hashlib.sha256(installed_sql.encode()).hexdigest(), 'compatibility_profile': 'isolated UTF8 immutable conversion helper; one explicit generated-expression replacement; original source unchanged', 'observed': observed, 'container': CONTAINER, 'qualification': '46 table and 2 function declaration installation only; no adopted complete runtime, catalog acceptance, mutation/feed, production authority or native semantic qualification'}, indent=2)+'\n')
print(json.dumps({'state': 'Truss source declarations installed', 'tables': observed['tables'], 'functions': observed['functions'], 'catalog_head': observed['head']}))
