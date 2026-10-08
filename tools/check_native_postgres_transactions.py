"""Exercise real PostgresPins read custody through the psycopg host adapter.

Uses the existing recovery scope; registers/releases no pins and makes no Delta
or retention claim. Container credentials stay in memory and are never emitted.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import psycopg

ROOT = Path(__file__).resolve().parents[1]
B = ROOT / 'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0, str(ROOT / 'src'))
from ashlar.pins import PinVector, PostgresPins
from postgres_transactions import PostgresTransactions, PostgresTransactionError

container = json.loads(subprocess.check_output(
    ['/usr/local/bin/docker', 'inspect', 'ashlar-e2e-truss-pg17']))[0]
if container['Config']['Labels'].get('ashlar.purpose') != 'end-to-end-development':
    raise ValueError('Wrong isolated development container')
password = next(value.split('=', 1)[1] for value in container['Config']['Env']
                if value.startswith('POSTGRES_PASSWORD='))
context = object()

def connect(role):
    return psycopg.connect(host='127.0.0.1', port=15432, user='postgres',
        password=password, dbname='truss_e2e', connect_timeout=5,
        options='-c role=' + role + ' -c statement_timeout=5000 -c lock_timeout=100')

def factory(received):
    if received is not context:
        raise PermissionError('Wrong isolated read context')
    return connect('ashlar_pin_reader')

raw = (B / 'out/native/graph_full_parity_20261008/summary.json').read_bytes()
proof = json.loads(raw)
uuids = json.loads((B / 'out/native/recoverable_graph_20261008/summary.json').read_text())['table_uuids']
vector = PinVector('private-development', 'recovery', 'recoverable-graph-final',
    hashlib.sha256(raw).hexdigest(), {table: (uuids[table], version)
                                   for table, version in proof['version_vector'].items()})

class Policy:
    def authorize_read(self, received, supplied):
        if supplied is not context or received != vector:
            raise PermissionError('Wrong existing development recovery scope')
    def admit_registration(self, *args):
        raise PermissionError('This check cannot register retention authority')

executor = PostgresTransactions(factory)
pins = PostgresPins(executor, Policy())
held = None
with pins.hold(vector, context=context):
    target = sorted(vector.targets)[0]
    parameters = vector.parameters(target)
    with connect('ashlar_pin_writer') as contender:
        try:
            contender.execute("SELECT ashlar_pins.release(%s,%s,decode(%s,'hex'))",
                              (parameters['id'], vector.authority, vector.custody_digest))
        except psycopg.errors.LockNotAvailable:
            contender.rollback()
            held = 'competing release blocked'
        else:
            contender.rollback()
            raise ValueError('Read guard did not hold native release exclusion')
assert held is not None
with executor.transaction(context) as session:
    role = session.query('SELECT current_user AS role', {}).rows
    assert role == [{'role': 'ashlar_pin_reader'}]
    native = session.query("SELECT current_user AS role,current_setting('server_version') AS version,pg_current_xact_id()::text AS xid", {}).rows
    inventory = session.query("SELECT table_name,table_uuid,version::text AS version,encode(custody_digest,'hex') AS digest,released FROM ashlar_pins.pin WHERE authority=:authority AND scope_kind=:kind AND scope_id=:scope LIMIT 129",
        {'authority': vector.authority, 'kind': vector.scope_kind, 'scope': vector.scope_id}).rows
    assert len(inventory) == len(vector.targets)
    for row in inventory:
        assert (row['table_uuid'], row['version'], row['digest'], row['released']) == (
            vector.targets[row['table_name']][0], str(vector.targets[row['table_name']][1]), vector.custody_digest, False)
try:
    session.query('SELECT current_user AS role', {})
except PostgresTransactionError:
    pass
else:
    raise ValueError('Ended session remained executable')
try:
    with executor.transaction(context) as session:
        session.query('SELECT pg_advisory_xact_lock(782390) AS held', {})
        raise ValueError('injected rollback')
except ValueError as error:
    assert str(error) == 'injected rollback'
with connect('ashlar_pin_reader') as observer:
    assert observer.execute('SELECT pg_try_advisory_xact_lock(782390)').fetchone() == (True,)
out = B / 'out/native/postgres_transactions_20261008'
out.mkdir(parents=True, exist_ok=True)
summary = {'state': 'passed', 'driver': 'psycopg/' + psycopg.__version__,
           'scope': vector.scope_id, 'pins': len(vector.targets),
           'native_reader': native, 'native_inventory': inventory,
           'checks': [held, 'actual ordinary reader role', 'ended session refused',
                      'injected exception releases transaction lock'],
           'qualification': 'Actual PostgresPins hold through host transaction adapter on existing recovery pins. No pin registration/release, Delta file availability, retention-operator authority, publication or native singleton claim.'}
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print('PostgresPins transaction adapter passed four native controls')
