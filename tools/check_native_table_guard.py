"""Actual table-wide pin guard controls; never runs native Delta cleanup."""
import hashlib
import json
from pathlib import Path
import sys
import psycopg
ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'))
from ashlar.maintenance import PostgresTableGuard
from postgres_transactions import PostgresTransactions
from sandbox_postgres import connect

context=object()
def factory(received):
    if received is not context:raise PermissionError('Wrong private guard context')
    return connect('ashlar_pin_maintenance')
class Policy:
    def authorize_guard(self,targets,received):
        if received is not context or dict(targets) not in [pinned,unregistered]:
            raise PermissionError('Guard-only fixed fixture scope')

old=json.loads((B/'out/native/recoverable_graph_20261008/summary.json').read_text())['table_uuids']
table=next(table for table in old if table.endswith('.object_current'))
pinned={table:old[table]}
private=json.loads((B/'out/native/private_setup_20261008/summary.json').read_text())
newtable=private['namespace']+'.object_current'
unregistered={newtable:private['tables'][newtable]['uuid']}
executor=PostgresTransactions(factory);guard=PostgresTableGuard(executor,Policy())
with executor.transaction(context) as session:
    native=session.query("SELECT current_user AS role,current_setting('server_version') AS version",{}).rows
    definition=session.query("SELECT prosrc AS body,prosecdef AS security_definer,proconfig::text AS settings FROM pg_proc WHERE oid='ashlar_pins.assert_table_unpinned(text,text)'::regprocedure",{}).rows
source=(ROOT/'sql/ashlar-pins/04-table-guard.sql').read_text()
assert len(definition)==1 and definition[0]['body']==source.split('AS $$',1)[1].split('$$;',1)[0]
assert definition[0]['security_definer'] is True
with executor.transaction(context) as session:
    # This older exact-version guard passes a version without a pin. It cannot
    # stand in for checking all snapshots affected by whole-table cleanup.
    session.query('SELECT ashlar_pins.assert_unpinned(:table,:uuid,999999)',
                  {'table':table,'uuid':old[table]})
try:
    with guard.hold(pinned,context=context):raise AssertionError('Pinned table admitted')
except psycopg.errors.RaiseException as error:
    assert 'Active table pin refuses cleanup' in str(error)
with guard.hold(unregistered,context=context):
    with connect('ashlar_pin_writer') as contender:
        try:
            contender.execute("SELECT ashlar_pins.register(%s,%s,%s,%s,%s,%s,%s,decode(%s,'hex'))",
                ('table-guard-control','private-development','recovery','table-guard-control',newtable,
                 unregistered[newtable],0,'a'*64))
        except psycopg.errors.LockNotAvailable:
            contender.rollback()
        else:
            contender.rollback()
            raise AssertionError('Table guard did not exclude new registration')
out=B/'out/native/table_guard_20261008';out.mkdir(parents=True,exist_ok=True)
summary={'state':'passed','native_role':native,'source_sha256':hashlib.sha256(source.encode()).hexdigest(),
         'native_definition':definition,'checks':['exact-version-only guard permits unrelated version',
         'whole-table guard refuses active pin at any version/scope','held guard blocks competing registration'],
         'qualification':'Native ordinary-role pin exclusion only. No pins added/released, Delta cleanup, operator closure, file availability or uncertain remote outcome containment qualification.'}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print('Table-wide guard passed three native controls; no cleanup or pin changes')
