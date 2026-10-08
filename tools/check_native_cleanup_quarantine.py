"""Metadata-only quarantine controls; starts no remote cleanup or pin lifecycle."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import psycopg
ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'))
from ashlar.quarantine import CleanupRequest,CleanupQuarantine
from postgres_transactions import PostgresTransactions
from sandbox_postgres import connect
installation=json.loads((B/'out/native/outbox_setup_20261008/summary.json').read_text())
table=installation['namespace']+'.object_current';uuid=installation['tables'][table]['uuid']
request=CleanupRequest('metadata-only-quarantine-control-20261008',table,uuid,
                       b'{"kind":"metadata-only-control","remote_action":"none"}')
terminal=b'{"kind":"metadata-only-control-closed","remote_action_started":false}'
context=object()
class Policy:
    def admit_cleanup(self,original,received):
        if original!=request or received is not context:raise PermissionError('Wrong control')
    def verify_terminal(self,original,receipt,received):
        # This program imports/submits no Delta/native cleanup path. This proof
        # is valid only for the explicit metadata-only control, not real cleanup.
        if original!=request or receipt!=terminal or received is not context:raise PermissionError('Control terminal differs')
def executor(role):
    def factory(received):
        if received is not context:raise PermissionError('Wrong control context')
        return connect(role)
    return PostgresTransactions(factory)
cleanup=CleanupQuarantine(executor('ashlar_pin_maintenance'),Policy())
cleanup.begin(request,context=context)
cleanup.begin(request,context=context)  # fresh connections, same committed original
with connect('ashlar_pin_writer') as writer:
    try:
        writer.execute("SELECT ashlar_pins.register(%s,%s,%s,%s,%s,%s,%s,decode(%s,'hex'))",
            ('quarantine-control','private-development','recovery','quarantine-control',
             installation['namespace']+'.renamed_object',uuid,0,'b'*64))
    except psycopg.errors.RaiseException as error:
        assert 'Unresolved cleanup refuses new pin' in str(error);writer.rollback()
    else:
        writer.rollback();raise AssertionError('Pending quarantine admitted alias pin')
with connect('ashlar_pin_maintenance') as maintenance:
    try:
        maintenance.execute('SELECT ashlar_pins.close_cleanup_quarantine(%s,%s,%s)',
                            (request.operation,request.original_intent,terminal))
    except psycopg.errors.InsufficientPrivilege:maintenance.rollback()
    else:maintenance.rollback();raise AssertionError('Maintenance role closed quarantine')
CleanupQuarantine(executor('ashlar_pin_recovery'),Policy()).close(request,terminal,context=context)
with connect('ashlar_pin_writer') as writer:
    writer.execute("SELECT ashlar_pins.register(%s,%s,%s,%s,%s,%s,%s,decode(%s,'hex'))",
        ('quarantine-control','private-development','recovery','quarantine-control',table,uuid,0,'b'*64))
    writer.rollback()  # control adds no pin
with connect('ashlar_pin_reader') as reader:
    original=reader.execute('SELECT operation_id,table_name,table_uuid,encode(original_intent,\'hex\'),encode(terminal_custody,\'hex\') FROM ashlar_pins.maintenance_quarantine WHERE operation_id=%s',(request.operation,)).fetchone()
assert original==(request.operation,table,uuid,request.original_intent.hex(),terminal.hex())
out=B/'out/native/cleanup_quarantine_20261008';out.mkdir(parents=True,exist_ok=True)
summary={'state':'passed','original_native_row':list(original),'checks':['committed pending custody survives connection close','exact pending repeat preserves original','pending UUID-wide custody refuses alias registration','maintenance role cannot close custody','separate recovery close retains exact terminal bytes','post-close control registration rolled back'],
 'qualification':'Metadata-only ordinary-role quarantine controls; no remote action started, Delta query/cleanup, pin addition/release or retention change. Real cleanup terminal verifier/operator closure and live publication/read remain unqualified.'}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print('Durable quarantine passed six native controls; no cleanup or pin changes')
