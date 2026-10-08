"""Host phase executor: original handle custody precedes claims of absence.

Use a unique explicitly admitted operation namespace per attempt table/authority.
This is transport recovery, not writer exclusion, source admission or a lock.
"""
import hashlib
import json
from ashlar.schema import _json
from durable_sql import SQLCustodyError


class JournaledAttemptExecutor:
    def __init__(self, transport, *, namespace):
        if not isinstance(namespace, str) or not namespace or len(namespace.encode()) > 1024 or '\x00' in namespace:
            raise SQLCustodyError('Explicit bounded attempt operation namespace required')
        self.transport = transport
        self.prefix = 'attempt:' + namespace + ':'

    def _reconcile(self):
        rows = self.transport.journal.db.execute(
            'SELECT operation,request FROM submission WHERE substr(operation,1,?)=? AND response IS NULL ORDER BY operation',
            (len(self.prefix), self.prefix)).fetchall()
        for operation, text in rows:
            original = _json(text.encode())
            body = original['body']
            parameters = body['parameters']
            if not isinstance(parameters, list) or any(set(value) != {'name', 'type', 'value'} or value['type'] != 'STRING' for value in parameters):
                raise SQLCustodyError('Original phase parameter custody invalid')
            names = [value['name'] for value in parameters]
            if len(set(names)) != len(names):raise SQLCustodyError('Duplicate original phase parameter')
            # DurableSQL verifies current authority/warehouse, exact body digest,
            # retained handle and complete terminal result. No missing-handle POST.
            self.transport.journal.query(operation, body['statement'],
                {value['name']: value['value'] for value in parameters})

    def query(self, sql, parameters):
        prefix = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ''
        self._reconcile()
        if prefix in ('DESCRIBE', 'SELECT', 'SHOW'):
            return self.transport.query(sql, parameters)
        if prefix != 'MERGE' or set(parameters) != {'payload'}:
            raise SQLCustodyError('Only exact immutable phase MERGE is supported')
        value = _json(parameters['payload'].encode())
        fields = {'stream', 'batch_id', 'phase', 'request_digest', 'payload_json', 'payload_digest'}
        if not isinstance(value, dict) or set(value) != fields or any(not isinstance(item, str) or not item for item in value.values()):
            raise SQLCustodyError('Exact phase custody row required')
        identity = json.dumps([value['stream'], value['batch_id'], value['phase']], separators=(',', ':'), ensure_ascii=False)
        operation = self.prefix + hashlib.sha256(identity.encode()).hexdigest()
        return self.transport.mutation(operation, sql, parameters)
