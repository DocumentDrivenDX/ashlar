"""Native immutable manifest with explicit original SQL-handle recovery."""
from ashlar.manifest import DeltaManifestStore
from durable_sql import SQLCustodyError
from databricks_transport import OperationExecutor


class JournaledManifestStore:
    def __init__(self, transport, policy, table, uuid, *, operation):
        if not isinstance(operation, str) or not operation:
            raise SQLCustodyError('Explicit original manifest operation required')
        self.transport = transport
        self.operation = operation
        self.store = DeltaManifestStore(OperationExecutor(transport, operation), policy, table, uuid)

    def commit(self, row, *, context):
        return self.store.commit(row, context=context)

    def recover(self, row, *, context):
        # Absence is not evidence that an interrupted commit was never sent.
        original = self.transport.journal.db.execute(
            'SELECT operation FROM submission WHERE operation=?', (self.operation,)).fetchone()
        if original is None:
            raise SQLCustodyError('No original manifest submission; explicit reconciliation required')
        # DeltaManifestStore renews admission and exact readback; OperationExecutor
        # recovers the original retained terminal receipt or GETs the same handle.
        # Missing handle, request conflict and terminal failure never POST again.
        return self.store.commit(row, context=context)
