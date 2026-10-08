"""Private sandbox-only role routing under the existing trusted postgres login.

No grants are changed. Only the exact generated inventory SELECT temporarily
uses the existing reader role in the same transaction; locks remain held. This
is not a production authentication strategy or an arbitrary SQL role switch.
"""
from contextlib import contextmanager
from ashlar.pins import INVENTORY_SQL
from postgres_transactions import PostgresTransactions, PostgresTransactionError


class PrivatePinTransactions:
    def __init__(self, context, role, *, connection_factory=None):
        if role not in ('ashlar_pin_writer', 'ashlar_pin_reader'):
            raise PermissionError('Explicit private ordinary pin role required')
        self.context, self.role = context, role
        if connection_factory is None:
            from sandbox_postgres import connect
            connection_factory = lambda supplied: connect(role)
        def factory(supplied):
            if supplied is not context:raise PermissionError('Wrong private authenticated pin context')
            return connection_factory(supplied)
        self.executor = PostgresTransactions(factory)

    @contextmanager
    def transaction(self, context):
        if context is not self.context:raise PermissionError('Wrong original private pin context')
        with self.executor.transaction(context) as session:
            def role(expected):
                if session.query('SELECT current_user AS role', {}).rows != [{'role': expected}]:
                    raise PermissionError('Current private ordinary role differs')
            role(self.role)
            writer_role = self.role
            class InventorySession:
                def query(self, sql, parameters):
                    if sql != INVENTORY_SQL or writer_role == 'ashlar_pin_reader':
                        return session.query(sql, parameters)
                    session.query('SET LOCAL ROLE ashlar_pin_reader', {})
                    role('ashlar_pin_reader')
                    # On failure the outer transaction rolls back. Do not mask
                    # an aborted transaction with another SET ROLE statement.
                    result = session.query(sql, parameters)
                    session.query('SET LOCAL ROLE ashlar_pin_writer', {})
                    role('ashlar_pin_writer')
                    return result
            yield InventorySession()
            role(self.role)
