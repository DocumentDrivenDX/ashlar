"""Host PostgreSQL transaction adapter for pin custody; psycopg 3 connection port.

The factory authenticates/configures a fresh connection for the supplied context.
No default connection, role, authorization or retention admission is provided.
"""
from contextlib import contextmanager
import re
from ashlar.native import SQLResult


class PostgresTransactionError(ValueError):
    pass


class Session:
    def __init__(self, connection):
        self.connection = connection
        self.active = True

    def query(self, sql, parameters):
        if not self.active:
            raise PostgresTransactionError('Transaction session has ended')
        if not isinstance(sql, str) or not sql:
            raise PostgresTransactionError('Explicit trusted SQL required')
        if not isinstance(parameters, dict) or any(
                not isinstance(key, str) or not re.fullmatch('[a-z_]+', key)
                or not isinstance(value, str) for key, value in parameters.items()):
            raise PostgresTransactionError('Named string parameters required')
        # This is the fixed generated pin SQL dialect, not a general SQL lexer.
        # Reject quoted/commented placeholder ambiguity instead of guessing.
        if '--' in sql or '/*' in sql or any(':' in part for part in sql.split("'")[1::2]):
            raise PostgresTransactionError('Unsupported placeholder SQL dialect')
        names = set(re.findall(r'(?<!:):([a-z_]+)', sql))
        if names != set(parameters):
            raise PostgresTransactionError('Exact parameter inventory required')
        statement = re.sub(r'(?<!:):([a-z_]+)', lambda match: '%(' + match[1] + ')s', sql)
        with self.connection.cursor() as cursor:
            cursor.execute(statement, parameters)
            if cursor.description is None:
                return SQLResult([])
            names = [column.name for column in cursor.description]
            if len(set(names)) != len(names):
                raise PostgresTransactionError('Ambiguous result columns')
            values = cursor.fetchmany(130)
            if len(values) > 129:
                raise PostgresTransactionError('Pin result inventory exceeds limit')
            return SQLResult([dict(zip(names, row)) for row in values])


class PostgresTransactions:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory
        self.active = False

    @contextmanager
    def transaction(self, context):
        if self.active:
            raise PostgresTransactionError('Nested custody transaction refused')
        connection = self.connection_factory(context)
        session = Session(connection)
        try:
            # psycopg TransactionStatus.IDLE = 0. A factory may not hand over
            # an existing transaction or autocommit connection with lost locks.
            if connection.autocommit or connection.info.transaction_status != 0:
                raise PostgresTransactionError('Fresh non-autocommit connection required')
            self.active = True
            try:
                yield session
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        finally:
            session.active = False
            self.active = False
            connection.close()
