"""Selected local host implementation; native qualification remains version-scoped."""
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
        if not isinstance(parameters, dict) or any((not isinstance(key, str) or not re.fullmatch('[a-z_]+', key) or (not isinstance(value, str)) for (key, value) in parameters.items())):
            raise PostgresTransactionError('Named string parameters required')
        if '--' in sql or '/*' in sql or any((':' in part for part in sql.split("'")[1::2])):
            raise PostgresTransactionError('Unsupported placeholder SQL dialect')
        names = set(re.findall('(?<!:):([a-z_]+)', sql))
        if names != set(parameters):
            raise PostgresTransactionError('Exact parameter inventory required')
        statement = re.sub('(?<!:):([a-z_]+)', lambda match: '%(' + match[1] + ')s', sql)
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
