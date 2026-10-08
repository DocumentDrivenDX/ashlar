"""Retain a server clock through original SQL custody; never refresh on replay."""
import datetime
import re
from databricks_transport import sql_result
from durable_sql import SQLCustodyError

CLOCK_OPERATION = 'native-example:materialization-clock/1'
CLOCK_SQL = 'SELECT cast(unix_micros(current_timestamp()) AS STRING) AS now_us, :workload AS workload'


def materialization_clock(journal, workload):
    if not isinstance(workload, str) or not re.fullmatch('[0-9a-f]{64}', workload):
        raise SQLCustodyError('Exact original workload digest required')
    result = sql_result(journal.query(CLOCK_OPERATION, CLOCK_SQL, {'workload': workload}))
    if result.columns != (('now_us', 'STRING'), ('workload', 'STRING')) or len(result.rows) != 1:
        raise SQLCustodyError('Exact original clock response required')
    row = result.rows[0]
    value = row.get('now_us')
    if row.get('workload') != workload or not isinstance(value, str) or not re.fullmatch('0|[1-9][0-9]*', value) or int(value) > 9223372036854775807:
        raise SQLCustodyError('Original server clock/workload mismatch')
    try:
        instant = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(microseconds=int(value))
    except OverflowError as error:
        raise SQLCustodyError('Server clock outside supported datetime range') from error
    return instant.isoformat()
