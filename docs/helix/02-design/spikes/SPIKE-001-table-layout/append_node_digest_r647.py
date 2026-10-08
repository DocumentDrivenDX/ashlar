"""All-field native digest mapping for the disjoint append node allocation."""
import re
from mixed_change_queries_r230 import row_hash_sql


def grouped_query(table, version, role, fields):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table):
        raise ValueError('Validated private table name required')
    if type(version) is not int or version<0 or role not in ['object_current','source_record','property_journal']:
        raise ValueError('Explicit role and pinned version required')
    if not fields or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',f) for f in fields):
        raise ValueError('Explicit complete field list required')
    if role=='source_record':
        ordinal="try_cast(element_at(split(delivery_id,':'),2) AS BIGINT)"
        valid="delivery_id RLIKE '^node:(0|[1-9][0-9]*):1$'"
    else:
        ordinal='(id-40000001)'
        valid="entity_kind='node'" if role=='property_journal' else 'true'
    bucket=f"CASE WHEN coalesce(({valid}) AND {ordinal}>=8000000 AND {ordinal}<16000000,false) THEN cast(({ordinal}-8000000) DIV 100000 AS BIGINT) ELSE cast(-1 AS BIGINT) END"
    # Every physical live row belongs to exactly one bucket, including malformed
    # membership at -1. Never hide unexpected rows with a range WHERE filter.
    return f"SELECT bucket,count(*),sha2(concat_ws('',sort_array(collect_list(row_digest))),256) FROM (SELECT {bucket} bucket,{row_hash_sql(fields)} row_digest FROM {table} VERSION AS OF {version}) GROUP BY bucket ORDER BY bucket"
