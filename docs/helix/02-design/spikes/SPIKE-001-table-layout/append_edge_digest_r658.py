"""Complete first8M new-edge field digests with explicit invalid membership."""
import re
from mixed_change_queries_r230 import row_hash_sql


def grouped_query(table,version,role,fields):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',table):
        raise ValueError('Validated private table name required')
    if type(version) is not int or version<0 or role not in ['edge_current','source_record','property_journal','adjacency_forward']:
        raise ValueError('Explicit role and pinned version required')
    if not fields or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',f) for f in fields):
        raise ValueError('Explicit complete field list required')
    if role=='source_record':
        ordinal="try_cast(element_at(split(delivery_id,':'),2) AS BIGINT)"
        valid="delivery_id RLIKE '^edge:(0|[1-9][0-9]*):1$'"
    else:
        ordinal='(id-16000001)'
        valid="entity_kind='edge'" if role=='property_journal' else 'true'
    bucket=f"CASE WHEN coalesce(({valid}) AND {ordinal}>=40000000 AND {ordinal}<48000000,false) THEN cast(({ordinal}-40000000) DIV 100000 AS BIGINT) ELSE cast(-1 AS BIGINT) END"
    return f"SELECT bucket,count(*),sha2(concat_ws('',sort_array(collect_list(row_digest))),256) FROM (SELECT {bucket} bucket,{row_hash_sql(fields)} row_digest FROM {table} VERSION AS OF {version}) GROUP BY bucket ORDER BY bucket"
