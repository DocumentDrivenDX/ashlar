"""Private read experiment for separately qualified conflict-free immutable pairs.
Not an arbitrary-input validator, production admission check or publisher fence.
"""
from normalized_apply_sql_r276 import pin
from overlay_sql_r395 import FIELDS

def qualified_pair_lookup(base_table,base_version,overlay_table,overlay_version):
 base=pin(base_table,base_version);overlay=pin(overlay_table,overlay_version);where='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)';fields=','.join(FIELDS);carrier='named_struct('+','.join("'"+f+"',"+f for f in FIELDS+('is_deleted',))+')';candidates=f'SELECT {fields},false AS is_deleted FROM {base} WHERE {where} UNION ALL SELECT {fields},is_deleted FROM {overlay} WHERE {where}';winner=f'SELECT max_by({carrier},entity_version) AS winner FROM ({candidates})';cols=','.join('CAST(winner.published_at AS STRING) AS published_at' if f=='published_at' else 'winner.'+f+' AS '+f for f in FIELDS);return f'SELECT {cols} FROM ({winner}) WHERE winner IS NOT NULL AND NOT winner.is_deleted'
