"""Full-carrier parameterized lookup over exact accepted immutable pins."""
from normalized_apply_sql_r276 import pin
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash_sql

def carrier_lookup(base_table,base_version,overlay_table,overlay_version):
 base=pin(base_table,base_version);overlay=pin(overlay_table,overlay_version);where='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)';fields=','.join(FIELDS);carrier='named_struct('+','.join("'"+f+"',"+f for f in FIELDS+('is_deleted',))+')';candidates=f'SELECT {fields},false AS is_deleted FROM {base} WHERE {where} UNION ALL SELECT {fields},is_deleted FROM {overlay} WHERE {where}';per=f'SELECT entity_version,count(DISTINCT {row_hash_sql(FIELDS+("is_deleted",))}) AS variants,first({carrier}) AS carrier FROM ({candidates}) GROUP BY entity_version';winner=f"SELECT CASE WHEN max(variants)>1 THEN raise_error('ASHLAR_OVERLAY_VERSION_CONFLICT') ELSE max_by(carrier,entity_version) END AS winner FROM ({per})";cols=','.join('CAST(winner.published_at AS STRING) AS published_at' if f=='published_at' else 'winner.'+f+' AS '+f for f in FIELDS);return f'SELECT {cols} FROM ({winner}) WHERE winner IS NOT NULL AND NOT winner.is_deleted'
