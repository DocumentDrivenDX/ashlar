"""Private override experiment: unique overlay keys proven newer than E5.
Requires separately audited immutable-pair dominance, not general version resolve.
"""
from normalized_apply_sql_r276 import pin
from overlay_sql_r395 import FIELDS

def override_lookup(base_table,base_version,overlay_table,overlay_version):
 base=pin(base_table,base_version);overlay=pin(overlay_table,overlay_version);where='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)';fields=','.join(FIELDS);cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in FIELDS)
 return f'WITH overlay_pick AS (SELECT {fields},is_deleted FROM {overlay} WHERE {where}) SELECT {cols} FROM overlay_pick WHERE NOT is_deleted UNION ALL SELECT {cols} FROM {base} WHERE {where} AND NOT EXISTS (SELECT 1 FROM overlay_pick)'
