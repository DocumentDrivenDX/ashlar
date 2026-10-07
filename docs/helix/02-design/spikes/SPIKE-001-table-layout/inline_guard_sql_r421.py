"""Private update/delete-only MERGE; requires qualified unique normalized input."""
from normalized_apply_sql_r276 import pin,mutation_source
from overlay_sql_r395 import FIELDS

def guard_merge_relation(target,source):
 pin(target,0)
 # Native null-safe equality includes all complete carrier fields. Exact STRING
 # bags are compared as UTF-8 bytes, not parsed/reencoded property values.
 ints={'rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','source_position'}
 comparisons=[]
 for f in FIELDS:
  comparisons.append(f'(b.{f} <=> s.{f})' if f in ints or f=='published_at' else f'(hex(encode(b.{f},\'UTF-8\')) <=> hex(encode(s.{f},\'UTF-8\')))')
 identity=' AND '.join(f's.after_{f} <=> s.{f}' for f in ['source_system','rel_type_id','id','lookup_hash'])
 valid=' AND '.join(comparisons)+f' AND s.entity_version IS NOT NULL AND (s.is_delete OR (s.after_entity_version=s.entity_version+1 AND ({identity})))'
 assignments=','.join(f'b.{f}=s.after_{f}' for f in FIELDS)
 # Evaluate guard for updates as well as deletes; do not silently skip a bad
 # predecessor. A missing match attempts an error expression in INSERT so a
 # missing deletion also fails the entire statement.
 insert=','.join("CAST(raise_error('ASHLAR_INLINE_MISSING_PREDECESSOR') AS STRING)" if f=='source_system' else 's.after_'+f for f in FIELDS)
 return f"MERGE INTO {target} b USING ({source}) s ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id WHEN MATCHED AND CASE WHEN ({valid}) THEN s.is_delete ELSE CAST(raise_error('ASHLAR_INLINE_PREDECESSOR_MISMATCH') AS BOOLEAN) END THEN DELETE WHEN MATCHED THEN UPDATE SET {assignments} WHEN NOT MATCHED THEN INSERT ({','.join(FIELDS)}) VALUES ({insert})"

def guard_merge(target,raw_table,raw_version,current_table,current_version):
 return guard_merge_relation(target,mutation_source(raw_table,raw_version,current_table,current_version))
