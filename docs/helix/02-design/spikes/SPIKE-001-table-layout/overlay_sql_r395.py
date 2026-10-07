"""Private accepted-carrier overlay SQL. No producer admission or writes on import."""
import re
from normalized_apply_sql_r276 import pin
from normalized_input_sql_r264 import role_row
from mixed_change_queries_r230 import row_hash_sql
FIELDS=tuple(role_row('current_replacement'));KEY=('source_system','rel_type_id','id')

def lookup_relations(base,overlay,key):
 # Relations are internal generated SQL. External table/version inputs use lookup().
 if set(key)!=set(KEY)|{'lookup_hash'} or not re.fullmatch('[0-9a-f]{64}',key['lookup_hash']):raise ValueError('Exact complete typed key and lowercase SHA256 required')
 for k in ['rel_type_id','id']:
  if type(key[k]) is not int or not 0<=key[k]<2**63:raise ValueError('Nonnegative BIGINT control key required')
 if not isinstance(key['source_system'],str) or not key['source_system']:raise ValueError('Exact source namespace required')
 lit=lambda s:"decode(unhex('"+s.encode().hex()+"'),'UTF-8')"
 where=f"lookup_hash='{key['lookup_hash']}' AND source_system={lit(key['source_system'])} AND rel_type_id={key['rel_type_id']} AND id={key['id']}"
 fields=','.join(FIELDS);carrier='named_struct('+','.join("'"+f+"',"+f for f in FIELDS+('is_deleted',))+')'
 candidates=f'SELECT {fields},false AS is_deleted FROM {base} WHERE {where} UNION ALL SELECT {fields},is_deleted FROM {overlay} WHERE {where}'
 # Aggregate yields one nullable winner even for missing/deleted keys; conflict
 # guard cannot vanish merely because no live rows would be returned.
 per_version=f'SELECT entity_version,count(DISTINCT {row_hash_sql(FIELDS+("is_deleted",))}) AS variants,first({carrier}) AS carrier FROM ({candidates}) GROUP BY entity_version'
 winner=f"SELECT CASE WHEN max(variants)>1 THEN raise_error('ASHLAR_OVERLAY_VERSION_CONFLICT') ELSE max_by(carrier,entity_version) END AS winner FROM ({per_version})"
 return f"SELECT CASE WHEN winner IS NULL THEN 'missing' WHEN winner.is_deleted THEN 'deleted' ELSE 'live' END AS outcome,CASE WHEN winner IS NULL THEN NULL ELSE {row_hash_sql(['winner.'+f for f in FIELDS])} END AS carrier_digest,CASE WHEN winner IS NULL THEN NULL ELSE {row_hash_sql(['winner.'+f for f in FIELDS+('is_deleted',)])} END AS marker_digest,winner.entity_version AS entity_version FROM ({winner})"

def lookup(base_table,base_version,overlay_table,overlay_version,key):return lookup_relations(pin(base_table,base_version),pin(overlay_table,overlay_version),key)
