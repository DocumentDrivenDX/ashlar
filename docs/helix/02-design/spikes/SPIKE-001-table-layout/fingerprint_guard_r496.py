"""Private update/delete guard using enforced full-carrier digest; import-safe."""
import re
from generated_fingerprint_r491 import expression
from overlay_sql_r395 import FIELDS
from normalized_apply_sql_r276 import pin

def qualified_expression(alias):
 if not re.fullmatch('[a-z][a-z0-9_]*',alias):raise ValueError('Internal simple alias required')
 return re.sub(r'\b('+'|'.join(FIELDS)+r')\b',lambda m:alias+'.'+m.group(0),expression())

def merge(target,source):
 pin(target,0)
 identity=' AND '.join(f's.after_{f} <=> s.{f}' for f in ['source_system','rel_type_id','id','lookup_hash'])
 valid=f'(b.carrier_fingerprint <=> {qualified_expression("s")}) AND s.entity_version IS NOT NULL AND (s.is_delete OR (s.after_entity_version=s.entity_version+1 AND ({identity})))'
 assignments=','.join(f'b.{f}=s.after_{f}' for f in FIELDS)
 insert=','.join("CAST(raise_error('ASHLAR_FINGERPRINT_MISSING_PREDECESSOR') AS STRING)" if f=='source_system' else 's.after_'+f for f in FIELDS)
 return f"MERGE INTO {target} b USING ({source}) s ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id WHEN MATCHED AND CASE WHEN ({valid}) THEN s.is_delete ELSE CAST(raise_error('ASHLAR_FINGERPRINT_PREDECESSOR_MISMATCH') AS BOOLEAN) END THEN DELETE WHEN MATCHED THEN UPDATE SET {assignments} WHEN NOT MATCHED THEN INSERT ({','.join(FIELDS)}) VALUES ({insert})"
