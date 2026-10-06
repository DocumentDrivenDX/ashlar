"""Bounded native release materialization from actual pinned canonical versions."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent
A=B/'adapters/release-r66'
plan=json.loads((A/'mapping-plan.json').read_text())
N='client_dev.ashlar_layout_v02_20261006_r65'
out=B/'out/native/ashlar_layout_v02_release_20261006_r68'
c=Client(out); versions={}; results={}
def key(prefix,typ,ident):
 return f"concat('{prefix}',length(source_system),':',source_system,':',cast({typ} AS STRING),':',cast({ident} AS STRING))"
for name,meta in plan['tables'].items():
 target=N+'.release_r66_'+name
 assert c.sql('absent-'+name,f"SHOW TABLES IN {N} LIKE 'release_r66_{name}'")==[], 'Existing target: recover prior materialization instead of replacing'
 if meta['kind']=='node':
  typ=int(name.split('_')[1]); source=N+'.object_current'
  select=f"source_system,cast(type_id AS STRING) type_id,cast(id AS STRING) native_id,logical_key_json,props_json,retained_json,{key('N','type_id','id')} node_key,{key('N','type_id','id')} id"
  predicate=f'type_id={typ}'
 else:
  _,rel,st,tt=name.split('_'); source=N+'.edge_current'
  select=f"source_system,cast(rel_type_id AS STRING) rel_type_id,cast(id AS STRING) native_id,cast(source_type AS STRING) source_type,cast(source_id AS STRING) source_id,cast(target_type AS STRING) target_type,cast(target_id AS STRING) target_id,props_json,retained_json,{key('E','rel_type_id','id')} edge_key,{key('E','rel_type_id','id')} id,{key('N','source_type','source_id')} src,{key('N','target_type','target_id')} dst"
  predicate=f'rel_type_id={int(rel)} AND source_type={int(st)} AND target_type={int(tt)}'
 c.sql('materialize-'+name,f"CREATE TABLE {target} USING DELTA AS SELECT {select} FROM {source} VERSION AS OF {int(plan['canonical_versions'][source])} WHERE {predicate}")
 versions[target]=int(c.sql('version-'+name,f'DESCRIBE HISTORY {target} LIMIT 1')[0][0])
 expected=json.loads((A/meta['file']).read_text())
 # Compare every exported field, independent of row order.
 fields=list(expected[0])
 rows=c.sql('verify-'+name,f"SELECT {','.join(fields)} FROM {target} VERSION AS OF {versions[target]}")
 actual=[dict(zip(fields,r)) for r in rows]
 normalize=lambda records: sorted(json.dumps(r,sort_keys=True,separators=(',',':')) for r in records)
 assert normalize(actual)==normalize(expected), name
 results[target]={'rows':len(rows),'exact_export_fields':'passed','version':versions[target]}
summary={'state':'passed','publication':plan['publication'],'canonical_versions':plan['canonical_versions'],'projection_versions':versions,'tables':results,'scope':'Four small native Delta release projections; exhaustive exact export-field comparison. No immutable ACL enforcement, atomic engine activation, external-engine execution, performance or scale claim.'}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print('Four native Delta projections passed exact export parity',flush=True)
