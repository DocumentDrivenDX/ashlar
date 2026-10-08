"""Read one unpublished development object at an explicit retained Delta version.

This diagnostic uses current UC permissions, not publication resolver admission.
It never acknowledges a source, registers pins or manufactures a publication.
"""
import argparse, hashlib, json, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from persistent_sql import Client
from databricks_transport import sql_result
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--version',type=int,required=True)
parser.add_argument('--id',type=int,required=True)
parser.add_argument('--output',type=Path,required=True)
a=parser.parse_args()
if a.version<0 or not -(2**63)<=a.id<2**63:parser.error('Invalid version or signed64 object ID')
if a.output.exists():parser.error('Use a new output directory to preserve original receipts')
c=Client(a.output)
table='ashlar_e2e_private_20261008.runtime_outbox.object_current'
expected='6acb58c4-f660-4f38-8221-1edbb3248eb0'
def query(label,sql,params=None):
    c.sql(label,sql,parameters=params)
    return sql_result(c.records[-1]['response']).rows
def check():
    rows=query('identity','DESCRIBE DETAIL '+table)
    if len(rows)!=1 or rows[0].get('id')!=expected:raise ValueError('Development table replaced')
check()
identity={'source_system':'local-example','type_id':17,'id':a.id}
digest=hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()
rows=query('unpublished-singleton','SELECT source_system,cast(type_id AS STRING) AS type_id,cast(id AS STRING) AS id,cast(entity_version AS STRING) AS entity_version,schema_revision,props_json,retained_json,lookup_hash FROM '+table+' VERSION AS OF '+str(a.version)+' WHERE lookup_hash=:hash AND source_system=:source AND type_id=cast(:type AS BIGINT) AND id=cast(:id AS BIGINT) LIMIT 2',
    [{'name':k,'type':'STRING','value':v} for k,v in {'hash':digest,'source':'local-example','type':'17','id':str(a.id)}.items()])
if len(rows)>1:raise ValueError('Ambiguous singleton')
if rows and any(rows[0].get(k)!=v for k,v in {'source_system':'local-example','type_id':'17','id':str(a.id),'lookup_hash':digest}.items()):raise ValueError('Identity mismatch')
check()
result={'table':table,'uuid':expected,'version':a.version,'row':dict(rows[0]) if rows else None,'qualification':'Unpublished development diagnostic under current UC access. No manifest, active publication pins, retained-file guarantee, source ACK or accepted Truss schema.'}
(a.output/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
