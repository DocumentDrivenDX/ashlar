"""Read-only protocol, grants and maintenance inventory for the private graph."""
import json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(B))
from persistent_sql import Client
OUT=B/'out/native/read_authority_20261008';c=Client(OUT)
N='client_dev.ashlar_recoverable_graph_20261008'
registration=json.loads((B/'out/native/recoverable_graph_20261008/summary.json').read_text())['table_uuids']
def named(label,sql):
    rows=c.sql(label,sql);cols=c.records[-1]['response'].get('manifest',{}).get('schema',{}).get('columns',[])
    return [dict(zip([x['name'] for x in cols],r)) for r in rows]
principal=named('authenticated-principal','SELECT current_user() AS principal')[0]['principal']
inventory=[]
for table,uuid in registration.items():
    detail=named('snapshot-protocol','DESCRIBE DETAIL '+table)
    assert len(detail)==1 and detail[0]['id']==uuid
    grants=named('table-grants','SHOW GRANTS ON TABLE '+table)
    properties=named('table-properties','SHOW TBLPROPERTIES '+table)
    inventory.append({'table':table,'detail':detail[0],'grants':grants,'properties':properties})
ancestors={target:named('ancestor-grants','SHOW GRANTS ON '+kind+' '+target) for kind,target in [('SCHEMA',N),('CATALOG','client_dev')]}
# Capture actual operation metadata; absence of an OPTIMIZE/VACUUM entry is not
# proof of disabled future maintenance or a read/write fence.
for item in inventory:item['history']=named('maintenance-history','DESCRIBE HISTORY '+item['table'])
summary={'state':'observed','authenticated_principal':principal,'tables':inventory,'ancestor_grants':ancestors,'qualification':'Read-only native inventory. Current UUID/protocol/properties/grants/history observations do not certify future retention, hierarchy-wide writer exclusion or protocol compatibility with external engines. No grant/property/retention changes, publication or data writes.'}
(OUT/'inventory.json').write_text(json.dumps(summary,indent=2)+'\n')
for item in inventory:
    d=item['detail'];print(json.dumps({'table':item['table'],'features':d.get('tableFeatures'),'reader':d.get('minReaderVersion'),'writer':d.get('minWriterVersion'),'grants':item['grants'],'properties':item['properties']}))
print('Ancestor grants: '+json.dumps(ancestors))
