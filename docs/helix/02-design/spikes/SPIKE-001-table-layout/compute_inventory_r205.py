"""Read-only sanitized inventory; never writes compute configuration or credentials."""
import json,datetime
from pathlib import Path
from databricks.sdk import WorkspaceClient
B=Path(__file__).resolve().parent;O=B/'out/compute-inventory-r205.json';assert not O.exists()
w=WorkspaceClient(profile='aidev-cus')
s={'checked_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'warehouses':[],'clusters':[],'qualification':'Read-only inventory, not authorization to provision, resize or attach to arbitrary job compute. Fields deliberately exclude credentials/configuration.'}
for x in w.warehouses.list():
 d=x.as_dict();s['warehouses'].append({k:d.get(k) for k in ['id','name','state','cluster_size','min_num_clusters','max_num_clusters','auto_stop_mins','enable_serverless_compute']})
for x in w.clusters.list():
 d=x.as_dict();s['clusters'].append({k:d.get(k) for k in ['cluster_id','cluster_name','state','spark_version','cluster_source']})
O.write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))
