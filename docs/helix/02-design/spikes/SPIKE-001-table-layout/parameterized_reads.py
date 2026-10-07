"""Paired literal-hash/native-parameter read comparison on existing r85 version2.
Identical full carrier outputs, cached results disabled, balanced AB order.
"""
import pathlib,json,hashlib
from driver_sql import DriverClient
B=pathlib.Path(__file__).resolve().parent;O=B/'out/native/ashlar_parameter_reads_20261006_r87';c=DriverClient(O)
F='client_dev.ashlar_scale_20261006_r85.edge_current'
query=f'SELECT * FROM {F} VERSION AS OF 2 WHERE lookup_hash=:lookup AND source_system=:source AND rel_type_id=:relationship AND id=:identity'
for i in range(50):
 key=1+(i*15485863)%20000000;source='other' if key%10==0 else 'pilot';typ=key%32+1
 h=hashlib.sha256(json.dumps({'source_system':source,'rel_type_id':typ,'id':key},ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
 params={'lookup':h,'source':source,'relationship':typ,'identity':key}
 literal=f"SELECT * FROM {F} VERSION AS OF 2 WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={typ} AND id={key}"
 results={}
 for mode in (['literal','parameter'] if i%2==0 else ['parameter','literal']):
  results[mode]=c.sql(mode+'-'+str(i),literal if mode=='literal' else query,parameters=params if mode=='parameter' else None,tag=False)
 assert len(results['literal'])==1 and results['literal']==results['parameter'] and results['literal'][0][2]==str(key)
c.history();c.close()
(O/'summary.json').write_text(json.dumps({'state':'100 full-carrier result checks pass; final cache/engine metrics need audit','pairs':50,'edge_version':2,'changes':'Client computes qualified ordered-tuple hash; compares literal predicates against static native named-parameter SQL text; no varying SQL comment','driver':'4.3.0','scope':'Existing 20M compressed-carrier table, warm data; not higher-entropy/billion-scale/publication admission','docs':'https://docs.databricks.com/aws/en/dev-tools/python-sql-connector'},indent=2)+'\n')
