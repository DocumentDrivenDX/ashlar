import base64,contextlib,hashlib,json,pathlib,sys
from collections import Counter
from pyspark.sql import SparkSession
ROOT=pathlib.Path(__file__).resolve().parents[6]; R59=pathlib.Path('/private/tmp/ashlar-r36-truss052-page-sequence-20261101'); D=R59/'delta'; PUB='truss-graph-678967ac3deeafa90b514ae8a5a1b05f78b0b32e5f89135f4c67205fede001ea'
sys.path.insert(0,str(ROOT/'src'))
from ashlar.native import SQLResult,NativeBackend
from ashlar.manifest import manifest_pin_vector
from ashlar.graph_release import read_graph_release
obs=json.loads((pathlib.Path(__file__).parent/'r59-vector-observation.json').read_text())
roles=('object_current','edge_current','tombstone','whole_source_history')
paths={r:str(D/r) for r in roles}; table_names={r:'local.trussgraph.'+r for r in roles}; name_to_path={table_names[r]:paths[r] for r in roles}
name_to_path['local.trussgraph.manifest']=str(D/'manifest')
JARS='/private/tmp/ashlar-r16-delta4-native-smoke-20261029/attempt-84c7b6002712486495c5dd66bfa0b8d6/jars/delta-spark_4.2_2.13-4.4.1.jar,/private/tmp/ashlar-r16-delta4-native-smoke-20261029/attempt-84c7b6002712486495c5dd66bfa0b8d6/jars/delta-storage-4.4.1.jar'
spark=(SparkSession.builder.master('local[1]').appName('ashlar-r59-graph-release-readonly').config('spark.ui.enabled','false').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.jars',JARS).config('spark.sql.warehouse.dir','/private/tmp/ashlar-local-release-native-read/warehouse-export').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false').config('spark.sql.legacy.parameterSubstitution.constantsOnly','true').getOrCreate());spark.sparkContext.setLogLevel('ERROR')
def replace_tables(sql):
 for name,path in name_to_path.items(): sql=sql.replace('`'+'`.`'.join(name.split('.'))+'`',f'delta.`{path}`')
 return sql
class Executor:
 def query(self,sql,parameters):
  frame=spark.sql(replace_tables(sql),args=dict(parameters)) if parameters else spark.sql(replace_tables(sql))
  cols=tuple((f.name,f.dataType.typeName().upper()) for f in frame.schema.fields)
  return SQLResult([r.asDict(recursive=True) for r in frame.collect()],cols)
executor=Executor()
def detail(role): return executor.query('DESCRIBE DETAIL '+('`local`.`trussgraph`.`manifest`' if role=='manifest' else '`local`.`trussgraph`.`'+role+'`'),{}).rows[0]
manifest_table=table_names['object_current'].rsplit('.',1)[0]+'.manifest'
manifest_uuid=detail('manifest')['id']
meta=detail('object_current');
record=executor.query('SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json,cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM `local`.`trussgraph`.`manifest` WHERE publication_id = :publication_id',{'publication_id':PUB}).rows
if len(record)!=1: raise RuntimeError('exact manifest descriptor absent')
manifest=dict(record[0]); uuid_map={t:obs['roles'][t.rsplit('.',1)[-1]]['uuid'] for t in json.loads(manifest['table_versions_json'])}
vector=manifest_pin_vector(manifest,uuid_map,authority='private-local-process')
context=object(); expected_heads={r:obs['roles'][r]['version'] for r in roles}
def current_heads():
 return {r:int(executor.query('DESCRIBE HISTORY `local`.`trussgraph`.`'+r+'` LIMIT 1',{}).rows[0]['version']) for r in roles}
class Policy:
 def authorize(self,c,p,tables):
  if c is not context or p!=PUB or set(tables)!=set(vector.targets): raise PermissionError('exact local publication selection required')
 def validate_descriptor(self,d,c):
  if c is not context or dict(d.raw)!=manifest: raise PermissionError('manifest differs from retained source page receipt')
 def validate_snapshot(self,t,u,v,columns):
  if t not in vector.targets or (u,v)!=vector.targets[t] or tuple(columns)!=schemas[t]: raise PermissionError('complete exact vector differs')
 def bind_descriptor(self,d,v,c):
  self.authorize(c,PUB,tuple(vector.targets))
  if d.profile!='ashlar-delta/0.4-xxhash64' or v!=vector or dict(d.raw)!=manifest: raise PermissionError('exact `.4` source manifest/vector required')
  return None
 def authorize_graph_row(self,d,table,row,c):
  self.bind_descriptor(d,vector,c)
 def authorize_graph_result(self,d,nodes,edges,c):
  self.bind_descriptor(d,vector,c)
  expected=source_bags()
  def nkey(r): return (r['source_system'],r['type_id'],r['id'],r['entity_version'],r['schema_revision'],r['props_json'],r['retained_json'])
  def ekey(r): return (r['source_system'],r['rel_type_id'],r['id'],r['source_type'],r['source_id'],r['target_type'],r['target_id'],r['entity_version'],r['schema_revision'],r['props_json'],r['retained_json'],r['order_key'])
  if Counter(map(nkey,nodes))!=Counter(expected['nodes']) or Counter(map(ekey,edges))!=Counter(expected['edges']): raise PermissionError('complete source-page-derived graph rows differ')
  by_key={(r['source_system'],r['type_id'],r['id']):r['graph_id'] for r in nodes}
  for r in edges:
   if r['src']!=by_key[(r['source_system'],r['source_type'],r['source_id'])] or r['dst']!=by_key[(r['source_system'],r['target_type'],r['target_id'])]: raise PermissionError('source edge endpoint identities differ')
class Pins:
 @contextlib.contextmanager
 def hold(self,v,context=None):
  if context is not globals()['context'] or v!=vector: raise PermissionError('exact R59 vector required')
  opening=current_heads()
  if opening!=expected_heads: raise PermissionError('opening Delta heads changed')
  yield
  if current_heads()!=opening: raise PermissionError('closing Delta heads changed')
from ashlar.native import NativeBackend
schemas={}
for r in roles:
 type_names={'bigint':'LONG','string':'STRING','timestamp':'TIMESTAMP'}
 schemas[table_names[r]]=tuple((x[0],type_names[x[1]]) for x in obs['roles'][r]['schema'])
policy=Policy();backend=NativeBackend(executor,policy,manifest_table,manifest_uuid,schemas)
def source_bags():
 nodes=[];edges=[]; installation=next(iter(json.loads(manifest['schema_revisions_json'])))
 for page_path in sorted(D.glob('source-page-*.json')):
  page=json.loads(page_path.read_text()); content=json.loads(base64.b64decode(page['content']))
  for change in content['changes']:
   event=json.loads(base64.b64decode(change['new_value']['bytesBase64'])); after=event['after']; identity=after['identity']; kind=after['kind']
   props={str(p['propertyId']): (p['value']['text'] if p['value']['kind']=='string' else int(p['value']['text']) if p['value']['kind']=='integer' else None) for p in after.get('properties',[])}
   raw=json.dumps(props,ensure_ascii=False,sort_keys=True,separators=(',',':'))
   common=(installation,str(identity['typeId']),str(identity['id']),str(after['recordVersion']),'1',raw,'{}')
   if kind=='object': nodes.append(common)
   elif kind=='edge':
    src=after['source'];dst=after['target'];order=after.get('orderKey',{})
    edges.append((installation,str(identity['typeId']),str(identity['id']),str(src['typeId']),str(src['id']),str(dst['typeId']),str(dst['id']),str(after['recordVersion']),'1',raw,'{}',order.get('text') if order.get('state')=='text' else None))
 if (len(nodes),len(edges))!=(6,3): raise RuntimeError('source page inventory not exact (6 objects / 3 edges)')
 return {'nodes':nodes,'edges':edges}
release=read_graph_release(executor,backend,Pins(),vector,policy,publication_id=PUB,node_table=table_names['object_current'],edge_table=table_names['edge_current'],context=context,supported_profiles=['ashlar-delta/0.4-xxhash64'],supported_revisions={next(iter(json.loads(manifest['schema_revisions_json']))):['1']},max_nodes=6,max_edges=3)
result=json.loads(release.payload)
out=pathlib.Path('/private/tmp/ashlar-local-release-native-read/r59-graph-release.json');out.write_bytes(release.payload)
print(json.dumps({'profile':manifest['profile_version'],'release_sha256':release.sha256,'release_bytes':len(release.payload),'nodes':len(result['nodes']),'edges':len(result['edges']),'four_role_snapshots':len(result['snapshots']),'capabilities':result['mapping']['capabilities'],'lookup_hash_profile':result['mapping'].get('lookupHashProfile'),'engine_execution':result['mapping']['capabilities']['engineExecution'],'source_page_hashes':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(D.glob('source-page-*.json'))},'native_heads':current_heads(),'output':str(out)},sort_keys=True));spark.stop()
