import sys,json,time,tempfile,os,hashlib
from pathlib import Path
root=Path(__file__).parent;(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from ashlar_host.graphframes_export import run_graphframes_export_command
from ashlar_host.graphframes_export_setup import run_configure_graphframes_export_command
start=time.monotonic();result=None
try:
 original=Path('/private/tmp/ashlar-truss-core08-evolution-export-20261013-c')
 jars=tuple('/private/tmp/ashlar-graphframes-installed-20261010-c/jars/'+name for name in ('io.graphframes_graphframes-spark3_2.12-0.12.3.jar','io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar'))
 values=[];configurations=[];nodes=[]
 for label,fields in (('seed',4),('evolved',5)):
  report=original/(label+'-export.json');configuration=root/(label+'-graphframes.json')
  run_configure_graphframes_export_command(str(report),hashlib.sha256(report.read_bytes()).hexdigest(),configuration,jars=jars,java_home='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home')
  configurations.append(json.loads(configuration.read_bytes()))
  node=json.loads((original/(label+'-export.json-release.json')).read_bytes())['nodes'][0];nodes.append(node)
  if len(json.loads(node['props_json']))!=fields:raise ValueError('Original exact field inventory differs')
  value=run_graphframes_export_command(configuration,root/(label+'-graphframes-result.json'),maximum_hops=2)
  if (value['nodes'],value['edges'])!=(1,0)or value['one_hop']or value['two_hop']or value['out_degrees']!=[{'id':node['graph_id'],'out_degree':0}]or value['two_hop_counts']!=[{'id':node['graph_id'],'path_count':0,'distinct_destinations':0}]:raise ValueError('Exact isolated node/no-edge GraphFrames result differs')
  values.append(value)
 if nodes[0]['graph_id']!=nodes[1]['graph_id']:raise ValueError('Original evolution graph identity differs')
 mixed=[]
 for left,right in ((0,1),(1,0)):
  config={**configurations[left],'custody_path':configurations[right]['custody_path'],'custody_sha256':configurations[right]['custody_sha256']}
  path=root/('mixed-'+str(left)+'.json');path.write_text(json.dumps(config))
  try:run_graphframes_export_command(path,root/('mixed-'+str(left)+'-result.json'),maximum_hops=2)
  except (ValueError,PermissionError)as error:mixed.append(type(error).__name__)
  else:raise ValueError('Mixed graph release/custody admitted')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'nodes_per_release':1,'edges_per_release':0,'field_counts':[4,5],'stable_graph_id':nodes[0]['graph_id'],'mixed_pairs_refused':mixed,'runtime':values[0]['runtime'],'release_digests':[v['release_sha256']for v in values],'custody_digests':[v['custody_sha256']for v in values],'qualification':'Actual public GraphFrames exact canonical node/edge occurrence-bag admission and isolated-node empty one/two-hop paths/counts over two immutable0.8 exports; original four/five props JSON carriers remain opaque STRING. Public engine entrypoint refused both mixed pairs before result release. No source access, mutations, ACK, ingest or graph numeric filtering qualification.'}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:(root/'run-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
