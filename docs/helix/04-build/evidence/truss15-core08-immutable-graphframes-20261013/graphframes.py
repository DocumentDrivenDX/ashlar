import sys,json,time,tempfile,os
from pathlib import Path
root=Path(__file__).parent;(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
sys.path.insert(0,'/private/tmp/ashlar-truss15-parallel-example-20261013-a/ashlar-source/src')
from ashlar_host.graphframes_export import run_graphframes_export_command
start=time.monotonic();result=None
try:
 value=run_graphframes_export_command('/private/tmp/ashlar-truss15-core08-export-native-20261013-a/graphframes.json',root/'graphframes-result.json',maximum_hops=2)
 if (value['nodes'],value['edges'],len(value['one_hop']),len(value['two_hop']))!=(1,0,0,0)or any(row['out_degree']!=0 for row in value['out_degrees'])or any((row['path_count'],row['distinct_destinations'])!=(0,0)for row in value['two_hop_counts']):raise ValueError('Independent isolate graph counts differ')
 if len(value['out_degrees'])!=1 or len(value['two_hop_counts'])!=1:raise ValueError('Complete isolated-node counts required')
 export=json.loads(Path('/private/tmp/ashlar-truss15-core08-export-native-20261013-a/export.json').read_bytes())
 release=json.loads(Path(export['release']['path']).read_bytes())
 if value['release_sha256']!=export['release']['sha256']or value['custody_sha256']!=export['custody']['sha256']:raise ValueError('Original opaque release/custody changed')

 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'publication_id':value['publication_id'],'nodes':1,'edges':0,'one_hop':0,'two_hop':0,'qualification':'Exact immutable known four scalar opaque carriers and one isolated node, source identity and release/custody pair. No unknown extension semantics, numeric filtering or protectedACK/UC.','release_sha256':value['release_sha256'],'custody_sha256':value['custody_sha256'],'runtime':value['runtime']}
except BaseException as e:
 result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(e).__name__};raise
finally:
 (root/'run-result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
