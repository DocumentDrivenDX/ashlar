from pathlib import Path
import subprocess,json,hashlib,time,sys,zipfile,importlib.metadata as metadata
from ashlar_host.pack_native_cli import PROFILE,SOURCE_PROFILE,parse_finite_pack_invocation
import ashlar_host.pack_configuration as module
root=Path(__file__).parent;console=root/'environment/bin/ashlar';selectors={name:root/'operator-inputs'/name for name in ('publication','output','jars','pack_file','model','graph','producer_source','bun','git')};environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','PYTHONDONTWRITEBYTECODE':'1'};routes=[('archaeology','publish',None),('ecology','publish',None),('payments','publish',None),('archaeology','query',None),('ecology','query',None),('payments','query','payments-count'),('payments','query','payments-scenario')];processes=[];start=time.monotonic()
for ordinal,(pack,mode,route) in enumerate(routes+[('supply-chain','publish',None)]):
 argv=[str(console),'configure-finite-pack','--pack',pack,'--mode',mode,'--profile',PROFILE if mode=='publish' else SOURCE_PROFILE,'--budget-profile','small-local']
 for name,path in selectors.items():argv+=['--'+name.replace('_','-'),str(path)]
 if mode=='query':argv+=['--index',str(root/'operator-inputs/index'),'--installation',str(root/'operator-inputs/installation')]
 if route:argv+=['--query-profile',route]
 elapsed=time.monotonic();p=subprocess.run(argv,env=environment,cwd=root,capture_output=True,timeout=5);seconds=time.monotonic()-elapsed;assert len(p.stdout)<=65536 and len(p.stderr)<=65536
 (root/(str(ordinal)+'.stdout')).write_bytes(p.stdout);(root/(str(ordinal)+'.stderr')).write_bytes(p.stderr)
 if ordinal<7:
  assert p.returncode==0 and not p.stderr;v=json.loads(p.stdout);invocation=parse_finite_pack_invocation(p.stdout,mode,query_profile=route);assert invocation.native.producer.definition.name==pack
  for name,path in selectors.items():assert (v['producer'][{'producer_source':'source','bun':'bun','git':'git'}[name]]if name in('producer_source','bun','git')else v[name])==str(path)
  assert v['producer']=={'source':str(selectors['producer_source']),'bun':str(selectors['bun']),'git':str(selectors['git']),'timeout_seconds':30,'maximum_output_bytes':1048576,'maximum_receipt_bytes':33554432};assert v['operation_capacity']['max_intent_bytes']==8388608
  if mode=='query':assert v['query']=={'index':str(root/'operator-inputs/index'),'installation':str(root/'operator-inputs/installation'),'maximum_artifact_bytes':1048576,'maximum_rows':128,'maximum_cell_bytes':1048576,'maximum_total_cell_bytes':16777216,'maximum_observation_bytes':16777216,'maximum_public_source_bytes':2097152}
 else:assert p.returncode==2 and not p.stdout and p.stderr==b'ashlar finite-pack configuration refused\n'
 processes.append({'pack':pack,'mode':mode,'query_profile':route,'argv':argv,'exit_code':p.returncode,'seconds':seconds,'stdout_sha256':hashlib.sha256(p.stdout).hexdigest(),'stderr_sha256':hashlib.sha256(p.stderr).hexdigest()})
assert not (root/'operator-inputs').exists();assert not any(name in sys.modules for name in('pyspark','duckdb','psycopg','graphframes'));wheel=next((root/'wheels').glob('*.whl'));modulefile=Path(module.__file__);clifile=modulefile.parents[1]/'ashlar/cli.py'
with zipfile.ZipFile(wheel)as z:
 assert z.read('ashlar_host/pack_configuration.py')==modulefile.read_bytes();assert z.read('ashlar/cli.py')==clifile.read_bytes()
result={'outcome':'passed','positive_routes':7,'unsupported_refusals':1,'total_cli_seconds':time.monotonic()-start,'processes':processes,'operator_resources_created':False,'installed_parser_roundtrip':True,'exact_presets_and_selectors':True,'sdk_modules_loaded_in_validation_process':False,'wheel':{'path':str(wheel),'bytes':wheel.stat().st_size,'sha256':hashlib.sha256(wheel.read_bytes()).hexdigest()},'modules':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in(modulefile,clifile)],'console_sha256':hashlib.sha256(console.read_bytes()).hexdigest(),'record_sha256':hashlib.sha256(metadata.distribution('ashlar-graph-toolkit').read_text('RECORD').encode()).hexdigest(),'scope':'Fresh installedconsole configuration generation only; no source/model validation, SDK client, compiler, native query, resource initialization or authority admission.'};(root/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'outcome':'passed','routes':7,'refusals':1,'seconds':result['total_cli_seconds']}))
