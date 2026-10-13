import os,sys,json,tempfile,time
from pathlib import Path
import psycopg,psycopg.pq,jsonschema,referencing
root=Path(__file__).parent
(root/'ipc').mkdir();os.environ['TMPDIR']=str(root/'ipc');tempfile.tempdir=None
os.environ.update(JAVA_HOME='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home',PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
for p in reversed(json.loads(Path('/private/tmp/ashlar-commerce-graphframes-installed-prep-j/command-j.json').read_bytes())['sdkRoots']['canonical']):sys.path.insert(0,p)
sys.path.insert(0,'/private/tmp/ashlar-truss-sdk-pinned-1f203768/packages/python/src');sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from ashlar_host.payments_scenario_cli import run_query_payments_scenario_command
from ashlar_host.truss_example import private_file
start=time.monotonic();result=None
try:
 config=json.loads(Path('/private/tmp/ashlar-payments-native-publication-20261013-a/configuration.json').read_bytes())
 config.update(profile='ashlar-original-finite-pack-invocation/0.2',mode='query',output=str(root/'observations'),query={'index':'/private/tmp/weft-042-integration-b/distributions/index.json','installation':'/private/tmp/ashlar-042-installed-cli-actual-b/qualification/installation','maximum_artifact_bytes':1048576,'maximum_rows':10,'maximum_cell_bytes':10000,'maximum_total_cell_bytes':100000,'maximum_observation_bytes':1048576,'maximum_public_source_bytes':1048576})
 private_file(root/'configuration.json',config)
 wrapper=run_query_payments_scenario_command(root/'configuration.json',case='currency')
 report=json.loads(Path(wrapper['report']).read_bytes())
 if report['case']!='currency' or report['outerCleanupComplete']is not True or report['invocationCustodyComplete']is not True:raise ValueError('Original currency case/closure differs')
 result={'outcome':'passed','duration_seconds':round(time.monotonic()-start,3),'case':'currency','report':wrapper['report'],'qualification':wrapper['qualification']}
except BaseException as error:result={'outcome':'failed','duration_seconds':round(time.monotonic()-start,3),'error_type':type(error).__name__};raise
finally:private_file(root/'run-result.json',result);print(json.dumps(result),flush=True)
