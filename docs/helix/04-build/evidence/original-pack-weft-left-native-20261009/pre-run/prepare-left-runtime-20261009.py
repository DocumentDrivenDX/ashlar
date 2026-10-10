from pathlib import Path
import subprocess,json,hashlib,os,shutil
repo=Path('/Users/erik/Projects/ashlar');commit='a6a58c6659f104ebf0e9831226e08c2037dd52ca';runtime=Path('/private/tmp/ashlar-left-runtime-a6a58c');plan=Path('/private/tmp/ashlar-left-native-plan-20261009-a');runtime.mkdir();plan.mkdir()
sha=lambda b:hashlib.sha256(b).hexdigest()
files=[]
for raw in subprocess.check_output(['git','ls-tree','-rz','--full-tree',commit],cwd=repo).split(b'\0'):
 if not raw:continue
 header,name=raw.split(b'\t',1);mode,kind,blob=header.decode().split();name=name.decode()
 if name.startswith(('docs/','website/')):continue
 if kind!='blob':raise ValueError('Unsupported tracked runtime object '+name)
 data=subprocess.check_output(['git','cat-file','blob',blob],cwd=repo);dest=runtime/name;dest.parent.mkdir(parents=True,exist_ok=True)
 if mode=='120000':dest.symlink_to(data.decode())
 else:dest.write_bytes(data);dest.chmod(int(mode[-3:],8))
 files.append({'path':name,'gitMode':mode,'gitBlob':blob,'size':len(data),'sha256':sha(data)})
manifest={'format':'ashlar-left-runtime-source/0.1','sourceCommit':commit,'scope':'All committed tracked roots except nonruntime docs/website; complete src/tools/sql/examples/tests/rootconfigs/.github; foreign primary WIP excluded','fileCount':len(files),'files':files}
(runtime/'runtime-source-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
shutil.copy2('/private/tmp/ashlar-left-engine-observation-20261009-a.json',plan/'engine-observation.json')
command=f'''Working directory: {runtime}

PYTHONPATH={runtime}/src:{runtime}/tools:/private/tmp/ashlar-spark4-python39-bridge \\
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \\
SPARK_LOCAL_IP=127.0.0.1 \\
PYSPARK_PYTHON=/private/tmp/ashlar-db-client/bin/python \\
PYSPARK_DRIVER_PYTHON=/private/tmp/ashlar-db-client/bin/python \\
/private/tmp/ashlar-db-client/bin/python {runtime}/tools/run_pack_left_weft.py \\
  --archaeology /private/tmp/ashlar-archaeology-native-publication-20261009-a \\
  --output /private/tmp/ashlar-pack-left-weft-20261009-a \\
  --jars /private/tmp/ashlar-delta4-jars \\
  --compiler /private/tmp/ashlar-weft-left-integration-final-20261009-a/weft-runtime \\
  --source-guard /private/tmp/ashlar-weft-mathematical-integer-f05/scripts/local/public_integer_source.ts \\
  --umf /private/tmp/ashlar-umf-dataset-45473 \\
  > /private/tmp/ashlar-pack-left-weft-20261009-a.log 2>&1
'''
(plan/'run-command.txt').write_text(command)
print(len(files),sha((runtime/'runtime-source-manifest.json').read_bytes()))
