import json,hashlib,subprocess,platform,os
from pathlib import Path
from local_delta_custody import encoded
from run_pack_left_weft import prepare_original_case,opt_in,tagged_oracle,admit_matched_source_strings,COMPILER_SHA
from run_pack_publication_weft import compiler_request,original_ports
from run_commerce_arithmetic_weft import compile_original
from weft_left_plan import admit_left_plan
pub=Path('/private/tmp/ashlar-archaeology-native-publication-20261009-a');out=Path('/private/tmp/ashlar-left-native-plan-20261009-a');runtime=Path('/private/tmp/ashlar-left-runtime-a6a58c');sha=lambda b:hashlib.sha256(b).hexdigest()
report=json.loads((pub/'report.json').read_bytes());model=(pub/'original-ontology.json').read_bytes();graph=(pub/'original-graph.json').read_bytes();bindings=json.loads((pub/'development-bindings.json').read_bytes());aliases={t:t.replace('local.','spark_catalog.',1)for t in json.loads(report['native_manifest']['table_versions_json'])}
case=prepare_original_case();request=opt_in(compiler_request('archaeology',case['original_scenario']['sql'],model,graph,bindings,report['native_manifest'],report['table_registry'],aliases));compiler=Path('/private/tmp/ashlar-weft-left-integration-final-20261009-a/weft-runtime');artifact=compile_original(compiler,request,COMPILER_SHA);admit_left_plan(artifact,json.loads(request['target']['bindingJson']),request['modules'])
for name,v in [('original-request.json',request),('original-artifact.json',artifact)]: (out/name).write_text(encoded(v)+'\n')
matched=admit_matched_source_strings(graph);expected=tagged_oracle(case['original_graph_expected'])
# Rebuild the exact original transaction/public custody without a native reader.
converter,native,admission_type,oracle=original_ports('archaeology');batch,rebuilt=converter.build_transaction(model,graph,source_system=native.SOURCE_SYSTEM)
assert rebuilt==bindings
admission=admission_type(batch,bindings,pub/'original-ontology.json',pub/'original-graph.json',pub/'development-bindings.json',pub/'public-dataset.json');admission.metadata()
originalfiles=[]
for row in report['table_registry']:
 for p in sorted(Path(row['path']).rglob('*')):
  if p.is_file():originalfiles.append({'path':str(p),'size':p.stat().st_size,'sha256':sha(p.read_bytes())})
(out/'original-native-files.json').write_text(json.dumps({'files':originalfiles},indent=2)+'\n')
source_guard=Path('/private/tmp/ashlar-weft-mathematical-integer-f05/scripts/local/public_integer_source.ts');umf=Path('/private/tmp/ashlar-umf-dataset-45473');compact=Path('/private/tmp/ashlar-umf-dataset-compact-fc78')
def gitfacts(root):
 head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip();dirty=subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=root,text=True)
 return {'path':str(root),'head':head,'trackedStatus':dirty,'scope':'Tracked checkout cleanliness; untracked dependencies not semantic source'}
assert gitfacts(umf)['head']=='c7c95e1c4ea5b72541f47fa0350ca467ff02f395' and not gitfacts(umf)['trackedStatus']
assert gitfacts(compact)['head']=='a95c3ec18a8f904decde884a4fa252988d2a5b0f' and not gitfacts(compact)['trackedStatus']
jars=[{'path':str(p),'sha256':sha(p.read_bytes()),'size':p.stat().st_size}for p in sorted(Path('/private/tmp/ashlar-delta4-jars').glob('*.jar'))]
p={'format':'ashlar-original-left-native-plan/0.1','qualification':'Pre-run source/case proof only; no native LEFT result or authority','sourceCommit':'a6a58c6659f104ebf0e9831226e08c2037dd52ca','compilerSha256':COMPILER_SHA,'compilerRealizationBase':'ee90571a5aa67b6d2e6069220f6e1cc0eec3822c','officialIntegratedWeftRevision':'6f003242dd3e3b2ffb030e6dc8f05aa3c70fc34b','ciGates':{'ashlar':'38021356288 SUCCESS','weft':'38021143166 terminal SUCCESS required; pending root confirmation'},'case':case,'expectedTaggedRows':expected,'matchedSourceStringWitness':matched,'publicationManifest':report['native_manifest'],'tableRegistry':report['table_registry'],'sourceSystem':native.SOURCE_SYSTEM,'modelSha256':sha(model),'graphSha256':sha(graph),'publicDatasetSha256':sha((pub/'public-dataset.json').read_bytes()),'originalNativeFileCount':len(originalfiles),'originalNativeManifestSha256':sha((out/'original-native-files.json').read_bytes()),'protectedAckScope':report['protected_ack_scope'],'sourceSchema':report['source_schema'],'sourceSignatureSha256':report['source_signature_sha256'],'publicDatasetProducer':gitfacts(compact),'publicIntegerSourceGuard':{'path':str(source_guard),'sha256':sha(source_guard.read_bytes()),'producer':gitfacts(umf),'scope':'Mandatory callback configured; this String-only LEFT subset has no publicSourceOnly check'},'jars':jars,'originalRequestSha256':sha((out/'original-request.json').read_bytes()),'originalArtifactSha256':sha((out/'original-artifact.json').read_bytes()),'budgets':{'master':'local[1]','driverMemory':'512m','shufflePartitions':1,'snapshotPartitions':1,'newSourceRows':0,'cloud':False}}
(out/'plan.json').write_text(json.dumps(p,indent=2)+'\n')
files=json.loads((runtime/'runtime-source-manifest.json').read_bytes())['files'];hosts=['tools/run_pack_left_weft.py','tools/weft_left_plan.py','tools/weft_field_plan.py','tools/run_commerce_arithmetic_weft.py','tools/run_commerce_publication_weft.py']
(out/'host-source-receipt.json').write_text(json.dumps({'sourceCommit':p['sourceCommit'],'files':[f for f in files if f['path']in hosts]},indent=2)+'\n')
print('plan',sha((out/'plan.json').read_bytes()),'original files',len(originalfiles),'tagged',expected)
