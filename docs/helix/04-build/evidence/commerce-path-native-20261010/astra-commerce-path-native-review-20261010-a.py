from pathlib import Path
import json,hashlib,subprocess,collections,os,sys
import pyarrow as pa,pyarrow.parquet as pq
from local_delta_custody import encoded
from commerce_source_transaction import build_transaction
from run_commerce_outbox_publication import original_commerce_oracle
from fixture_oracle import fixture_columns
from commerce_path_oracle import original_commerce_path_oracle,COLLECTION,COUNT,DISTINCT
from weft_path_plan import PathAdmissionConfig,admit_path_artifact,SCHEMA_SHA256
from weft_path_schema import make_offline_path_schema_validation
sha=lambda b:hashlib.sha256(b).hexdigest()
read=lambda p:json.loads(Path(p).read_bytes())
meta=lambda p:dict(path=str(p),bytes=Path(p).stat().st_size,sha256=sha(Path(p).read_bytes()))
ROOT=Path('/private/tmp/ashlar-commerce-path-runtime-20261010-a/source');P=Path('/private/tmp/ashlar-indexed-commerce-publication-20261010-c');Q=Path('/private/tmp/ashlar-commerce-path-native-20261010-a');CMD=Path('/private/tmp/ashlar-commerce-path-native-command-20261010-b.json')
assert sha(CMD.read_bytes())=='5c1a5bcae5d91ab580e2136468ac53f6d1092dbd17f490fcf41249ab8691b520'
command=read(CMD);r=read(Q/'report.json');p=read(P/'report.json');model=(P/'original-ontology.json').read_bytes();graph=(P/'original-graph.json').read_bytes();bindings=read(P/'development-bindings.json');g=json.loads(graph)
assert sha((Q/'report.json').read_bytes())=='f3f1ffe1cd79e4bf8875d0d12b044ef6a956d9aa7c4871c35a85c812dc0fe491'
assert sha(model)=='51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a' and sha(graph)=='8052dbcf2cf48a5e344c492d661872bc0a3f80a5c7be033ab5008dea9c86119e'
batch,b2=build_transaction(model,graph,source_system='private-original-commerce-fixture',binding_profile=bindings['profile']);assert b2==bindings
assert batch.begin+b''.join(x.raw for x in batch.records)+batch.commit==(P/'source.jsonl').read_bytes()
expected=original_commerce_oracle(model,graph,bindings,batch,fixture_columns(ROOT));assert expected==p['complete_original_oracle']
bag=lambda rows:collections.Counter(encoded(row)for row in rows)
versions=json.loads(p['native_manifest']['table_versions_json']);tables=[];active_rows={};metadata={}
for item in p['table_registry']:
 role=item['table'].split('.')[-1];path=Path(item['path']);logs=sorted((path/'_delta_log').glob('*.json'));latest=int(logs[-1].stem);version=versions.get(item['table'],latest);active={};md=None
 for log in logs:
  if int(log.stem)>version:continue
  for line in log.read_text().splitlines():
   a=json.loads(line)
   if 'metaData'in a:md=a['metaData']
   if 'add'in a:active[a['add']['path']]=a['add']
   if 'remove'in a:active.pop(a['remove']['path'],None)
 assert md['id']==item['uuid'];metadata[role]=json.loads(md['schemaString']);rows=[]
 for name in active:
  data=pq.ParquetFile(path/name).read();cols={}
  for field in data.schema:
   arr=data[field.name]
   if pa.types.is_timestamp(field.type):
    vals=arr.cast(pa.int64()).to_pylist();unit=field.type.unit;assert unit in('us','ns')
    if unit=='ns':assert all(v is None or v%1000==0 for v in vals);vals=[None if v is None else v//1000 for v in vals]
   else:vals=arr.to_pylist()
   cols[field.name]=[str(v)if type(v)is int else v for v in vals]
  rows.extend({k:vals[i]for k,vals in cols.items()}for i in range(data.num_rows))
 active_rows[role]=rows
 if role in expected:assert bag(rows)==bag(expected[role]),role
 if role=='manifest':assert rows==[p['native_manifest']]
 tables.append(dict(role=role,uuid=md['id'],version=version,latest=latest,rows=len(rows),active_files=sorted(active),original_full_cell_parity=role in expected))
assert [len(active_rows[x])for x in('object_current','edge_current','tombstone','whole_source_history')]==[11,10,0,21]
# Independent raw Parquet endpoint traversal, retaining actual edge occurrence IDs.
objects={x['id']:x for x in active_rows['object_current']};edges=active_rows['edge_current'];e4=next(x for x in edges if x['id']=='4');e1=next(x for x in edges if x['id']=='1')
assert e4['target_id']==e1['source_id'] and e4['source_id']=='5'and e4['target_id']=='3'and e1['target_id']=='2'
assert e4['rel_type_id']=='4'and e1['rel_type_id']=='7'
assert [objects[i]['type_id']for i in('5','3','2')]==['13','16','19']
properties={tuple(x['identity']):x['property_id']for x in bindings['properties']}
values=[]
for native_id,field in [('5','order_lines.id'),('3','products.id'),('2','suppliers.id')]:
 props=json.loads(objects[native_id]['props_json']);v=props[properties[('urn:umf:domain:commerce','domain',field)]];values.append(v)
# Every emitted request is exact original model; byte replay and real public schema/admission.
compiler=Path('/private/tmp/weft-paths-native-c6fe2a6-20261010-a/weft-paths');assert sha(compiler.read_bytes())=='8946ada68a7132a2ba6c1abdc1c9a0e904922cb1dce2c500922cb0dd0a96562b'
schema_dir=compiler.parent/'source/docs/helix/02-design/contracts';bundle={n:(schema_dir/n).read_bytes()for n in SCHEMA_SHA256};validator=make_offline_path_schema_validation(bundle);config=PathAdmissionConfig(2097152,validator)
compiles=[];source_ids={'ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity','outerJoin.matchIntegrity'}
for c,sql in zip(r['cases'],[COLLECTION,COUNT,DISTINCT]):
 raw=bytes.fromhex(c['original_request']['hex']);response=bytes.fromhex(c['original_response']['hex']);recompiled=bytes.fromhex(c['original_recompiled']['hex']);request=json.loads(raw);artifact=json.loads(response);v=c['provisional']
 assert request['sql']==sql and request['modules'][0]['documentJson'].encode()==model
 out=subprocess.run([str(compiler)],input=raw,capture_output=True,check=True,timeout=10);assert out.stdout==response==recompiled and out.stderr==b''
 admit_path_artifact(request,artifact,json.loads(out.stdout),config=config)
 expected_guards=[{'obligation':o['id'],'check':check}for phase in(True,False)for o in artifact['obligations']if((o['id']in source_ids)is phase)for check in o['parameters'].get('checks',o['parameters'].get('scans',[]))]
 assert [{k:x[k]for k in('obligation','check')}for x in v['guards']]==expected_guards
 assert all(x['native']['rows']==[['0']]for x in v['guards'])
 oracle=original_commerce_path_oracle(model,graph,request);assert v['oracle']==oracle and v['native_result']['rows']==oracle['rows']
 assert v['opening_native_files']==v['closing_native_files']==r['opening_native_files']
 assert all(x['schema']==metadata[x['table']['name'][-1]]for x in v['native_schemas'])
 assert len(v['native_result']['rows'])<=128 and sum(len(cell.encode())for row in v['native_result']['rows']for cell in row)<=4194304
 compiles.append(dict(case=c['case'],request_bytes=len(raw),request_sha256=sha(raw),response_bytes=len(response),response_sha256=sha(response),exact_full_replay=True,real_schema_and_admission=True,guards=len(v['guards']),original_rows=oracle['rows'],witnesses=oracle['witnesses']))
assert [c['guards']for c in compiles]==[24,23,23]
assert values==['[42,0,"L1"]','[42,0,"P1"]','[42,0,"S1"]']
collection=json.loads(compiles[0]['original_rows'][0][1]);assert collection=={'items':[{'intermediate':[values[1]],'terminal':[values[2]],'edges':['4','1']}],'truncated':False}
assert compiles[0]['original_rows'][0][0]==values[0] and compiles[1]['original_rows']==compiles[2]['original_rows']==[['1']]
assert r['opening_native_files']==r['closing_native_files'] and len(r['opening_native_files'])==66
for path,digest in r['closing_native_files'].items():assert sha(Path(path).read_bytes())==digest
assert r['original_manifest']==p['native_manifest'] and r['original_publication_report_sha256']==sha((P/'report.json').read_bytes())
for name,digest in r['source_hashes'].items():assert sha((P/name).read_bytes())==digest
assert (Q/'fresh-public-dataset.json').read_bytes()==(P/'public-dataset.json').read_bytes();assert read(P/'public-dataset.json')['receipt']['datasetValidation']=={'valid':True,'complete':True,'diagnostics':[]}
# Closing complete source and selected resource vectors. No SQLite connections.
inv=read(ROOT.parent/'source-inventory.json');resource=read('/private/tmp/ashlar-commerce-path-native-resource-observation-20261010-a.json');umf=read('/private/tmp/ashlar-indexed-commerce-umf-resources-20261010-a.json')
for x in inv['files']:
 data=(ROOT/x['path']).read_bytes();assert sha(data)==x['sha256'] and len(data)==x['bytes']
assert {str(x.relative_to(ROOT))for x in ROOT.rglob('*')if x.is_file()}=={x['path']for x in inv['files']}
for x in umf['files']+resource['selectedCompilerSchemaJars']:
 data=Path(x['path']).read_bytes();assert sha(data)==x['sha256']and len(data)==x['bytes']
changes=[]
for x in resource['publicationFiles']:
 obs=meta(x['path'])
 if obs['sha256']!=x['sha256']or obs['bytes']!=x['bytes']:changes.append({'opening':x,'closing':obs})
actual={str(x)for x in P.rglob('*')if x.is_file()};opening={x['path']for x in resource['publicationFiles']};assert actual==opening
assert all(x['opening']['kind']=='coordination'for x in changes)
outcome=read('/private/tmp/ashlar-commerce-path-native-custody-20261010-a/outcome.json');assert outcome['exit_code']==0 and outcome['phase']=='terminal'and outcome['report_exists']is True and outcome['opening_pg']==outcome['closing_pg']and outcome['command_sha256']==sha(CMD.read_bytes())
for stream in('stdout','stderr'):
 b=Path('/private/tmp/ashlar-commerce-path-native-custody-20261010-a/'+stream+'.bin').read_bytes();assert len(b)==outcome[stream+'_bytes']and sha(b)==outcome[stream+'_sha256']
assert r['cleanup']=={'reader_closed':True,'spark_stopped':True}
result=dict(verdict='Approved actual three-case finite local native evidence',scope='Original commerce collection, path count and distinct target count on private Spark4.0.1/Delta4.0.0; no general path profile, cloud, indexed-distribution, Truss or Fabric qualification.',reviewer_runtime={'python':sys.version,'pyarrow':pa.__version__},report=meta(Q/'report.json'),command=meta(CMD),compiler=meta(compiler),original_source_hashes=r['source_hashes'],native_tables=tables,raw_native_endpoint_values=values,raw_native_edge_pair=['4','1'],cases=compiles,closed_intervals=4,ack_observations=8,ack_schema_independent_receipt=meta('/private/tmp/astra-path-native-custody-review-20261010-a.json'),closing_native_files=66,closing_project_files=len(inv['files']),closing_umf_resources=len(umf['files']),closing_compiler_schema_jars=len(resource['selectedCompilerSchemaJars']),publication_file_count=len(actual),publication_changes=changes,publication_scope='All non-coordination publication bytes unchanged; any coordination changes listed without cause inference.',wrapper_outcome=meta('/private/tmp/ashlar-commerce-path-native-custody-20261010-a/outcome.json'),cleanup=r['cleanup'],limitations=['No native engine/SQL/database connection opened by reviewer; independent compiler processes and offline Delta/Parquet only.','Runtime package/JDK versions and imported modules are observed; no whole external binary closure or deterministic rebuild claim.','Only these three original data cases were executed; no parallel-edge, inverse, self-loop, LEFT, overflow or truncated-result native qualification.'])
Path('/private/tmp/astra-commerce-path-native-review-20261010-a.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
