from pathlib import Path
import json,hashlib,tarfile,gzip,collections,subprocess,sqlite3,shutil,tempfile
import pyarrow as pa,pyarrow.parquet as pq
from local_delta_custody import encoded
from commerce_source_transaction import build_transaction
from run_commerce_outbox_publication import original_commerce_oracle
from fixture_oracle import fixture_columns
from protected_outbox_ack import receipt_bytes,AckScope
E=Path('/private/tmp/ashlar-indexed-commerce-evidence-20261010-b');P=Path('/private/tmp/ashlar-indexed-commerce-publication-20261010-a');Q=Path('/private/tmp/ashlar-indexed-commerce-queries-20261010-b');ROOT=Path('/private/tmp/ashlar-indexed-commerce-runtime-12dd-20261010-a')
sha=lambda b:hashlib.sha256(b).hexdigest()
read=lambda p:json.loads(Path(p).read_bytes())
bag=lambda rows:collections.Counter(encoded(row)for row in rows)
freeze=Path('/private/tmp/ashlar-indexed-commerce-native-evidence-frozen-20261010-c.json');f=read(freeze)
assert sha(freeze.read_bytes())=='e89c3198286ea434e88c0475e8ea08c26dbe12d5168bffeda2b1212d69f9873b'
for x in f['files']:
 b=Path(x['path']).read_bytes();assert len(b)==x['bytes'] and sha(b)==x['sha256'],x['path']
assert {str(p)for p in E.rglob('*')if p.is_file()}=={x['path']for x in f['files']}
archives=[]
for label,root in [('publication',P),('queries',Q)]:
 inv=read(E/(label+'-original-files.json'));expected={label+'/'+str(Path(x['path']).relative_to(root)):x for x in inv}
 with tarfile.open(E/(label+'-artifacts.tar.gz'),'r:gz')as tar:
  members=tar.getmembers();assert all(m.isfile()for m in members)and len({m.name for m in members})==len(members)
  assert {m.name for m in members}==set(expected)
  for m in members:
   b=tar.extractfile(m).read();x=expected[m.name];assert b==Path(x['path']).read_bytes()and len(b)==x['bytes']and sha(b)==x['sha256']
 assert gzip.decompress((E/(label+'-report.json.gz')).read_bytes())==(root/'report.json').read_bytes()
 archives.append({'label':label,'files':len(inv),'original_bytes':sum(x['bytes']for x in inv),'exact_member_parity':True})
p=read(P/'report.json');q=read(Q/'report.json');model=(P/'original-ontology.json').read_bytes();graph=(P/'original-graph.json').read_bytes();bindings=read(P/'development-bindings.json')
assert sha(model)=='51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a'and sha(graph)=='8052dbcf2cf48a5e344c492d661872bc0a3f80a5c7be033ab5008dea9c86119e'
batch,b2=build_transaction(model,graph,source_system='private-original-commerce-fixture',binding_profile=bindings['profile']);assert b2==bindings
assert batch.begin+b''.join(r.raw for r in batch.records)+batch.commit==(P/'source.jsonl').read_bytes()
expected=original_commerce_oracle(model,graph,bindings,batch,fixture_columns(ROOT));assert expected==p['complete_original_oracle']
versions=json.loads(p['native_manifest']['table_versions_json']);tables=[];active_rows={};metadata={}
for item in p['table_registry']:
 role=item['table'].split('.')[-1];path=Path(item['path']);logs=sorted((path/'_delta_log').glob('*.json'));latest=int(logs[-1].stem);version=versions.get(item['table'],latest);active={};meta=None
 for log in logs:
  if int(log.stem)>version:continue
  for line in log.read_text().splitlines():
   a=json.loads(line)
   if 'metaData'in a:meta=a['metaData']
   if 'add'in a:active[a['add']['path']]=a['add']
   if 'remove'in a:active.pop(a['remove']['path'],None)
 assert meta['id']==item['uuid'];metadata[role]=json.loads(meta['schemaString']);rows=[]
 for name in active:
  data=pq.ParquetFile(path/name).read();cols={}
  for field in data.schema:
   arr=data[field.name]
   if pa.types.is_timestamp(field.type):
    vals=arr.cast(pa.int64()).to_pylist();unit=field.type.unit
    assert unit in ('us','ns')
    if unit=='ns':assert all(v is None or v%1000==0 for v in vals);vals=[None if v is None else v//1000 for v in vals]
   else:vals=arr.to_pylist()
   cols[field.name]=[str(v)if type(v)is int else v for v in vals]
  rows.extend({k:vals[i]for k,vals in cols.items()}for i in range(data.num_rows))
 active_rows[role]=rows
 if role in expected:assert bag(rows)==bag(expected[role]),role
 if role=='manifest':assert rows==[p['native_manifest']]
 tables.append({'role':role,'uuid':meta['id'],'version':version,'latest':latest,'rows':len(rows),'active_files':sorted(active),'original_full_cell_parity':role in expected})
assert len(active_rows['object_current'])==11 and len(active_rows['edge_current'])==10 and len(active_rows['whole_source_history'])==21 and not active_rows['tombstone']
for observed in q['relationship']['schemas']:
 role=observed['table']['name'][-1];item=next(x for x in p['table_registry']if x['table'].endswith('.'+role));assert observed['table']['uuid']==item['uuid'] and observed['table']['version']==versions[item['table']]
 assert observed['schema']==metadata[role],role
 types={'string':'STRING','long':'BIGINT','timestamp':'TIMESTAMP'}
 assert observed['nativeTypes']==[[field['name'],types[field['type']]]for field in metadata[role]['fields']]
assert q['opening_native_files']==q['closing_native_files']and len(q['opening_native_files'])==66
for path,digest in q['closing_native_files'].items():assert sha(Path(path).read_bytes())==digest
assert q['original_publication_report_sha256']==sha((P/'report.json').read_bytes())and q['original_native_manifest']==p['native_manifest']
assert (Q/'fresh-public-dataset.json').read_bytes()==(P/'public-dataset.json').read_bytes()
public=read(P/'public-dataset.json');assert public['receipt']['datasetValidation']=={'valid':True,'complete':True,'diagnostics':[]}
g=json.loads(graph);products=[o for o in g['objects']if o['type']=={'document':'urn:umf:domain:commerce','module':'domain','element':'products'}];assert len(products)==1
scalar_expect=[[{'id':o['values']['products.id']}for o in products],[{'n':str(len(products))}]]
for item,exp in zip(q['queries'],scalar_expect):assert item['result']['rows']==item['original_source_expected']==exp
objects={o['key']:o for o in g['objects']};relations=[]
for product in products:
 keys=[]
 for edge in g['edges']:
  if edge['relationship']=={'document':'urn:umf:domain:commerce','module':'domain','id':'products.supplier_id'}and edge['source']==product['key']:
   supplier=objects[edge['target']];assert supplier['type']=={'document':'urn:umf:domain:commerce','module':'domain','element':'suppliers'};keys.append([supplier['values']['suppliers.id']])
 keys.sort(key=lambda row:row[0].encode());relations.append({'id':product['values']['products.id'],'suppliers':{'items':keys[:2],'truncated':len(keys)>2}})
relations.sort(key=lambda row:row['id'].encode());rel=q['relationship'];assert rel['decoded']==rel['independent_original_expected']==relations
assert [{'id':x['id'],'suppliers':json.loads(x['suppliers'])}for x in rel['native_rows']]==relations
compiler=Path('/private/tmp/ashlar-weft-fresh-cli-20261010-a/ashlar/out/weft-installation/weft-runtime');assert sha(compiler.read_bytes())=='ab899150d217a26368ab0c0954a627401d41031fc768f758247231ea513a3437'
compiles=[];checks=[]
for item in q['queries']+[rel]:
 request=item['request'];artifact=item['artifact'];assert request['modules'][0]['documentJson'].encode()==model
 request_bytes=((json.dumps(request,ensure_ascii=False,separators=(',',':')) if 'result'in item else encoded(request))+'\n').encode();out=subprocess.run([str(compiler)],input=request_bytes,capture_output=True,check=True,timeout=30);assert out.stderr==b''and json.loads(out.stdout)==artifact
 if 'response_sha256'in item:assert sha(out.stdout)==item['response_sha256']and sha(request_bytes)==item['request_sha256']
 if 'result'in item:
  original=next(x for x in artifact['obligations']if x['id']=='ashlar.candidate.scalarIntegrity')['parameters']['checks'];assert [x['check']for x in item['result']['integrity']]==original;assert all(x['rows']==[{'violations':'0'}]for x in item['result']['integrity']);n=len(original)
 else:
  original=[{'family':o['id'],'check':check,'rows':[{'violations':'0'}]}for family in ('ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity')for o in artifact['obligations']if o['id']==family for check in o['parameters']['checks']];assert item['checks']==original;n=len(original)
 compiles.append({'sql':request['sql'],'request_bytes':len(request_bytes),'request_sha256':sha(request_bytes),'stdout_bytes':len(out.stdout),'stdout_sha256':sha(out.stdout),'full_artifact_equal':True,'native_emitted_checks':n})
assert [x['native_emitted_checks']for x in compiles]==[1,1,11]
intervals=[q['alias_interval']]+[v['closed_interval']for v in q['queries']]+[rel['closed_interval']];sessions=[]
for interval in intervals:
 assert interval['scope']==p['protected_ack_scope']and interval['source_schema']==p['source_schema']and interval['source_signature_sha256']==p['source_signature_sha256']
 req=bytes.fromhex(interval['original_request_hex']);man=bytes.fromhex(interval['original_manifest_hex']);assert json.loads(man)==p['native_manifest']
 assert interval['original_request_hex']==p['protected_ack_observation']['request_hex']and interval['original_manifest_hex']==p['protected_ack_observation']['manifest_hex']
 receipt=receipt_bytes(AckScope(**interval['scope']),req,man)[-1].hex()
 for boundary in ('opening_ack','closing_ack'):
  a=interval[boundary];assert a['observation']['position']=='1'and a['observation']['request_hex']==req.hex()and a['observation']['manifest_hex']==man.hex()and a['observation']['receipt_hex']==receipt
  role=interval['scope']['service_schema'].replace('pipeline_','operator_');s=a['session'];assert s['current_user']==s['session_user']==role and s['database']=='truss_e2e'and s['source_schema']==p['source_schema']and s['source_signature_sha256']==p['source_signature_sha256']
 assert interval['opening_ack']==interval['closing_ack'];sessions.append(interval['opening_ack']['session']['backend_pid'])
assert len(set(sessions))==4
opening=read(E/'ashlar-indexed-commerce-phase2-reservation-20261010-b.json')['opening_publication_files'];closing=read(E/'ashlar-indexed-commerce-post-run-20261010-b.json')['closing_publication_files'];op={x['path']:x for x in opening};cl={x['path']:x for x in closing};assert op.keys()==cl.keys();changed=[k for k in op if op[k]!=cl[k]];assert changed==['operations.sqlite-shm']
for x in closing:raw=(P/x['path']).read_bytes();assert sha(raw)==x['sha256']and len(raw)==x['bytes']
# Inspect an independent copied database, never opening the original SQLite journal.
with tempfile.TemporaryDirectory(prefix='astra-journal-review-',dir='/private/tmp')as temp:
 dest=Path(temp)
 for name in ('operations.sqlite','operations.sqlite-wal','operations.sqlite-shm'):shutil.copyfile(P/name,dest/name)
 con=sqlite3.connect((dest/'operations.sqlite').as_uri()+'?mode=ro',uri=True);assert con.execute('PRAGMA integrity_check').fetchall()==[('ok',)]
 counts={name:con.execute('SELECT count(*) FROM "'+name+'"').fetchone()[0]for name, in con.execute("SELECT name FROM sqlite_master WHERE type='table'")};con.close()
assert not (Path('/private/tmp/ashlar-indexed-commerce-queries-20261010-a')/'report.json').exists()
result={'scope':'Independent offline raw Delta/Parquet, full immutable compiler replay, retained ACK interval and original source/custody review; no native engine rerun','evidence_freeze_sha256':sha(freeze.read_bytes()),'publication_report_sha256':sha((P/'report.json').read_bytes()),'query_report_sha256':sha((Q/'report.json').read_bytes()),'archive_member_checks':archives,'pyarrow_version':pa.__version__,'native_tables':tables,'scalar_results':scalar_expect,'relationship':relations,'compiler_replays':compiles,'held_intervals':4,'ack_observations':8,'distinct_ordinary_pg_sessions':sessions,'observed_right_schemas':2,'complete_native_files_unchanged':66,'sqlite_copy_integrity':'ok','sqlite_table_counts':counts,'all_publication_file_changes':[{'path':k,'opening':op[k],'closing':cl[k]}for k in changed],'sqlite_scope':'Only volatile SHM differs; opening bytes not retained, so no exact changed-offset or causal attribution. DB and WAL byte-identical. No blanket all-file-invariance claim.','failed_query_a_preserved':True}
out=Path('/private/tmp/astra-indexed-commerce-actual-review-20261010.json');out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'receipt':str(out),'sha256':sha(out.read_bytes()),'tables':[(t['role'],t['rows'])for t in tables],'compiler_replays':len(compiles),'archives':archives,'sqlite':counts,'changed':changed}))
