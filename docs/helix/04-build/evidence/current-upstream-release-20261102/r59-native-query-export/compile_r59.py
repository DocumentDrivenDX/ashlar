import hashlib,json,pathlib
import weft
ROOT=pathlib.Path(__file__).resolve().parents[6]
R21='ashlar-local-dev-typed-graph-r21-20261101'
INSTALLATION='87406914-c3f1-49d4-a687-d5dbec313389'
PUB='truss-graph-678967ac3deeafa90b514ae8a5a1b05f78b0b32e5f89135f4c67205fede001ea'
model_path=ROOT/'docs/helix/04-build/evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json'
model=json.loads(model_path.read_text()); model_hash=hashlib.sha256(model_path.read_bytes()).hexdigest()
obs=json.loads((pathlib.Path(__file__).parent/'r59-vector-observation.json').read_text())
manifest=json.loads(obs['publication']['table_versions_json'])
ids={'object_current':'object_current','edge_current':'edge_current','tombstone':'tombstone','whole_source_history':'whole_source_history'}
vector=[]
for name,role in ids.items():
 v=obs['roles'][role]; vector.append({'name':['spark_catalog','ashlar_r59_'+PUB[-12:],role],'uuid':v['uuid'],'version':v['version']})
# Reuse the released Weft request's logical binding shape, then replace every source/model/vector field with the exact R21 identities.
source=ROOT/'docs/helix/04-build/evidence/current-upstream-release-20261101/g03-weft-native-consumer/candidate-r12/grouped.request.json'
base=json.loads(source.read_text()); binding=json.loads(base['target']['bindingJson'])
for pin in binding['modelPins']:
 pin.update(documentId=R21,revision='1',sha256=model_hash,umfVersion='0.10.0')
binding['layoutRevision']='ashlar-delta/0.4-xxhash64'; binding['lookupHashProfile']='spark-xxhash64-typed-key/0.1'
binding['publication']={'id':PUB,'manifestUuid':'168368b0-0e76-4567-8110-6c081dabf8a1','tables':vector}
propmap={'21':'1','22':'2','23':'3','24':'4','25':'5'}; typemap={'17':'1','18':'2','31':'1','32':'2'}
def rewrite(o):
 if isinstance(o,dict):
  for k,v in list(o.items()):
   if k=='documentId' and v=='typed-graph-example': o[k]=R21
   elif k=='sourceSystem' and v=='typed-graph-source': o[k]=INSTALLATION
   elif k=='typeId' and str(v) in typemap: o[k]=typemap[str(v)]
   elif k=='propertyId' and str(v) in propmap: o[k]=propmap[str(v)]
   else: rewrite(v)
 elif isinstance(o,list):
  for v in o: rewrite(v)
rewrite(binding)
def inject(req,b):
 raw=json.dumps(b,ensure_ascii=False,separators=(',',':'),sort_keys=True)
 req['target']['bindingJson']=raw; req['target']['bindingSha256']=hashlib.sha256(raw.encode()).hexdigest()
 req['modules']=[{'documentJson':model_path.read_text(),'pin':dict(b['modelPins'][0]),'selectedModuleIds':['m']}]
 return req
req=inject(base,binding)
responses={}
for name,sql in [('singleton',"SELECT p.person_key, p.person_name FROM Person p WHERE p.person_key = 'local-dana-r21-20261101'"),('grouped',req['sql'])]:
 selected=dict(req); selected['sql']=sql
 response=json.loads(weft.compile_paths_keys_json(json.dumps(selected,ensure_ascii=False,separators=(',',':'))))
 if response.get('status')!='compiled': raise RuntimeError(name+' compile refused: '+json.dumps(response)[:500])
 responses[name]={'request':selected,'response':response}
page_path=ROOT/'docs/helix/04-build/evidence/current-upstream-release-20261101/g03-weft-native-consumer/candidate-r12/page.request.json'
page=json.loads(page_path.read_text()); request=page['request']; page_binding=json.loads(request['target']['bindingJson'])
for pin in page_binding['modelPins']:
 pin.update(documentId=R21,revision='1',sha256=model_hash,umfVersion='0.10.0')
page_binding['layoutRevision']='ashlar-delta/0.4-xxhash64'; page_binding['lookupHashProfile']='spark-xxhash64-typed-key/0.1'
page_binding['publication']={'id':PUB,'manifestUuid':'168368b0-0e76-4567-8110-6c081dabf8a1','tables':vector}
rewrite(page_binding); inject(request,page_binding)
page_resp=json.loads(weft.compile_engine_page_json(json.dumps(page,ensure_ascii=False,separators=(',',':'))))
if page_resp.get('status')!='compiled': raise RuntimeError('page compile refused: '+json.dumps(page_resp)[:500])
continuation=json.loads(weft.check_engine_page_json(json.dumps(page_resp,separators=(',',':'),ensure_ascii=False),json.dumps(request,separators=(',',':'),ensure_ascii=False),json.dumps(['local-ben-20261101'],separators=(',',':'))))
if continuation.get('status')!='compiled': raise RuntimeError('continuation refused: '+json.dumps(continuation)[:500])
responses['page']={'request':page,'response':page_resp}; responses['continuation']={'request':request,'response':continuation}
out=pathlib.Path('/private/tmp/ashlar-local-release-native-read/r59-weft-compiled.json'); out.write_text(json.dumps(responses,sort_keys=True,indent=2,ensure_ascii=False)+'\n')
print(json.dumps({'status':'compiled','weft':'0.4.56','publication':PUB,'layout':binding['layoutRevision'],'queries':{n:{'status':v['response'].get('status'),'sql_sha256':hashlib.sha256(v['response'].get('sql','').encode()).hexdigest()} for n,v in responses.items() if n!='continuation'},'continuation':continuation.get('status'),'alias_database':'ashlar_r59_'+PUB[-12:],'output':str(out)},sort_keys=True))
