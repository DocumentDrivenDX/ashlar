/** Public core0.10 Record+compact dataset checks. No private UMF imports or authority. */
import {createHash} from 'node:crypto';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
const PIN='2ede4c879bea5db13794a6fa30e67c821306c17f';
const [repoArg,modelPath,requestPath,git]=process.argv.slice(2);
const refuse=():never=>{throw Error('Original typed graph admission refused');};
if(!repoArg||!modelPath||!requestPath||!git)refuse();
const repo=resolve(repoArg);
function clean(){for(const [args,expected]of [[['rev-parse','HEAD'],PIN],[['status','--porcelain','--untracked-files=normal'],'']]as const){const p=Bun.spawnSync([git,'-c','core.fsmonitor=false','-c','core.hooksPath=/dev/null','-C',repo,...args],{maxBuffer:1048576});if(p.exitCode||p.stderr.length||new TextDecoder().decode(p.stdout).trim()!==expected)refuse();}}
const hash=(v:Uint8Array)=>createHash('sha256').update(v).digest('hex');
const keys=(v:any,wanted:string[])=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join(',')===[...wanted].sort().join(',');
const text=(v:any)=>typeof v==='string'&&v.length>0&&!v.includes('\0');
const sha=(v:any)=>typeof v==='string'&&/^[0-9a-f]{64}$/.test(v);
const from64=(v:any)=>{if(typeof v!=='string'||v.length>1398104)refuse();const b=Buffer.from(v,'base64');if(b.toString('base64')!==v||b.length>1048576)refuse();return b;};
clean();
if(Bun.file(modelPath).size>65536||Bun.file(requestPath).size>1048576)refuse();
const source=await Bun.file(modelPath).bytes(),request=await Bun.file(requestPath).bytes();
if(!source.length||source.length>65536||!request.length||request.length>1048576)refuse();
const {readDocument,readJsonValue,validateCoreRecordValues,validateCoreDatasetValuesCompact,verifyCoreDatasetValuesCompact}=await import(pathToFileURL(join(repo,'src/index.ts')).href);
const decoder=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true});
const model=readDocument(decoder.decode(source),'json');if(model.umf!=='0.10.0')refuse();
const input:any=readJsonValue(decoder.decode(request),'json');
if(!keys(input,['profile','configurationSha256','sourceSha256','bindingSha256','modelSha256','documentRevision','sourceSystem','feed','epoch','groups'])||input.profile!=='ashlar-typed-graph-check-request/0.1'||!['configurationSha256','sourceSha256','bindingSha256','modelSha256'].every(k=>sha(input[k]))||input.modelSha256!==hash(source)||!['documentRevision','sourceSystem','feed','epoch'].every(k=>text(input[k]))||!Array.isArray(input.groups)||input.groups.length<1||input.groups.length>4)refuse();
const rawSource=createHash('sha256');let offset=0,eventCount=0,objectCount=0;
const readLine=(raw:Uint8Array):any=>{if(!raw.length||raw[raw.length-1]!==10||raw.slice(0,-1).includes(10))refuse();return readJsonValue(decoder.decode(raw),'json');};
const groups=[];
for(const group of input.groups){
 if(!keys(group,['batch','events','records','dataset'])||!Array.isArray(group.events)||!Array.isArray(group.records))refuse();
 const batch=group.batch;
 if(!keys(batch,['feed','epoch','batchId','recordCount','recordsSha256','cursorBefore','cursorAfter','beginBase64','commitBase64'])||batch.feed!==input.feed||batch.epoch!==input.epoch||!text(batch.batchId)||!Number.isSafeInteger(batch.recordCount)||batch.recordCount!==group.events.length||!sha(batch.recordsSha256)||batch.cursorBefore!==String(offset))refuse();
 const begin=from64(batch.beginBase64),commit=from64(batch.commitBase64),start=readLine(begin),end=readLine(commit);
 if(!keys(start,['kind','batch_id'])||start.kind!=='begin'||start.batch_id!==batch.batchId||!keys(end,['kind','batch_id','record_count','records_sha256'])||end.kind!=='commit'||end.batch_id!==batch.batchId||end.record_count!==batch.recordCount||end.records_sha256!==batch.recordsSha256)refuse();
 rawSource.update(begin);offset+=begin.length;const recordDigest=createHash('sha256');const objects:any[]=[];const seen=new Set<string>(),deliveries=new Set<string>();
 for(const event of group.events){
  if(++eventCount>1000||!keys(event,['deliveryId','recordSha256','rawBase64'])||!text(event.deliveryId)||!sha(event.recordSha256)||deliveries.has(event.deliveryId))refuse();deliveries.add(event.deliveryId);
  const raw=from64(event.rawBase64),value=readLine(raw),length=Buffer.alloc(8);length.writeBigUInt64BE(BigInt(raw.length));recordDigest.update(length);recordDigest.update(raw);rawSource.update(raw);offset+=raw.length;
  if(hash(raw)!==event.recordSha256||value.kind!=='event'||value.delivery_id!==event.deliveryId||value.source_profile!=='ashlar-whole-entity/0.1'||value.source_system!==input.sourceSystem||value.schema_revision!==input.documentRevision||!['object','edge'].includes(value.entity_kind)||!['create','replace','delete'].includes(value.operation)||!['type_id','id','entity_version'].every(k=>typeof value[k]==='string'&&/^-?(0|[1-9][0-9]*)$/.test(value[k]))||typeof value.props_json!=='string'||typeof value.retained_json!=='string')refuse();
  const expected=['kind','delivery_id','source_profile','source_system','entity_kind','type_id','id','entity_version','schema_revision','operation','props_json','retained_json'];if(value.entity_kind==='edge')expected.push('endpoints');if(!keys(value,expected))refuse();
  const identity=JSON.stringify([value.entity_kind,value.type_id,value.id]);if(seen.has(identity))refuse();seen.add(identity);
  if(value.entity_kind==='edge'){
   if(!keys(readJsonValue(value.props_json,'json'),[])||!Array.isArray(value.endpoints)||value.endpoints.length!==2||value.endpoints.some((e:any)=>!keys(e,['type_id','id'])||!['type_id','id'].every(k=>text(e[k]))))refuse();
  }else objects.push({event,value});
 }
 if(recordDigest.digest('hex')!==batch.recordsSha256||objects.length!==group.records.length)refuse();
 rawSource.update(commit);offset+=commit.length;if(offset>1048576||batch.cursorAfter!==String(offset))refuse();
 const records=group.records.map((item:any,index:number)=>{
  const {event,value}=objects[index];
  if(++objectCount>1000||!keys(item,['deliveryId','recordSha256','typeId','propsSha256','identity','values'])||item.deliveryId!==event.deliveryId||item.recordSha256!==event.recordSha256||!Number.isSafeInteger(item.typeId)||item.typeId<1||item.typeId>=2147483648||String(item.typeId)!==value.type_id||item.propsSha256!==hash(new TextEncoder().encode(value.props_json)))refuse();
  const result=validateCoreRecordValues(model,item.identity,item.values);
  // Public core0.10 Record checks retain these exact dataset-context deferrals.
  // They are not erased/promoted: the joint compact dataset must complete below.
  const owner=model.modules.find((m:any)=>m.id===item.identity.module)?.elements.find((e:any)=>e.id===item.identity.element);
  const warnings=[];
  if(owner?.keys?.length)warnings.push({code:'RECORD_KEY_CONTEXT_REQUIRED',path:'/identity',message:'Dataset key equality/uniqueness needs a separately qualified check',severity:'warning'});
  if(model.modules.some((m:any)=>Array.isArray(m.relationships)&&m.relationships.length))warnings.push({code:'RECORD_RELATIONSHIP_CONTEXT_REQUIRED',path:'/identity',message:'Relationship/endpoint constraints need a separately qualified dataset check',severity:'warning'});
  if(result.validation.valid!==true||result.validation.complete!==(warnings.length===0)||result.validation.diagnostics.length!==warnings.length||result.validation.diagnostics.some((d:any,i:number)=>!keys(d,['code','path','message','severity'])||Object.entries(warnings[i]).some(([k,v])=>d[k]!==v))||!result.fields.every((f:any)=>f.validation.valid===true&&f.validation.complete===true&&f.validation.diagnostics.length===0))refuse();
  return {request:item,result};
 });
 const dataset=validateCoreDatasetValuesCompact(model,group.dataset);verifyCoreDatasetValuesCompact(dataset,model,group.dataset);
 if(dataset.datasetValidation.valid!==true||dataset.datasetValidation.complete!==true||dataset.datasetValidation.diagnostics.length||dataset.residuals.length||dataset.obligations.some((o:any)=>o.state!=='satisfied'))refuse();
 groups.push({records,dataset});
}
if(!objectCount||rawSource.digest('hex')!==input.sourceSha256)refuse();
clean();
console.log(JSON.stringify({format:'ashlar-typed-graph-check/0.1',producerRevision:PIN,sourceBase64:Buffer.from(source).toString('base64'),sourceSha256:hash(source),requestBase64:Buffer.from(request).toString('base64'),requestSha256:hash(request),groups,qualification:'Original public core0.10 carried Record checks and complete supplied-dataset-only compact checks. Explicit development IDs; no source, catalog, native publication, query or ACK authority.'}));
