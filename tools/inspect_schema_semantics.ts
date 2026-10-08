/** Host composition of pinned UMF interpretation APIs; never defines UMF meaning. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [rootArg,pin,input]=process.argv.slice(2);
if(!rootArg || !/^[0-9a-f]{40}$/.test(pin??'') || !input)
  throw Error('Usage: bun tools/inspect_schema_semantics.ts UMF_SOURCE REVISION DOCUMENT');
const root=resolve(rootArg);
function check(){
  const head=Bun.spawnSync(['git','-C',root,'rev-parse','HEAD']);
  const status=Bun.spawnSync(['git','-C',root,'status','--porcelain']);
  if(head.exitCode || status.exitCode || new TextDecoder().decode(head.stdout).trim()!==pin || status.stdout.length)
    throw Error('UMF source must be clean and pinned');
}
check();
const load=(name:string)=>import(pathToFileURL(join(root,'src',name+'.ts')).href);
const {readDocument}=await load('model/document');
const {validateDocument}=await load('validation/document');
const {inspectCoreElementKind}=await load('model/field-kind');
const {inspectCoreNullability}=await load('model/nullability');
const {inspectCoreCardinality}=await load('model/cardinality');
const {inspectCoreFacets}=await load('model/facets');
const {inspectCoreKeys}=await load('model/keys');
const {inspectCoreRelationships}=await load('model/relationships');
const {inspectCoreSchemaProperties}=await load('model/schema-properties');
const bytes=new Uint8Array(await Bun.file(input).arrayBuffer());
const source=readDocument(new TextDecoder('utf-8',{fatal:true}).decode(bytes),'json');
function schemaProperties(identity:any) {
  try {return {state:'inspected',receipt:inspectCoreSchemaProperties(source,{scope:'element',...identity})};}
  catch(error:any) {
    if(error?.code!=='CORE_SCHEMA_PROPERTIES' || error.message!=='Expected valid 0.8.0 envelope')throw error;
    return {state:'unavailable',diagnostic:{code:error.code,message:error.message,path:error.path},qualification:'Selected UMF API refuses this source envelope; no interpretation substituted'};
  }
}
const elements=source.modules.flatMap((m:any)=>m.elements.map((e:any)=>{
  const identity={module:m.id,element:e.id};
  return {identity:[source.id,m.id,e.id],
    kind:inspectCoreElementKind(source,identity),
    nullability:inspectCoreNullability(source,identity),
    cardinality:inspectCoreCardinality(source,identity),
    facets:inspectCoreFacets(source,identity),
    keys:inspectCoreKeys(source,identity),
    schemaProperties:schemaProperties(identity)};
}));
const relationships=source.modules.map((m:any)=>inspectCoreRelationships(source,{module:m.id}));
check();
console.log(JSON.stringify({format:'ashlar-umf-interpretation/0.1',validatorRevision:pin,
  sourceBase64:Buffer.from(bytes).toString('base64'),sourceSha256:createHash('sha256').update(bytes).digest('hex'),
  validation:validateDocument(source),elements,relationships,
  qualification:'Actual UMF interpretation receipts, original warnings and source retained. No target/native enforcement, accepted binding or schema revision.'},null,2));
