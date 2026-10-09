/** Host-only UMF 0.8 metadata inspection. No storage or old-receipt conversion. */
import {createHash} from 'node:crypto';
import {resolve, join} from 'node:path';
import {pathToFileURL} from 'node:url';

const [rootArg, revision, input, requestText='{}'] = process.argv.slice(2);
if (!rootArg || !revision || !input || !/^[0-9a-f]{40}$/.test(revision))
  throw Error('Usage: bun tools/inspect_domain_semantics.ts UMF_SOURCE EXACT_COMMIT MODEL.json [REQUEST_JSON]');
const root=resolve(rootArg);
function git(...args:string[]) {
  const p=Bun.spawnSync(['git','-C',root,...args]);
  if(p.exitCode!==0)throw Error('Unable to verify pinned UMF source');
  return new TextDecoder('utf-8',{fatal:true}).decode(p.stdout).trim();
}
function checkSource() {
  if(git('rev-parse','HEAD')!==revision || git('status','--porcelain'))
    throw Error('UMF source must remain clean at the exact pinned revision');
}
checkSource();
const load=(path:string)=>import(pathToFileURL(join(root,path)).href);
const [{readDocument},{readJsonValue},{validateDocument},{selectCoreElements},properties,
  {selectCoreRelationships},{createValidator}]=await Promise.all([
  load('src/model/document.ts'),load('src/model/serialization.ts'),
  load('src/validation/document.ts'),load('src/model/selection.ts'),
  load('src/model/schema-properties.ts'),load('src/model/relationship-selection.ts'),
  load('src/validation/schema.ts'),
]);
const bytes=new Uint8Array(await Bun.file(input).arrayBuffer());
const document=readDocument(new TextDecoder('utf-8',{fatal:true}).decode(bytes),'json');
if(document.umf!=='0.8.0')throw Error('Explicit core 0.8.0 required');
const request=readJsonValue(requestText,'json');
if(!request || typeof request!=='object' || Array.isArray(request) ||
   Object.keys(request).some(k=>!['identities','valueProbes'].includes(k)) ||
   request.valueProbes!==undefined && !Array.isArray(request.valueProbes))
  throw Error('Expected explicit identities and optional typed-literal valueProbes');
const query={references:'transitive',...(request.identities!==undefined?{identities:request.identities}:{})};
const validation=validateDocument(document);
const selection=selectCoreElements(document,query);
const validator=createValidator();
const core=readJsonValue(await Bun.file(join(root,'spec/core/schema-properties-document.schema.json')).text(),'json');
const result=readJsonValue(await Bun.file(join(root,'spec/core/schema-properties-selection.schema.json')).text(),'json');
validator.addSchema(core);
const checkSelection=validator.compile(result);
if(!checkSelection(selection))throw Error('UMF current selection result schema refused: '+JSON.stringify(checkSelection.errors));
const selected=request.identities??selection.selection.map((entry:any)=>({module:entry.module,element:entry.element.id}));
const propertyInspections=selected.map((identity:any)=>properties.inspectCoreSchemaProperties(document,{scope:'element',...identity}));
let relationshipInspection:any;
try {
  relationshipInspection={status:'returned',receipt:selectCoreRelationships(document,{})};
} catch(error:any) {
  if(typeof error?.code!=='string')throw error;
  relationshipInspection={status:'refused',code:error.code,path:error.path,message:error.message};
}
const valueProbes=(request.valueProbes??[]).map((probe:any)=>{
  if(!probe || typeof probe!=='object' || Array.isArray(probe) ||
    Object.keys(probe).some(k=>!['field','value'].includes(k)) || !Object.hasOwn(probe,'field') || !Object.hasOwn(probe,'value'))
    throw Error('Value probes require explicit field identity and typed literal');
  return {request:probe,validation:properties.validateCoreFieldValue(document,probe.field,probe.value)};
});
checkSource();
console.log(JSON.stringify({
  format:'ashlar-domain-interpretation/0.1',validatorRevision:revision,
  sourceBase64:Buffer.from(bytes).toString('base64'),sourceSha256:createHash('sha256').update(bytes).digest('hex'),
  sourceBytes:bytes.length,umfCoreVersion:document.umf,documentId:document.id,
  request,validation,selection,resultSchema:{id:result.$id,valid:true},propertyInspections,
  relationshipInspection,valueProbes,
  qualification:'Original public UMF 0.8 metadata selection and explicit typed-literal probes only. Retained full source is authority. No canonical CSV conversion, record-data validator, Weft admission, storage binding, graph consumer acceptance or constraint-enforcement claim. A refused older relationship API does not erase relationships retained by the validated source.',
},null,2));
