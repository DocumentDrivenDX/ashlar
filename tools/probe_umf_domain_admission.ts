/** Evidence-only probe of pinned public UMF APIs; no schema reinterpretation. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [rootArg,pin,input,moduleId,elementId]=process.argv.slice(2);
if(!rootArg || !/^[0-9a-f]{40}$/.test(pin??'') || !input || !moduleId || !elementId)
  throw Error('Usage: bun tools/probe_umf_domain_admission.ts UMF_SOURCE REVISION DOCUMENT MODULE ELEMENT');
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
const {UmfError}=await load('model/types');
const {validateDocument}=await load('validation/document');
const {inspectCoreElementKind}=await load('model/field-kind');
const bytes=await Bun.file(input).bytes();
const source=readDocument(new TextDecoder('utf-8',{fatal:true}).decode(bytes),'json');
const validation=validateDocument(source);let kindInspection;
if(!validation.valid)kindInspection={status:'not-run',reason:'invalid-source'};
else try {kindInspection={status:'inspected',receipt:inspectCoreElementKind(source,{module:moduleId,element:elementId})};}
catch(error:any){
  if(!(error instanceof UmfError))throw error;
  kindInspection={status:'refused',code:error.code,path:error.path,message:error.message};
}
check();
console.log(JSON.stringify({umfRevision:pin,sourceSha256:createHash('sha256').update(bytes).digest('hex'),
  umfVersion:source.umf,identity:{module:moduleId,element:elementId},validation,kindInspection,
  qualification:'Actual public UMF APIs on exact original model; no target binding, canonical value reinterpretation or ingest authority.'},null,2));
