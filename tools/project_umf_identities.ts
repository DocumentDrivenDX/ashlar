/** Host composition: authored identity inventory from the actual pinned UMF reader. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [rootArg,pin,input]=process.argv.slice(2);
if (!rootArg || !/^[0-9a-f]{40}$/.test(pin ?? '') || !input)
  throw Error('Usage: bun tools/project_umf_identities.ts UMF_SOURCE REVISION DOCUMENT');
const root=resolve(rootArg);
function git(...args:string[]) {
  const r=Bun.spawnSync(['git','-C',root,...args]);
  if(r.exitCode)throw Error('UMF source verification failed');
  return new TextDecoder().decode(r.stdout).trim();
}
function check(){if(git('rev-parse','HEAD')!==pin || git('status','--porcelain'))throw Error('UMF must be clean and pinned');}
check();
const bytes=new Uint8Array(await Bun.file(input).arrayBuffer());
const text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);
const {readDocument}=await import(pathToFileURL(join(root,'src/model/document.ts')).href);
const {validateDocument}=await import(pathToFileURL(join(root,'src/validation/document.ts')).href);
const model=readDocument(text,'json');
const validation=validateDocument(model);
const elements=model.modules.flatMap((module:any,mi:number)=>module.elements.map((element:any,ei:number)=>({
  authoredIdentity:[model.id,module.id,element.id],
  sourcePointer:`/modules/${mi}/elements/${ei}`,
  // Preserve the original assertion; this tool assigns no target kind or semantics.
  kind:element.kind ?? null,
  displayName:element.name ?? null,
})));
check();
console.log(JSON.stringify({format:'ashlar-authored-identity-inventory/0.1',sourceSha256:createHash('sha256').update(bytes).digest('hex'),validatorRevision:pin,elements,validation,qualification:'Original UMF element identity inventory only; no target binding, property ownership inference, relationship inventory, native ID allocation or acceptance'},null,2));
