/** Host-only ordered DDL proposal using UMF's public generator. Never executes SQL. */
import {resolve,join,dirname} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {openSync,writeFileSync,fsyncSync,closeSync,statSync} from 'node:fs';
export const UMF_REVISION='385c8cf119a3c2e40ba59cc5f15e24edab52f3b2';
const originalFolder=resolve(import.meta.dir,'../docs/helix/02-design/models/ashlar-delta-runtime');
export async function generateBundle(source:string,catalog:string,schema:string,output:string,folder=originalFolder){
  source=resolve(source);output=resolve(output);
  for(const name of [catalog,schema])if(!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name))throw Error('Explicit portable catalog/schema identifiers required');
  const git=(...args:string[])=>{const r=Bun.spawnSync(['git','-C',source,...args]);if(r.exitCode)throw Error('Unable to verify UMF source');return new TextDecoder().decode(r.stdout).trim();};
  const check=()=>{if(git('rev-parse','HEAD')!==UMF_REVISION||git('status','--porcelain'))throw Error('UMF source must be clean at '+UMF_REVISION);};
  check();
  const read=async(path:string)=>{const file=Bun.file(path);if(file.size>1048576)throw Error('Model input exceeds 1MiB');return file.text();};
  const indexText=await read(join(folder,'index.json')),index=JSON.parse(indexText);
  const digest=(text:string)=>createHash('sha256').update(text).digest('hex');
  const inputs:Record<string,string>={'index.json':digest(indexText)};
  if(index.profile!=='ashlar-delta-runtime-model/0.1'||index.umfRevision!=='fac1497a5ac5cb39bbaaac34eb0da8dc8b208c69'||JSON.stringify(index.tables)!==JSON.stringify(['object_current','edge_current','source_record','property_journal','tombstone','publication_manifest','publication_attempt_phase','whole_source_history']))throw Error('Original eight-table model required');
  const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
  const {generateDeltaDDLBundle}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
  const names=new Set<string>(),documents=[];
  for(const name of index.tables){
    if(typeof name!=='string'||! /^[a-z][a-z0-9_]*$/.test(name)||names.has(name))throw Error('Invalid/duplicate model table');names.add(name);
    const text=await read(join(folder,name+'.umf.json'));inputs[name+'.umf.json']=digest(text);
    const document=readDocument(text,'json');
    const definition=document.extensions?.['umf.delta.definition'];
    if(!definition||JSON.stringify(definition.name)!==JSON.stringify([name]))throw Error('Original native table name differs');
    // Only qualify the native definition name on the public reader's cloned document.
    definition.name=[catalog,schema,name];documents.push(document);
  }
  const bundle=generateDeltaDDLBundle(documents);check();
  const value=JSON.stringify({profile:'ashlar-delta-ddl-bundle/0.1',generatorRevision:UMF_REVISION,authoredCarrierGeneratorRevision:index.umfRevision,inputs,...bundle},null,2)+'\n';
  if(Buffer.byteLength(value)>16777216)throw Error('Bundle exceeds 16MiB');
  if(!statSync(dirname(output)).isDirectory())throw Error('Existing output parent required');
  const fd=openSync(output,'wx',0o600);try{writeFileSync(fd,value);fsyncSync(fd);}finally{closeSync(fd);}
  return bundle;
}
if(import.meta.main){
  const args=process.argv.slice(2);if(args.length!==4)throw Error('Usage: bun tools/generate_delta_bundle.ts UMF_SOURCE CATALOG SCHEMA FRESH_OUTPUT_JSON');
  await generateBundle(...args as [string,string,string,string]);console.log('Generated complete eight-table DDL proposal; no SQL applied');
}
