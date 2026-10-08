/** Host tooling: actual pinned UMF generator; no SQL execution or migration. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const root=resolve(import.meta.dir,'..'),folder=join(root,'docs/helix/02-design/models/ashlar-delta-runtime');
const [sourceArg,mode='--check']=process.argv.slice(2);
if(!sourceArg||!['--check','--write'].includes(mode))throw Error('Usage: bun tools/generate_delta_model.ts UMF_SOURCE [--check|--write]');
const source=resolve(sourceArg),index=await Bun.file(join(folder,'index.json')).json();
function git(...args:string[]){const result=Bun.spawnSync(['git','-C',source,...args]);if(result.exitCode)throw Error('Unable to verify UMF source');return new TextDecoder().decode(result.stdout).trim();}
if(git('rev-parse','HEAD')!==index.umfRevision||git('status','--porcelain'))throw Error('UMF source must be clean at the model-pinned revision');
const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
const {generateDeltaDDL}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
const digest=(text:string)=>createHash('sha256').update(text).digest('hex');
const carriers:Record<string,unknown>={},inputs:Record<string,string>={};
for(const name of index.tables){
  if(typeof name!=='string'||!/^\w+$/.test(name)||Object.hasOwn(carriers,name))throw Error('Invalid model inventory');
  const relative='docs/helix/02-design/models/ashlar-delta-runtime/'+name+'.umf.json';
  const text=await Bun.file(join(root,relative)).text(),result=generateDeltaDDL(readDocument(text,'json'));
  if(JSON.stringify(result.definition.name)!==JSON.stringify([name]))throw Error('Carrier identity differs');
  inputs[relative]=digest(text);
  const schema=JSON.parse(result.schemaJson);
  carriers[name]={sql:result.sql,columns:schema.fields.map((f:any)=>({name:f.name,deltaType:f.type,nullable:f.nullable})),layout:result.definition};
}
inputs['docs/helix/02-design/models/ashlar-delta-runtime/index.json']=digest(await Bun.file(join(folder,'index.json')).text());
if(git('rev-parse','HEAD')!==index.umfRevision||git('status','--porcelain'))throw Error('UMF changed during generation');
const value=JSON.stringify({profile:'ashlar-generated-delta-carriers/0.1',generator:{project:'UMF',revision:index.umfRevision,extension:'umf.delta.definition/0.1.0'},inputs,carriers,qualification:'Generated fresh managed CREATE proposals from canonical UMF documents; no application, migration, enforcement or native compatibility admission.'},null,2)+'\n';
const target=join(root,'sql/ashlar-delta-v03/runtime-carriers.generated.json');
if(mode==='--write')await Bun.write(target,value);
else if(await Bun.file(target).text()!==value)throw Error('Stale generated carriers; review model and regenerate');
console.log((mode==='--write'?'Generated':'Verified')+' '+Object.keys(carriers).length+' UMF-defined Delta carriers');
