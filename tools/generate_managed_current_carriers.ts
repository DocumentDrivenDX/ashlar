/** Generate current six-role UMF 0.10 Delta DDL proposals; never execute SQL. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';

export const UMF_REVISION='2ede4c879bea5db13794a6fa30e67c821306c17f';
const root=resolve(import.meta.dir,'..');
const folder=join(root,'src/ashlar_host/resources/managed-current-core-010');
const oldFolder=join(root,'docs/helix/02-design/models/ashlar-delta-runtime');
const [sourceArg,mode='--check']=process.argv.slice(2);
if(!sourceArg||!['--check','--write'].includes(mode))throw Error('Usage: bun tools/generate_managed_current_carriers.ts UMF_SOURCE [--check|--write]');
const source=resolve(sourceArg);
function git(...args:string[]){const result=Bun.spawnSync(['git','-C',source,...args]);if(result.exitCode)throw Error('Unable to verify UMF source');return new TextDecoder().decode(result.stdout).trim();}
function check(){if(git('rev-parse','HEAD')!==UMF_REVISION||git('status','--porcelain'))throw Error('UMF source must be clean at the pinned current revision');}
check();
const index=await Bun.file(join(folder,'index.json')).json();
if(index.profile!=='ashlar-managed-current-carrier-migration/0.1'||index.generatorRevision!==UMF_REVISION||index.umfCore!=='0.10.0')throw Error('Current six-role migration profile required');
const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
const {generateDeltaDDL}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
const digest=(text:string)=>createHash('sha256').update(text).digest('hex');
const inputs:Record<string,string>={},carriers:Record<string,unknown>={};
for(const name of ['index.json',...index.roles.map((role:string)=>role+'.umf.json')]){
 const text=await Bun.file(join(folder,name)).text();inputs['src/ashlar_host/resources/managed-current-core-010/'+name]=digest(text);
 if(name==='index.json')continue;
 const role=name.slice(0,-9),model=JSON.parse(text),oldText=await Bun.file(join(oldFolder,name)).text(),old=JSON.parse(oldText);
 if(digest(oldText)!==index.migration.sourceSha256[role]||digest(text)!==index.migration.targetSha256[role])throw Error('Original/current input digest differs from migration provenance');
 old.umf='0.10.0';
 if(JSON.stringify(old)!==JSON.stringify(model))throw Error('Only the authored UMF core header may differ in the current migration input');
 if(model.umf!=='0.10.0'||model.vocabularies['umf.delta'].version!=='0.1.0'||model.vocabularies['umf.delta.definition'].version!=='0.1.0')throw Error('Exact current core and retained Delta vocabulary versions required');
 const generated=generateDeltaDDL(readDocument(text,'json'));
 if(JSON.stringify(generated.definition.name)!==JSON.stringify([role]))throw Error('Native role identity differs');
 const schema=JSON.parse(generated.schemaJson);
 const oldResult=generateDeltaDDL(readDocument(JSON.stringify(old),'json'));
 const oldSchema=JSON.parse(oldResult.schemaJson);
 const project=(value:any)=>value.fields.map((field:any)=>[field.name,field.type,field.nullable]);
 if(JSON.stringify(project(schema))!==JSON.stringify(project(oldSchema)))throw Error('Current generated physical role fields differ');
 carriers[role]={sql:generated.sql,columns:schema.fields.map((field:any)=>({name:field.name,deltaType:field.type,nullable:field.nullable})),layout:generated.definition};
}
const result={profile:'ashlar-managed-current-carriers/0.1',generator:{project:'UMF',revision:UMF_REVISION,extension:'umf.delta.definition/0.1.0'},inputs,carriers,qualification:'Current public UMF-generated six-role CREATE proposals; no native UC application or UUID/protocol/schema admission.'};
const value=JSON.stringify(result,null,2)+'\n';check();
const target=join(folder,'runtime-carriers.generated.json');
if(mode==='--write')await Bun.write(target,value);else if(await Bun.file(target).text()!==value)throw Error('Stale current managed carriers; review migration and regenerate');
console.log((mode==='--write'?'Generated':'Verified')+' six current UMF 0.10 managed carriers; no SQL applied');
