/** Reproduce only proposed original-commerce local physical variants; no native effects. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const root=resolve(import.meta.dir,'..');
const relative='docs/helix/02-design/models/original-commerce-local-runtime';
const folder=join(root,relative),[sourceArg,mode='--check']=process.argv.slice(2);
if(!sourceArg||!['--write','--check'].includes(mode))throw Error('Explicit pinned UMF source and --write/--check required');
const source=resolve(sourceArg),index=await Bun.file(join(folder,'index.json')).json();
function git(...args:string[]){const value=Bun.spawnSync(['git','-C',source,...args]);if(value.exitCode)throw Error('Unable to verify selected UMF source');return new TextDecoder().decode(value.stdout).trim();}
function sourceCurrent(){if(git('rev-parse','HEAD')!==index.umfRevision||git('status','--porcelain'))throw Error('Exact clean pinned UMF source required');}
sourceCurrent();
const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
const {generateDeltaDDL}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
const digest=(value:string)=>createHash('sha256').update(value).digest('hex');
const roles=['object_current','edge_current','tombstone','whole_source_history','attempts','manifest'];
if(index.profile!=='ashlar-original-commerce-local-runtime-model/0.1'||JSON.stringify(index.tables)!==JSON.stringify(roles))throw Error('Exact six local physical models required');
const correspondence=await Bun.file(join(folder,'e-correspondence.json')).json();
if(correspondence.profile!=='ashlar-original-commerce-local-schema-correspondence/0.1'||Object.keys(correspondence.roles).sort().join()!==[...roles].sort().join())throw Error('Exact independently retained E schema correspondence required');
const inputs:Record<string,string>={},carriers:Record<string,unknown>={};
for(const role of roles){
 const path=relative+'/'+role+'.umf.json',text=await Bun.file(join(root,path)).text();
 const result=generateDeltaDDL(readDocument(text,'json')),schema=JSON.parse(result.schemaJson),expected=correspondence.roles[role];
 if(JSON.stringify(result.definition.name)!==JSON.stringify([role])||result.definition.clusterBy.length||result.definition.partitionBy.length||Object.keys(result.definition.properties).length)throw Error('Exact local E names/layout/properties required');
 if(JSON.stringify(schema)!==JSON.stringify(expected.schema)||Object.keys(expected.configuration).length||expected.partitionColumns.length)throw Error('Complete ordered E physical schema metadata differs');
 inputs[path]=digest(text);carriers[role]={sql:result.sql,schemaJson:result.schemaJson,layout:result.definition,qualification:result.qualification};
}
for(const name of ['index.json','e-correspondence.json'])inputs[relative+'/'+name]=digest(await Bun.file(join(folder,name)).text());
sourceCurrent();
const text=JSON.stringify({profile:'ashlar-generated-original-commerce-local-carriers/0.1',generator:{project:'UMF',revision:index.umfRevision,extension:'umf.delta.definition/0.1.0'},inputs,carriers,qualification:'Public UMF generated DDL/schema proposals matching retained E local metadata. Managed-definition rendering subset; no native path application, UC qualification, source grant or migration.'},null,2)+'\n';
const output=join(root,'sql/ashlar-original-commerce-local/runtime-carriers.generated.json');
if(mode==='--write')await Bun.write(output,text);
else if(await Bun.file(output).text()!==text)throw Error('Generated local carriers stale');
console.log('Verified six original-commerce local UMF physical carriers');
