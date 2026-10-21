/** Generate the separately versioned managed-current xxhash64 DDL proposal. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';

export const UMF_REVISION='2ede4c879bea5db13794a6fa30e67c821306c17f';
const root=resolve(import.meta.dir,'..');
const folder=join(root,'src/ashlar_host/resources/managed-current-core-010-xxhash64');
const legacyFolder=join(root,'src/ashlar_host/resources/managed-current-core-010');
const [sourceArg,mode='--check']=process.argv.slice(2);
if(!sourceArg||!['--check','--write'].includes(mode))throw Error('Usage: bun tools/generate_managed_current_xxhash64_carriers.ts UMF_SOURCE [--check|--write]');
const source=resolve(sourceArg);
function git(...args:string[]){const result=Bun.spawnSync(['git','-C',source,...args]);if(result.exitCode)throw Error('Unable to verify UMF source');return new TextDecoder().decode(result.stdout).trim();}
const archiveMarker=join(source,'.umf-source-revision');
const isPinnedArchive=await Bun.file(archiveMarker).text().catch(()=> '')===UMF_REVISION+'\n';
function check(){if(isPinnedArchive){return;}if(git('rev-parse','HEAD')!==UMF_REVISION||git('status','--porcelain'))throw Error('UMF source must be clean at the pinned current revision or exact git archive');}
check();
const index=await Bun.file(join(folder,'index.json')).json();
if(index.profile!=='ashlar-managed-current-carrier-layout/0.4-xxhash64'||index.physicalProfile!=='ashlar-delta/0.4-xxhash64'||index.generatorRevision!==UMF_REVISION||index.umfCore!=='0.10.0')throw Error('Versioned current xxhash64 carrier profile required');
const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
const {generateDeltaDDL}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
const digest=(text:string)=>createHash('sha256').update(text).digest('hex');
const inputs:Record<string,string>={},carriers:Record<string,unknown>={};
const roles=index.roles;
if(!Array.isArray(roles)||roles.length!==6||new Set(roles).size!==6)throw Error('Exact six-role current inventory required');
for(const name of ['index.json',...roles.map((role:string)=>role+'.umf.json')]){
 const text=await Bun.file(join(folder,name)).text();inputs['src/ashlar_host/resources/managed-current-core-010-xxhash64/'+name]=digest(text);
 if(name==='index.json')continue;
 const role=name.slice(0,-9),model=JSON.parse(text),legacyText=await Bun.file(join(legacyFolder,name)).text(),legacy=JSON.parse(legacyText);
 if(digest(legacyText)!==index.migration.sourceSha256[role])throw Error('Original current carrier input digest differs from migration provenance');
 legacy.umf='0.10.0';
 const expected=JSON.parse(JSON.stringify(legacy));
 if(role==='object_current'||role==='edge_current'){
  const typed=role==='object_current'?'type_id':'rel_type_id';let matches=0;
  const walk=(value:any)=>{if(Array.isArray(value)){for(const x of value)walk(x);return;}if(value&&typeof value==='object'){
   if(value.members?.name?.value==='lookup_hash'){
    value.members.type.value='long';
    value.members.metadata.members.comment={kind:'string',value:`Spark SQL xxhash64 seed 42 over typed identity (source_system STRING, ${typed} BIGINT, id BIGINT); pruning only, exact key remains authoritative.`};
    matches++;
   }
   for(const child of Object.values(value))walk(child);
  }};walk(expected);
  if(matches!==1)throw Error('Exactly one lookup_hash field must be versioned');
  const definition=expected.extensions?.['umf.delta.definition'];
  if(!definition||!definition.clusterBy?.includes('lookup_hash'))throw Error(typed+' carrier must cluster on the lookup fingerprint');
  definition.properties['delta.dataSkippingStatsColumns']=`lookup_hash,source_system,${typed},id`;
 }
 if(JSON.stringify(expected)!==JSON.stringify(model))throw Error('Current xxhash64 carrier may change only lookup_hash type/comment and exact-key statistics list');
 if(model.umf!=='0.10.0'||model.vocabularies['umf.delta'].version!=='0.1.0'||model.vocabularies['umf.delta.definition'].version!=='0.1.0')throw Error('Exact UMF core and retained Delta vocabularies required');
 const generated=generateDeltaDDL(readDocument(text,'json'));
 if(JSON.stringify(generated.definition.name)!==JSON.stringify([role]))throw Error('Native role identity differs');
 const schema=JSON.parse(generated.schemaJson),oldSchema=JSON.parse(generateDeltaDDL(readDocument(legacyText,'json')).schemaJson);
 const project=(value:any)=>value.fields.map((field:any)=>[field.name,field.type,field.nullable]);
 const projected=project(schema),prior=project(oldSchema);
 if(projected.length!==prior.length||projected.some((field:any,i:number)=>field[0]!==prior[i][0]||field[2]!==prior[i][2]||field[1]!==((role==='object_current'||role==='edge_current')&&field[0]==='lookup_hash'?'long':prior[i][1])))throw Error('Current carrier schema differs outside the versioned hash type');
 carriers[role]={sql:generated.sql,columns:schema.fields.map((field:any)=>({name:field.name,deltaType:field.type,nullable:field.nullable})),layout:generated.definition};
}
const result={profile:'ashlar-managed-current-carriers/0.4-xxhash64',physicalProfile:index.physicalProfile,generator:{project:'UMF',revision:UMF_REVISION,extension:'umf.delta.definition/0.1.0'},inputs,carriers,qualification:'UMF-generated fresh six-role managed Delta CREATE proposals for the versioned xxhash64 profile; no SQL applied, migration, native compatibility or enforcement claim.'};
const value=JSON.stringify(result,null,2)+'\n';check();
const target=join(folder,'runtime-carriers.generated.json');
if(mode==='--write')await Bun.write(target,value);else if(await Bun.file(target).text()!==value)throw Error('Stale current managed xxhash64 carriers; review model and regenerate');
console.log((mode==='--write'?'Generated':'Verified')+' six current UMF 0.10 managed xxhash64 carriers; no SQL applied');
