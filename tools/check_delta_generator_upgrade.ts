/** Read-only compatibility check; never repins an existing installation proof. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const root=resolve(import.meta.dir,'..');
const [sourceArg,revision]=process.argv.slice(2);
if(!sourceArg||!revision||!/^[0-9a-f]{40}$/.test(revision))throw Error('Usage: bun tools/check_delta_generator_upgrade.ts UMF_SOURCE EXACT_REVISION');
const source=resolve(sourceArg);
function pinned(){
  for(const [args,expected] of [[['rev-parse','HEAD'],revision],[['status','--porcelain'],'']] as const){
    const result=Bun.spawnSync(['git','-C',source,...args]);
    if(result.exitCode||new TextDecoder().decode(result.stdout).trim()!==expected)throw Error('Clean exact candidate UMF source required');
  }
}
pinned();
const artifactText=await Bun.file(join(root,'sql/ashlar-delta-v03/runtime-carriers.generated.json')).text();
const original=JSON.parse(artifactText);
const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
const {generateDeltaDDL}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
const digest=(text:string)=>createHash('sha256').update(text).digest('hex');
const checked:string[]=[];
for(const [relative,sha] of Object.entries(original.inputs)){
  const text=await Bun.file(join(root,relative)).text();
  if(digest(text)!==sha)throw Error('Original model custody differs: '+relative);
  if(relative.endsWith('/index.json'))continue;
  const name=relative.split('/').at(-1)!.replace('.umf.json','');
  const result=generateDeltaDDL(readDocument(text,'json')),schema=JSON.parse(result.schemaJson);
  const candidate={sql:result.sql,columns:schema.fields.map((f:any)=>({name:f.name,deltaType:f.type,nullable:f.nullable})),layout:result.definition};
  if(JSON.stringify(candidate)!==JSON.stringify(original.carriers[name]))throw Error('Candidate changes original carrier: '+name);
  checked.push(name);
}
if(checked.length!==8||JSON.stringify([...checked].sort())!==JSON.stringify(Object.keys(original.carriers).sort()))throw Error('Incomplete carrier inventory');
pinned();
if(await Bun.file(join(root,'sql/ashlar-delta-v03/runtime-carriers.generated.json')).text()!==artifactText)throw Error('Original artifact changed during check');
console.log(JSON.stringify({state:'passed',profile:'ashlar-delta-generator-upgrade/0.1',originalGenerator:original.generator,candidateRevision:revision,originalArtifactSha256:digest(artifactText),modelInputs:original.inputs,checkedTables:checked,qualification:'Actual candidate UMF generation matches all eight original SQL, ordered columns and layouts. No default repin, installation proof rewrite, SQL application, migration or native admission.'},null,2));
