/** Public document validation for every original pinned pack model; no data admission. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [validatorArg,packsArg,outputArg]=process.argv.slice(2);
if(!validatorArg||!packsArg||!outputArg)throw Error('Usage: bun tools/check_domain_pack_models.ts CLEAN_UMF_VALIDATOR PINNED_PACK_REPO FRESH_OUTPUT');
const revision='c7c95e1c4ea5b72541f47fa0350ca467ff02f395',packRevision='1f7b5f5d2a355c4b476e3a96b289b9048f03f567';
const validator=resolve(validatorArg),packs=resolve(packsArg),output=resolve(outputArg);
function git(root:string,args:string[]){const r=Bun.spawnSync(['git','-C',root,...args]);if(r.exitCode)throw Error('Pinned Git source unavailable');return r.stdout;}
function check(){if(new TextDecoder().decode(git(validator,['rev-parse','HEAD'])).trim()!==revision||git(validator,['status','--porcelain']).length)throw Error('Exact clean UMF validator required');}
check();
const inventoryBytes=await Bun.file(resolve(import.meta.dir,'../examples/domain-packs/inventory.json')).bytes();
const inventory=JSON.parse(new TextDecoder().decode(inventoryBytes));
if(inventory.commit!==packRevision||inventory.packs.length!==24)throw Error('Original full pack inventory required');
const load=(p:string)=>import(pathToFileURL(join(validator,p)).href);
const [{readDocument},{validateDocument},{UmfError}]=await Promise.all([load('src/model/document.ts'),load('src/validation/document.ts'),load('src/model/types.ts')]);
const hash=(b:Uint8Array)=>createHash('sha256').update(b).digest('hex');
const results=[];
for(const pack of inventory.packs){
 if(!pack.ontology){results.push({pack:pack.directory,operation:{status:'not-available',reason:'No UMF ontology declared in the original pinned inventory'}});continue;}
 const path=pack.ontology.path;
 if(typeof path!=='string'||!/^[-a-zA-Z0-9_/]+\.json$/.test(path)||path.split('/').some((x:string)=>x==='..'))throw Error('Exact normalized model path required');
 const original=git(packs,['show',packRevision+':spec/domain-packs/'+pack.directory+'/'+path]);
 const file=pack.files.find((f:any)=>f.path===path);
 if(!file||file.sha256!==hash(original)||file.bytes!==original.length)throw Error('Original model inventory custody differs');
 let operation;
 try {const source=readDocument(new TextDecoder('utf-8',{fatal:true}).decode(original),'json');operation={status:'returned',source,validation:validateDocument(source)};}
 catch(error:any){if(!(error instanceof UmfError))throw error;operation={status:'refused',code:error.code,path:error.path,message:error.message};}
 results.push({pack:pack.directory,modelPath:path,sourceSha256:hash(original),sourceBytes:original.length,sourceBase64:Buffer.from(original).toString('base64'),operation});
}
check();
if(await Bun.file(output).exists())throw Error('Fresh output required');
await Bun.write(output,JSON.stringify({profile:'ashlar-pack-model-public-validation/0.1',packRevision,validatorRevision:revision,inventorySha256:hash(inventoryBytes),results,qualification:'Original model document validation only. No value, Record, dataset, source authority, storage binding, Weft or engine execution admission.'})+'\n');
console.log(JSON.stringify({packs:results.length,returned:results.filter(x=>x.operation.status==='returned').length,refused:results.filter(x=>x.operation.status==='refused').length}));
