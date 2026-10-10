import {writeFileSync} from 'node:fs';
/** Separately authored path lexical carriers -> public UMF finite-dataset admission. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const PIN='c7c95e1c4ea5b72541f47fa0350ca467ff02f395';
const [repoArg,outputArg]=process.argv.slice(2);
if(!repoArg||!outputArg)throw Error('Usage: bun tools/check_path_fixture_dataset.ts CLEAN_UMF_SOURCE FRESH_OUTPUT');
const repo=resolve(repoArg),output=resolve(outputArg);
function pin(){for(const [args,expected] of [[['rev-parse','HEAD'],PIN],[['status','--porcelain'],'']] as const){const result=Bun.spawnSync(['git','-C',repo,...args]);if(result.exitCode||new TextDecoder().decode(result.stdout).trim()!==expected)throw Error('Clean exact UMF source required');}}
pin();
const load=(name:string)=>import(pathToFileURL(join(repo,'src',name+'.ts')).href);
const {readDocument}=await load('model/document');
const {readJsonValue}=await load('model/serialization');
const {validateCoreDatasetValues,verifyCoreDatasetValues}=await load('model/dataset-values');
const base=resolve(import.meta.dir,'../examples/end-to-end/weft-paths');
const sourceBytes=await Bun.file(join(base,'model.umf.json')).bytes(),graphBytes=await Bun.file(join(base,'graph.json')).bytes();
const hash=(bytes:Uint8Array)=>createHash('sha256').update(bytes).digest('hex');
if(hash(sourceBytes)!=='5a6c6de5abb1918e71807c0bc9f5e3ddd31c1aaf5341f1772da0f97ac083148c')throw Error('Original commerce model changed');
if(hash(graphBytes)!=='cd5d72bfe58cb532c6a17c2df9cb45f56086cf8126e9830bb7a335cbf098d915')throw Error('Original commerce graph changed');
const source=readDocument(new TextDecoder().decode(sourceBytes),'json'),graph=readJsonValue(new TextDecoder().decode(graphBytes),'json');
if(source.umf!=='0.8.0'||graph.format!=='ashlar.path-qualification-graph'||graph.version!=='0.1')throw Error('Original declared source profiles required');
const elements=new Map(source.modules.flatMap((m:any)=>m.elements.map((e:any)=>[JSON.stringify([m.id,e.id]),e])));
const objects=new Map(graph.objects.map((o:any)=>[o.key,o]));
function literal(ref:any,object:any){
 const field:any=elements.get(JSON.stringify([ref.module,ref.element]));const token=object.values[ref.element];
 if(typeof token!=='string')throw Error('Original candidate lexical text required');
 if(field.scalarType==='string')return {string:token};
 if(field.scalarType==='integer')return {integerToken:token};
 if(field.scalarType==='decimal')return {decimalToken:token};
 if(field.scalarType==='boolean'){if(token!=='true'&&token!=='false')throw Error('Canonical Boolean required');return {boolean:token==='true'};}
 throw Error('Unsupported explicit source carrier family');
}
const input={scope:{id:'ashlar-authored-path-fixture',closure:'supplied-dataset-only'},context:{fixtureFormat:graph.format,fixtureVersion:graph.version,qualification:graph.qualification},
 records:graph.objects.map((o:any)=>{if(o.type.document!==source.id)throw Error('Original source identity differs');const identity={module:o.type.module,element:o.type.element};const record:any=elements.get(JSON.stringify([identity.module,identity.element]));return {instanceId:o.key,identity,values:record.members.map((ref:any)=>({field:ref,state:'present',value:literal(ref,o)}))};}),
 relationships:graph.edges.map((e:any)=>{if(e.relationship.document!==source.id)throw Error('Original relationship document differs');const module=source.modules.find((m:any)=>m.id===e.relationship.module),r=module.relationships.find((r:any)=>r.id===e.relationship.id),target:any=objects.get(e.target);const endpoint=r.target.find((t:any)=>t.module===target.type.module&&t.element===target.type.element),record:any=elements.get(JSON.stringify([target.type.module,target.type.element])),key=record.keys.find((k:any)=>k.id===endpoint.key);return {instanceId:e.key,identity:{module:module.id,id:r.id},sourceInstanceId:e.source,target:{identity:{module:target.type.module,element:target.type.element,key:key.id},values:key.fields.map((ref:any)=>literal(ref,target))}};})};
const receipt=validateCoreDatasetValues(source,input);
verifyCoreDatasetValues(receipt,source,input);
if(receipt.datasetValidation.valid!==true||receipt.datasetValidation.complete!==true||receipt.records.length!==10||receipt.keys.length!==10||receipt.relationships.length!==10)throw Error('Complete finite original commerce dataset required');
for(const edge of graph.edges){const actual=receipt.relationships.find((r:any)=>r.instanceId===edge.key);if(!actual||actual.sourceInstanceId!==edge.source||actual.targetInstanceId!==edge.target)throw Error('Resolved original endpoint identity differs');}
const controls=[];
for(const name of ['duplicate-key','unresolved-target']){
 const changed=structuredClone(input);
 if(name==='duplicate-key'){const record=structuredClone(changed.records[0]);record.instanceId='separately-authored-duplicate';changed.records.push(record);}
 if(name==='unresolved-target')changed.relationships[0].target.values[0]={integerToken:'999'};
 if(name==='missing-required-relationship')changed.relationships=[];
 const result=validateCoreDatasetValues(source,changed);
 if(result.datasetValidation.valid)throw Error('Adversarial dataset unexpectedly valid: '+name);
 controls.push({name,input:changed,receipt:result});
}
pin();
if(await Bun.file(output).exists())throw Error('Fresh receipt output required');
writeFileSync(output,JSON.stringify({profile:'ashlar-authored-path-public-dataset/0.1',umfRevision:PIN,sourceSha256:hash(sourceBytes),graphSha256:hash(graphBytes),receipt,controls,qualification:'Separately authored UMF0.8 source and candidate lexical carriers, supplied finite dataset only. No canonical storage IDs, source authority, Delta publication, Weft runtime or ACK admission.'})+'\n',{flag:'wx'});
console.log('Admitted authored path dataset: 10 records, 10 keys, 10 occurrences; two negative controls refused');
