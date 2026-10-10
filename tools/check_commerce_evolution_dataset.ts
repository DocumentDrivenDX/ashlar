/** Original commerce lexical carriers -> public UMF finite-dataset admission. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const PIN='a95c3ec18a8f904decde884a4fa252988d2a5b0f';
const [repoArg,candidateArg,outputArg]=process.argv.slice(2);
if(!repoArg||!candidateArg||!outputArg)throw Error('Usage: bun tools/check_commerce_evolution_dataset.ts CLEAN_UMF_SOURCE CANDIDATE FRESH_OUTPUT');
const repo=resolve(repoArg),output=resolve(outputArg);
function pin(){for(const [args,expected] of [[['rev-parse','HEAD'],PIN],[['status','--porcelain'],'']] as const){const result=Bun.spawnSync(['git','-C',repo,...args]);if(result.exitCode||new TextDecoder().decode(result.stdout).trim()!==expected)throw Error('Clean exact UMF source required');}}
pin();
const load=(name:string)=>import(pathToFileURL(join(repo,'src',name+'.ts')).href);
const {readDocument}=await load('model/document');
const {readJsonValue}=await load('model/serialization');
const {validateCoreDatasetValuesCompact:validateCoreDatasetValues,verifyCoreDatasetValuesCompact:verifyCoreDatasetValues}=await load('model/dataset-values-compact');
const candidateBytes=await Bun.file(resolve(candidateArg)).bytes();
const hash=(bytes:Uint8Array)=>createHash('sha256').update(bytes).digest('hex');
const candidate=readJsonValue(new TextDecoder().decode(candidateBytes),'json');
if(candidate.profile!=='ashlar-commerce-evolution-preparation/0.1'||candidate.revisions.length!==5)throw Error('Separate finite candidate required');
const outputs=[];
for(const revision of candidate.revisions){
const source=readDocument(JSON.stringify(revision.source),'json'),graph=readJsonValue(JSON.stringify(revision.graph),'json');
if(source.umf!=='0.8.0'||graph.format!=='umf.domain-graph'||graph.version!=='1.0.0')throw Error('Original declared profiles required');
const elements=new Map(source.modules.flatMap((m:any)=>m.elements.map((e:any)=>[JSON.stringify([m.id,e.id]),e])));
const objects=new Map(graph.objects.map((o:any)=>[o.key,o]));
function literal(ref:any,object:any){
 const field:any=elements.get(JSON.stringify([ref.module,ref.element]));const token=object.values[ref.element];
 if(typeof token!=='string')throw Error('Original candidate lexical text required');
 if(field.scalarType==='string')return {string:token};
 if(field.scalarType==='integer')return {integerToken:token};
 if(field.scalarType==='decimal')return {decimalToken:token};
 throw Error('Unsupported explicit source carrier family');
}
const input={scope:{id:'ashlar-original-commerce-fixture',closure:'supplied-dataset-only'},context:{fixtureFormat:graph.format,fixtureVersion:graph.version,qualification:graph.qualification},
 records:graph.objects.map((o:any)=>{if(o.type.document!==source.id)throw Error('Original source identity differs');const identity={module:o.type.module,element:o.type.element};const record:any=elements.get(JSON.stringify([identity.module,identity.element]));return {instanceId:o.key,identity,values:record.members.map((ref:any)=>Object.hasOwn(o.values,ref.element)?{field:ref,state:'present',value:literal(ref,o)}:{field:ref,state:'absent'})};}),
 relationships:graph.edges.map((e:any)=>{if(e.relationship.document!==source.id)throw Error('Original relationship document differs');const module=source.modules.find((m:any)=>m.id===e.relationship.module),r=module.relationships.find((r:any)=>r.id===e.relationship.id),target:any=objects.get(e.target);const endpoint=r.target.find((t:any)=>t.module===target.type.module&&t.element===target.type.element),record:any=elements.get(JSON.stringify([target.type.module,target.type.element])),key=record.keys.find((k:any)=>k.id===endpoint.key);return {instanceId:e.key,identity:{module:module.id,id:r.id},sourceInstanceId:e.source,target:{identity:{module:target.type.module,element:target.type.element,key:key.id},values:key.fields.map((ref:any)=>literal(ref,target))}};})};
const receipt=validateCoreDatasetValues(source,input);
verifyCoreDatasetValues(receipt,source,input);
if(receipt.datasetValidation.valid!==true||receipt.datasetValidation.complete!==true||receipt.records.length!==graph.objects.length||receipt.keys.length!==graph.objects.length||receipt.relationships.length!==graph.edges.length)throw Error('Complete finite original commerce dataset required');
for(const edge of graph.edges){const actual=receipt.relationships.find((r:any)=>r.instanceId===edge.key);if(!actual||actual.sourceInstanceId!==edge.source||actual.targetInstanceId!==edge.target)throw Error('Resolved original endpoint identity differs');}
const controls=[];
for(const name of ['duplicate-key','unresolved-target','missing-required-relationship']){
 const changed=structuredClone(input);
 if(name==='duplicate-key'){const record=structuredClone(changed.records[0]);record.instanceId='separately-authored-duplicate';changed.records.push(record);}
 if(name==='unresolved-target')changed.relationships[0].target.values[0]={string:'separately-authored-missing-target'};
 if(name==='missing-required-relationship')changed.relationships=[];
 const result=validateCoreDatasetValues(source,changed);
 if(result.datasetValidation.valid)throw Error('Adversarial dataset unexpectedly valid: '+name);
 controls.push({name,input:changed,receipt:result});
}
outputs.push({revisionId:revision.id,source,input,receipt,controls,
 evolutionCompatibility:'not-admitted: independent dataset validity does not establish revision compatibility'});
}
pin();
if(await Bun.file(output).exists())throw Error('Fresh receipt output required');
await Bun.write(output,JSON.stringify({profile:'ashlar-commerce-evolution-public-datasets/0.1',umfRevision:PIN,candidateSha256:hash(candidateBytes),outputs,
 qualification:'Separately authored finite candidate datasets, public Record/key/relationship validity only; no old-to-new compatibility, native publication, source authority or catalog admission.'})+'\n');
console.log('Admitted five separate finite datasets; compatibility remains unadmitted');
