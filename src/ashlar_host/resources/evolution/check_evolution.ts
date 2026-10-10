/** Separately authored R4 presence control; original assets and public APIs unchanged. */
import {createHash} from 'node:crypto';
import {gunzipSync} from 'node:zlib';
import {writeFile} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
const PIN='e44cd15f336dfb33db35acf20eee13dd120a1a28';
const CANDIDATE_SHA='1c210a1bb3a5ef510d832715b15d238cdcb5126aa23b3f93d01d9be9c4c9d692';
const PROOF_SHA='8d7f8e214222504fedb082336159ced54ea4782218008efe069bb1b44ef2268c';
const hash=(b:Uint8Array)=>createHash('sha256').update(b).digest('hex');
export function augment(candidate:any,proof:any){
 const result=structuredClone(candidate);result.profile='ashlar-commerce-evolution-presence-preparation/0.1';
 const productKey='ashlar-authored-evolution-product-present',edgeKey='ashlar-authored-evolution-product-supplier-edge';
 const r4=result.revisions[3],original=r4.graph.objects.find((o:any)=>o.type.element==='products');
 const product=structuredClone(original);product.key=productKey;product.values['products.id']='ashlar-added-product';product.values['products.name']='Added presence control';product.values['products.evolution_note']='Explicit newly authored String';
 const edge=structuredClone(r4.graph.edges.find((e:any)=>e.source===original.key));edge.key=edgeKey;edge.source=productKey;
 r4.graph.objects.push(product);r4.graph.edges.push(edge);result.revisions[4].graph=structuredClone(r4.graph);
 for(const [kind,key]of [['object',productKey],['edge',edgeKey]]){
  const originalBinding=result.registry.entities.find((b:any)=>b.kind===kind&&b.originalKey===(kind==='object'?original.key:r4.graph.edges.find((e:any)=>e.key!==edgeKey&&e.source===original.key).key));
  result.registry.entities.push({...originalBinding,originalKey:key,id:String(Math.max(...result.registry.entities.filter((b:any)=>b.kind===kind).map((b:any)=>Number(b.id)))+1)});
 }
 result.registry.profile='ashlar-commerce-evolution-presence-development-registry/0.1';
 const inputs=proof.datasetChecks.map((d:any)=>structuredClone(d.input));
 for(const index of [3,4]){
  const input=inputs[index],record=structuredClone(input.records.find((r:any)=>r.instanceId===original.key));record.instanceId=productKey;
  for(const value of record.values){if(value.field.element==='products.id')value.value={string:product.values['products.id']};if(value.field.element==='products.name')value.value={string:product.values['products.name']};if(value.field.element==='products.evolution_note'){value.state='present';value.value={string:product.values['products.evolution_note']};}}
  input.records.push(record);const occurrence=structuredClone(input.relationships.find((e:any)=>e.sourceInstanceId===original.key));occurrence.instanceId=edgeKey;occurrence.sourceInstanceId=productKey;input.relationships.push(occurrence);input.scope.id='ashlar-commerce-evolution-presence-control';
 }
 result.qualification='Separately authored presence control; exact original rows/model/IDs retained, one new product and independent required supplier edge appended. New development source keys are opaque fixture identities, never UMF canonical Value or Truss accepted IDs.';
 return {candidate:result,inputs,authored:{product,edge,originalProductKey:original.key}};
}
export async function run(repoArg:string,candidateArg:string,proofArg:string,outputArg:string){
 const raw=await Bun.file(candidateArg).bytes(),compressed=await Bun.file(proofArg).bytes();if(raw.length>4_000_000||compressed.length>4_000_000)throw Error('Bounded archived inputs required');
 const originalProof=new Uint8Array(gunzipSync(compressed,{maxOutputLength:4_000_000}));if(hash(raw)!==CANDIDATE_SHA||hash(originalProof)!==PROOF_SHA)throw Error('Original reviewed byte custody differs');
 const seed=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw)),proof=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(originalProof));
 const {candidate,inputs,authored}=augment(seed,proof);const repo=resolve(repoArg),output=resolve(outputArg);
 if(await Bun.file(output).exists())throw Error('Fresh output required');
 const {validateCoreDatasetValuesCompact,verifyCoreDatasetValuesCompact,inspectCoreEvolution,verifyCoreEvolution}=await import(pathToFileURL(join(repo,'src/index.ts')).href);
 const datasetChecks=[];
 for(let i=0;i<5;i++){
  const source=candidate.revisions[i].source,input=inputs[i],receipt=validateCoreDatasetValuesCompact(source,input);verifyCoreDatasetValuesCompact(receipt,source,input);
  if(receipt.datasetValidation.valid!==true||receipt.datasetValidation.complete!==true||receipt.records.length!==candidate.revisions[i].graph.objects.length||receipt.relationships.length!==candidate.revisions[i].graph.edges.length)throw Error('Complete original+authored supplied dataset required');
  for(const occurrence of candidate.revisions[i].graph.edges){const actual=receipt.relationships.find((r:any)=>r.instanceId===occurrence.key);if(!actual||actual.sourceInstanceId!==occurrence.source||actual.targetInstanceId!==occurrence.target)throw Error('Authored independent edge correspondence differs');}
  const controls=[];
  for(const name of ['duplicate-key','unresolved-target','missing-required-relationship']){
   const changed=structuredClone(input);if(name==='duplicate-key'){const duplicate=structuredClone(changed.records[0]);duplicate.instanceId='separately-authored-duplicate';changed.records.push(duplicate);}if(name==='unresolved-target')changed.relationships[0].target.values[0]={string:'separately-authored-missing-target'};if(name==='missing-required-relationship')changed.relationships=[];
   const negative=validateCoreDatasetValuesCompact(source,changed);verifyCoreDatasetValuesCompact(negative,source,changed);if(negative.datasetValidation.valid!==false)throw Error('Public adversarial dataset unexpectedly valid');controls.push({name,input:changed,receipt:negative});
  }
  datasetChecks.push({revisionId:candidate.revisions[i].id,source,input,receipt,controls});
 }
 const policy={profile:'core-0.8-absent-string-additions/0.1'}as const,transitions=[];
 for(let i=1;i<5;i++){const before=candidate.revisions[i-1].source,after=candidate.revisions[i].source,receipt=inspectCoreEvolution(before,after,policy);verifyCoreEvolution(receipt,before,after,policy);if(receipt.classification!==(i===4?'breaking':'preserved'))throw Error('Exact source preservation classification differs');transitions.push({expectedBeforeSource:before,expectedAfterSource:after,policy,receipt,dataTransition:'not-admitted'});}
 const receipt=JSON.stringify({profile:'ashlar-commerce-evolution-presence-public/0.1',umfRevision:PIN,originalCandidateSha256:CANDIDATE_SHA,originalPreservationSha256:PROOF_SHA,candidate,authored,datasetChecks,transitions,qualification:'Public finite source/data validity and source-model preservation only; no source authority/native migration/catalog/publication/ACK claim.'})+'\n';const receiptBytes=new TextEncoder().encode(receipt);if(receiptBytes.length>4_000_000)throw Error('Bounded receipt required');await writeFile(output,receiptBytes,{flag:'wx'});
}
if(import.meta.main){const a=process.argv.slice(2);if(a.length!==4)throw Error('Usage: bun check_evolution.ts CLEAN_UMF ORIGINAL_CANDIDATE ORIGINAL_PUBLIC_PROOF_GZIP FRESH_OUTPUT');await run(a[0]!,a[1]!,a[2]!,a[3]!);console.log('Publicly admitted separately authored Field35 presence dataset; native transitions remain unadmitted');}
