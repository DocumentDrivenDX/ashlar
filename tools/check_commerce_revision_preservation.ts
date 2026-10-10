/** Tool-only composition of public dataset and source-preservation APIs; no migration authority. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {gunzipSync} from 'node:zlib';
import {writeFile} from 'node:fs/promises';
export const PIN='e44cd15f336dfb33db35acf20eee13dd120a1a28';
export const CANDIDATE_SHA='1c210a1bb3a5ef510d832715b15d238cdcb5126aa23b3f93d01d9be9c4c9d692';
export const DATASET_SHA='40ce6e670308c9749b1f13139c136a7ce1b134335fc49a19669cb68771b19952';
const hash=(bytes:Uint8Array)=>createHash('sha256').update(bytes).digest('hex');
const IDS=['R1','R2-update','R3-delete','R4-additive-candidate','breaking-candidate'];
export function admitRetainedBytes(candidateBytes:Uint8Array,datasetBytes:Uint8Array){
 if(candidateBytes.length>4_000_000||datasetBytes.length>4_000_000||hash(candidateBytes)!==CANDIDATE_SHA||hash(datasetBytes)!==DATASET_SHA)throw Error('Original retained input byte custody differs');
 const decode=(b:Uint8Array)=>JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(b));
 const candidate=decode(candidateBytes),datasets=decode(datasetBytes);
 if(candidate.profile!=='ashlar-commerce-evolution-preparation/0.1'||datasets.profile!=='ashlar-commerce-evolution-public-datasets/0.1'||datasets.candidateSha256!==CANDIDATE_SHA||JSON.stringify(candidate.revisions.map((r:any)=>r.id))!==JSON.stringify(IDS)||JSON.stringify(datasets.outputs.map((r:any)=>r.revisionId))!==JSON.stringify(IDS))throw Error('Exact separately admitted finite inventory differs');
 return {candidate,datasets};
}
export async function run(repoArg:string,candidateArg:string,datasetArg:string,outputArg:string){
 const output=resolve(outputArg);if(await Bun.file(output).exists())throw Error('Fresh output required');
 const candidateBytes=await Bun.file(resolve(candidateArg)).bytes(),compressed=await Bun.file(resolve(datasetArg)).bytes();
 if(compressed.length>4_000_000)throw Error('Bounded retained archive required');
 const datasetBytes=new Uint8Array(gunzipSync(compressed,{maxOutputLength:4_000_000}));
 const {candidate,datasets}=admitRetainedBytes(candidateBytes,datasetBytes);
 const repo=resolve(repoArg);
 function pin(){for(const [args,expected]of [[['rev-parse','HEAD'],PIN],[['status','--porcelain'],'']]as const){const result=Bun.spawnSync(['git','-C',repo,...args]);if(result.exitCode||new TextDecoder().decode(result.stdout).trim()!==expected)throw Error('Exact clean official UMF producer required');}}
 pin();const load=(name:string)=>import(pathToFileURL(join(repo,'src',name+'.ts')).href);
 const {validateCoreDatasetValuesCompact,verifyCoreDatasetValuesCompact}=await load('model/dataset-values-compact');
 const {inspectCoreEvolution,verifyCoreEvolution}=await load('model/evolution');
 const datasetChecks=[];
 for(let i=0;i<IDS.length;i++){
  const revision=candidate.revisions[i],retained=datasets.outputs[i];
  if(JSON.stringify(revision.source)!==JSON.stringify(retained.source))throw Error('Original expected source correspondence differs');
  const fresh=validateCoreDatasetValuesCompact(revision.source,retained.input);
  verifyCoreDatasetValuesCompact(fresh,revision.source,retained.input);
  verifyCoreDatasetValuesCompact(retained.receipt,revision.source,retained.input);
  if(JSON.stringify(fresh)!==JSON.stringify(retained.receipt)||fresh.datasetValidation.valid!==true||fresh.datasetValidation.complete!==true)throw Error('Complete original public dataset receipt differs');
  const controls=[];
  for(const control of retained.controls){const result=validateCoreDatasetValuesCompact(revision.source,control.input);verifyCoreDatasetValuesCompact(result,revision.source,control.input);if(JSON.stringify(result)!==JSON.stringify(control.receipt)||result.datasetValidation.valid!==false)throw Error('Original negative public receipt differs');controls.push({name:control.name,input:control.input,receipt:result});}
  datasetChecks.push({revisionId:revision.id,source:revision.source,input:retained.input,receipt:fresh,controls});
 }
 const policy={profile:'core-0.8-absent-string-additions/0.1'}as const,transitions=[];
 for(let i=1;i<IDS.length;i++){
  const before=candidate.revisions[i-1],after=candidate.revisions[i];
  const receipt=inspectCoreEvolution(before.source,after.source,policy);
  verifyCoreEvolution(receipt,before.source,after.source,policy);
  const expected=i===4?'breaking':'preserved';
  if(receipt.classification!==expected)throw Error('Named public source-preservation disposition differs');
  transitions.push({beforeRevision:before.id,afterRevision:after.id,expectedBeforeSource:before.source,expectedAfterSource:after.source,policy,receipt,sourceCompatibilityEligible:receipt.classification==='preserved',dataTransition:'not-admitted: standalone dataset validity and source preservation do not validate update/delete effects or native migration'});
 }
 pin();if(await Bun.file(output).exists())throw Error('Output appeared during admission');
 await writeFile(output,JSON.stringify({profile:'ashlar-commerce-public-source-preservation/0.1',umfRevision:PIN,originalCandidateSha256:CANDIDATE_SHA,originalPublicDatasetSha256:DATASET_SHA,originalCompressedDatasetSha256:hash(compressed),datasetChecks,transitions,qualification:'Public finite datasets and narrowly named source-preservation only; original numeric development registry remains unchanged. No data-transition/native migration/source authority/catalog/publication/ACK claim.'})+'\n',{flag:'wx'});
}
if(import.meta.main){const args=process.argv.slice(2);if(args.length!==4)throw Error('Usage: bun tools/check_commerce_revision_preservation.ts CLEAN_UMF CANDIDATE ORIGINAL_DATASETS_GZIP FRESH_OUTPUT');await run(args[0]!,args[1]!,args[2]!,args[3]!);console.log('Revalidated five complete datasets and four public source-preservation transitions; native migration remains unadmitted');}
