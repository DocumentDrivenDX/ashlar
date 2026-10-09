/** Exact original current medical CSV -> public UMF compact dataset admission. */
import {resolve,join} from 'node:path';import {pathToFileURL} from 'node:url';import {createHash} from 'node:crypto';
const PIN='a95c3ec18a8f904decde884a4fa252988d2a5b0f';
const [repoArg,inputArg,outputArg]=process.argv.slice(2);if(!repoArg||!inputArg||!outputArg)throw Error('Explicit clean UMF, original medical input and fresh output required');
const repo=resolve(repoArg),output=resolve(outputArg),base=resolve(import.meta.dir,'../examples/domain-packs/medical');
function pin(){for(const [args,expected]of [[['rev-parse','HEAD'],PIN],[['status','--porcelain'],'']]as const){const result=Bun.spawnSync(['git','-C',repo,...args]);if(result.exitCode||new TextDecoder().decode(result.stdout).trim()!==expected)throw Error('Exact clean public UMF required');}}
pin();const hash=(v:Uint8Array)=>createHash('sha256').update(v).digest('hex');
const custody=await Bun.file(join(base,'source-custody.json')).json();
for(const entry of custody.files){const raw=await Bun.file(join(base,entry.path)).bytes();if(raw.length!==entry.bytes||hash(raw)!==entry.sha256)throw Error('Original medical source custody differs');}
const sourceBytes=await Bun.file(join(base,'upstream/ontology.json')).bytes();if(hash(sourceBytes)!=='fc9938f152e129e041096219be4d7bef70aa35e16f56745411af232ee9cab1c8')throw Error('Original medical model differs');
const load=(n:string)=>import(pathToFileURL(join(repo,'src/model',n+'.ts')).href);
const {readDocument}=await load('document');const {validateCoreDatasetValuesCompact,verifyCoreDatasetValuesCompact}=await load('dataset-values-compact');
const source=readDocument(new TextDecoder().decode(sourceBytes),'json');const inputBytes=await Bun.file(resolve(inputArg)).bytes();if(hash(inputBytes)!=='94ed12620d51933fd5338f212a238bec15e1613832d3a5f6ff28f0efc64b0e5f')throw Error('Exact independently authored original input bytes required');const envelope=JSON.parse(new TextDecoder().decode(inputBytes));
if(envelope.input.context.modelSha256!==hash(sourceBytes)||envelope.input.context.packVersion!=='1.1.0'||envelope.input.context.sourceProfile!=='ashlar-medical-current-csv/0.1'||envelope.input.records.length!==51||envelope.input.relationships.length!==62||envelope.coordinates.length!==51)throw Error('Explicit original current medical source input required');
const receipt=validateCoreDatasetValuesCompact(source,envelope.input);verifyCoreDatasetValuesCompact(receipt,source,envelope.input);
if(receipt.datasetValidation.valid!==true||receipt.datasetValidation.complete!==true||receipt.records.length!==51||receipt.keys.length!==51||receipt.relationships.length!==62)throw Error('Complete original current medical dataset required');
const controls=[];
for(const name of ['duplicate-key','wrong-boolean-carrier','unresolved-reference-target','missing-required-relationship']){
 const changed=structuredClone(envelope.input);
 if(name==='duplicate-key'){const record=structuredClone(changed.records[0]);record.instanceId='separately-authored-duplicate';changed.records.push(record);}
 if(name==='wrong-boolean-carrier'){const record=changed.records.find((r:any)=>r.identity.element==='patients');record.values.find((v:any)=>v.field.element==='patients.active').value={string:'true'};}
 if(name==='unresolved-reference-target')changed.relationships[0].target.values[0]={string:'separately-authored-missing-resource'};
 if(name==='missing-required-relationship')changed.relationships=[];
 const result=validateCoreDatasetValuesCompact(source,changed);if(result.datasetValidation.valid!==false)throw Error('Adversarial medical input accepted');controls.push({name,input:changed,receipt:result});
}
pin();if(await Bun.file(output).exists())throw Error('Fresh receipt required');
await Bun.write(output,JSON.stringify({profile:'ashlar-medical-current-public-dataset/0.1',umfRevision:PIN,sourceSha256:hash(sourceBytes),inputSha256:hash(inputBytes),originalInputText:new TextDecoder().decode(inputBytes),receipt,controls,qualification:'Original current1.1 CSV carriers only; 51 supplied records/62 occurrences with raw resources/reference/type oracle separate. No historical graph, clinical/FHIR validation, storage/publication, Weft/native or ACK claim.'})+'\n');
