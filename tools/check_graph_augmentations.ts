/** Separately authored fixture -> original public UMF finite dataset receipt. */
import {createHash} from 'node:crypto';import {join,resolve}from'node:path';import{pathToFileURL}from'node:url';
const PIN='c7c95e1c4ea5b72541f47fa0350ca467ff02f395';
const [repoArg,modelPath,sourcePath,outputPath]=process.argv.slice(2);if(!repoArg||!modelPath||!sourcePath||!outputPath)throw Error('Explicit cleanUMF/model/source/freshoutput required');
const repo=resolve(repoArg);function pin(){for(const[args,expected]of[[['rev-parse','HEAD'],PIN],[['status','--porcelain'],'']]as const){const r=Bun.spawnSync(['git','-C',repo,...args]);if(r.exitCode||new TextDecoder().decode(r.stdout).trim()!==expected)throw Error('Exact clean public UMF pin required');}}pin();
const load=(n:string)=>import(pathToFileURL(join(repo,'src',n+'.ts')).href);
const {readDocument}=await load('model/document'),{readJsonValue}=await load('model/serialization'),{validateCoreDatasetValues,verifyCoreDatasetValues}=await load('model/dataset-values');
const modelBytes=await Bun.file(modelPath).bytes(),sourceBytes=await Bun.file(sourcePath).bytes();
const model=readDocument(new TextDecoder('utf-8',{fatal:true}).decode(modelBytes),'json'),source:any=readJsonValue(new TextDecoder('utf-8',{fatal:true}).decode(sourceBytes),'json');
if(model.umf!=='0.8.0'||source.format!=='ashlar-authored-graph-augmentations/0.1'||source.nodes.length!==26||source.edges.length!==6)throw Error('Original authored complete fixture required');
const input={scope:{id:'ashlar-authored-graph-augmentations',closure:'supplied-dataset-only'},context:{qualification:source.qualification},records:source.nodes.map((n:any)=>({instanceId:n.key,identity:{module:'fixture',element:'Item'},values:n.values})),relationships:source.edges.map((e:any)=>({instanceId:e.key,identity:{module:'fixture',id:'link'},sourceInstanceId:e.source,target:{identity:{module:'fixture',element:'Item',key:'identity'},values:[{string:e.target}]}}))};
const receipt=validateCoreDatasetValues(model,input);verifyCoreDatasetValues(receipt,model,input);
if(receipt.datasetValidation.valid!==true||receipt.datasetValidation.complete!==true)throw Error('Public finite fixture refused '+JSON.stringify(receipt.datasetValidation));
const controls=[];
for(const name of ['absent-required','null-excluded']){
 const changed=structuredClone(input);const item=changed.records[0].values[0];
 if(name==='absent-required'){item.state='absent';delete item.value;}else item.value=null;
 const result=validateCoreDatasetValues(model,changed);if(result.datasetValidation.valid!==false)throw Error('Invalid control unexpectedly admitted');controls.push({name,input:changed,receipt:result});
}
pin();if(await Bun.file(outputPath).exists())throw Error('Fresh receipt required');
const hash=(v:Uint8Array)=>createHash('sha256').update(v).digest('hex');
await Bun.write(outputPath,JSON.stringify({format:'ashlar-authored-graph-augmentations-public/0.1',umfRevision:PIN,modelSha256:hash(modelBytes),sourceSha256:hash(sourceBytes),originalModelBase64:Buffer.from(modelBytes).toString('base64'),originalSourceBase64:Buffer.from(sourceBytes).toString('base64'),input,receipt,controls,qualification:'Public finite dataset admission only; separately authored original fixture, no native storage/publication/ACK authority.'})+'\n');
console.log('Publicly admitted authored26records/26keys/6parallel-preservingoccurrences;2invalid controls refused');
