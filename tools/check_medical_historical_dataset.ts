/** Explicit original historical medical graph lexical conversion and public admission. */
import {resolve,join} from 'node:path';import {pathToFileURL} from 'node:url';import {createHash} from 'node:crypto';
const PIN='8e76c74d14203225d1ef159c9132bb9e9b0cdffe';
const [repoArg,oracleArg,outputArg]=process.argv.slice(2);if(!repoArg||!oracleArg||!outputArg)throw Error('Explicit clean public UMF, exact independent historical oracle and fresh output required');
const repo=resolve(repoArg),base=resolve(import.meta.dir,'../examples/domain-packs/medical'),output=resolve(outputArg);
function pin(){for(const [args,expected]of [[['rev-parse','HEAD'],PIN],[['status','--porcelain'],'']]as const){const result=Bun.spawnSync(['git','-C',repo,...args]);if(result.exitCode||new TextDecoder().decode(result.stdout).trim()!==expected)throw Error('Exact clean public UMF required');}}
const hash=(v:Uint8Array|string)=>createHash('sha256').update(v).digest('hex');pin();
const custody=await Bun.file(join(base,'source-custody.json')).json();
for(const entry of custody.files){const raw=await Bun.file(join(base,entry.path)).bytes();if(raw.length!==entry.bytes||hash(raw)!==entry.sha256)throw Error('Original source bytes differ');}
const packBytes=await Bun.file(join(base,'historical/original-pack.json')).bytes(),archiveBytes=await Bun.file(join(base,'historical/original-1.0.0.zip')).bytes();if(hash(packBytes)!==custody.historicalPackSha256||hash(archiveBytes)!==custody.historicalArchiveSha256)throw Error('Own original archive/pack differs');
const sourceBytes=await Bun.file(join(base,'historical/archive/schemas/ontology.json')).bytes(),graphBytes=await Bun.file(join(base,'upstream/graph/fixture.json')).bytes(),graph=JSON.parse(new TextDecoder().decode(graphBytes)),pack=JSON.parse(new TextDecoder().decode(packBytes));
if(pack.version!=='1.0.0'||graph.pack.version!=='1.0.0'||graph.pack.sha256!==hash(packBytes)||graph.schema.sha256!==hash(sourceBytes)||graph.schema.revision!==pack.version)throw Error('Own historical pairing differs');
const oracleBytes=await Bun.file(resolve(oracleArg)).bytes(),oracle=JSON.parse(new TextDecoder().decode(oracleBytes));
// Bound to the independently regenerated original archive CSV/key/FK oracle, not user expectations.
if(hash(oracleBytes)!=='8afdddd0e30bfb06d27ca5f30ece8271abd75dfc519cfb9de6e463a12755b508')throw Error('Exact original independent oracle bytes required');
if(oracle.packVersion!=='1.0.0'||oracle.packSha256!==hash(packBytes)||oracle.archiveSha256!==hash(archiveBytes)||oracle.modelSha256!==hash(sourceBytes)||oracle.graphSha256!==hash(graphBytes)||JSON.stringify(oracle.objects)!==JSON.stringify(graph.objects)||JSON.stringify(oracle.edges)!==JSON.stringify(graph.edges))throw Error('Original oracle differs');
const load=(name:string)=>import(pathToFileURL(join(repo,'src/model',name+'.ts')).href);
const {readDocument}=await load('document'),{validateCsvBooleanLexical,verifyCsvBooleanLexical}=await load('csv-boolean-lexical'),{validateCoreDatasetValuesCompact,verifyCoreDatasetValuesCompact}=await load('dataset-values-compact');
const source=readDocument(new TextDecoder().decode(sourceBytes),'json'),booleanReceipts:any[]=[];
const coordinates=new Map(oracle.coordinates.map((c:any)=>[c.objectKey,c]));
const records=graph.objects.map((object:any)=>{
 if(object.type.document!==source.id)throw Error('Foreign original object document');
 const module=source.modules.find((m:any)=>m.id===object.type.module),record=module?.elements.find((e:any)=>e.id===object.type.element);if(record?.kind!=='record')throw Error('Original Record required');
 const coordinate:any=coordinates.get(object.key);if(!coordinate||coordinate.table!==object.type.element)throw Error('Original archive source row required');
 if(Object.keys(object.values).length!==record.members.length)throw Error('Complete original membership required');
 const values=record.members.map((member:any)=>{
  if(member.module!==object.type.module||!Object.hasOwn(object.values,member.element))throw Error('Exact original Field membership required');
  const field=module.elements.find((e:any)=>e.id===member.element),token=object.values[member.element];if(field?.kind!=='field')throw Error('Original Field required');
  let value:any;
  if(token===null)value=null;
  else if(typeof token!=='string')throw Error('Original lexical token required');
  else if(field.scalarType==='string')value={string:token};
  else if(field.scalarType==='integer')value={integerToken:token};
  else if(field.scalarType==='boolean'){
   const request={profile:'umf.csv-boolean-lexical/1.0.0',field:{module:member.module,element:member.element},token,sourceContext:{packVersion:pack.version,packSha256:hash(packBytes),archiveSha256:hash(archiveBytes),graphSha256:hash(graphBytes),objectKey:object.key,sourceRow:coordinate,fieldIdentity:member}};
   const receipt=validateCsvBooleanLexical(source,request);verifyCsvBooleanLexical(receipt,source,request);if(receipt.validation.valid!==true||receipt.validation.complete!==true)throw Error('Public Boolean semantics remain unadmitted');booleanReceipts.push(receipt);value=receipt.value;
  }else throw Error('Unsupported selected original scalar type');
  return {field:{module:member.module,element:member.element},state:'present',value};
 });return {instanceId:object.key,identity:{module:object.type.module,element:object.type.element},values};
});
const recordMap=new Map(records.map((r:any)=>[r.instanceId,r]));
const relationships=graph.edges.map((edge:any)=>{
 if(edge.relationship.document!==source.id)throw Error('Foreign original relationship');
 const target:any=recordMap.get(edge.target);if(!target||!recordMap.has(edge.source))throw Error('Original endpoints absent');
 const module=source.modules.find((m:any)=>m.id===target.identity.module),record=module.elements.find((e:any)=>e.id===target.identity.element),primary=record.keys.filter((k:any)=>k.primary===true);if(primary.length!==1)throw Error('Explicit original primary Key required');
 return {instanceId:edge.key,identity:{module:edge.relationship.module,id:edge.relationship.id},sourceInstanceId:edge.source,target:{identity:{...target.identity,key:primary[0].id},values:primary[0].fields.map((f:any)=>{const member=target.values.find((v:any)=>v.field.module===f.module&&v.field.element===f.element);if(!member||member.value===null)throw Error('Original target key literal required');return member.value;})}};
});
const input={scope:{id:'ashlar-original-historical-medical-graph',closure:'supplied-dataset-only'},context:{sourceProfile:'ashlar-medical-historical-graph-lexical/0.1',booleanProfile:'umf.csv-boolean-lexical/1.0.0',packVersion:pack.version,packSha256:hash(packBytes),archiveSha256:hash(archiveBytes),modelSha256:hash(sourceBytes),graphSha256:hash(graphBytes),schemaRevisionQualification:oracle.schemaRevisionQualification},records,relationships};
const receipt=validateCoreDatasetValuesCompact(source,input);verifyCoreDatasetValuesCompact(receipt,source,input);
if(receipt.datasetValidation.valid!==true||receipt.datasetValidation.complete!==true||receipt.records.length!==51||receipt.keys.length!==51||receipt.relationships.length!==62||booleanReceipts.length!==3)throw Error('Complete original historical supplied dataset required');
const controls=[];
for(const name of ['duplicate-key','wrong-boolean-carrier','unresolved-target','missing-required-relationship']){const changed=structuredClone(input);if(name==='duplicate-key'){const row=structuredClone(changed.records[0]);row.instanceId='separately-authored-duplicate';changed.records.push(row);}if(name==='wrong-boolean-carrier')changed.records.find((r:any)=>r.identity.element==='patients').values.find((v:any)=>v.field.element==='patients.active').value={string:'True'};if(name==='unresolved-target')changed.relationships[0].target.values[0]={string:'separately-authored-missing'};if(name==='missing-required-relationship')changed.relationships=[];const result=validateCoreDatasetValuesCompact(source,changed);if(result.datasetValidation.valid!==false)throw Error('Changed supplied input admitted');controls.push({name,input:changed,receipt:result});}
pin();if(await Bun.file(output).exists())throw Error('Fresh output required');
await Bun.write(output,JSON.stringify({format:'ashlar-medical-historical-public-dataset/0.1',umfRevision:PIN,originalOracleSha256:hash(oracleBytes),originalPackSha256:hash(packBytes),originalArchiveSha256:hash(archiveBytes),originalModelSha256:hash(sourceBytes),originalGraphSha256:hash(graphBytes),input,booleanReceipts,receipt,controls,qualification:'Explicit original historical1.0 graph lexical conversion with exact public Boolean profile and own archive/model/source-row custody. Finite51records/62occurrences only; original graphkeys remain source identities, not canonical/native IDs. Native null remains present null; model has no authored revision, original graph schema revision is owning pack version per producer. No native ingestion, publication, ACK, clinical/FHIR or terminology claim.'})+'\n');
