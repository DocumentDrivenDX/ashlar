/** Host runner for actual pinned UMF logical checks; fixture mappings confer no native authority. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [rootArg,pin,schemaPath,requestPath]=process.argv.slice(2);
if(!rootArg||! /^[0-9a-f]{40}$/.test(pin??'')||!schemaPath||!requestPath)throw Error('Usage: bun tools/check_umf_record_values.ts UMF_SOURCE REVISION SCHEMA REQUEST');
const root=resolve(rootArg);
const check=()=>{const head=Bun.spawnSync(['git','-C',root,'rev-parse','HEAD']),status=Bun.spawnSync(['git','-C',root,'status','--porcelain']);if(head.exitCode||status.exitCode||new TextDecoder().decode(head.stdout).trim()!==pin||status.stdout.length)throw Error('Exact clean UMF source required');};
check();
const load=(path:string)=>import(pathToFileURL(join(root,'src',path+'.ts')).href);
const {readDocument}=await load('model/document'),{readJsonValue}=await load('model/serialization'),{validateDocument}=await load('validation/document');
const {upgradeSchemaPropertiesEnvelope,verifySchemaPropertiesUpgrade}=await load('model/schema-properties-transition');
const {validateCoreRecordValues}=await load('model/record-values');
if(Bun.file(schemaPath).size>65536||Bun.file(requestPath).size>1048576)throw Error('Example input byte limit');
const original=await Bun.file(schemaPath).bytes(),requestBytes=await Bun.file(requestPath).bytes();
if(original.length>65536||requestBytes.length>1048576)throw Error('Example input byte limit');
const source=readDocument(new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(original),'json');
// UMF reader enforces duplicate/number/Unicode boundaries; this request contains
// only strings, qualified identities and typed literal wrappers.
const request:any=readJsonValue(new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(requestBytes),'json');
if(!request||Object.keys(request).sort().join(',')!=='identity,records'||!Array.isArray(request.records)||request.records.length>1000)throw Error('Explicit bounded Record request required');
const upgrade=upgradeSchemaPropertiesEnvelope(source);verifySchemaPropertiesUpgrade(upgrade);
const records=request.records.map((record:any)=>{
 if(!record||Object.keys(record).sort().join(',')!=='deliveryId,recordSha256,values'||typeof record.deliveryId!=='string'||! /^[0-9a-f]{64}$/.test(record.recordSha256))throw Error('Original source record locator required');
 const result=validateCoreRecordValues(upgrade.target,request.identity,record.values);
 if(!result.validation.valid||!result.validation.complete)throw Error('Original UMF logical Record check unavailable/invalid: '+JSON.stringify(result.validation));
 return {deliveryId:record.deliveryId,recordSha256:record.recordSha256,result};
});
check();
const hash=(bytes:Uint8Array)=>createHash('sha256').update(bytes).digest('hex');
console.log(JSON.stringify({format:'ashlar-umf-record-check/0.1',producerRevision:pin,sourceBase64:Buffer.from(original).toString('base64'),sourceSha256:hash(original),requestSha256:hash(requestBytes),originalValidation:validateDocument(source),upgrade,records,qualification:'Actual logical Record checks on explicit upgrade. Original schema/source custody remains distinct; no native accepted IDs, defaults, dataset key/relationship checks, qualified validator isolation or source ACK.'}));
