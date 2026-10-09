/** Host-only original-core0.8 public Record check; no downgrade/private validator. */
import {createHash} from 'node:crypto';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
const [rootArg,sourcePath,requestPath]=process.argv.slice(2);
if(!rootArg||!sourcePath||!requestPath)throw Error('Usage: bun check_typed_umf_records.ts CLEAN_UMF SOURCE REQUEST');
const root=resolve(rootArg),pin='c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3';
function check(){for(const [args,expected] of [[['rev-parse','HEAD'],pin],[['status','--porcelain'],'']] as const){const p=Bun.spawnSync(['git','-C',root,...args]);if(p.exitCode||new TextDecoder().decode(p.stdout).trim()!==expected)throw Error('Exact clean public Record producer required');}}
check();
if(Bun.file(sourcePath).size>1048576||Bun.file(requestPath).size>1048576)throw Error('Bounded source/request required');
const source=await Bun.file(sourcePath).bytes(),request=await Bun.file(requestPath).bytes();
const load=(path:string)=>import(pathToFileURL(join(root,'src',path+'.ts')).href);
const {readDocument}=await load('model/document'),{readJsonValue}=await load('model/serialization'),{validateCoreRecordValues}=await load('model/record-values');
const model=readDocument(new TextDecoder('utf-8',{fatal:true}).decode(source),'json');if(model.umf!=='0.8.0')throw Error('Original authored0.8 required; no upgrade/downcast');
const query:any=readJsonValue(new TextDecoder('utf-8',{fatal:true}).decode(request),'json');
if(!query||Object.keys(query).join(',')!=='records'||!Array.isArray(query.records)||query.records.length>1000)throw Error('Explicit bounded records required');
const records=query.records.map((item:any)=>{
 if(!item||Object.keys(item).sort().join(',')!=='deliveryId,identity,propsSha256,recordSha256,typeId,values'||typeof item.deliveryId!=='string'||!item.deliveryId||!Number.isSafeInteger(item.typeId)||item.typeId<1||item.typeId>=2147483648||! /^[0-9a-f]{64}$/.test(item.recordSha256)||! /^[0-9a-f]{64}$/.test(item.propsSha256))throw Error('Original typed record locator required');
 const result=validateCoreRecordValues(model,item.identity,item.values);
 if(!result.validation.valid||!result.validation.complete)throw Error('Public Record check refused: '+JSON.stringify(result.validation));
 return {request:item,result};
});check();
const hash=(v:Uint8Array)=>createHash('sha256').update(v).digest('hex');
console.log(JSON.stringify({format:'ashlar-typed-record-check/0.1',producerRevision:pin,sourceBase64:Buffer.from(source).toString('base64'),sourceSha256:hash(source),requestBase64:Buffer.from(request).toString('base64'),requestSha256:hash(request),records,qualification:'Original public core0.8 Record value/presence checks; no native catalog authority, dataset keys/relationships, query carrier admission, publication or ACK.'}));
