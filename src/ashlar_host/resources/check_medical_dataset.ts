/** Exact original public medical operations; no native or source authority. */
import {openSync,readSync,closeSync,fstatSync,lstatSync,writeFileSync} from 'node:fs';
import {join,resolve} from 'node:path';import {pathToFileURL} from 'node:url';import {createHash} from 'node:crypto';
const PIN='8e76c74d14203225d1ef159c9132bb9e9b0cdffe';
const [source,output,model,graph,baseline,git]=process.argv.slice(2);
if(!source||!output||!model||!graph||!baseline||!git||process.argv.slice(2).length!==6)throw Error('Explicit original public selections required');
function read(path:string){const fd=openSync(path,'r');try{const s=fstatSync(fd),n=lstatSync(path);if(!s.isFile()||s.dev!==n.dev||s.ino!==n.ino||s.nlink!==1||s.size<1||s.size>4194304)throw Error('Bounded regular original required');const raw=new Uint8Array(s.size);let offset=0;while(offset<raw.length){const size=readSync(fd,raw,offset,raw.length-offset,null);if(!size)throw Error('Truncated input');offset+=size;}const a=fstatSync(fd),b=lstatSync(path);if(a.size!==s.size||a.mtimeMs!==s.mtimeMs||b.dev!==s.dev||b.ino!==s.ino)throw Error('Changed original');return raw;}finally{closeSync(fd);}}
const sha=(raw:Uint8Array)=>createHash('sha256').update(raw).digest('hex');
const inputs=[read(resolve(model)),read(resolve(graph)),read(resolve(baseline))];
const hashes=['fc9938f152e129e041096219be4d7bef70aa35e16f56745411af232ee9cab1c8','b22d788dcf23e2889f82bd407241d7af4bb5f8a8d4c3316768458a97448f18c4','1400eebbca3b082f19fd7699e74c708dfbd69e5ed21f7d3f5e77bd6f4e46b20e'];
if(inputs.some((raw,i)=>sha(raw)!==hashes[i]))throw Error('Exact historical medical inputs required');
function pin(){for(const[args,expected]of[[['rev-parse','HEAD'],PIN],[['status','--porcelain'],'']]as const){const p=Bun.spawnSync([git,'-C',source,...args]);if(p.exitCode||new TextDecoder().decode(p.stdout).trim()!==expected||p.stderr.length)throw Error('Clean pinned public UMF required');}}
pin();
const publicAPI=await import(pathToFileURL(join(source,'src/index.ts')).href);
const document=publicAPI.readDocument(new TextDecoder().decode(inputs[0]),'json'),original=JSON.parse(new TextDecoder().decode(inputs[2]));
if(document.umf!=='0.8.0')throw Error('Original declared version required');
const booleanReceipts=original.booleanReceipts.map((expected:any)=>{const receipt=publicAPI.validateCsvBooleanLexical(document,expected.request);publicAPI.verifyCsvBooleanLexical(receipt,document,expected.request);if(receipt.validation.valid!==true||receipt.validation.complete!==true)throw Error('Public Boolean refused');return receipt;});
const receipt=publicAPI.validateCoreDatasetValuesCompact(document,original.input);
publicAPI.verifyCoreDatasetValuesCompact(receipt,document,original.input);
if(receipt.datasetValidation.valid!==true||receipt.datasetValidation.complete!==true||receipt.records.length!==51||receipt.keys.length!==51||receipt.relationships.length!==62||booleanReceipts.length!==3)throw Error('Complete public original dataset required');
pin();if([model,graph,baseline].some((p,i)=>sha(read(resolve(p)))!==hashes[i]))throw Error('Original inputs changed');
const result={profile:'ashlar-original-medical-public-revalidation/0.1',umfRevision:PIN,modelSha256:hashes[0],graphSha256:hashes[1],baselineSha256:hashes[2],receipt,booleanReceipts};
// Owner results are already allocated; bound host serialization representation first.
function budget(v:any,depth=0):number{if(depth>128)throw Error('Result depth');if(v===null||typeof v==='boolean')return 5;if(typeof v==='number')return 32;if(typeof v==='string'){let n=2;for(const c of v){const code=c.codePointAt(0)!;n+=code<32?(['\b','\f','\n','\r','\t'].includes(c)?2:6):(c==='\"'||c==='\\'?2:code<128?1:code<2048?2:code>=0xd800&&code<=0xdfff?6:code<65536?3:4);if(n>4194304)throw Error('Result bound');}return n;}if(Array.isArray(v)){let n=2;for(const x of v){n+=budget(x,depth+1)+1;if(n>4194304)throw Error('Result bound');}return n;}if(typeof v==='object'){let n=2;for(const[k,x]of Object.entries(v)){n+=budget(k,depth+1)+budget(x,depth+1)+2;if(n>4194304)throw Error('Result bound');}return n;}throw Error('Closed public result required');}
if(budget(result)+1>4194304)throw Error('Result bound');writeFileSync(resolve(output),JSON.stringify(result)+'\n',{flag:'wx',mode:0o600});
