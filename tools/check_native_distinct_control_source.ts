// Public semantic admission of original separately authored String rows.
import {readFileSync,writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const [umf,input,output]=process.argv.slice(2);if(!umf||!input||!output)throw Error('Explicit source/input/output required');
const revision='a95c3ec18a8f904decde884a4fa252988d2a5b0f';
if(execFileSync('git',['-C',umf,'rev-parse','HEAD'],{encoding:'utf8'}).trim()!==revision||execFileSync('git',['-C',umf,'diff','--name-only','HEAD'],{encoding:'utf8'}).trim())throw Error('Exact clean public implementation/schema required');
const raw=readFileSync(input,'utf8'),request=JSON.parse(raw);
if(Object.keys(request).sort().join(',')!=='model,rows')throw Error('Closed independent source request required');
const api=await import(resolve(umf,'src/model/record-values.ts'));
const receipts=request.rows.map((row:any)=>{
 if(Object.keys(row).sort().join(',')!=='id,value'||typeof row.id!=='string'||typeof row.value!=='string')throw Error('Original exact String source required');
 const receipt=api.validateCoreRecordValues(request.model,{module:'control',element:'Item'},['id','value'].map(element=>({field:{module:'control',element},state:'present',value:{string:row[element]}})));
 if(receipt.validation.valid!==true||receipt.fields.some((f:any)=>f.validation.valid!==true||f.validation.complete!==true))throw Error('Original public scalar validity incomplete');
 return{originalRow:row,originalPublicReceipt:receipt};
});
writeFileSync(output,JSON.stringify({format:'ashlar-unpublished-distinct-public-source/0.1',umfRevision:revision,originalRequestText:raw,originalRequestSha256:createHash('sha256').update(raw).digest('hex'),receipts,qualification:'Separate authored String fixture; no canonical publication/ACK or source pack claim.'},null,2)+'\n',{flag:'wx'});
