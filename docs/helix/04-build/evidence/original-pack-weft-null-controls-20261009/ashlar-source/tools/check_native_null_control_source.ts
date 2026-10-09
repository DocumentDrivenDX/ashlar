// Actual public UMF receipts for separately authored unpublished native controls.
import {readFileSync,writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
const [umf,input,output]=process.argv.slice(2);if(!umf||!input||!output)throw Error('Explicit original source/input/output required');
const revision='a95c3ec18a8f904decde884a4fa252988d2a5b0f';
if(execFileSync('git',['-C',umf,'rev-parse','HEAD'],{encoding:'utf8'}).trim()!==revision||execFileSync('git',['-C',umf,'diff','--name-only','HEAD'],{encoding:'utf8'}).trim())throw Error('Exact clean public API required');
const raw=readFileSync(input,'utf8'),request=JSON.parse(raw);
if(Object.keys(request).sort().join(',')!=='controls,model')throw Error('Closed authored source request required');
const api=await import(resolve(umf,'src/model/schema-properties.ts'));
const records=await import(resolve(umf,'src/model/record-values.ts'));
const fields=request.model.modules[0].elements.filter((e:any)=>e.kind==='field');
const receipts=request.controls.map((control:any)=>{
 const original=JSON.parse(control.originalPropsJson);const values=fields.filter((field:any)=>Object.hasOwn(original,field.id)).map((field:any)=>{
  const raw=original[field.id];const value=raw===null?null:field.scalarType==='string'?{string:raw}:field.scalarType==='boolean'?{boolean:raw}:field.scalarType==='integer'?{integerToken:String(raw)}:typeof raw==='number'?{decimalToken:String(raw)}:{string:raw};
  return{field:{module:'control',element:field.id},state:'present',value};
 });
 const receipt=records.validateCoreRecordValues(request.model,{module:'control',element:'Item'},values);
 const selected=fields.map((field:any)=>({field:{module:'control',element:field.id},supplied:Object.hasOwn(original,field.id),original:original[field.id],validation:Object.hasOwn(original,field.id)?api.validateCoreFieldValue(request.model,{module:'control',element:field.id},values.find((v:any)=>v.field.element===field.id).value):null}));
 const invalid=selected.some((s:any)=>s.validation?.valid===false);
 if((control.expectedDisposition==='source-invalid')!==invalid)throw Error('Independent expected source disposition differs: '+control.name);
 if(control.name==='missing-optional'&&!receipt.fields.some((f:any)=>f.field.element==='amount'&&f.state==='absent'&&f.validation.valid===true))throw Error('Original missing state not preserved');
 return{control,originalRecordReceipt:receipt,selectedFields:selected,sourceDisposition:invalid?'invalid':'valid'};
});
writeFileSync(output,JSON.stringify({format:'ashlar-unpublished-null-controls-public-source/0.1',umfRevision:revision,originalRequestText:raw,originalRequestSha256:createHash('sha256').update(raw).digest('hex'),receipts,qualification:'Separately authored experimental source; original public receipts retained. No canonical publication/ACK or original pack admission claim.'},null,2)+'\n',{flag:'wx'});
