const fs=require('fs'),crypto=require('crypto');
const Ajv=require('/private/tmp/ashlar-weft-distribution-d2d/node_modules/ajv/dist/2020.js');
const root='/private/tmp/ashlar-supply-chain-compile-candidate-20261010-h';
const schemas='/private/tmp/ashlar-weft-distribution-d2d/docs/helix/02-design/contracts';
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=p=>JSON.parse(fs.readFileSync(p));
const assert=(v,m)=>{if(!v)throw Error(m)};
const equal=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const ajv=new Ajv({strict:false,allErrors:true});
const plan=read(schemas+'/logical-plan-v0.4.schema.json'),response=read(schemas+'/compile-response-v0.4.schema.json');ajv.addSchema(plan);const validate=ajv.compile(response);
const observations=[];
for(const name of ['split-excursion','excursion','replay','lineage','sensor']){
 const request=read(root+'/'+name+'.request.json'),path=root+'/candidate-output/'+name+'.response.json',raw=fs.readFileSync(path),r=JSON.parse(raw),binding=JSON.parse(request.target.bindingJson);
 assert(validate(r),JSON.stringify(validate.errors));
 if(name==='replay'){
  assert(r.status==='blocked'&&r.diagnostics.length===1,'replay status');const d=r.diagnostics[0];assert(d.code==='WFT-UNSUPPORTED'&&d.phase==='parse'&&d.sourceSpan.start===86&&d.sourceSpan.end===87,'replay refusal');assert(request.sql.slice(86,87)==='*','original span');
 }else{
  assert(r.status==='compiled'&&r.diagnostics.length===0,'compiled');assert(r.bindingSha256===hash(Buffer.from(request.target.bindingJson)),'binding');assert(equal(r.modelPins,binding.modelPins)&&equal(r.logicalPlan.modulePins,binding.modelPins),'model pins');
  for(const k of ['backendId','backendVersion','targetProfile'])assert(r.backend[k]===request.target[k],'target '+k);
  assert(r.targetContext.id===request.target.targetProfile,'context');
  const required=r.logicalPlan.requiredCapabilities,ops=r.qualification.operations;assert(equal(required,[...new Set(required)].sort()),'capability unique sorted');assert(equal(required,ops.map(o=>o.assessment.id)),'complete capability inventory');
  for(const op of ops){assert(op.assessment.id===op.declaration.id&&op.assessment.status==='candidate'&&op.declaration.status==='candidate','candidate');assert(op.declaration.targetProfiles.includes(request.target.targetProfile),'profile');}
  const obligations=r.obligations;assert(new Set(obligations.map(o=>o.id)).size===obligations.length,'obligation unique');const pub=obligations.find(o=>o.id==='ashlar.candidate.publication');assert(pub&&equal(pub.parameters.publication,binding.publication)&&equal(pub.parameters.modelPins,binding.modelPins),'publication vector');assert(pub.parameters.layoutSha256===binding.layoutSha256&&pub.parameters.layoutRevision===binding.layoutRevision,'layout');assert(obligations.some(o=>o.id==='ashlar.candidate.scalarIntegrity'),'scalar guards');
  assert(r.columns.length===r.logicalPlan.outputs.length,'column inventory');r.columns.forEach((c,i)=>assert(c.position===i+1&&c.outputName===r.logicalPlan.outputs[i].name,'frame descriptor'));
  assert(r.parameters.every((p,i)=>p.position===i+1),'slot inventory');assert(typeof r.sql==='string'&&r.sql.length>0,'sql');
 }
 observations.push({case:name,responseSha256:hash(raw),status:r.status,capabilities:r.logicalPlan?.requiredCapabilities,obligations:r.obligations?.map(o=>o.id),columns:r.columns?.length});
}
const receipt={scope:'Offline retained response schema validation and original request/pin/capability/obligation/frame inventory correspondence; no compiler rerun/native/runtime/semantic-result qualification. Original125 dataset vectors untouched.',schemaPins:[plan,response].map(s=>({id:s.$id})),observations};fs.writeFileSync('/private/tmp/astra-supplychain-compiler-evidence-review-20261010-h.json',JSON.stringify(receipt,null,2)+'\n');console.log(JSON.stringify(receipt));
