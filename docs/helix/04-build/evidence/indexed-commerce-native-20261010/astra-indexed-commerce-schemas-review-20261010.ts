import Ajv2020 from '/private/tmp/ashlar-weft-distribution-d2d/node_modules/ajv/dist/2020.js';
import { readdir, readFile, writeFile } from 'node:fs/promises';import {createHash}from'node:crypto';
const root='/private/tmp/ashlar-weft-distribution-candidate-20261009-d/source-subset';const dir=root+'/docs/helix/02-design/contracts';const ajv=new Ajv2020({strict:false,allErrors:true});
for(const folder of[dir,root+'/spec/upstream'])for(const name of await readdir(folder)){if(!name.endsWith('.schema.json'))continue;const schema=JSON.parse(await readFile(folder+'/'+name,'utf8'));ajv.addSchema(schema);}
const req=ajv.getSchema(JSON.parse(await readFile(dir+'/compile-request-v0.2.schema.json','utf8')).$id)!;const res=ajv.getSchema(JSON.parse(await readFile(dir+'/compile-response-v0.2.schema.json','utf8')).$id)!;
const report=JSON.parse(await readFile('/private/tmp/ashlar-indexed-commerce-queries-20261010-b/report.json','utf8'));const cases=[];
for(const item of[...report.queries,report.relationship]){if(!req(item.request))throw new Error(JSON.stringify(req.errors));if(!res(item.artifact))throw new Error(JSON.stringify(res.errors));cases.push({sql:item.request.sql,requestSchema:true,responseSchema:true});}
const out='/private/tmp/astra-indexed-commerce-schemas-review-20261010.json';const text=JSON.stringify({scope:'Original indexed source362 public0.2 schemas; three actual native requests/responses',cases},null,2)+'\n';await writeFile(out,text);console.log(JSON.stringify({receipt:out,sha256:createHash('sha256').update(text).digest('hex'),cases:cases.length}));
