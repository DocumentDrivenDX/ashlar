import Ajv2020 from '/private/tmp/ashlar-weft-land-5856/node_modules/ajv/dist/2020.js';
import {readdir,readFile,writeFile} from 'node:fs/promises';
const [schemaRoot,nativeRoot,receiptPath]=process.argv.slice(2);
if(!schemaRoot||!nativeRoot||!receiptPath)throw Error('Explicit schema root, native artifact directory and receipt required');
const ajv=new Ajv2020({allErrors:true,strict:false});const ids:any={};
for(const n of await readdir(schemaRoot))if(n.endsWith('.schema.json')){const s=JSON.parse(await readFile(schemaRoot+'/'+n,'utf8'));ajv.addSchema(s);ids[n]=s.$id;}
const rows=[];for(const c of ['original','global','global-empty','all-null-groups','having-excludes-all','global-having-empty']){const q=JSON.parse(await readFile(`${nativeRoot}/${c}-request.json`,'utf8')),a=JSON.parse(await readFile(`${nativeRoot}/${c}-artifact.json`,'utf8'));const rq=ajv.getSchema(ids['compile-request-v0.3.schema.json'])!,rs=ajv.getSchema(ids['compile-response-v0.3.schema.json'])!;if(!rq(q))throw Error(JSON.stringify(rq.errors));if(!rs(a))throw Error(JSON.stringify(rs.errors));rows.push({case:c,requestValid:true,responseValid:true});}
await writeFile(receiptPath,JSON.stringify(rows,null,2)+'\n');console.log('Six actual native03 pairs pass');
