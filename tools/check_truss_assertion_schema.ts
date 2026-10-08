/** Shape checks for candidate entries; no completeness or qualification claim. */
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [source,ajvModule,input,output]=process.argv.slice(2);
if(!source||!ajvModule||!input||!output)throw Error('Usage: bun tools/check_truss_assertion_schema.ts TRUSS_SOURCE AJV_2020_MODULE INPUT OUTPUT');
const bytes=new Uint8Array(await Bun.file(resolve(source,'docs/helix/02-design/contracts/enforcement-report-v0.1.schema.json')).arrayBuffer());
const schema=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
if(schema.$id!=='urn:truss:draft:enforcement-report:0.1.0')throw Error('Wrong original schema');
const {default:Ajv}=await import(pathToFileURL(resolve(ajvModule)).href);
const ajv=new Ajv({strict:true});ajv.addSchema(schema);
const validate=ajv.getSchema(schema.$id+'#/$defs/entry');if(!validate)throw Error('Missing original entry schema');
const raw=new Uint8Array(await Bun.file(input).arrayBuffer());
const inventory=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
if(inventory.format!=='ashlar-selected-assertion-inventory/0.1'||!Array.isArray(inventory.entries))throw Error('Wrong candidate inventory');
for(const entry of inventory.entries)if(!validate(entry))throw Error(JSON.stringify(validate.errors));
await Bun.write(output,JSON.stringify({schema:schema.$id+'#/$defs/entry',schemaSha256:createHash('sha256').update(bytes).digest('hex'),inputSha256:createHash('sha256').update(raw).digest('hex'),entries:inventory.entries.length,valid:true,qualification:'Original entry shapes only; no complete accepted report, semantic completeness or enforcement authority'},null,2)+'\n');
console.log('Candidate entries match original Truss enforcement-entry shapes');
