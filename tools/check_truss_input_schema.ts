/** Independent original-schema shape check; custody/admission are separate. */
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [source,ajvModule,input,output]=process.argv.slice(2);
if(!source||!ajvModule||!input||!output)throw Error('Usage: bun tools/check_truss_input_schema.ts TRUSS_SOURCE AJV_2020_MODULE INPUT OUTPUT');
const schemaFile=Bun.file(resolve(source,'docs/helix/02-design/contracts/acceptance-input-v0.1.schema.json'));
const schemaBytes=new Uint8Array(await schemaFile.arrayBuffer());
const schema=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(schemaBytes));
if(schema.$id!=='urn:truss:acceptance-input:0.1.0')throw Error('Wrong original schema');
const inputBytes=new Uint8Array(await Bun.file(input).arrayBuffer());
const value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(inputBytes));
const {default:Ajv}=await import(pathToFileURL(resolve(ajvModule)).href);
const validate=new Ajv({strict:true}).compile(schema);
if(!validate(value))throw Error(JSON.stringify(validate.errors));
const hash=(bytes:Uint8Array)=>createHash('sha256').update(bytes).digest('hex');
await Bun.write(output,JSON.stringify({schema:schema.$id,schema_sha256:hash(schemaBytes),input_sha256:hash(inputBytes),valid:true,scope:'Original Draft 2020-12 schema shape only. Duplicate-member, artifact, profile, order and original native checks remain separate. No acceptance authority.'},null,2)+'\n');
console.log('Actual complete input passes the original Truss Draft 2020-12 schema');
