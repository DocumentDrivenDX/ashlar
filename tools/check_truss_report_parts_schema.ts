/** Check report fragments against originals; never synthesize missing report context. */
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [source,ajvModule,input,output]=process.argv.slice(2);
if(!source||!ajvModule||!input||!output)throw Error('Usage: bun tools/check_truss_report_parts_schema.ts TRUSS_SOURCE AJV_2020_MODULE INPUT OUTPUT');
const {default:Ajv}=await import(pathToFileURL(resolve(ajvModule)).href);
const ajv=new Ajv({strict:true});
const originalSources=[];
for(const name of new Bun.Glob('*.schema.json').scanSync(resolve(source,'docs/helix/02-design/contracts'))){
  const raw=new Uint8Array(await Bun.file(resolve(source,'docs/helix/02-design/contracts',name)).arrayBuffer());
  const schema=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
  ajv.addSchema(schema);originalSources.push({name,sha256:createHash('sha256').update(raw).digest('hex')});
}
const raw=new Uint8Array(await Bun.file(input).arrayBuffer());
const value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));
if(value.format!=='ashlar-truss-initial-report-parts/0.1')throw Error('Wrong candidate report parts');
const checked=[];
for(const field of ['documents','diagnostics','documentInterpretations','counts']){
  const id='urn:truss:draft:acceptance-report:0.1.0#/properties/'+field;
  const validate=ajv.getSchema(id);if(!validate)throw Error('Missing original fragment '+field);
  if(!validate(value[field]))throw Error(field+': '+JSON.stringify(validate.errors));
  checked.push(id);
}
await Bun.write(output,JSON.stringify({checked,inputSha256:createHash('sha256').update(raw).digest('hex'),originalSources:originalSources.sort((a,b)=>a.name.localeCompare(b.name)),valid:true,qualification:'Original report fragment shapes only. Not complete accepted-report validation, original profile/authority admission or native persistence.'},null,2)+'\n');
console.log('Candidate parts match all four original report fragment schemas');
