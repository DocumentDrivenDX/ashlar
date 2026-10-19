// SPDX-License-Identifier: MIT OR Apache-2.0
import {test,expect} from 'bun:test';
import {mkdtempSync,cpSync,readFileSync,writeFileSync,existsSync,rmSync,statSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {generateBundle} from '../tools/generate_delta_bundle';
const source=process.env.ASHLAR_UMF_BUNDLE_SOURCE;
if(!source)throw Error('Explicit ASHLAR_UMF_BUNDLE_SOURCE required');
const models=resolve(import.meta.dir,'../docs/helix/02-design/models/ashlar-delta-runtime');
test('public UMF bundle preserves eight authored schemas and order',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'delta-bundle-'));try{
  const output=join(dir,'bundle.json'),bundle=await generateBundle(source,'catalog','runtime',output);
  const index=JSON.parse(readFileSync(join(models,'index.json'),'utf8'));
  const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
  const {generateDeltaDDL}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
  expect(bundle.tables.map(t=>t.definition.name)).toEqual(index.tables.map((name:string)=>['catalog','runtime',name]));
  for(let i=0;i<index.tables.length;i++){
   const original=generateDeltaDDL(readDocument(readFileSync(join(models,index.tables[i]+'.umf.json'),'utf8'),'json'));
   expect(bundle.tables[i].schemaJson).toBe(original.schemaJson);
   expect({...bundle.tables[i].definition,name:original.definition.name}).toEqual(original.definition);
  }
  const saved=JSON.parse(readFileSync(output,'utf8'));
  for(const name of ['index.json',...index.tables.map((n:string)=>n+'.umf.json')])expect(saved.inputs[name]).toBe(createHash('sha256').update(readFileSync(join(models,name))).digest('hex'));
  expect(saved.authoredCarrierGeneratorRevision).toBe(index.umfRevision);
  expect(bundle.sql).toBe(bundle.tables.map(t=>t.sql).join('\n'));
  expect(JSON.parse(readFileSync(output,'utf8')).tables).toEqual(bundle.tables);expect(statSync(output).mode&0o777).toBe(0o600);
 }finally{rmSync(dir,{recursive:true});}
});
test('malformed final table withholds the entire output',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'delta-bundle-'));try{
  const copy=join(dir,'models');cpSync(models,copy,{recursive:true});
  const path=join(copy,'whole_source_history.umf.json'),doc=JSON.parse(readFileSync(path,'utf8'));
  doc.extensions['umf.delta.definition'].properties.unknown='discard forbidden';writeFileSync(path,JSON.stringify(doc));
  const output=join(dir,'bundle.json');await expect(generateBundle(source,'catalog','runtime',output,copy)).rejects.toThrow();expect(existsSync(output)).toBe(false);
 }finally{rmSync(dir,{recursive:true});}
});
