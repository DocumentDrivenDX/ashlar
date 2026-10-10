/** Bounded public DOM/download qualification; no private UMF renderer or validator. */
import {resolve,join,sep} from 'node:path';
import {pathToFileURL} from 'node:url';
import {mkdir,readFile} from 'node:fs/promises';
const [rootArg,umfArg,outArg]=process.argv.slice(2);
if(!rootArg||!umfArg||!outArg)throw Error('Usage: bun tools/check_schema_browser_browser.ts ASHLAR_ROOT UMF_SOURCE FRESH_OUTPUT');
const root=resolve(rootArg),output=resolve(outArg),assets=join(root,'website/static/model/schema-browser');
await mkdir(output,{recursive:false});
const {parse:parseYaml}=await import(pathToFileURL(join(resolve(umfArg),'node_modules/yaml/dist/index.js')).href);
const {chromium}=await import(pathToFileURL(join(resolve(umfArg),'node_modules/playwright/index.mjs')).href);
const sha=(data:Uint8Array|string)=>new Bun.CryptoHasher('sha256').update(data).digest('hex');
const catalog=JSON.parse(await readFile(join(assets,'schema-catalog.json'),'utf8'));
if(catalog.entries.length!==9)throw Error('Original nine-source catalog required');
const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch(req){const path=resolve(assets,'.'+decodeURIComponent(new URL(req.url).pathname));if(!path.startsWith(assets+sep))return new Response('missing',{status:404});return new Response(Bun.file(path));}});
let browser;let report:any;
try{
 browser=await chromium.launch({headless:true});const checks=[];
 for(const width of [1440,390]){
  const page=await browser.newPage({viewport:{width,height:900},acceptDownloads:true});const errors:string[]=[];page.on('pageerror',(error:Error)=>errors.push(String(error)));
  for(const entry of catalog.entries){
   await page.goto(`http://127.0.0.1:${server.port}/index.html#${new URLSearchParams({schema:entry.id})}`);
   const source=page.locator('#inspector details').filter({has:page.locator('summary',{hasText:/^Original schema source$/})}).locator('pre');
   await source.waitFor({state:'attached'});if(await source.textContent()!==entry.text)throw Error('DOM source byte mismatch: '+entry.id);
   const original=await readFile(join(root,'docs/helix/02-design/models/ashlar-delta-runtime',entry.path));
   if(Buffer.from(entry.text).compare(original))throw Error('Catalog original source mismatch');
   const waiting=page.waitForEvent('download');await page.getByRole('link',{name:'Download source',exact:true}).click();const download=await waiting;const saved=join(output,`${width}-${entry.path}`);await download.saveAs(saved);if(Buffer.compare(await readFile(saved),original))throw Error('Download bytes changed');
   if(entry.id.startsWith('ashlar-delta:')){
    const sourceDoc=JSON.parse(entry.text),module=sourceDoc.modules[0],definition=module.elements[0];
    await page.goto(`http://127.0.0.1:${server.port}/index.html#${new URLSearchParams({schema:entry.id,definition:JSON.stringify([module.id,definition.id])})}`);
    const retained=page.locator('#inspector details').filter({has:page.locator('summary',{hasText:/^Inspect extension content$/})}).locator('pre');
    await retained.waitFor({state:'attached'});
    if(JSON.stringify(parseYaml(await retained.textContent()))!==JSON.stringify(definition.extensions))throw Error('Retained native extension display changed: '+entry.id);
    if(!(await page.locator('div.callout').textContent())?.includes('Some semantics are not interpreted'))throw Error('Unknown Delta semantics claimed complete');
    checks.push({width,id:entry.id,retainedExtensionDisplayExact:true,unknownDeltaSemanticsExplicit:true});
   }
   if(await page.locator('.catalog-item').count()!==9)throw Error('Catalog count changed');
   if(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth))throw Error('Whole-page overflow');
   checks.push({width,id:entry.id,source_sha256:sha(original),domOriginalSourceExact:true,downloadOriginalSourceExact:true,catalogCount:9,noOverflow:true});
  }
  const logical=catalog.entries[0],doc=JSON.parse(logical.text);const module=doc.modules[0];
  for(const record of module.elements.filter((e:any)=>e.kind==='record')){
   await page.goto(`http://127.0.0.1:${server.port}/index.html#${new URLSearchParams({schema:logical.id,definition:JSON.stringify([module.id,record.id])})}`);
   await page.locator('.definition-heading').waitFor();
   const actual=await page.locator('.detail-block').filter({has:page.getByRole('heading',{name:'Fields and members',exact:true})}).locator('tr').evaluateAll((rows:any[])=>rows.slice(1).map(r=>Array.from(r.querySelectorAll('td')).map((c:any,i:number)=>i===0?(c.querySelector('a')?.textContent??c.textContent):c.textContent)));
   const expected=record.members.map((m:any)=>{const ref=m.field??m,field=module.elements.find((e:any)=>e.id===ref.element);return [String(field.title??field.name??field.id),`${field.scalarType??field.kind??'Not declared'} · ${field.cardinality??'shape unspecified'}`,String(field.nullability??'Not declared')];});
   if(JSON.stringify(actual)!==JSON.stringify(expected))throw Error('Original ordered field/availability glyph mismatch: '+record.id+' '+JSON.stringify({actual,expected}));
   checks.push({width,record:record.id,orderedOriginalMembers:expected});
  }
  await page.locator('#search').fill('edge_current');await page.waitForTimeout(30);if(!(await page.locator('#inspector').textContent())?.includes(module.elements.filter((e:any)=>e.kind==='record').at(-1).id))throw Error('Search replaced deep-linked inspector');
  if(errors.length)throw Error(errors.join('\n'));
  await page.screenshot({path:join(output,`browser-${width}.png`)});await page.close();
 }
 report={profile:'ashlar-public-schema-browser-dom/0.1',browser:browser.version(),checks,qualification:'Exact nine original source catalogs, DOM raw source and download bytes, ordered logical member/type/cardinality/availability glyphs and desktop/mobile navigation/overflow. Delta extensions remain retained raw source; no native Delta semantics, loader/research pack execution or broad browser library support claim.'};
}finally{await browser?.close();server.stop(true);}
await Bun.write(join(output,'report.json'),JSON.stringify(report,null,2)+'\n');console.log('Verified9 exact sources/downloads and8 logical records at2 viewport sizes');
