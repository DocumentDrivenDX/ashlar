/** Host tooling: two small local Chromium views of the generated Hugo page. */
import {pathToFileURL} from 'node:url';
import {resolve,join} from 'node:path';
const [siteArg,umfArg]=process.argv.slice(2);
if(!siteArg||!umfArg)throw Error('Usage: bun tools/check_runtime_diagram_browser.ts BUILT_SITE UMF_SOURCE');
const {chromium}=await import(pathToFileURL(join(resolve(umfArg),'node_modules/playwright/index.mjs')).href);
const root=resolve(siteArg),project=resolve(import.meta.dir,'..');
const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch(req){let path=decodeURIComponent(new URL(req.url).pathname);if(!path.startsWith('/ashlar/'))return new Response('missing',{status:404});path=resolve(root,path.slice('/ashlar/'.length));if(!path.startsWith(root+'/'))return new Response('missing',{status:404});if(path.endsWith('/schema'))path+='/index.html';const file=Bun.file(path);return new Response(file);}});
let browser;
try{browser=await chromium.launch({headless:true});const results=[];
 for(const [name,width,height] of [['desktop',1440,1200],['mobile',390,844]] as const){
  const page=await browser.newPage({viewport:{width,height}});const errors:string[]=[];page.on('pageerror',e=>errors.push(String(e)));
  await page.goto('http://127.0.0.1:'+server.port+'/ashlar/schema/');
  const image=page.locator('img[src$="runtime-er.svg"]');await image.waitFor();
  if(!(await image.evaluate((el:any)=>el.complete&&el.naturalWidth>0)))throw Error('Diagram asset did not load');
  await page.evaluate(()=>window.scrollTo(0,0));await page.screenshot({path:'/private/tmp/ashlar-er-'+name+'-top.png',fullPage:false});
  if(await page.locator('.runtime-model-table').count()!==8)throw Error('Missing physical tables');
  const item=page.locator('.runtime-model-table').first();await item.locator('summary').click();
  if(await item.locator('tbody tr').count()!==17)throw Error('Missing object fields');
  const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>window.innerWidth);
  if(overflow)throw Error('Whole page overflow');
  if(errors.length)throw Error(errors.join('\n'));
  await page.screenshot({path:'/private/tmp/ashlar-er-'+name+'.png',fullPage:false});
  results.push({viewport:name,diagramLoaded:true,tables:8,expandedObjectFields:17,noPageOverflow:true,noPageErrors:true});await page.close();
 }
 const report={browser:browser.version(),results,qualification:'Local Hugo output; actual image load, eight expandable tables, 17 object columns and whole-page overflow/page-error checks at desktop/mobile widths. Source seals remain unchanged; generated template/assets are outside their signature coverage.'};
 await Bun.write(join(project,'docs/helix/02-design/models/ashlar-delta-runtime/diagram-browser.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));
}finally{await browser?.close();server.stop(true);}
