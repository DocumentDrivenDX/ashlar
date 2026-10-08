/** Host-only generation from actual pinned UMF schema and relationship readers. */
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const root=resolve(import.meta.dir,'..'),folder=join(root,'docs/helix/02-design/models/ashlar-delta-runtime');
const [sourceArg,mode='--check']=process.argv.slice(2);
if(!sourceArg||!['--check','--write'].includes(mode))throw Error('Usage: bun tools/generate_runtime_diagram.ts UMF_SOURCE [--check|--write]');
const source=resolve(sourceArg),index=await Bun.file(join(folder,'index.json')).json();
function git(...args:string[]){const result=Bun.spawnSync(['git','-C',source,...args]);if(result.exitCode)throw Error('Unable to verify UMF source');return new TextDecoder().decode(result.stdout).trim();}
function pinned(){if(git('rev-parse','HEAD')!==index.umfRevision||git('status','--porcelain'))throw Error('Clean pinned UMF source required');}
pinned();
const {readDocument}=await import(pathToFileURL(join(source,'src/model/document.ts')).href);
const {validateDocument}=await import(pathToFileURL(join(source,'src/validation/document.ts')).href);
const {generateDeltaDDL}=await import(pathToFileURL(join(source,'src/adapters/delta/ddl.ts')).href);
const digest=(text:string)=>createHash('sha256').update(text).digest('hex');
const exact=(value:object,keys:string[])=>{if(Object.keys(value).some(key=>!keys.includes(key)))throw Error('Unknown meaning must remain in UMF; diagram projection refused');};
const escape=(value:unknown)=>String(value).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]!));
const logicalText=await Bun.file(join(folder,'relationships.umf.json')).text(),logical=readDocument(logicalText,'json');
const validation=validateDocument(logical);
if(!validation.valid||validation.diagnostics.some((d:any)=>!/^EXPERIMENTAL_CORE_(NULLABILITY|CARDINALITY|FACETS|KEYS|RELATIONSHIPS)$/.test(d.code)))throw Error('Unsupported logical model interpretation');
exact(logical,['umf','id','vocabularies','modules']);
if(logical.umf!=='0.7.0'||Object.keys(logical.vocabularies).length||logical.modules.length!==1)throw Error('Expected authored core relationship model');
const module=logical.modules[0];exact(module,['id','namespace','elements','relationships']);
if(module.id!=='runtime'||module.namespace!=='ashlar')throw Error('Unexpected module');
const elements=new Map<string,any>();
for(const element of module.elements){
  exact(element,element.kind==='record'?['id','name','kind','members','keys','extensions']:['id','name','kind','scalarType','nullability','cardinality','extensions']);
  if(Object.keys(element.extensions).length||elements.has(element.id))throw Error('Unexpected element semantics');
  elements.set(element.id,element);
}
const inputs:Record<string,string>={'relationships.umf.json':digest(logicalText),'index.json':digest(await Bun.file(join(folder,'index.json')).text())};
const tables:any[]=[];
const families:Record<string,string>={string:'string',long:'integer',boolean:'boolean',timestamp:'timestamp'};
for(const name of index.tables){
  const text=await Bun.file(join(folder,name+'.umf.json')).text(),native=generateDeltaDDL(readDocument(text,'json'));
  if(JSON.stringify(native.definition.name)!==JSON.stringify([name]))throw Error('Native table identity differs');
  inputs[name+'.umf.json']=digest(text);
  const record=elements.get(name),fields=JSON.parse(native.schemaJson).fields;
  if(!record||record.kind!=='record'||record.name!==name||record.members.length!==fields.length)throw Error('Record/physical membership differs');
  const columns=fields.map((field:any,i:number)=>{
    const ref=record.members[i];exact(ref,['module','element']);
    if(ref.module!=='runtime'||ref.element!==name+'.'+field.name)throw Error('Field mapping/order differs');
    const logicalField=elements.get(ref.element);
    if(logicalField?.kind!=='field'||logicalField.scalarType!==families[field.type]||logicalField.cardinality!=='one'||!['required','unspecified'].includes(logicalField.nullability))throw Error('Logical/native field interpretation differs');
    return {name:field.name,deltaType:field.type,nullable:field.nullable,logicalAvailability:logicalField.nullability};
  });
  const keys=(record.keys??[]).map((key:any)=>{
    exact(key,['id','name','primary','fields']);
    return {...key,fields:key.fields.map((ref:any)=>{exact(ref,['module','element']);const member=elements.get(ref.element);if(ref.module!=='runtime'||!record.members.some((m:any)=>m.element===ref.element)||member?.nullability!=='required')throw Error('Unqualified key field');return ref.element.slice(name.length+1);})};
  });
  tables.push({name,columns,keys,clusterBy:native.definition.clusterBy,properties:native.definition.properties});
}
if(elements.size!==tables.reduce((n,t)=>n+t.columns.length+1,0))throw Error('Unprojected logical content');
const relationships=module.relationships.map((relationship:any)=>{
  exact(relationship,['id','name','source','target','sourceMultiplicity','targetMultiplicity','targetLifecycle','directed']);
  if(relationship.source.length!==1||relationship.target.length!==1||relationship.targetLifecycle!=='independent'||!relationship.directed)throw Error('Unsupported relationship shape');
  const from=relationship.source[0],to=relationship.target[0];exact(from,['module','element']);exact(to,['module','element','key']);
  for(const bounds of [relationship.sourceMultiplicity,relationship.targetMultiplicity])exact(bounds,['min','max']);
  if(from.module!=='runtime'||to.module!=='runtime'||from.element!=='edge_current'||to.element!=='object_current'||to.key!=='identity')throw Error('Unsupported endpoint mapping');
  if(JSON.stringify(relationship.sourceMultiplicity)!==JSON.stringify({min:0,max:'*'})||JSON.stringify(relationship.targetMultiplicity)!==JSON.stringify({min:1,max:1}))throw Error('Unqualified cardinality');
  return {id:relationship.id,name:relationship.name,source:from.element,target:to.element,targetKey:to.key,sourceMultiplicity:relationship.sourceMultiplicity,targetMultiplicity:relationship.targetMultiplicity,enforcement:'authored logical requirement; publisher responsibility, not Delta foreign-key enforcement'};
});
if(relationships.length!==2)throw Error('Two authored endpoint relationships required');
pinned();
const generator={tool:'tools/generate_runtime_diagram.ts',sha256:digest(await Bun.file(import.meta.path).text())};
const model={profile:'ashlar-runtime-er/0.1',umfRevision:index.umfRevision,generator,inputs,tables,relationships,qualification:'Eight current runtime tables. Physical columns/layout come from exact UMF Delta definitions; logical keys and endpoint relationships are separate authored core 0.7.0 requirements. No native equality, FK enforcement, optional projection coverage or end-to-end runtime completion is implied.'};
const ordered=[...tables].sort((a,b)=>index.tables.indexOf(a.name)-index.tables.indexOf(b.name));
let svg='<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 780" role="img" aria-labelledby="title desc"><title id="title">UMF-generated Ashlar runtime entity relationship diagram</title><desc id="desc">Eight runtime tables. Edges have source and target endpoint relationships to keyed objects. These are authored logical requirements, not Delta enforced foreign keys. Every physical field is listed below the diagram.</desc><defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0 0L10 5L0 10Z" fill="#234fbc"/></marker></defs>';
for(const [i,r] of relationships.entries()){
  const y=30+i*42;
  svg+='<path d="M720 126V'+y+'H240V126" fill="none" stroke="#234fbc" stroke-width="2" stroke-dasharray="5 4" marker-end="url(#arrow)"/><rect x="315" y="'+(y-13)+'" width="330" height="24" fill="#eaede5"/><text x="480" y="'+(y+4)+'" text-anchor="middle" fill="#202b2d" font-family="sans-serif" font-size="15">'+escape(r.name)+' · each edge → 1 object</text>';
}
for(const [i,table] of ordered.entries()){
 const x=40+(i%2)*480,y=128+Math.floor(i/2)*160;
 svg+='<rect x="'+x+'" y="'+y+'" width="400" height="116" rx="4" fill="#f5f3ec" stroke="#234fbc"/><text x="'+(x+18)+'" y="'+(y+29)+'" font-family="monospace" font-weight="bold" font-size="18" fill="#202b2d">'+escape(table.name)+'</text><text x="'+(x+18)+'" y="'+(y+56)+'" font-family="sans-serif" font-size="15" fill="#526267">'+table.columns.length+' physical columns · '+(table.keys.length?'authored typed identity':'no logical key asserted here')+'</text><text x="'+(x+18)+'" y="'+(y+82)+'" font-family="monospace" font-size="13" fill="#526267">'+escape(table.keys[0]?.fields.join(' + ')??(table.clusterBy.length?table.clusterBy.length+' clustering columns':'no explicit clustering'))+'</text>';
}
svg+='</svg>\n';
let html='<section aria-labelledby="runtime-er-title"><h2 id="runtime-er-title">Runtime model generated from UMF</h2><p>The eight current runtime roles below share their model with UMF-generated DDL. Dashed arrows are authored logical endpoint requirements; the publisher must validate them. Delta does not enforce these foreign keys. Each object may have zero or many incoming or outgoing edges.</p><figure class="schema-map"><img src="{{ "model/runtime-er.svg" | relURL }}" alt="Eight runtime tables; source and target edge endpoints reference the object identity." width="960" height="780" style="width:100%;min-width:680px;height:auto"><figcaption>Columns and layout: exact UMF Delta definitions. Keys and relationships: authored UMF core 0.7.0. Optional projections in the conceptual map below are outside this runtime inventory.</figcaption></figure><p><a href="{{ "model/runtime-er.json" | relURL }}">Download the generated model and input fingerprints</a>. Expand a table to inspect all physical columns. K marks an authored logical identity component; NOT NULL is the Delta declaration.</p>';
for(const table of ordered){
 html+='<details class="runtime-model-table"><summary>'+escape(table.name)+' · '+table.columns.length+' columns</summary><div class="runtime-model-fields" role="region" tabindex="0" aria-label="'+escape(table.name)+' physical columns"><table><thead><tr><th>Column</th><th>Delta type</th><th>Declaration</th><th>Logical identity</th></tr></thead><tbody>';
 for(const column of table.columns)html+='<tr><td><code>'+escape(column.name)+'</code></td><td>'+escape(column.deltaType)+'</td><td>'+(column.nullable?'NULL allowed':'NOT NULL')+'</td><td>'+(table.keys.some((key:any)=>key.fields.includes(column.name))?'K':'—')+'</td></tr>';
 html+='</tbody></table></div><p>Clustering: '+escape(table.clusterBy.join(', ')||'none explicitly authored')+'.</p></details>';
}
html+='</section>\n';
const outputs={'website/static/model/runtime-er.svg':digest(svg),'website/layouts/_partials/runtime-er.generated.html':digest(html)};
for(const [path,value] of [['website/static/model/runtime-er.json',JSON.stringify({...model,outputs},null,2)+'\n'],['website/static/model/runtime-er.svg',svg],['website/layouts/_partials/runtime-er.generated.html',html]]){
 const target=join(root,path!);
 if(mode==='--write')await Bun.write(target,value!);
 else if(await Bun.file(target).text()!==value)throw Error('Stale diagram output: '+path);
}
console.log((mode==='--write'?'Generated':'Verified')+' eight UMF runtime tables and two authored endpoint relationships');
