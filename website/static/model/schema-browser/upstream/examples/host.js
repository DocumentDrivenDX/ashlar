import {mount} from '../index.js';
const status=document.getElementById('status');
const schema={umf:'0.1.0',id:'urn:example:customer',vocabularies:{},modules:[{id:'model',namespace:'urn:example:',elements:[{id:'customer',kind:'record',title:'Customer',members:[],extensions:{}}]}]};
const browser=mount(document.getElementById('viewer'),{assetsUrl:new URL('../assets/',import.meta.url).href,entries:[{id:'customer-model',title:'Customer model',category:'registered',path:'customer.json',format:'json',schemaFormat:'umf',text:JSON.stringify(schema)}],categories:[{id:'registered',label:'Registered schemas'}],selection:{schema:'customer-model'},hideHero:true,readOnlyLocalFiles:true,theme:'auto',height:'100%',onNavigate:selection=>status.textContent='Selected '+selection.schema,onError:error=>status.textContent=error.code+': '+error.message});
browser.ready.catch(error=>status.textContent=String(error));
document.getElementById('select').onclick=()=>browser.select({schema:'customer-model',definition:JSON.stringify(['model','customer'])}).catch(error=>status.textContent=String(error));
document.getElementById('focus').onclick=()=>browser.focus();
window.addEventListener('pagehide',()=>browser.destroy());
