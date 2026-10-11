import {copyJsonCharged} from '../json';
import {reserveSourceSchemaWork,reserveLiteralSchemaWork,reserveInputSchemaWork} from './schema-work';
import {UmfError,type Json,type Document,type Validation,type Element} from '../types';
import {validateDocument} from '../../validation/document';
import {checkSchemaLiteralTracked,type CoreLiteral} from '../schema-literals';
import {evaluateFieldValue} from './field-body';
import {evaluateRecordBody} from './record-body';
import {encodeCoreKeyTupleBody,type CoreKeyIdentity,type CoreKeyTupleValue} from '../key-tuple';
import type {CoreRecordValueIdentity,CoreRecordFieldValue} from '../record-values';
import type {CoreDatasetInput} from '../dataset-values';
import type {CoreCompactRecordValueCheck,CoreCompactKeyTupleReceipt} from '../dataset-values-compact';
export class WorkExceeded extends Error{}
export class ValueWork {
 used=0;categories:Record<string,number>=Object.create(null);
 charge(category:string,visits:number){if(!Number.isSafeInteger(visits)||visits<0||this.used+visits>1_000_000)throw new WorkExceeded('Compact dataset JSON traversal/copy/semantic budget exceeded');this.used+=visits;this.categories[category]=(this.categories[category]??0)+visits;}
 count(value:any):number{let n=0;const visit=(v:any)=>{this.charge('count',1);n++;if(v!==null&&typeof v==='object')for(const child of Object.values(v))visit(child);};visit(value);return n;}
 walk(category:string,value:any,multiplier=1){const n=this.count(value);this.charge(category,n*multiplier);return n;}
 copy(value:unknown):Json{return copyJsonCharged(value,n=>this.charge('copy',n));}
 snapshot(){return {used:this.used,categories:{...this.categories}};}
}
/** Streaming exact UTF-8 JSON byte bound before any semantic validator. */
export function preflightBytes(value:any,work:ValueWork){
 let bytes=0;const add=(n:number)=>{bytes+=n;if(bytes>4_000_000)throw new WorkExceeded('Compact source/input serialized bytes exceeded');};
 const string=(s:string)=>{add(2);for(let i=0;i<s.length;i++){const c=s.charCodeAt(i);if(c===34||c===92)add(2);else if(c<32)add([8,9,10,12,13].includes(c)?2:6);else if(c>=0xd800&&c<=0xdbff){const next=s.charCodeAt(i+1);if(next>=0xdc00&&next<=0xdfff){add(4);i++;}else add(6);}else if(c>=0xdc00&&c<=0xdfff)add(6);else add(c<128?1:c<2048?2:3);}};
 const visit=(v:any)=>{work.charge('preflight-byte-traversal',1);if(typeof v==='string'){string(v);return;}if(v===null||typeof v!=='object'){add(JSON.stringify(v).length);return;}if(Array.isArray(v)){add(2+Math.max(0,v.length-1));for(const child of v)visit(child);return;}const entries=Object.entries(v);add(2+Math.max(0,entries.length-1));for(const [key,child] of entries){string(key);add(1);visit(child);}};
 visit(value);return bytes;
}
function freeze(value:any):void{if(value!==null&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}}
/** Constructor is internal; public compact APIs never accept contexts or a ledger. */
export function createValueContext(sourceInput:Document,inputInput:unknown,work=new ValueWork()){
 const source=work.copy(sourceInput) as unknown as Document,input=work.copy(inputInput) as unknown as CoreDatasetInput;
 const sourceCount=work.count(source);
 if(preflightBytes(source,work)+preflightBytes(input,work)>4_000_000)throw new WorkExceeded('Compact aggregate source/input serialized bytes exceeded');
 // Eight original copy/view passes: document0.8, schema-properties,
 // legacy0.7 view, document0.7, relationship candidate, key0.6 view,
 // key candidate and its0.5 view. Twenty linear walks conservatively cover
 // document unknown/reference/facet walks (8), relationship/key indices and
 // declaration walks (8), diagnostic merges/status and context indices (4).
 // Source schemas and every possible recursive branch are reserved explicitly;
 // input-dependent semantic products and declarations are charged below.
 work.charge('source-copy-linear-semantics',28*sourceCount);
 reserveSourceSchemaWork(source,work);
 const modules=source!==null&&typeof source==='object'&&Array.isArray(source.modules)?source.modules:[];
 const elements=modules.flatMap(m=>m!==null&&typeof m==='object'&&Array.isArray(m.elements)?m.elements:[]);
 const relationships=modules.flatMap(m=>m!==null&&typeof m==='object'&&Array.isArray(m.relationships)?m.relationships:[]);
 const endpoints=relationships.reduce((n,r)=>n+(r!==null&&typeof r==='object'?(Array.isArray(r.source)?r.source.length:0)+(Array.isArray(r.target)?r.target.length:0)+(r.associationRecord?1:0):0),0);
 const keyFieldOccurrences=elements.reduce((n,e)=>n+(e!==null&&typeof e==='object'&&Array.isArray(e.keys)?e.keys.reduce((a:number,k:any)=>a+(k!==null&&typeof k==='object'&&Array.isArray(k.fields)?k.fields.length:0),0):0),0);
 const maxFieldReferences=elements.reduce((n,e)=>Math.max(n,e!==null&&typeof e==='object'&&Array.isArray(e.references)?e.references.length:0),0);
 work.charge('source-key-reference-scans',keyFieldOccurrences*maxFieldReferences);
 const maxKeys=elements.reduce((n,e)=>Math.max(n,e!==null&&typeof e==='object'&&Array.isArray(e.keys)?e.keys.length:0),0);
 // Endpoint occurrences, including invalid duplicates, drive presentation scans;
 // relationship declaration count does not bound those nested arrays.
 work.charge('source-index-collision-bound',elements.length**2+2*endpoints**2+endpoints*maxKeys);


 for(const e of elements){
  if(e===null||typeof e!=='object')continue;
  const declarations=[...(Array.isArray(e.examples)?e.examples:[]),...(Array.isArray(e.allowedValues)?e.allowedValues:[]),...((e.default as any)?.value!==undefined?[(e.default as any).value]:[])];
  for(const value of declarations)work.charge('source-declaration-validation',sourceCount*work.count(value));
 }
 const documentValidation=validateDocument(source);
 if(!documentValidation.valid||source?.umf!=='0.8.0')throw new UmfError('CORE_DATASET_SOURCE','Original valid core0.8 source required');
 work.walk('context-freeze',source);work.walk('context-freeze',input);freeze(source);freeze(input);work.walk('validation-freeze',documentValidation);freeze(documentValidation);
 const locations=new Map<string,{node:Element,path:string}>();
 source.modules.forEach((m,mi)=>m.elements.forEach((node,ei)=>locations.set(JSON.stringify([m.id,node.id]),{node,path:`/modules/${mi}/elements/${ei}`})));
 work.walk('context-key-index',source);
 const keyLocations=new Map<string,{record:Element;recordPath:string;key:any;keyPath:string;fields:{field:Element;path:string;member:string}[]}>();
 source.modules.forEach((m,mi)=>m.elements.forEach((record,ei)=>{
  if(record.kind!=='record')return;const path=`/modules/${mi}/elements/${ei}`,members=new Map<string,number>();
  for(const [index,ref] of ((record.members??[]) as any[]).entries()){const id=JSON.stringify([ref.module,ref.element]);if(!members.has(id))members.set(id,index);}
  for(const [ki,key] of ((record.keys??[]) as any[]).entries())keyLocations.set(JSON.stringify([m.id,record.id,key.id]),{record,recordPath:path,key,keyPath:path+`/keys/${ki}`,fields:key.fields.map((ref:any)=>{const id=JSON.stringify([ref.module,ref.element]),field=locations.get(id)!;return {field:field.node,path:field.path+'/facets',member:path+`/members/${members.get(id)}`};})});
 }));
 const literal=(doc:Document,field:Element,value:CoreLiteral)=>checkSchemaLiteralTracked(doc,field,value,(_d,_f,v)=>{if(['array','map'].includes(_f.cardinality as string))work.charge('literal-item-source-access',sourceCount);
  work.walk('literal-field-access',_f,4);work.walk('literal-value',v,4);});
 const field=(identity:CoreRecordValueIdentity,value:CoreLiteral)=>evaluateFieldValue(identity,value,id=>{
  const found=locations.get(JSON.stringify([id.module,id.element]));if(!found)throw new UmfError('CORE_SCHEMA_PROPERTIES','Element not found');
  work.walk('field-identity',id);return {source,node:found.node};
 },v=>work.copy(v),literal,v=>reserveLiteralSchemaWork(v,work));
 const record=(identity:CoreRecordValueIdentity,values:CoreRecordFieldValue[])=>{
  const found=locations.get(JSON.stringify([identity.module,identity.element]));if(!found||found.node.kind!=='record')throw new UmfError('CORE_RECORD_VALUE_IDENTITY','Record identity required');
  work.walk('record-index',found.node);work.walk('record-values',values);work.walk('record-document-diagnostics',documentValidation);
  return work.copy({...evaluateRecordBody(source,identity,values,documentValidation,field,ref=>{work.walk('record-member-lookup',ref);return locations.get(JSON.stringify([ref.module,ref.element]))?.node;},relationships.length>0),sourceRef:'#/source'}) as unknown as CoreCompactRecordValueCheck;
 };
 const key=(identity:CoreKeyIdentity,values:CoreKeyTupleValue[])=>{
  work.walk('key-index-identity',identity);work.charge('key-component-index',values.length);work.charge('key-relevant-diagnostic-scans',3*(1+2*values.length)*documentValidation.diagnostics.length);work.walk('key-values',values);work.walk('key-document-diagnostics',documentValidation);
  const selected=keyLocations.get(JSON.stringify([identity.module,identity.element,identity.key]));if(!selected)throw new UmfError('KEY_TUPLE_MISSING','Explicit Record/Key identity does not resolve','/identity');
  return work.copy({...encodeCoreKeyTupleBody(source as unknown as Json,identity,values,documentValidation,literal,selected),sourceRef:'#/source'}) as unknown as CoreCompactKeyTupleReceipt;
 };
 return {source,input,documentValidation,work,record,key,reserveInput:()=>reserveInputSchemaWork(input,work),copy:(v:unknown)=>work.copy(v),literal};
}
