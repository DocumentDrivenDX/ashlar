import {copyJson,LIMITS} from './json';
import {UmfError,type Document,type Diagnostic,type Validation,type Json} from './types';
import {knownSchemaMembers} from './schema-literals';
import {validateDocument} from '../validation/document';
import {validateCoreRecordValues,type CoreRecordValueIdentity,type CoreRecordFieldValue,type CoreRecordValueCheck} from './record-values';
import {encodeCoreKeyTuple,type CoreKeyIdentity,type CoreKeyTupleValue,type CoreKeyTupleReceipt} from './key-tuple';

import {createValidator} from '../validation/schema';
import coreSchema from '../../spec/core/schema-properties-document.schema.json';
import keySchema from '../../spec/core/key-tuple-operation-v3.schema.json';
import operationSchema from '../../spec/core/dataset-value-operation.schema.json';
const validator=createValidator();validator.addSchema(coreSchema);validator.addSchema(keySchema);
const checkReceipt=validator.compile(operationSchema),checkInput=validator.compile({$schema:operationSchema.$schema,$id:operationSchema.$id+':input',$ref:'#/$defs/input',$defs:operationSchema.$defs});
export {operationSchema as coreDatasetValueOperationSchema};

export interface CoreDatasetRecord {instanceId:string;identity:CoreRecordValueIdentity;values:CoreRecordFieldValue[]}
export interface CoreDatasetRelationship {instanceId:string;identity:{module:string;id:string};sourceInstanceId:string;target:{identity:CoreKeyIdentity;values:CoreKeyTupleValue[]}}
export interface CoreDatasetInput {scope:{id:string;closure:'supplied-dataset-only'};records:CoreDatasetRecord[];relationships:CoreDatasetRelationship[];context?:Json}
export interface CoreDatasetValueCheck {
 operation:'validate-core-dataset-values';version:'1.0.0';scope:'supplied-dataset-only';provenance:'unverified';
 source:Document;input:CoreDatasetInput;documentValidation:Validation;
 records:{instanceId:string;result:CoreRecordValueCheck}[];
 keys:{instanceId:string;result:CoreKeyTupleReceipt}[];
 relationships:{instanceId:string;identity:{module:string;id:string};sourceInstanceId:string;targetInstanceId:string;targetKey:CoreKeyTupleReceipt}[];
 datasetValidation:Validation;obligations:{id:'dataset.keys'|'dataset.relationships';state:'satisfied'|'invalid'|'unresolved';scope:'supplied-dataset-only'}[];
 residuals:{path:string;value:Json;reason:string}[];
}
const qualified=(identity:CoreRecordValueIdentity)=>JSON.stringify([identity.module,identity.element]);
const tupleId=(identity:CoreKeyIdentity,bytes:string)=>JSON.stringify([identity.module,identity.element,identity.key,bytes]);
const canonical=(v:Json):string=>Array.isArray(v)?'['+v.map(canonical).join(',')+']':v!==null&&typeof v==='object'?'{'+Object.keys(v).sort().map(k=>JSON.stringify(k)+':'+canonical(v[k]!)).join(',')+'}':JSON.stringify(v);
const fail=(message:string):never=>{throw new UmfError('CORE_DATASET_INPUT',message);};
function identity(value:any,keys:string[]){knownSchemaMembers(value,keys,'/input');if(Object.keys(value).length!==keys.length||keys.some(k=>typeof value[k]!=='string'||!value[k]))fail('Exact nonempty qualified identity required');}
/** Finite supplied dataset only; no global/native coverage or execution authority. */
export function validateCoreDatasetValues(sourceInput:Document,inputInput:CoreDatasetInput):CoreDatasetValueCheck{
 const source=copyJson(sourceInput) as unknown as Document,input=copyJson(inputInput) as unknown as CoreDatasetInput;
 if(!checkInput(input))fail('Dataset request violates its versioned operation schema');
 const documentValidation=validateDocument(source);
 if(source.umf!=='0.8.0'||!documentValidation.valid)throw new UmfError('CORE_DATASET_SOURCE','Original valid core0.8 source required');
 knownSchemaMembers(input,['scope','records','relationships','context'],'/input');
 knownSchemaMembers(input.scope,['id','closure'],'/input/scope');
 if(Object.keys(input.scope).length!==2||typeof input.scope.id!=='string'||!input.scope.id||input.scope.closure!=='supplied-dataset-only')fail('Explicit finite supplied-dataset scope required');
 if(!Array.isArray(input.records)||input.records.length>1000||!Array.isArray(input.relationships)||input.relationships.length>10000)fail('Explicit bounded records/relationships required');
 // Bound repeated full-source receipts and semantic work before composing any
 // per-record/field/key operation. Original source copies are never truncated.
 const count=(value:any):number=>1+(Array.isArray(value)?value.reduce((n,v)=>n+count(v),0):value!==null&&typeof value==='object'?Object.values(value).reduce<number>((n,v)=>n+count(v),0):0);
 const elements=source.modules.flatMap(m=>m.elements),sourceValues=count(source),inputValues=count(input);
 const maxKeys=Math.max(0,...elements.map(e=>Array.isArray(e.keys)?e.keys.length:0));
 const maxMembers=Math.max(0,...elements.map(e=>Array.isArray(e.members)?e.members.length:0));
 const declarations=source.modules.reduce((n,m)=>n+(Array.isArray(m.relationships)?m.relationships.length:0),0);
 // Each retained Record result and Key/relationship target-Key receipt
 // embeds source once. Temporary validation copies belong to the independent
 // semantic-work guard; they are not duplicated in the retained JSON output.
 const copies=3+input.records.length*(1+maxKeys)+input.relationships.length+2*declarations;
 const bytes=(value:any):number=>{
  let size=0;const add=(n:number)=>{size+=n;if(size>4_000_000)throw new UmfError('LIMIT','Aggregate dataset source/input bytes exceeded');};
  const visit=(v:any)=>{
   if(v===null||typeof v!=='object'){add(new TextEncoder().encode(JSON.stringify(v)).length);return;}
   if(Array.isArray(v)){add(2+Math.max(0,v.length-1));for(const child of v)visit(child);return;}
   const entries=Object.entries(v);add(2+Math.max(0,entries.length-1));
   for(const [key,child] of entries){add(new TextEncoder().encode(JSON.stringify(key)).length+1);visit(child);}
  };visit(value);return size;
 };
 if(sourceValues*copies+inputValues*8>LIMITS.maxValues||bytes(source)*copies+bytes(input)*8>4_000_000||sourceValues*(input.records.length*(1+maxMembers+maxKeys)+input.relationships.length*2)>1_000_000)throw new UmfError('LIMIT','Dataset work/retained receipt budget exceeded');
 let retainedBytes=bytes(source)+bytes(input)+bytes(documentValidation)+512;
 const retainBudget=(value:any)=>{retainedBytes+=bytes(value)+2;if(retainedBytes>4_000_000)throw new UmfError('LIMIT','Actual aggregate dataset receipt-byte budget exceeded');};
 const diagnostics:Diagnostic[]=[],residuals:CoreDatasetValueCheck['residuals']=[];
 const add=(code:string,path:string,message:string,severity:'error'|'warning'='error')=>{const diagnostic={code,path,message,severity};retainBudget(diagnostic);diagnostics.push(diagnostic);};
 const retain=(path:string,value:any,reason:string)=>{const residual={path,value:copyJson(value),reason};retainBudget(residual);residuals.push(residual);add('DATASET_UNRESOLVED',path,reason,'warning');};
 const unresolvedDocument=documentValidation.diagnostics.filter(d=>d.severity==='warning'&&!d.code.startsWith('EXPERIMENTAL_'));
 for(const d of unresolvedDocument){
  retainBudget(d);diagnostics.push({...d});let value:any=source;
  for(const segment of d.path.split('/').slice(1)){const key=segment.replace(/~1/g,'/').replace(/~0/g,'~');value=value!==null&&typeof value==='object'&&Object.hasOwn(value,key)?value[key]:null;}
  const residual={path:d.path,value:copyJson(value),reason:d.message};retainBudget(residual);residuals.push(residual);
 }

 const declared=new Map<string,{record:any;path:string}>();source.modules.forEach((m,mi)=>m.elements.forEach((record,ei)=>declared.set(qualified({module:m.id,element:record.id}),{record,path:`/modules/${mi}/elements/${ei}`})));
 const recordResults:CoreDatasetValueCheck['records']=[],keys:CoreDatasetValueCheck['keys']=[],relationships:CoreDatasetValueCheck['relationships']=[];
 const instances=new Map<string,CoreDatasetRecord>(),tuples=new Map<string,string[]>();let keyInvalid=false,keyUnresolved=unresolvedDocument.length>0,relationshipInvalid=false,relationshipUnresolved=unresolvedDocument.length>0;
 const unresolvedKeys=new Set<string>();
 const encode=(id:CoreKeyIdentity,values:CoreKeyTupleValue[],path:string):CoreKeyTupleReceipt|undefined=>{
  try{return encodeCoreKeyTuple(source,id,values);}catch(error){
   if(!(error instanceof UmfError)||error.code==='LIMIT')throw error;
   const unknown=error.code==='KEY_TUPLE_UNKNOWN';add(error.code,path,error.message,unknown?'warning':'error');
   if(unknown){keyUnresolved=true;unresolvedKeys.add(JSON.stringify([id.module,id.element,id.key]));retain(path,values,error.message);}else keyInvalid=true;
  }
 };
 input.records.forEach((instance,index)=>{
  knownSchemaMembers(instance,['instanceId','identity','values'],'/input/records/'+index);
  if(Object.keys(instance).length!==3||typeof instance.instanceId!=='string'||!instance.instanceId)fail('Exact record locator required');identity(instance.identity,['module','element']);
  if(instances.has(instance.instanceId)){add('DUPLICATE_DATASET_INSTANCE','/input/records/'+index,'Duplicate record instance locator');keyInvalid=true;}else instances.set(instance.instanceId,instance);
  const result=validateCoreRecordValues(source,instance.identity,instance.values);const recordReceipt={instanceId:instance.instanceId,result};retainBudget(recordReceipt);recordResults.push(recordReceipt);
  for(const d of result.validation.diagnostics)if(!['RECORD_KEY_CONTEXT_REQUIRED','RECORD_RELATIONSHIP_CONTEXT_REQUIRED'].includes(d.code)){const diagnostic={...d,path:'/input/records/'+index+d.path};retainBudget(diagnostic);diagnostics.push(diagnostic);}
  const owner=declared.get(qualified(instance.identity))!;
  for(const key of owner.record.keys??[]){
   const values=key.fields.map((ref:CoreRecordValueIdentity)=>instance.values.find(v=>qualified(v.field)===qualified(ref))).map((v:CoreRecordFieldValue|undefined)=>v?.state==='present'?v.value:null);
   const id={...instance.identity,key:key.id},receipt=encode(id,values,'/input/records/'+index);
   if(!receipt)continue;const keyReceipt={instanceId:instance.instanceId,result:receipt};retainBudget(keyReceipt);keys.push(keyReceipt);
   const k=tupleId(id,receipt.bytesHex),previous=tuples.get(k)??[];previous.push(instance.instanceId);tuples.set(k,previous);
   if(previous.length>1){keyInvalid=true;add('DUPLICATE_DATASET_KEY',owner.path+'/keys','Declared Key tuple duplicates within supplied Record collection');}
  }
 });
 const relations=new Map<string,{value:any;path:string;supported:boolean}>();
 source.modules.forEach((m,mi)=>((m.relationships??[]) as any[]).forEach((r:any,ri:number)=>{
  const path=`/modules/${mi}/relationships/${ri}`;
  const unknown=documentValidation.diagnostics.some(d=>d.severity==='warning'&&d.path.startsWith(path+'/'));
  const supported=!unknown&&r.directed===true&&r.source.length===1&&r.target.length===1&&r.targetLifecycle==='independent'&&r.associationRecord===undefined;
  if(!supported){relationshipUnresolved=true;retain(path,r,'Relationship context outside directed monomorphic independent-lifecycle subset');}
  relations.set(JSON.stringify([m.id,r.id]),{value:r,path,supported});
 }));
 const occurrences=new Set<string>(),outgoing=new Map<string,Set<string>>(),incoming=new Map<string,Set<string>>();
 input.relationships.forEach((link,index)=>{
  const path='/input/relationships/'+index;
  knownSchemaMembers(link,['instanceId','identity','sourceInstanceId','target'],path);
  if(Object.keys(link).length!==4||typeof link.instanceId!=='string'||!link.instanceId||typeof link.sourceInstanceId!=='string'||!link.sourceInstanceId)fail('Exact relationship occurrence/source locator required');
  identity(link.identity,['module','id']);knownSchemaMembers(link.target,['identity','values'],path+'/target');if(Object.keys(link.target).length!==2)fail('Exact target Key locator required');identity(link.target.identity,['module','element','key']);
  if(occurrences.has(link.instanceId)){relationshipInvalid=true;add('DUPLICATE_DATASET_RELATIONSHIP',path,'Duplicate relationship occurrence locator');}occurrences.add(link.instanceId);
  const relationId=JSON.stringify([link.identity.module,link.identity.id]),declaration=relations.get(relationId);
  if(!declaration){relationshipInvalid=true;add('DATASET_RELATIONSHIP_IDENTITY',path,'Undeclared relationship identity');return;}
  if(!declaration.supported){retain(path,link,'Occurrence depends on unsupported relationship declaration');return;}
  const r=declaration.value,start=instances.get(link.sourceInstanceId);
  if(!start||qualified(start.identity)!==qualified(r.source[0])){relationshipInvalid=true;add('DATASET_SOURCE_ENDPOINT',path,'Source locator missing or not an allowed exact Record type');return;}
  const end=r.target[0],id=link.target.identity;
  if(qualified(id)!==qualified(end)||id.key!==end.key){relationshipInvalid=true;add('DATASET_TARGET_ENDPOINT',path,'Target locator does not name declared Record/Key');return;}
  const receipt=encode(id,link.target.values,path+'/target');
  if(!receipt){if(unresolvedKeys.has(JSON.stringify([id.module,id.element,id.key])))relationshipUnresolved=true;else relationshipInvalid=true;return;}
  const targets=tuples.get(tupleId(id,receipt.bytesHex))??[];
  if(targets.length!==1){relationshipInvalid=true;add('DATASET_TARGET_RESOLUTION',path,'Target Key has zero or ambiguous supplied record matches');return;}
  const target=targets[0]!;
  const resolved={instanceId:link.instanceId,identity:link.identity,sourceInstanceId:link.sourceInstanceId,targetInstanceId:target,targetKey:receipt};retainBudget(resolved);relationships.push(resolved);
  for(const [map,a,b] of [[outgoing,link.sourceInstanceId,target],[incoming,target,link.sourceInstanceId]] as const){const k=JSON.stringify([relationId,a]),set=map.get(k)??new Set<string>();set.add(b);map.set(k,set);}
 });
 for(const [relationId,declaration] of relations){if(!declaration.supported)continue;const r=declaration.value;
  if(unresolvedKeys.has(JSON.stringify([r.target[0].module,r.target[0].element,r.target[0].key]))){relationshipUnresolved=true;retain(declaration.path,r,'Target Key equality is unresolved; missing participation cannot be inferred');continue;}
  for(const [map,endpoint,bounds] of [[outgoing,r.source[0],r.targetMultiplicity],[incoming,r.target[0],r.sourceMultiplicity]] as const){
   for(const instance of input.records)if(qualified(instance.identity)===qualified(endpoint)){
    const count=map.get(JSON.stringify([relationId,instance.instanceId]))?.size??0;
    if(count<bounds.min||(bounds.max!=='*'&&count>bounds.max)){relationshipInvalid=true;add('DATASET_RELATIONSHIP_MULTIPLICITY',declaration.path,'Distinct related record count violates declared multiplicity within supplied dataset');}
   }
  }
 }
 const valid=!diagnostics.some(d=>d.severity==='error'),complete=diagnostics.length===0&&residuals.length===0;
 const state=(invalid:boolean,unresolved:boolean)=>invalid?'invalid' as const:unresolved?'unresolved' as const:'satisfied' as const;
 const result=copyJson({operation:'validate-core-dataset-values',version:'1.0.0',scope:'supplied-dataset-only',provenance:'unverified',source,input,documentValidation,records:recordResults,keys,relationships,datasetValidation:{valid,complete,diagnostics},obligations:[{id:'dataset.keys',state:state(keyInvalid,keyUnresolved),scope:'supplied-dataset-only'},{id:'dataset.relationships',state:state(relationshipInvalid,relationshipUnresolved),scope:'supplied-dataset-only'}],residuals}) as unknown as CoreDatasetValueCheck;
 bytes(result); // Exact final serialized-byte check also covers all envelope overhead.
 if(!checkReceipt(result))throw new UmfError('CORE_DATASET_RESULT',JSON.stringify(checkReceipt.errors));return result;
}
export function verifyCoreDatasetValues(receiptInput:CoreDatasetValueCheck,current:Document,expectedInput:CoreDatasetInput):CoreDatasetValueCheck{
 const receipt=copyJson(receiptInput) as unknown as CoreDatasetValueCheck;
 if(!checkReceipt(receipt))throw new UmfError('CORE_DATASET_RECEIPT','Dataset receipt violates its versioned schema');
 if(receipt.operation!=='validate-core-dataset-values'||receipt.version!=='1.0.0'||canonical(copyJson(receipt.source))!==canonical(copyJson(current))||canonical(copyJson(receipt.input))!==canonical(copyJson(expectedInput)))throw new UmfError('CORE_DATASET_RECEIPT','Invalid/stale original dataset receipt');
 const expected=validateCoreDatasetValues(receipt.source,receipt.input);
 if(canonical(copyJson(receipt))!==canonical(copyJson(expected)))throw new UmfError('CORE_DATASET_RECEIPT','Dataset receipt differs from complete recomputation');return expected;
}
