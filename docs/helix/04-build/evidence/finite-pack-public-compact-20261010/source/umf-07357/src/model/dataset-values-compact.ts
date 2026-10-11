import {type Document,type Json,UmfError} from './types';
import {copyJson} from './json';
import type {CoreRecordValueCheck} from './record-values';
import type {CoreKeyTupleReceipt} from './key-tuple';
import {composeCoreDatasetValues,type CoreDatasetInput,type CoreDatasetValueCheck} from './dataset-values';
import {reserveCompactSchemaWork} from './internal/schema-work';
import {snapshotSchema} from '../validation/internal-schema';
import {createValueContext,ValueWork,WorkExceeded} from './internal/value-context';
import {createValidator} from '../validation/schema';
import keySchema from '../../spec/core/key-tuple-operation-v3.schema.json';
import coreSchema from '../../spec/core/schema-properties-document.schema.json';
import operationSchema from '../../spec/core/dataset-value-compact-operation.schema.json';
export {operationSchema as coreDatasetValueCompactOperationSchema};
export type CoreCompactRecordValueCheck=Omit<CoreRecordValueCheck,'source'>&{sourceRef:'#/source'};
export type CoreCompactKeyTupleReceipt=Omit<CoreKeyTupleReceipt,'source'>&{sourceRef:'#/source'};
export interface CoreDatasetCompactValueCheck extends Omit<CoreDatasetValueCheck,'operation'|'records'|'keys'|'relationships'>{
 operation:'validate-core-dataset-values-compact';
 records:{instanceId:string;result:CoreCompactRecordValueCheck}[];
 keys:{instanceId:string;result:CoreCompactKeyTupleReceipt}[];
 relationships:{instanceId:string;identity:{module:string;id:string};sourceInstanceId:string;targetInstanceId:string;targetKey:CoreCompactKeyTupleReceipt}[];
}
const validator=createValidator();validator.addSchema(snapshotSchema(coreSchema));validator.addSchema(snapshotSchema(keySchema));const check=validator.compile(snapshotSchema(operationSchema));
const canonical=(v:Json,work:ValueWork):string=>{work.charge('verify-canonical-visit',1);if(Array.isArray(v))return '['+v.map(x=>canonical(x,work)).join(',')+']';if(v!==null&&typeof v==='object'){const keys=Object.keys(v);work.charge('verify-canonical-key-sort',keys.length*Math.ceil(Math.log2(keys.length+1)));return '{'+keys.sort().map(k=>JSON.stringify(k)+':'+canonical(v[k]!,work)).join(',')+'}';}return JSON.stringify(v);};
function limit(error:unknown):never{if(error instanceof WorkExceeded)throw new UmfError('LIMIT',error.message);throw error;}
/** Internal accounting receipt for tests, not exported from package API. */
export function evaluateCompactWithWork(source:Document,input:CoreDatasetInput,work=new ValueWork()){
 try{
  const context=createValueContext(source,input,work);
  const receipt=composeCoreDatasetValues(source,input,context) as CoreDatasetCompactValueCheck;
  reserveCompactSchemaWork(receipt,work);
  if(!check(receipt))throw new UmfError('CORE_DATASET_COMPACT_RESULT',JSON.stringify(check.errors));
  return {receipt,work:work.snapshot()};
 }catch(error){return limit(error);}
}
export function validateCoreDatasetValuesCompact(source:Document,input:CoreDatasetInput):CoreDatasetCompactValueCheck{return evaluateCompactWithWork(source,input).receipt;}
export function verifyCoreDatasetValuesCompact(receiptInput:CoreDatasetCompactValueCheck,current:Document,expectedInput:CoreDatasetInput):CoreDatasetCompactValueCheck{return verifyCompactWithWork(receiptInput,current,expectedInput).receipt;}
/** Internal accounting receipt for tests, not exported from package API. */
export function verifyCompactWithWork(receiptInput:CoreDatasetCompactValueCheck,current:Document,expectedInput:CoreDatasetInput){
 const work=new ValueWork();
 try{
  const receipt=work.copy(receiptInput) as unknown as CoreDatasetCompactValueCheck;
  reserveCompactSchemaWork(receipt,work);
  if(!check(receipt))throw new UmfError('CORE_DATASET_COMPACT_RECEIPT','Compact receipt violates its versioned schema');
  const source=work.copy(current),input=work.copy(expectedInput);
  work.walk('verify-source-comparison',receipt.source);work.walk('verify-source-comparison',source);
  work.walk('verify-input-comparison',receipt.input);work.walk('verify-input-comparison',input);
  if(canonical(receipt.source as unknown as Json,work)!==canonical(source,work)||canonical(receipt.input as unknown as Json,work)!==canonical(input,work))throw new UmfError('CORE_DATASET_COMPACT_RECEIPT','Invalid/stale original compact dataset receipt');
  // Candidate schema admission plus complete equality below proves the fresh
  // result's schema too; avoid validating that identical result a second time.
  const context=createValueContext(source as unknown as Document,input as unknown as CoreDatasetInput,work);
  const expected=composeCoreDatasetValues(context.source,context.input,context) as CoreDatasetCompactValueCheck;
  work.walk('verify-result-comparison',receipt);work.walk('verify-result-comparison',expected);
  if(canonical(receipt as unknown as Json,work)!==canonical(expected as unknown as Json,work))throw new UmfError('CORE_DATASET_COMPACT_RECEIPT','Compact receipt differs from complete recomputation');
  return {receipt:expected,work:work.snapshot()};
 }catch(error){return limit(error);}
}
