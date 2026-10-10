import copy,json,sys,unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from test_host_paths_keys_admission import PAIRS
from test_host_paths_keys_execution import PathsKeysExecutionTests
import test_run_commerce_path_weft as ports
from test_weft_path_plan import SCHEMAS
from ashlar_host import path_execution as execution
from ashlar_host.path_admission import PathAdmissionConfig,PathSchemaValidation,PathPlanError
from ashlar_host.path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig
class Controls(unittest.TestCase):
 def setup(self,name='original',rows=None):
  request,artifact=copy.deepcopy(PAIRS[name]);rows=[] if rows is None else rows
  provider=ports.Provider(artifact,rows)
  opened=SimpleNamespace(provider=provider,context=provider.context,model=request['modules'][0]['documentJson'].encode(),graph=b'mock original graph',original_native_files={'file':'exact'},native_files=lambda:{'file':'exact'})
  def public(request,artifact,checks):
   import hashlib
   raw=json.dumps({'sourceText':request['modules'][0]['documentJson'],'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':checks})
   receipt=json.dumps({'originalRequestText':raw,'umfRevision':'mock-explicit-revision','admitted':True})
   return {'originalRequestText':raw,'originalReceiptText':receipt,'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
  config=execution.PathExecutionConfig(PathAdmissionConfig(16777216,PathSchemaValidation(SCHEMAS,lambda *a:None),'paths-keys'),PathCaptureConfig(100,100000,1000000),PathDecodeConfig(100000),public,'mock-explicit-revision')
  oracle=lambda *a:{'rows':rows,'witnesses':{'mock':True},'scope':'execution-port control only'}
  return request,artifact,opened,config,oracle
 def run_case(self,case):
  r,a,o,c,oracle=case
  return execution.execute_commerce_path(o,r,a,copy.deepcopy(a),config=c,original_oracle=oracle)
 def test_all10_actual_metadata_complete_guard_and_schema_before_query(self):
  total=0
  for name,(r,a) in PAIRS.items():
   if a['status']!='compiled':continue
   with self.subTest(name=name):
    case=self.setup(name);p=case[2].provider;seen=[];schema=p.native_table_schema;sql=p.sql
    def native(table,*args):seen.append(('schema',copy.deepcopy(table)));return schema(table,*args)
    def query(statement,args):seen.append(('sql',statement,copy.deepcopy(args)));return sql(statement,args)
    p.native_table_schema=native;p.driver.transport.spark.sql=query
    result=self.run_case(case)
    expected=[(o['id'],check) for o in case[1]['obligations'] for check in o['parameters'].get('checks',o['parameters'].get('scans',[]))]
    actual=[(x['obligation'],x['check']) for x in result['guards']]
    self.assertEqual(sorted(json.dumps(x,sort_keys=True) for x in actual),sorted(json.dumps(x,sort_keys=True) for x in expected))
    native_events=[x for x in seen if x[0]=='schema'];self.assertTrue(native_events)
    first_sql=next(i for i,e in enumerate(seen) if e[0]=='sql');self.assertTrue(all(e[0]=='schema' for e in seen[:first_sql]))
    for ob in case[1]['obligations']:
     for declaration in ob['parameters'].get('edgeSchemas',[]):self.assertIn(('schema',declaration['table']),seen[:first_sql])
    calls=[e for e in seen if e[0]=='sql'];self.assertEqual(calls[-1][1],case[1]['sql'])
    slots={'p'+str(v['position']):v['value'] for v in case[1]['parameters']}
    self.assertTrue(all(e[2]==slots for e in calls));self.assertFalse(p.active);self.assertTrue(result['interval']['closed']);total+=1
  self.assertEqual(total,10)
 def test_standalone_schema_only_wrong_bigint_refuses_before_sql(self):
  case=self.setup();p=case[2].provider;artifact=case[1]
  table=next(o for o in artifact['obligations'] if o['id']=='ashlar.relatedKeys.collectionIntegrity')['parameters']['edgeSchemas'][0]['table'];original=p.native_table_schema;calls=[]
  def schema(current,*args):
   value=original(current,*args);calls.append(copy.deepcopy(current))
   if current==table:
    value['schema']['fields'][0]['type']='string';value['nativeTypes'][0][1]='STRING'
   return value
  p.native_table_schema=schema
  with self.assertRaises(execution.PathExecutionError):self.run_case(case)
  self.assertIn(table,calls);self.assertFalse(any(isinstance(e,tuple) for e in p.calls));self.assertFalse(p.active)
 def test_ordinal_failure_withholds_user_even_empty(self):
  case=self.setup();a,p=case[1],case[2].provider;bad=next(o for o in a['obligations'] if o['id']=='ashlar.relatedKeys.ordinalCapacity')['parameters']['checks'][0]['sql'];original=p.sql;calls=[]
  def sql(statement,args):
   calls.append(statement)
   return ports.frame(['violations'],[['1']]) if statement==bad else original(statement,args)
  p.driver.transport.spark.sql=sql
  with self.assertRaises(execution.PathExecutionError):self.run_case(case)
  self.assertNotIn(a['sql'],calls);self.assertFalse(p.active)
 def test_new_decoder_never_reached_under_old_profile(self):
  case=list(self.setup());c=case[3];case[3]=execution.PathExecutionConfig(PathAdmissionConfig(16777216,c.admission.schema_validation,'paths'),c.capture,c.decoder,c.public_source,c.public_source_revision)
  with patch.object(execution,'decode_related_keys',side_effect=AssertionError('decoder reached')) as decoder:
   with self.assertRaises(PathPlanError):self.run_case(case)
  decoder.assert_not_called();self.assertEqual(case[2].provider.calls,[])
 def test_decode_primary_survives_cleanup_replacement(self):
  case=self.setup(rows=[['P1','{"items":[],"truncated":false}']]);p=case[2].provider;primary=KeyboardInterrupt('synthetic decode interruption')
  @contextmanager
  def interval(context):
   p.active=True
   try:yield
   finally:p.active=False;raise OSError('synthetic close')
  p.interval=interval
  with patch.object(execution,'decode_related_keys',side_effect=primary):
   with self.assertRaises(KeyboardInterrupt) as raised:self.run_case(case)
  self.assertIs(raised.exception,primary);self.assertTrue(primary.interval_cleanup_failed);self.assertFalse(p.active)
 def test_closing_ack_custody_refusal_no_provisional_return(self):
  case=self.setup(rows=[['P1','{"items":[],"truncated":false}']]);p=case[2].provider;error=ValueError('synthetic closing ACK refusal')
  p.closed_interval_custody=lambda _:(_ for _ in ()).throw(error)
  with self.assertRaises(ValueError) as raised:self.run_case(case)
  self.assertIs(raised.exception,error);self.assertFalse(p.active)
 def test_decoder_bound_and_original_bytes(self):
  raw=' {"items":[["S1"],["S1"]],"truncated":true} ';case=self.setup(rows=[['P1',raw]])
  result=self.run_case(case);decoded=result['decoded'][0][1]
  self.assertEqual(decoded.items,(('S1',),('S1',)));self.assertEqual(decoded.original,raw.encode());self.assertTrue(decoded.truncated)
if __name__=='__main__':unittest.main()
