"""Focused original-shaped authored path oracle and unchanged guard ports."""
import copy,hashlib,json,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_two_hop_count import admit_truss_two_hop_count
from ashlar_host.truss_weft_query import _execute_truss_two_hop_count_artifact

class Tests(unittest.TestCase):
 def fixture(self):
  raw=Path('examples/truss-native/typed-related-record.umf.json').read_bytes();model=json.loads(raw);doc=model['id'];digest=hashlib.sha256(raw).hexdigest();logical=lambda field:{'documentId':doc,'revision':digest,'module':'m','element':field};pin={'documentId':doc,'revision':digest,'sha256':digest,'umfVersion':model['umf']};relid={'documentId':doc,'revision':digest,'module':'m','relationship':'related'};declaration=model['modules'][0]['relationships'][0]
  record={'logical':logical('TypedRecord'),'kind':'object','typeId':'1','sourceSystem':'original','schemaRevision':'1','table':0,'properties':[{'logical':logical('record-id'),'home':{'kind':'props','propertyId':'1'}}]}
  relation={'logical':relid,'acceptedDefinition':declaration,'table':1,'kind':'edge','sourceSystem':'original','typeId':'1','schemaRevision':'1','source':record['logical'],'target':record['logical']}
  admission=object.__new__(TrussGraphAdmission);admission.revisions={'1':{'models':{doc:(model,raw)},'types':{'1':{'owner':(doc,'m','TypedRecord'),'required':{'1'},'keys':{'primary':('1',)}}},'edges':{'1':{'owner':(doc,'m','related'),'declaration':declaration,'endpoints':('1','1')}}}}
  tables=[{'name':['local','graph',role],'uuid':'original-'+role,'version':1}for role in ('object_current','edge_current')];binding={'modelPins':[pin],'publication':{'manifestUuid':'original','tables':tables},'records':[record],'relationships':[relation]};sql='SELECT COUNT(*) AS paths FROM TypedRecord a CROSS JOIN EXPAND_PATHS(a.related,related) AS p';request={'sql':sql,'target':{'bindingJson':encoded(binding),'bindingSha256':hashlib.sha256(encoded(binding).encode()).hexdigest()}}
  key={'id':'primary','fields':[logical('record-id')],'types':[{'family':'string','facets':{},'nullable':False}]};hop={'from':record['logical'],'to':record['logical'],'identity':relid,'inverse':False,'sourceKey':key,'targetKey':key,'sourceMultiplicity':declaration['sourceMultiplicity'],'targetMultiplicity':declaration['targetMultiplicity'],'targetLifecycle':'independent'};span={'start':0,'end':len(sql)};path={'hops':[hop,copy.deepcopy(hop)],'hopSpans':[span,span],'span':span,'startScan':'s0'};integer={'family':'integer','facets':{},'nullable':False}
  plan={'aggregate':True,'filters':[],'groups':[],'irVersion':'weft-ir/0.4.2','joins':[],'limit':None,'modulePins':[pin],'order':[],'outputs':[{'name':'paths','expression':{'op':'count','type':integer}}],'pageKey':None,'pathExpansion':{'occurrence':'p0','path':path},'readProfile':None,'requiredCapabilities':['aggregate','aggregate.count','project','relationship.pathExpansion','relationship.twoHopPaths','scan'],'source':{'occurrence':'s0','pin':pin,'record':record['logical']},'typeGraph':[]}
  artifact={'logicalPlan':plan,'modelPins':[pin],'bindingSha256':request['target']['bindingSha256'],'columns':[{'nullable':False,'outputName':'paths','position':1,'representation':{'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'},'sourceIdentities':[record['logical']]}]};manifest={'table_versions_json':encoded({'.'.join(t['name']):1 for t in tables})}
  node=lambda id:{'id':id,'source_system':'original','type_id':'1','schema_revision':'1','props_json':'{"1":"R'+id+'","2":9007199254740993,"3":1.2300}'};edge=lambda id,src,dst:{'id':id,'source_system':'original','rel_type_id':'1','schema_revision':'1','source_id':src,'target_id':dst,'source_type':'1','target_type':'1'};expected={'object_current':[node('1'),node('2')],'edge_current':[edge('1','1','2'),edge('2','2','2')]}
  return admission,artifact,request,manifest,expected,doc
 def admit(self,args):
  admission,artifact,request,manifest,expected,doc=args
  with patch('ashlar_host.truss_two_hop_count.truss_implicit_arithmetic_request',return_value=request):return admit_truss_two_hop_count(admission,artifact,request,manifest,expected,revision='1',document_id=doc)
 def test_forward_selfloop_repeated_edge_position_and_empty_zero(self):
  args=self.fixture();projection=self.admit(args);self.assertEqual(projection.expected_count,2);projection.require_complete_bag([{'paths':2}]);self.assertEqual(projection.decode(args[1]['columns'][0],'2'),2)
  with self.assertRaisesRegex(ValueError,'complete original'):projection.require_complete_bag([{'paths':1}])
  args[4]['edge_current']=[];self.admit(args).require_complete_bag([{'paths':0}])
 def test_wrong_authored_hop_or_duplicate_edge_identity_refuses(self):
  for mutation in ('hop','edge'):
   args=self.fixture()
   if mutation=='hop':args[1]['logicalPlan']['pathExpansion']['path']['hops'][1]['targetLifecycle']='owned'
   else:args[4]['edge_current'].append(copy.deepcopy(args[4]['edge_current'][0]))
   with self.assertRaises(ValueError):self.admit(args)
 def test_all_original_guards_precede_count_and_failure_withholds_sql(self):
  args=self.fixture();projection=self.admit(args);artifact=args[1];artifact.update(sql='SELECT original_result',parameters=[])
  phases=['ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity','ashlar.path.occurrenceIntegrity','ashlar.path.countCapacity']
  artifact['obligations']=[{'id':'ashlar.candidate.publication','parameters':{}}]+[{'id':name,'parameters':{'checks':[{'sql':'SELECT guard_'+str(i)}]}}for i,name in enumerate(phases)]
  for refusal in (None,'SELECT guard_2'):
   seen=[]
   def capture(sql,*,config):seen.append(sql);return {'schema':[['paths','STRING']],'rows':[['2'if sql=='SELECT original_result'else'1'if sql==refusal else'0']]}
   with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture):
    if refusal:
     with self.assertRaisesRegex(ValueError,'guard refused'):_execute_truss_two_hop_count_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,projection,1)
     self.assertNotIn('SELECT original_result',seen)
    else:
     rows,checks=_execute_truss_two_hop_count_artifact(SimpleNamespace(sql=lambda sql,args:sql),artifact,projection,1);projection.require_complete_bag(rows);self.assertEqual(len(checks),4);self.assertEqual(seen,['SELECT guard_0','SELECT guard_1','SELECT guard_2','SELECT guard_3','SELECT original_result'])
