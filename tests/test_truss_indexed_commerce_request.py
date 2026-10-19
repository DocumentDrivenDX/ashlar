# SPDX-License-Identifier: MIT OR Apache-2.0
"""Effect-free original commerce binding correspondence with inert allocations."""
import copy,hashlib,json,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_indexed_commerce_request import truss_indexed_commerce_request

ROOT=Path(__file__).resolve().parents[1]
class Tests(unittest.TestCase):
 def fixture(self):
  raw=(ROOT/'examples/domain-packs/commerce/upstream/ontology.json').read_bytes();model=json.loads(raw);new=copy.deepcopy(model)
  module=new['modules'][0];module['elements'].append({'id':'products.display_label','name':'display_label','kind':'field','scalarType':'string','nullability':'absent-allowed','cardinality':'one','extensions':{}})
  product=next(e for e in module['elements']if e['id']=='products');product['members'].append({'module':'domain','element':'products.display_label'})
  new_raw=encoded(new).encode();admission=object.__new__(TrussGraphAdmission);revisions={};records=[];rows=[]
  for rev,m,original in [('1',model,raw),('2',new,new_raw)]:
   elements={e['id']:e for e in m['modules'][0]['elements']};types={}
   for index,record in enumerate((e for e in m['modules'][0]['elements']if e['kind']=='record'),1):
    tid=str(index);properties={};domains={};facets={};homes=[];ids={}
    for member in record['members']:
     field=elements[member['element']];pid=field['id'];properties[pid]=hashlib.sha256(encoded(field).encode()).hexdigest();domains[pid]=(field['scalarType'],field['nullability']);facets[pid]=field.get('facets',{});ids[member['element']]=pid
     homes.append({'logical':{'documentId':m['id'],'revision':hashlib.sha256(new_raw).hexdigest(),'module':'domain','element':field['id']},'home':{'kind':'props','propertyId':pid}})
    owner=(m['id'],'domain',record['id']);types[tid]={'owner':owner,'fields':properties,'domains':domains,'facets':facets,'keys':{k['id']:tuple(ids[f['element']]for f in k['fields'])for k in record.get('keys',[])}}
    if rev=='2':
     records.append({'logical':{'documentId':m['id'],'revision':hashlib.sha256(new_raw).hexdigest(),'module':'domain','element':record['id']},'typeId':tid,'sourceSystem':'actual-source','schemaRevision':'2','properties':homes})
     rows.append({'source_system':'actual-source','type_id':tid,'id':tid,'schema_revision':'2'if record['id']=='products'else'1','props_json':'{}'})
   revisions[rev]={'models':{m['id']:(m,original)},'types':types}
  admission.revisions=revisions
  request={'interfaceVersion':'weft-compile/0.4.2','dialect':'weft-sql/0.4.2','sql':'','modules':[{'documentJson':new_raw.decode(),'pin':{'umfVersion':'0.8.0','sha256':hashlib.sha256(new_raw).hexdigest()}}],'target':{'bindingJson':encoded({'records':records,'relationships':[{'schemaRevision':'2','logical':{'relationship':'original-unused'}}],'publication':{'tables':['object_current','edge_current','tombstone','whole_source_history']}}),'bindingSha256':''}}
  return admission,request,{'object_current':rows},(ROOT/'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
 def build(self,args,case='refund',empty=()):
  admission,request,expected,pack=args
  def builder(*pos,**kw):value=copy.deepcopy(request);value['sql']=pos[4];return value
  with patch('ashlar_host.truss_indexed_commerce_request.truss_implicit_arithmetic_request',side_effect=builder):return truss_indexed_commerce_request(admission,{}, {},'manifest',case_id=case,pack_bytes=pack,expected=expected,revision='2',document_id='urn:umf:domain:commerce',empty_record_revisions=empty)
 def test_exact_original_sql_model_and_per_record_revision(self):
  args=self.fixture();original=args[1]['modules'][0]['documentJson'];answer=self.build(args)
  self.assertEqual(answer['modules'][0]['documentJson'],original)
  self.assertEqual(answer['sql'],'SELECT r.id FROM refunds r JOIN returns x ON x.id=r.return_id JOIN order_lines l ON l.id=x.line_id JOIN products p ON p.id=l.product_id WHERE r.amount=x.quantity*p.unit_price')
  binding=json.loads(answer['target']['bindingJson']);homes={r['logical']['element']:r['schemaRevision']for r in binding['records']}
  self.assertEqual(homes['products'],'2');self.assertTrue(all(v=='1'for k,v in homes.items()if k!='products'))
  self.assertNotIn('relationships',binding);self.assertEqual(binding['publication'],json.loads(args[1]['target']['bindingJson'])['publication'])
  self.assertTrue(json.loads(answer['modules'][0]['documentJson'])['modules'][0]['relationships'])
  self.assertTrue(all('compatibleSchemaRevisions'not in r for r in binding['records']));self.assertEqual(answer['target']['bindingSha256'],hashlib.sha256(answer['target']['bindingJson'].encode()).hexdigest())
  self.assertTrue(all(r['schemaRevision']=='2'for r in json.loads(args[1]['target']['bindingJson'])['records']))
 def test_mixed_changed_pin_and_explicit_empty_selection(self):
  args=self.fixture();rows=args[2]['object_current'];rows.append({**rows[0],'id':'second','schema_revision':'2'})
  with self.assertRaisesRegex(ValueError,'homogeneous'):self.build(args)
  args=self.fixture();admission=args[0];record=next(t for t in admission.revisions['1']['types'].values()if t['owner'][2]=='returns');record['fields']['returns.quantity']='changed'
  with self.assertRaisesRegex(ValueError,'pin/domain'):self.build(args)
  args=self.fixture();row=args[2]['object_current'].pop();owner=args[0].revisions['2']['types'][row['type_id']]['owner']
  with self.assertRaisesRegex(ValueError,'Empty record'):self.build(args)
  self.build(args,empty=((owner,'1'),))
  with self.assertRaisesRegex(ValueError,'historical'):self.build(args,empty=((owner,'3'),))
