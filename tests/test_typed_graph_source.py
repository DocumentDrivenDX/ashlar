# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert receipt-shaped graph correspondence; no UMF semantic/producer proof."""
import base64,hashlib,json,tempfile,unittest
from pathlib import Path
from ashlar.source import records_digest
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration,GRAPH_CHECK_PIN
from ashlar.typed_graph_source import build_typed_graph_request,verify_typed_graph_receipt,TypedGraphSourcePolicy


def dump(v):return json.dumps(v,ensure_ascii=False,separators=(',',':')).encode()
def sha(v):return hashlib.sha256(v).hexdigest()
def b64(v):return base64.b64encode(v).decode()
def fixture(root,*,wrong_endpoint=False,edgeprops='{}',duplicate=False):
    field={'id':'id','kind':'field','scalarType':'integer','cardinality':'one','nullability':'required','extensions':{}}
    record=lambda name:{'id':name,'kind':'record','members':[{'module':'m','element':'id'}],'keys':[{'id':'pk','fields':[{'module':'m','element':'id'}]}],'extensions':{}}
    relation={'id':'links','directed':True,'source':[{'module':'m','element':'A'}],'target':[{'module':'m','element':'B','key':'pk'}],
        'sourceMultiplicity':{'min':0,'max':'*'},'targetMultiplicity':{'min':0,'max':'*'},'targetLifecycle':'independent'}
    model={'umf':'0.8.0','id':'graph','vocabularies':{},'modules':[{'id':'m','namespace':'urn:test:','elements':[field,record('A'),record('B')],'relationships':[relation]}]}
    fields={'21':['m','id','integer']};bindings={'profile':'ashlar-authored-typed-graph-source/0.1','document_revision':'1','source_system':'s',
        'records':{'17':{'record':['m','A'],'fields':fields},'18':{'record':['m','B'],'fields':fields}},
        'relationships':{'19':{'identity':['m','links'],'source_type_id':17,'target_type_id':18,'target_key':['m','B','pk']}},'qualification':'Development only'}
    def event(delivery,kind,tid,eid,operation,version,props,endpoints=None):
        v={'kind':'event','delivery_id':delivery,'source_profile':'ashlar-whole-entity/0.1','source_system':'s','entity_kind':kind,'type_id':str(tid),'id':str(eid),
            'entity_version':str(version),'schema_revision':'1','operation':operation,'props_json':props,'retained_json':'{"unknown":{"keep":true}}'}
        if endpoints is not None:v['endpoints']=endpoints
        return dump(v)+b'\n'
    ends=[{'type_id':'17','id':'1'},{'type_id':'17'if wrong_endpoint else'18','id':'1'}]
    edge=event('edge','edge',19,1,'create',1,edgeprops,ends)
    groups=[[event('a','object',17,1,'create',1,'{"21":1}'),event('b','object',18,1,'create',1,'{"21":2}'),edge],
        [event('replace','object',17,1,'replace',2,'{"21":3}')],[event('delete','edge',19,1,'delete',2,edgeprops,ends)]]
    if duplicate:groups[0].append(event('duplicate','object',17,1,'replace',2,'{"21":4}'))
    source=b''
    for i,events in enumerate(groups):
        source+=dump({'kind':'begin','batch_id':str(i)})+b'\n'+b''.join(events)+dump({'kind':'commit','batch_id':str(i),'record_count':len(events),'records_sha256':records_digest(events)})+b'\n'
    refs={}
    for name,raw in (('schema',dump(model)),('bindings',dump(bindings)),('source',source)):
        path=root/name;path.write_bytes(raw);refs[name]={'path':name,'sha256':sha(raw)}
    config={'profile':'ashlar-jsonl-typed-graph-source/0.1','feed':'feed','epoch':'epoch','sourceSystem':'s','schemaAlias':'graph','documentRevision':'1','producerRevision':GRAPH_CHECK_PIN,**refs}
    path=root/'config';path.write_bytes(dump(config));return load_typed_graph_jsonl_configuration(path)


def inert_receipt(config,request):
    source=json.loads(config.model_source);groups=[];valid={'valid':True,'complete':True,'diagnostics':[]}
    warnings=[{'code':'RECORD_KEY_CONTEXT_REQUIRED','path':'/identity','message':'Dataset key equality/uniqueness needs a separately qualified check','severity':'warning'},
        {'code':'RECORD_RELATIONSHIP_CONTEXT_REQUIRED','path':'/identity','message':'Relationship/endpoint constraints need a separately qualified dataset check','severity':'warning'}]
    record_validation={'valid':True,'complete':False,'diagnostics':warnings}
    def record(item,compact=False):
        result={'operation':'validate-core-record-values','version':'1.0.0','identity':item['identity'],'values':item['values'],
            'documentValidation':valid,'validation':record_validation,'fields':[{'field':{'module':'m','element':'id'},'state':'present','validation':valid}]}
        result.update({'sourceRef':'#/source'}if compact else{'source':source});return result
    def key(identity,values):return {'operation':'encode-core-key-tuple','version':'3.0.0','profile':'umf-key-tuple-v1','sourceRef':'#/source','identity':identity,'values':values,'keyPath':'/modules/0/elements/'+('1'if identity['element']=='A'else'2')+'/keys/0','bytesHex':'00'}
    for g in json.loads(request)['groups']:
        records=[{'request':wanted,'result':record(wanted)}for wanted in g['records']]
        dataset={'operation':'validate-core-dataset-values-compact','version':'1.0.0','source':source,'input':g['dataset'],'scope':'supplied-dataset-only','provenance':'unverified',
            'documentValidation':valid,'records':[{'instanceId':r['instanceId'],'result':record(r,True)}for r in g['dataset']['records']],
            'keys':[{'instanceId':r['instanceId'],'result':key(dict(r['identity'],key='pk'),[r['values'][0]['value']])}for r in g['dataset']['records']],
            'relationships':[{'instanceId':r['instanceId'],'identity':r['identity'],'sourceInstanceId':r['sourceInstanceId'],'targetInstanceId':json.dumps(['s','object','18','1'],separators=(',',':')),
                'targetKey':key(r['target']['identity'],r['target']['values'])}for r in g['dataset']['relationships']],
            'datasetValidation':valid,'residuals':[],'obligations':[{'id':n,'state':'satisfied','scope':'supplied-dataset-only'}for n in ('dataset.keys','dataset.relationships')]}
        groups.append({'records':records,'dataset':dataset})
    return dump({'format':'ashlar-typed-graph-check/0.1','producerRevision':GRAPH_CHECK_PIN,'sourceBase64':b64(config.model_source),'sourceSha256':sha(config.model_source),
        'requestBase64':b64(request),'requestSha256':sha(request),'groups':groups,'qualification':'INERT fixture only'})

class Tests(unittest.TestCase):
    def test_complete_graph_request_receipt_apply_replay_and_custody(self):
        with tempfile.TemporaryDirectory()as t:
            c=fixture(Path(t).resolve());request=build_typed_graph_request(c);r=json.loads(request)
            self.assertIsNone(c.policy);self.assertEqual(c.type_ids,(17,18));self.assertEqual(len(r['groups']),3)
            self.assertEqual(r['sourceSha256'],sha(c.source_bytes));self.assertEqual(r['configurationSha256'],sha(c.original));self.assertEqual(r['bindingSha256'],sha(c.binding_source))
            first=r['groups'][0];self.assertEqual(len(first['events']),3);self.assertEqual(len(first['records']),2)
            self.assertEqual(len(first['dataset']['records']),2);self.assertEqual(len(first['dataset']['relationships']),1)
            link=first['dataset']['relationships'][0];self.assertEqual(json.loads(link['sourceInstanceId']),['s','object','17','1'])
            self.assertEqual(link['target']['values'],[{'integerToken':'2'}]);self.assertEqual(r['groups'][-1]['dataset']['relationships'],[])
            receipt=inert_receipt(c,request);verify_typed_graph_receipt(c.model_source,request,receipt)
            for mutation in ('unknown-warning','unknown-document-warning','missing-joint','wrong-target','missing-record'):
                bad=json.loads(receipt)
                if mutation=='unknown-warning':bad['groups'][0]['records'][0]['result']['validation']['diagnostics'][0]['code']='UNKNOWN'
                elif mutation=='unknown-document-warning':bad['groups'][0]['records'][0]['result']['documentValidation']={'valid':True,'complete':False,'diagnostics':[{'code':'UNKNOWN','path':'/','severity':'warning','message':'Unresolved'}]}
                elif mutation=='missing-joint':bad['groups'][0]['dataset']['obligations']=[]
                elif mutation=='wrong-target':bad['groups'][0]['dataset']['relationships'][0]['targetInstanceId']=bad['groups'][0]['dataset']['records'][0]['instanceId']
                else:bad['groups'][0]['dataset']['records'].pop()
                with self.subTest(mutation=mutation),self.assertRaises(ValueError):c.with_receipt(dump(bad))
            admitted=c.with_receipt(receipt);self.assertIs(type(admitted.policy),TypedGraphSourcePolicy);admitted.verify()
            admitted.policy._changes.clear()
            with self.assertRaises(ValueError):admitted.verify()
            changed=json.loads(receipt);changed['groups'][0]['dataset']['input']['relationships'].clear()
            with self.assertRaises(ValueError):c.with_receipt(dump(changed))
            (Path(t)/'source').write_bytes(c.source_bytes+b'\n')
            with self.assertRaises(ValueError):c.verify()
    def test_foreign_endpoint_nonempty_edge_and_same_group_identity_refuse(self):
        for kwargs in ({'wrong_endpoint':True},{'edgeprops':'{"x":1}'},{'duplicate':True}):
            with self.subTest(kwargs=kwargs),tempfile.TemporaryDirectory()as t:
                c=fixture(Path(t).resolve(),**kwargs)
                with self.assertRaises(ValueError):build_typed_graph_request(c)
