# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real source/read lifetime over inert native/producer ports; no engine evidence."""
from contextlib import contextmanager
import copy,json,re,sqlite3,tempfile,unittest,uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.native import SQLResult
from ashlar.publication import descriptor_from_manifest_row
from ashlar.attempt_store import PHASES
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
from ashlar_host.config import ProducerConfig
from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
from ashlar_host.configured_source_local import open_configured_local_source
from ashlar_host.configured_local_read import open_configured_local_read
from ashlar_host.configured_typed_graph_weft_binding import configured_typed_graph_count_star_request
from ashlar_host.configured_typed_graph_weft_projection import configured_typed_graph_projection_oracle,STRING,INTEGER
from ashlar_host.count_star_admission import BACKEND
from ashlar_host.delta_custody import LocalDeltaTransport,encoded,sha
from ashlar_host.file_progress import JournaledJsonlProgress
from ashlar_host.managed_carriers import load_managed_carriers
from ashlar_host.managed_source_oracle import configured_prefix_carrier_rows
from ashlar.typed_graph_source import GRAPH_CHECK_PIN
import base64,hashlib

def inert_receipt(config,request):
    model=json.loads(config.model_source);elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    paths={(m['id'],e['id'],k['id']):f'/modules/{mi}/elements/{ei}/keys/{ki}'for mi,m in enumerate(model['modules'])for ei,e in enumerate(m['elements'])for ki,k in enumerate(e.get('keys',[]))}
    valid={'valid':True,'complete':True,'diagnostics':[]}
    warnings=[{'code':code,'path':'/identity','message':message,'severity':'warning'}for code,message in [('RECORD_KEY_CONTEXT_REQUIRED','Dataset key equality/uniqueness needs a separately qualified check'),('RECORD_RELATIONSHIP_CONTEXT_REQUIRED','Relationship/endpoint constraints need a separately qualified dataset check')]]
    def record(item,compact=False):
        members=elements[item['identity']['module'],item['identity']['element']]['members'];values={(v['field']['module'],v['field']['element']):v for v in item['values']}
        return {'operation':'validate-core-record-values','version':'1.0.0','identity':item['identity'],'values':item['values'],'documentValidation':valid,
            'validation':{'valid':True,'complete':False,'diagnostics':warnings},'fields':[{'field':f,'state':values.get((f['module'],f['element']),{}).get('state','absent'),'validation':valid}for f in members],**({'sourceRef':'#/source'}if compact else{'source':model})}
    def key(identity,values):return {'operation':'encode-core-key-tuple','version':'3.0.0','profile':'umf-key-tuple-v1','sourceRef':'#/source','identity':identity,'values':values,'keyPath':paths[identity['module'],identity['element'],identity['key']],'bytesHex':'00'}
    groups=[]
    for group in json.loads(request)['groups']:
        records=group['dataset']['records'];keys=[]
        for r in records:
            values={(v['field']['module'],v['field']['element']):v['value']for v in r['values']}
            for k in elements[r['identity']['module'],r['identity']['element']].get('keys',[]):keys.append({'instanceId':r['instanceId'],'result':key(dict(r['identity'],key=k['id']),[values[f['module'],f['element']]for f in k['fields']])})
        links=[]
        for link in group['dataset']['relationships']:
            identity=link['target']['identity'];definition=next(k for k in elements[identity['module'],identity['element']]['keys']if k['id']==identity['key'])
            def match(r):
                values={(v['field']['module'],v['field']['element']):v['value']for v in r['values']}
                return r['identity']=={k:v for k,v in identity.items()if k!='key'}and [values.get((f['module'],f['element']))for f in definition['fields']]==link['target']['values']
            target=next(r for r in records if match(r))
            links.append({'instanceId':link['instanceId'],'identity':link['identity'],'sourceInstanceId':link['sourceInstanceId'],'targetInstanceId':target['instanceId'],'targetKey':key(identity,link['target']['values'])})
        dataset={'operation':'validate-core-dataset-values-compact','version':'1.0.0','source':model,'input':group['dataset'],'scope':'supplied-dataset-only','provenance':'unverified','documentValidation':valid,
            'records':[{'instanceId':r['instanceId'],'result':record(r,True)}for r in records],'keys':keys,'relationships':links,'datasetValidation':valid,'residuals':[],
            'obligations':[{'id':n,'state':'satisfied','scope':'supplied-dataset-only'}for n in ('dataset.keys','dataset.relationships')]}
        groups.append({'records':[{'request':r,'result':record(r)}for r in group['records']],'dataset':dataset})
    return encoded({'format':'ashlar-typed-graph-check/0.1','producerRevision':GRAPH_CHECK_PIN,'sourceBase64':base64.b64encode(config.model_source).decode(),'sourceSha256':hashlib.sha256(config.model_source).hexdigest(),'requestBase64':base64.b64encode(request).decode(),'requestSha256':hashlib.sha256(request).hexdigest(),'groups':groups,'qualification':'INERT fixture only'}).encode()


@contextmanager
def graph_scope(ordinal):
    with tempfile.TemporaryDirectory()as directory:
        root=Path(directory).resolve();root.chmod(0o700)
        example=Path(__file__).resolve().parents[1]/'examples/end-to-end/configured-typed-graph-source.json'
        document=json.loads(example.read_bytes())
        for name in ('source','schema','bindings'):
            raw=(example.parent/document[name]['path']).read_bytes();(root/name).write_bytes(raw);document[name]['path']=name
        path=root/'configuration.json';path.write_text(json.dumps(document))
        config=ConfiguredSourceAdmissionConfig(path,ProducerConfig(root/'producer',root/'bun',root/'git',30,1048576,4194304),root/'receipts',4194304);context=object()
        with patch('ashlar_host.configured_typed_graph_source_admission.admit_typed_graph',side_effect=lambda selection,model,request:inert_receipt(load_typed_graph_jsonl_configuration(path),request)),open_configured_local_source(config,stream='typed-original',context=context)as source:
            clock='1791547200000000';carriers=load_managed_carriers();roles=('object_current','edge_current','tombstone','whole_source_history')
            tables={r:'local.original.'+r for r in (*roles,'manifest','attempts')}
            transport=object.__new__(LocalDeltaTransport);transport.journal_path=root/'journal';transport.installation_id='inert-original';transport.db=sqlite3.connect(transport.journal_path)
            (root/'.configured-source-writer.lock').write_bytes(b'')
            transport.targets={table:SimpleNamespace(table=table,uuid=str(uuid.uuid5(uuid.NAMESPACE_URL,table)),path=root/role)for role,table in tables.items()}
            snapshots={};manifests={};phases={};requests={}
            try:
                for n in (1,2,3):
                    spec=source.spec(n,context);request=spec.request();requests[n]=request
                    expected=configured_prefix_carrier_rows(source.configuration,source.batches[:n],materialization_clock_us=clock,carriers=carriers).rows();snapshots[n]=expected
                    row={'publication_id':spec.publication_id,'profile_version':'ashlar-delta/0.3','table_versions_json':encoded({tables[r]:n for r in roles}),
                        'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':encoded({source.configuration.feed:json.loads(request['source_checkpoint_json'])}),
                        'validation_report_json':encoded({'complete':True,'request_digest':request['request_digest'],'source_oracle_sha256':sha(encoded(expected)),'source_admission':source.metadata(context)}),'recorded_at':clock}
                    manifests[spec.publication_id]=row;artifact=encoded({'effects':{'scope':'inert native fixture port'},'manifest':row});records=[]
                    for i,phase in enumerate(PHASES):
                        payload=encoded({'request':request,'result_json':None if i<2 else artifact,'descriptor_json':encoded(row)if i==4 else None})
                        records.append({'phase':phase,'request_digest':request['request_digest'],'payload_json':payload,'payload_digest':sha(payload)})
                    phases[request['batch_id']]=records
                @contextmanager
                def resolver(descriptor,ctx):assert ctx is context;yield descriptor
                progress=JournaledJsonlProgress(transport,source.configuration.source,source.metadata(context)['source_sha256'],SimpleNamespace(admit=lambda *a:None),resolver,stream=source.stream,feed=source.configuration.feed,epoch=source.configuration.epoch)
                for n in (1,2,3):progress.acknowledge(requests[n],descriptor_from_manifest_row(manifests[source.spec(n,context).publication_id]),context)
                reader=JournaledJsonlProgress.open_existing(transport,source.configuration.source,source.metadata(context)['source_sha256'],stream=source.stream,feed=source.configuration.feed,epoch=source.configuration.epoch)
                def query(sql,parameters):
                    assert sql.startswith(('SELECT ','DESCRIBE ','SHOW '))
                    table=next(t for t in transport.targets if '.'.join('`'+p+'`'for p in t.split('.'))in sql);role=table.rsplit('.',1)[1]
                    if sql.startswith('DESCRIBE'):return SQLResult([{'id':transport.targets[table].uuid}])
                    if role=='manifest':return SQLResult([manifests[parameters['publication_id']]])
                    if role=='attempts':return SQLResult(phases[parameters['batch']])
                    if 'LIMIT 0'in sql:return SQLResult([],carriers.ordered_columns(role))
                    n=int(re.search('VERSION AS OF ([0-9]+)',sql)[1]);return SQLResult(snapshots[n][role])
                transport.query=query
                def admission(s,request,manifest,ctx):assert s is source and ctx is context
                with open_configured_local_read(transport,source,reader,ordinal=ordinal,tables=tables,context=context,materialization_clock_us=clock,current_admission=admission)as scope:
                    yield scope
            finally:transport.db.close()


def authored(scope,mode='related',bound=2):
    config=scope.source.configuration;model=json.loads(config.model_source)
    aliases={n:'spark_catalog.ashlar_configured_query_test.'+n.split('.')[-1]for n in scope.descriptor.versions}
    request=configured_typed_graph_count_star_request(config,dict(scope.descriptor.raw),scope.vector,scope.manifest_uuid,aliases,'Uninterpreted original SQL',layout_sha256=LAYOUT_SHA256)
    binding=json.loads(request['target']['bindingJson']);record=next(r for r in binding['records']if r['typeId']=='17');pin=request['modules'][0]['pin']
    identity=next(p['logical']for p in record['properties']if p['logical']['element']=='person_key');field={'op':'field','identity':identity,'scan':'s0'}
    mapping=next(r for r in binding['relationships']if r['typeId']=='31');definition=mapping['acceptedDefinition'];key={'id':'identity','fields':[identity],'types':[STRING]}
    relation={'from':record['logical'],'to':record['logical'],'identity':mapping['logical'],'inverse':False,'sourceKey':key,'targetKey':key,
        'sourceMultiplicity':definition['sourceMultiplicity'],'targetMultiplicity':definition['targetMultiplicity'],'targetLifecycle':definition['targetLifecycle']}
    count=mode in ('count','path-count');outputs=[{'name':'n'if count else'key','expression':{'op':'count','type':INTEGER}if count else field}]
    columns=[{'nullable':False,'outputName':outputs[0]['name'],'position':1,'representation':{'kind':'scalar','logicalType':INTEGER if count else STRING,'carrier':'text','decoder':'exact-integer'if count else'text'},'sourceIdentities':[record['logical']if count else identity]}]
    caps={'project','scan'}|({'aggregate','aggregate.count'}if count else{'type.string'})
    plan={'aggregate':count,'filters':[],'groups':[],'irVersion':'weft-ir/0.4.1','joins':[],'limit':None,'modulePins':[pin],'order':[],'outputs':outputs,'pageKey':None,'readProfile':None,'requiredCapabilities':None,'source':{'occurrence':'s0','pin':pin,'record':record['logical']},'typeGraph':[]if count else[{'availability':'required','identity':identity,'kind':'scalar','type':STRING}]}
    if mode=='related':
        caps.add('relationship.boundedKeys');outputs.append({'name':'related','expression':{'op':'relatedKeys','scan':'s0','relationship':relation,'bound':bound}})
        columns.append({'nullable':False,'outputName':'related','position':2,'representation':{'kind':'relatedKeys','relationship':mapping['logical'],'key':key,'bound':bound},'sourceIdentities':[record['logical'],record['logical'],identity]})
    if mode in ('paths','path-count'):
        caps.update(('relationship.pathExpansion','relationship.twoHopPaths'));plan['pathExpansion']={'occurrence':'p0','path':{'startScan':'s0','hops':[relation,copy.deepcopy(relation)],'span':{'start':0,'end':1},'hopSpans':[{'start':0,'end':1},{'start':0,'end':1}]}}
    plan['requiredCapabilities']=sorted(caps)
    artifact={'status':'compiled','backend':dict(BACKEND),'interfaceVersion':'weft-compile/0.4.1','dialect':'weft-sql/0.4.1','bindingSha256':request['target']['bindingSha256'],'modelPins':[pin],'logicalPlan':plan,'columns':columns}
    return request,artifact


def aggregate_authored(scope,*,cross=False,grouped=True,distinct_only=False):
    request,artifact=authored(scope,'path-count');plan=artifact['logicalPlan'];binding=json.loads(request['target']['bindingJson'])
    path=plan['pathExpansion']['path'];root=plan['source']['record'];identity=next(p['logical']for r in binding['records']if r['logical']==root for p in r['properties']if p['logical']['element']=='person_key')
    if cross:
        mapping=next(r for r in binding['relationships']if r['typeId']=='32');target=next(r for r in binding['records']if r['logical']==mapping['target'])
        target_field=next(p['logical']for p in target['properties']if p['logical']['element']=='place_key')
        target_key={'id':'identity','fields':[target_field],'types':[STRING]};declaration=mapping['acceptedDefinition']
        path['hops'][1]={'from':root,'to':mapping['target'],'identity':mapping['logical'],'inverse':False,'sourceKey':path['hops'][0]['sourceKey'],'targetKey':target_key,
            'sourceMultiplicity':declaration['sourceMultiplicity'],'targetMultiplicity':declaration['targetMultiplicity'],'targetLifecycle':declaration['targetLifecycle']}
    terminal=path['hops'][-1]
    outputs=[];columns=[];caps={'aggregate','project','scan','relationship.pathExpansion','relationship.twoHopPaths','aggregate.pathTargetDistinctCount'}
    def add(name,expression,representation,identities):
        outputs.append({'name':name,'expression':expression});columns.append({'nullable':False,'outputName':name,'position':len(outputs),'representation':representation,'sourceIdentities':identities})
    scalar=lambda ty,decoder:{'kind':'scalar','logicalType':ty,'carrier':'text','decoder':decoder}
    if grouped:
        plan['groups']=[{'scan':'s0','identity':identity,'type':STRING,'span':{'start':0,'end':1}}];caps.update(('group','type.string'))
        plan['typeGraph']=[{'availability':'required','identity':identity,'kind':'scalar','type':STRING}]
        add('key',{'op':'field','scan':'s0','identity':identity},scalar(STRING,'text'),[identity])
    if not distinct_only:
        caps.add('aggregate.count');add('paths',{'op':'count','type':INTEGER},scalar(INTEGER,'exact-integer'),[root])
    add('targets',{'op':'countDistinctPathTargets','pathOccurrence':'p0','type':INTEGER},dict(scalar(INTEGER,'exact-integer'),pathTarget={'pathOccurrence':'p0','record':terminal['to'],'key':terminal['targetKey']}),[terminal['to'],*terminal['targetKey']['fields']])
    plan['outputs']=outputs;artifact['columns']=columns;plan['requiredCapabilities']=sorted(caps)
    return request,artifact


class Tests(unittest.TestCase):
    def result(self,scope,mode='related',bound=2):
        request,artifact=authored(scope,mode,bound);oracle=configured_typed_graph_projection_oracle(scope,request,artifact)
        return oracle(scope.source.configuration.model_source,scope.source.original_source,request),oracle,request,artifact
    def test_actual_held_original_prefix_parallel_self_loop_isolate_paths_and_expiry(self):
        expired=[]
        for ordinal in (1,3):
            with graph_scope(ordinal)as scope:
                result,oracle,request,_=self.result(scope);by_key={r[0]:json.loads(r[1])for r in result['rows']}
                self.assertEqual(by_key['person-1'],{'items':[['person-1'],['person-2']],'truncated':True}if ordinal==1 else{'items':[['person-1']],'truncated':False})
                self.assertTrue(any(value=={'items':[],'truncated':False}for value in by_key.values()))
                if ordinal==1:
                    self.assertEqual(self.result(scope,'paths')[0]['rows'],[['person-1']]*3)
                    counted=self.result(scope,'path-count')[0];self.assertEqual(counted['rows'],[['3']]);self.assertEqual(len(counted['witnesses']['path_occurrences']),3)
                    self.assertEqual(len(set(map(encoded,counted['witnesses']['path_occurrences']))),3)
                    self.assertEqual(self.result(scope,'count')[0]['rows'],[['3']])
                    self.assertEqual(len(self.result(scope,'direct')[0]['rows']),3)
                expired.append((oracle,scope.source.configuration.model_source,scope.source.original_source,request))
            with self.assertRaises(ValueError):expired[-1][0](*expired[-1][1:])
    def test_grouped_parallel_paths_distinct_keys_cross_type_and_empty(self):
        for ordinal in (1,3):
            with graph_scope(ordinal)as scope:
                def execute(**choices):
                    request,artifact=aggregate_authored(scope,**choices)
                    oracle=configured_typed_graph_projection_oracle(scope,request,artifact)
                    return oracle(scope.source.configuration.model_source,scope.source.original_source,request)
                same=execute();cross=execute(cross=True)
                self.assertEqual(sorted(same['rows']),[['person-1','3','2']]if ordinal==1 else[['person-1','1','1']])
                self.assertEqual(cross['rows'],[['person-1','2','1']]if ordinal==1 else[])
                self.assertEqual(execute(cross=True,grouped=False)['rows'],[['2','1']]if ordinal==1 else[['0','0']])
                self.assertEqual(execute(grouped=False,distinct_only=True)['rows'],[['2']]if ordinal==1 else[['1']])
                self.assertEqual(len(same['witnesses']['path_occurrences']),3 if ordinal==1 else 1)
                request,artifact=aggregate_authored(scope)
                for mutation in ('target','group-type','group-field','occurrence'):
                    bad=copy.deepcopy(artifact)
                    if mutation=='target':
                        target=bad['columns'][-1]['representation']['pathTarget'];target['record']={**target['record'],'element':'Place'}
                    elif mutation=='group-type':bad['logicalPlan']['groups'][0]['type']=INTEGER
                    elif mutation=='group-field':bad['logicalPlan']['groups'][0]['identity']['element']='quantity'
                    else:bad['logicalPlan']['outputs'][-1]['expression']['pathOccurrence']='foreign'
                    with self.subTest(ordinal=ordinal,mutation=mutation),self.assertRaises(ValueError):configured_typed_graph_projection_oracle(scope,request,bad)

    def test_foreign_request_ir_descriptor_direction_bound_and_source_drift_refuse(self):
        with graph_scope(1)as scope:
            request,artifact=authored(scope)
            for mutation in ('direction','bound','descriptor','filter','hop'):
                bad=copy.deepcopy(artifact)
                if mutation=='direction':bad['logicalPlan']['outputs'][1]['expression']['relationship']['inverse']=True
                elif mutation=='bound':bad['logicalPlan']['outputs'][1]['expression']['bound']=0
                elif mutation=='descriptor':bad['columns'][1]['representation']['key']['id']='foreign'
                elif mutation=='filter':bad['logicalPlan']['filters']=[True]
                else:bad['logicalPlan']['pathExpansion']={'occurrence':'p0','path':{}}
                with self.subTest(mutation=mutation),self.assertRaises((ValueError,KeyError)):configured_typed_graph_projection_oracle(scope,request,bad)
            _,oracle,request,_=self.result(scope)
            with self.assertRaises(ValueError):oracle(scope.source.configuration.model_source,b'foreign',request)
            source=scope.source.configuration.source;original=source.read_bytes();source.write_bytes(original+b' ')
            try:
                with self.assertRaises(ValueError):oracle(scope.source.configuration.model_source,scope.source.original_source,request)
            finally:source.write_bytes(original)
