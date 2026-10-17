"""Exact source-to-carrier controls; no compiler/native or authority substitute."""
import unittest
from decimal import localcontext
from ashlar.typed_source_policy import NumericToken
from ashlar_host.configured_typed_weft_projection import typed_projection_carrier,TypedProjectionCapacityError


def representation(family,facets=None):
    return {'kind':'scalar','carrier':'text','decoder':{'integer':'exact-integer','decimal':'exact-decimal','boolean':'boolean','string':'text'}[family],
            'logicalType':{'family':family,'facets':facets or{},'nullable':False}}


class Tests(unittest.TestCase):
    def test_original_numeric_tokens_and_exact_selected_carriers(self):
        integer=representation('integer',{'integerWidth':{'bits':64,'signed':False}})
        decimal=representation('decimal',{'precision':18,'scale':2})
        for token,wanted in (('18446744073709551615','18446744073709551615'),('9007199254740993','9007199254740993'),('1E+2','100'),('10.0','10'),('-0.0','0')):
            atom=NumericToken(token);self.assertEqual(typed_projection_carrier(atom,integer),wanted);self.assertEqual(atom.text,token)
        with localcontext()as context:
            context.prec=2
            for token,wanted in (('12.5000','12.50'),('1234567890123456.78','1234567890123456.78'),('-0.00','0.00'),('1.25e1','12.50'),('0e999999999999999999999999','0.00')):
                atom=NumericToken(token);self.assertEqual(typed_projection_carrier(atom,decimal),wanted);self.assertEqual(atom.text,token)
        self.assertEqual(typed_projection_carrier(True,representation('boolean')),'true')
        self.assertEqual(typed_projection_carrier(False,representation('boolean')),'false')
        self.assertEqual(typed_projection_carrier('雪',representation('string')),'雪')
        for token in ('0.1','18446744073709551616'):
            with self.subTest(token=token),self.assertRaises(ValueError):typed_projection_carrier(NumericToken(token),integer)
        with self.assertRaises(TypedProjectionCapacityError):typed_projection_carrier(NumericToken('1e999999999999999999999'),integer)
        with self.assertRaises(TypedProjectionCapacityError):typed_projection_carrier(NumericToken('9'*5000),integer)
        with self.assertRaises(ValueError):typed_projection_carrier(NumericToken('1.001'),decimal)
        with self.assertRaises(ValueError):typed_projection_carrier('true',representation('boolean'))


# Real source/progress/read owners over an explicitly inert native SQL port.
# The original named example is committed; no private compiler response is a test
# dependency. The small manually authored IR below follows its observed041 shape.
from contextlib import contextmanager
import copy,hashlib,json,re,sqlite3,tempfile,uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.native import SQLResult
from ashlar.publication import descriptor_from_manifest_row
from ashlar.attempt_store import PHASES
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar_host.config import ProducerConfig
from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
from ashlar_host.configured_source_local import open_configured_local_source
from ashlar_host.configured_local_read import open_configured_local_read
from ashlar_host.configured_typed_weft_binding import configured_typed_count_star_request
from ashlar_host.configured_typed_weft_projection import configured_typed_projection_oracle
from ashlar_host.count_star_admission import BACKEND
from ashlar_host.delta_custody import LocalDeltaTransport,encoded,sha
from ashlar_host.file_progress import JournaledJsonlProgress
from ashlar_host.managed_carriers import load_managed_carriers
from ashlar_host.managed_source_oracle import configured_prefix_carrier_rows
from test_configured_typed_source_local import inert_receipt


@contextmanager
def typed_scope(ordinal):
    with tempfile.TemporaryDirectory()as directory:
        root=Path(directory).resolve();root.chmod(0o700)
        example=Path(__file__).resolve().parents[1]/'examples/end-to-end/configured-typed-named-source.json'
        document=json.loads(example.read_bytes())
        for name in ('source','schema','bindings'):
            raw=(example.parent/document[name]['path']).read_bytes();(root/name).write_bytes(raw);document[name]['path']=name
        path=root/'configuration.json';path.write_text(json.dumps(document))
        config=ConfiguredSourceAdmissionConfig(path,ProducerConfig(root/'producer',root/'bun',root/'git',30,1048576,4194304),root/'receipts',4194304);context=object()
        with patch('ashlar_host.configured_typed_source_admission.admit_typed_records',side_effect=inert_receipt),open_configured_local_source(config,stream='typed-original',context=context)as source:
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


def authored_artifact(scope,selected=('quantity','amount','active','note'),*,count=False):
    config=scope.source.configuration;model=json.loads(config.model_source);elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    aliases={name:'spark_catalog.ashlar_configured_query_test.'+name.split('.')[-1]for name in scope.descriptor.versions}
    sql='SELECT COUNT(*) AS n FROM Item i'if count else'SELECT '+','.join('i.'+field+' AS output_'+str(i)for i,field in enumerate(selected))+' FROM Item i'
    request=configured_typed_count_star_request(config,dict(scope.descriptor.raw),scope.vector,scope.manifest_uuid,aliases,sql,layout_sha256=LAYOUT_SHA256)
    binding=json.loads(request['target']['bindingJson']);record=binding['records'][0];pin=request['modules'][0]['pin'];outputs=[];columns=[];graph={};caps={'project','scan'}
    if count:
        logical={'family':'integer','facets':{},'nullable':False};outputs=[{'name':'n','expression':{'op':'count','type':logical}}]
        columns=[{'nullable':False,'outputName':'n','position':1,'representation':{'kind':'scalar','carrier':'text','decoder':'exact-integer','logicalType':logical},'sourceIdentities':[record['logical']]}];caps.update(('aggregate','aggregate.count'))
    else:
        for index,name in enumerate(selected):
            prop=next(p for p in record['properties']if p['logical']['element']==name);identity=prop['logical'];field=elements[identity['module'],identity['element']]
            logical={'family':field['scalarType'],'facets':field.get('facets',{}),'nullable':False};rep=representation(logical['family'],logical['facets']);caps.add('type.'+logical['family'])
            if field['nullability']=='absent-allowed':rep={'kind':'value','descriptor':identity,'nativeNull':True};caps.update(('value.presence','value.nativeNull'))
            outputs.append({'name':'output_'+str(index),'expression':{'op':'field','scan':'s0','identity':identity}})
            columns.append({'nullable':False,'outputName':'output_'+str(index),'position':index+1,'representation':rep,'sourceIdentities':[identity]})
            graph[name]={'availability':field['nullability'],'identity':identity,'kind':'scalar','type':logical}
    ir={'aggregate':count,'filters':[],'groups':[],'irVersion':'weft-ir/0.4.1','joins':[],'limit':None,'modulePins':[pin],'order':[],'outputs':outputs,'pageKey':None,'readProfile':None,'requiredCapabilities':sorted(caps),'source':{'occurrence':'s0','pin':pin,'record':record['logical']},'typeGraph':list(graph.values())}
    return request,{'status':'compiled','backend':dict(BACKEND),'interfaceVersion':'weft-compile/0.4.1','dialect':'weft-sql/0.4.1','bindingSha256':request['target']['bindingSha256'],'modelPins':[pin],'logicalPlan':ir,'columns':columns}


def filtered_artifact(scope,literal='18446744073709551615',selected=('quantity','amount','active')):
    request,artifact=authored_artifact(scope,selected)
    request['sql']+=' WHERE i.quantity = '+literal
    binding=json.loads(request['target']['bindingJson']);record=binding['records'][0]
    identity=next(p['logical']for p in record['properties']if p['logical']['element']=='quantity')
    logical={'family':'integer','facets':{'integerWidth':{'bits':64,'signed':False}},'nullable':False}
    left=request['sql'].rindex('i.quantity');right=len(request['sql'])-len(literal)
    artifact['logicalPlan']['filters']=[{'op':'legacy','predicate':{'op':'equal',
        'left':{'identity':identity,'scan':'s0','span':{'start':left,'end':left+len('i.quantity')},'type':logical},
        'right':{'kind':'literal','value':literal,'span':{'start':right,'end':len(request['sql'])},'type':logical}}}]
    graph=artifact['logicalPlan']['typeGraph']
    if not any(d['identity']==identity for d in graph):graph.append({'availability':'required','identity':identity,'kind':'scalar','type':logical})
    artifact['logicalPlan']['requiredCapabilities']=sorted(set(artifact['logicalPlan']['requiredCapabilities'])|{'filter','equal','type.integer'})
    return request,artifact


class FactoryTests(unittest.TestCase):
    def test_actual_minted_prefix_distinct_presence_metadata_and_lifetime(self):
        expired=[]
        for ordinal in (1,2):
            with self.subTest(ordinal=ordinal),typed_scope(ordinal)as scope:
                request,artifact=authored_artifact(scope);oracle=configured_typed_projection_oracle(scope,request,artifact)
                model=scope.source.configuration.model_source;rawsource=scope.source.original_source
                result=oracle(model,rawsource,request)
                wanted=[['18446744073709551615','12.50','true','{"state":"absent"}'],['9007199254740993','0.00','false','{"state":"null"}']]if ordinal==1 else[['9007199254740994','40.00','false','{"state":"value","value":"雪"}'],['9007199254740993','0.00','false','{"state":"null"}']]
                self.assertEqual(result['rows'],wanted)
                self.assertEqual(result['witnesses']['original_atoms'][1]['fields'][1]['numeric_token'],'-0.00')
                self.assertIn('12.5000'if ordinal==1 else'40.00',result['witnesses']['original_atoms'][0]['props_json'])
                result['rows'].clear();self.assertEqual(oracle(model,rawsource,request)['rows'],wanted)
                repeated,repeated_artifact=authored_artifact(scope,('quantity','quantity'))
                repeated_oracle=configured_typed_projection_oracle(scope,repeated,repeated_artifact)
                self.assertTrue(all(row[0]==row[1]for row in repeated_oracle(model,rawsource,repeated)['rows']))
                count,count_artifact=authored_artifact(scope,count=True)
                self.assertEqual(configured_typed_projection_oracle(scope,count,count_artifact)(model,rawsource,count)['rows'],[['2']])
                for member,value in (('filters',[{}]),('joins',[{}]),('order',[{}]),('groups',[{}]),('limit',1),('pageKey',{}),('readProfile',{})):
                    changed=copy.deepcopy(artifact);changed['logicalPlan'][member]=value
                    with self.subTest(member=member),self.assertRaises(ValueError):configured_typed_projection_oracle(scope,request,changed)
                changed=copy.deepcopy(artifact);changed['columns'][-1]['representation']['nativeNull']=False
                with self.assertRaises(ValueError):configured_typed_projection_oracle(scope,request,changed)
                changed=copy.deepcopy(artifact);changed['logicalPlan']['typeGraph'][0]['type']['facets']['integerWidth']['signed']=True
                with self.assertRaises(ValueError):configured_typed_projection_oracle(scope,request,changed)
                with self.assertRaises(ValueError):oracle(model,rawsource+b'changed',request)
                expired.append((oracle,model,rawsource,request))
            with self.assertRaises(ValueError):expired[-1][0](*expired[-1][1:])

    def test_exact_required_uint64_equality_complete_population_and_refusals(self):
        expired=[]
        for ordinal in (1,2):
            with typed_scope(ordinal)as scope:
                model=scope.source.configuration.model_source;source=scope.source.original_source
                request,artifact=filtered_artifact(scope);oracle=configured_typed_projection_oracle(scope,request,artifact)
                result=oracle(model,source,request)
                self.assertEqual(result['rows'],[['18446744073709551615','12.50','true']]if ordinal==1 else[])
                self.assertEqual(result['witnesses']['source_records'],2);self.assertEqual(len(result['witnesses']['original_atoms']),2)
                self.assertIn('-0.00',result['witnesses']['original_atoms'][1]['props_json'])
                for literal,wanted in (('9007199254740993',[['9007199254740993','0.00','false']]),('9007199254740992',[])):
                    selected,compiled=filtered_artifact(scope,literal)
                    self.assertEqual(configured_typed_projection_oracle(scope,selected,compiled)(model,source,selected)['rows'],wanted)
                # Predicate homes and graph remain required even if not projected.
                selected,compiled=filtered_artifact(scope,selected=('amount','active'))
                self.assertEqual(configured_typed_projection_oracle(scope,selected,compiled)(model,source,selected)['rows'],[['12.50','true']]if ordinal==1 else[])
                if ordinal==1:
                    for mutation in ('literal-span','wrong-type','foreign-field','parameter','optional-field','extra-filter'):
                        bad=copy.deepcopy(artifact);predicate=bad['logicalPlan']['filters'][0]['predicate']
                        if mutation=='literal-span':predicate['right']['span']={'start':0,'end':1}
                        elif mutation=='wrong-type':predicate['right']['type']={'family':'integer','facets':{},'nullable':False}
                        elif mutation=='foreign-field':predicate['left']['identity']={**predicate['left']['identity'],'element':'foreign'}
                        elif mutation=='parameter':predicate['right']['kind']='parameter'
                        elif mutation=='optional-field':predicate['left']['identity']={**predicate['left']['identity'],'element':'note'}
                        else:bad['logicalPlan']['filters'].append(copy.deepcopy(bad['logicalPlan']['filters'][0]))
                        with self.subTest(mutation=mutation),self.assertRaises(ValueError):configured_typed_projection_oracle(scope,request,bad)
                expired.append((oracle,model,source,request))
            with self.assertRaises(ValueError):expired[-1][0](*expired[-1][1:])
