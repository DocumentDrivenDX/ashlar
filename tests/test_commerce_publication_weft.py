import copy,json,unittest
from contextlib import contextmanager
from types import SimpleNamespace
from run_commerce_publication_weft import execute_guarded,encoded,compiler_request,PublicationProvider
from commerce_source_transaction import ROOT,build_transaction

class Provider:
    def __init__(self):self.calls=[];self.resolves=0;self.drift=False;self.suppress=False;self.invalid=False
    @contextmanager
    def interval(self,context):
        try:yield
        except ValueError:
            if not self.suppress:raise
    def resolve(self,context):
        self.resolves+=1
        return SimpleNamespace(descriptor=SimpleNamespace(raw={'publication':'changed' if self.drift and self.resolves>1 else 'original'}),snapshots={'object':('uuid',2),'edge':('edge-uuid',3)})
    def admit_binding(self,*args):pass
    def runtime(self,*args):pass
    def sql(self,sql,params):
        self.calls.append(sql)
        return [{'violations':'1' if self.invalid else '0'}] if sql=='integrity SQL' else [{'n':'2'}]

class Tests(unittest.TestCase):
    def fixture(self):
        binding={'publication':{'id':'publication','manifestUuid':'original','tables':[{'name':['local','graph','object'],'uuid':'uuid','version':2},{'name':['local','graph','edge'],'uuid':'edge-uuid','version':3}]},'modelPins':[{'documentId':'original','revision':'0.8'}],'layoutRevision':'layout','layoutSha256':'layout-sha'}
        request={'target':{'bindingJson':encoded(binding),'bindingSha256':'exact'}}
        obligations=[{'id':'ashlar.candidate.publication','parameters':binding},{'id':'ashlar.candidate.scalarIntegrity','parameters':{'phase':'before-user-query','samePublicationRequired':True,'noPartialPublication':True,'checks':[{'sql':'integrity SQL'}]}}]
        return request,{'status':'compiled','bindingSha256':'exact','modelPins':binding['modelPins'],'obligations':obligations,'parameters':[],'sql':'original user SQL'}
    def test_exact_checks_user_sql_and_closing_resolver(self):
        request,artifact=self.fixture();provider=Provider()
        self.assertEqual(execute_guarded(provider,request,artifact,context=object())['rows'],[{'n':'2'}]);self.assertEqual(provider.calls,['integrity SQL','original user SQL']);self.assertEqual(provider.resolves,2)
    def test_full_vector_closing_drift_withholds_buffered_result(self):
        request,artifact=self.fixture();provider=Provider();provider.drift=True
        with self.assertRaises(ValueError):execute_guarded(provider,request,artifact,context=object())
        self.assertEqual(provider.calls,['integrity SQL','original user SQL'])
    def test_unknown_duplicate_or_changed_publication_refuses_before_sql(self):
        for mutate in (lambda a:a['obligations'].append(copy.deepcopy(a['obligations'][0])),lambda a:a['obligations'][0].update(id='unknown'),lambda a:a['obligations'][0]['parameters']['publication'].update(id='forged')):
            request,artifact=self.fixture();mutate(artifact);provider=Provider()
            with self.assertRaises(ValueError):execute_guarded(provider,request,artifact,context=object())
            self.assertEqual(provider.calls,[])
    def test_failed_integrity_and_suppressed_context_never_release_user_rows(self):
        for suppress in (False,True):
            request,artifact=self.fixture();provider=Provider();provider.invalid=True;provider.suppress=suppress
            with self.assertRaises(ValueError):execute_guarded(provider,request,artifact,context=object())
            self.assertEqual(provider.calls,['integrity SQL'])

    def test_canonical_field_binding_uses_exact_original_identity_and_full_vector(self):
        model=(ROOT/'ontology.json').read_bytes();graph=(ROOT/'graph/fixture.json').read_bytes()
        batch,bindings=build_transaction(model,graph,source_system='private-original-commerce-fixture')
        roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
        registry=[{'table':'local.commerce.'+role,'uuid':role+'-fixture'} for role in roles]
        versions={r['table']:2 for r in registry[:4]}
        manifest={'publication_id':'fixture-only','table_versions_json':encoded(versions),'source_progress_json':encoded({batch.feed:{}})}
        aliases={t:t.replace('local.','spark_catalog.') for t in versions}
        request=compiler_request('SELECT p.unit_price FROM products p',model,bindings,manifest,registry,aliases,fields=True)
        binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(len(binding['publication']['tables']),4)
        admitted={tuple(p['identity']):p['property_id'] for p in bindings['properties']}
        for record in binding['records']:
            for field in record['properties']:
                identity=field['logical'];self.assertEqual(field['home']['propertyId'],admitted[(identity['documentId'],identity['module'],identity['element'])])
        legacy,old=build_transaction(model,graph,source_system=batch.feed,binding_profile='ashlar-commerce-development-bindings/0.1')
        with self.assertRaises(ValueError):compiler_request('SELECT p.unit_price FROM products p',model,old,manifest,registry,aliases,fields=True)
        aliases[registry[1]['table']]=aliases[registry[0]['table']]
        with self.assertRaises(ValueError):compiler_request('SELECT COUNT(*) FROM products p',model,bindings,manifest,registry,aliases)

    def test_provider_freezes_complete_original_binding_and_refuses_changed_source(self):
        context=object();binding={'records':[{'sourceSystem':'original','typeId':'16'}]}
        provider=PublicationProvider(None,{},None,b'request',b'manifest',binding,context=context)
        provider.active=True;binding['records'][0]['sourceSystem']='forged'
        with self.assertRaisesRegex(ValueError,'binding changed'):provider.admit_binding(binding,None,context)

class LifecycleTests(unittest.TestCase):
    def provider(self):
        from protected_outbox_ack import AckScope
        from unittest.mock import Mock
        context=object();driver=Mock();driver.policy.active={'manifest':{'table_versions_json':'{"local.graph.object_current":2}'}}
        driver.transport.targets={'local.graph.object_current':SimpleNamespace(uuid='uuid')}
        @contextmanager
        def held(*args,**kwargs):yield
        driver.writer=held;driver.hold=held
        scope=AckScope('ashlar_ack_fixture','11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','consumer','feed','epoch')
        provider=PublicationProvider(driver,{},dict(scope=scope,source='ashlar_ack_source_fixture',signature='signature',role='ashlar_ack_operator_fixture'),b'request',b'manifest',{},context=context)
        provider._ack=Mock(side_effect=[{'session':{'backend_pid':'41'},'observation':{'position':'1'}},{'session':{'backend_pid':'41'},'observation':{'position':'1'}}])
        return provider,driver,context
    def interval_patches(self):
        from unittest.mock import patch,Mock
        from contextlib import ExitStack
        stack=ExitStack();connection=Mock()
        stack.enter_context(patch('ashlar.manifest.manifest_pin_vector',return_value='vector'))
        stack.enter_context(patch('local_outbox_connection.connect',return_value=connection))
        stack.enter_context(patch('postgres_transactions.Session',return_value='actual-session-port'))
        return stack,connection
    def test_complete_native_pin_and_pg_closure_precedes_immutable_custody(self):
        provider,driver,context=self.provider()
        with self.interval_patches()[0]:
            with provider.interval(context):
                self.assertTrue(provider.active)
                with self.assertRaises(PermissionError):provider.closed_interval_custody(context)
            receipt=provider.closed_interval_custody(context)
        self.assertEqual(provider._ack.call_count,2);self.assertEqual(receipt['opening_ack']['session']['backend_pid'],'41')
        receipt['closing_ack']['observation']['position']='forged'
        self.assertEqual(provider.closed_interval_custody(context)['closing_ack']['observation']['position'],'1')
        with self.assertRaises(PermissionError):provider.closed_interval_custody(object())
    def test_closing_whole_vector_or_pg_failure_discards_custody(self):
        from unittest.mock import Mock
        for failure in ('native-vector','pg-closing'):
            provider,driver,context=self.provider()
            if failure=='native-vector':
                @contextmanager
                def changed(*args,**kwargs):
                    yield
                    raise ValueError('closing whole-vector failure')
                driver.hold=changed
            else:provider._ack=Mock(side_effect=[{'observation':'opening'},ValueError('closing PG changed')])
            with self.interval_patches()[0]:
                with self.assertRaises(ValueError):
                    with provider.interval(context):pass
            with self.assertRaises(PermissionError):provider.closed_interval_custody(context)
    def test_stale_custody_is_reset_and_closed_reader_cannot_reenter(self):
        provider,driver,context=self.provider()
        with self.interval_patches()[0]:
            with provider.interval(context):pass
            provider._ack.side_effect=ValueError('new opening failed')
            with self.assertRaises(ValueError):
                with provider.interval(context):pass
            with self.assertRaises(PermissionError):provider.closed_interval_custody(context)
        provider._reader_closed=True
        with self.assertRaises(PermissionError):
            with provider.interval(context):pass
    def test_connection_cleanup_failure_withholds_buffered_rows_and_custody(self):
        from unittest.mock import Mock
        for cleanup in ('rollback','close'):
            provider,driver,context=self.provider();stack,connection=self.interval_patches()
            getattr(connection,cleanup).side_effect=ValueError('native '+cleanup+' failed')
            provider.resolve=Mock(return_value=SimpleNamespace(descriptor=SimpleNamespace(raw={'original':'manifest'}),snapshots={'node':('uuid',2)}))
            provider.admit_binding=Mock();provider.runtime=Mock()
            provider.sql=Mock(side_effect=[[{'violations':'0'}],[{'n':'2'}]])
            request,artifact=Tests().fixture()
            with stack:
                with self.assertRaises(ValueError):execute_guarded(provider,request,artifact,context=context)
            self.assertEqual(provider.sql.call_count,2)
            with self.assertRaises(PermissionError):provider.closed_interval_custody(context)
            connection.close.assert_called_once()
