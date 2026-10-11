"""Generated schema custody and actual local owner checks; no native operation."""
import json,os,tempfile,sys
from pathlib import Path
from unittest import TestCase,mock
from ashlar_host.original_commerce_local_setup import generated_carriers,_identity

class Tests(TestCase):
    def test_complete_packaged_generated_schema_correspondence(self):
        generated=generated_carriers()
        root=Path(__file__).resolve().parents[1]
        expected=json.loads((root/'docs/helix/02-design/models/original-commerce-local-runtime/e-correspondence.json').read_bytes())
        self.assertEqual(set(generated['carriers']),set(expected['roles']))
        for role,carrier in generated['carriers'].items():
            self.assertEqual(json.loads(carrier['schemaJson']),expected['roles'][role]['schema'])
            self.assertEqual(carrier['layout']['name'],[role])
    def test_provenance_mutation_refuses_before_native_import(self):
        import ashlar_host.original_commerce_local_setup as owner
        with mock.patch.object(owner,'_PROVENANCE','0'*64):
            with self.assertRaises(ValueError):generated_carriers()
    def test_named_held_directory_substitution_and_mode_refuse(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve()/'original';root.mkdir(mode=0o700)
            fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
            try:
                _identity(root,fd,True,0o700)
                root.rename(root.with_name('retained'));root.mkdir(mode=0o700)
                with self.assertRaises(PermissionError):_identity(root,fd,True,0o700)
                self.assertTrue(root.exists());self.assertTrue(root.with_name('retained').exists())
            finally:os.close(fd)

class NativeCompositionTests(TestCase):
    def run_setup(self,*,cancel=None,replace_output=False,mutate_uuid=False,registered_drift=None,reopen_cancel=None):
        import types,uuid
        from contextlib import contextmanager
        import ashlar_host.original_commerce_local_setup as owner
        from ashlar_host.config import ProducerConfig
        from ashlar_host.original_commerce_local_configuration import OriginalCommerceLocalInitializationConfig,OriginalCommerceLocalRuntime
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);base=Path(temporary.name).resolve()
        config=OriginalCommerceLocalInitializationConfig(base/'native',base/'observations','closed-original',base/'model',base/'graph',base/'jars',ProducerConfig(base/'producer',base/'bun',base/'git',30,1024,4096),None,OriginalCommerceLocalRuntime(base/'python','1'*64,base/'java','2'*64,'3'*64))
        events=[];tables={};self.state=(config,base,events,tables)
        class Frame:
            def __init__(self,schema=None,path=None):self.document=schema;self.path=path;self.write=self;self.schema=types.SimpleNamespace(json=lambda:json.dumps(self.document))
            def format(self,name):self.asserted=name;return self
            def option(self,*args):return self
            def save(self,path):
                target=Path(path);target.mkdir(mode=0o700);tables[path]={'schema':self.document,'uuid':str(uuid.uuid4())};events.append(('create',target.name))
                if cancel is not None and len(tables)==3:raise cancel
            def load(self,path):return Frame(tables[path]['schema'],path)
            def limit(self,count):return self
            def count(self):return tables[self.path].get('count',0)if self.path else 0
        settings={'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true'}
        class Row:
            def __init__(self,value):self.value=value
            def asDict(self):return dict(self.value)
            def __getitem__(self,key):return self.value['version']if key==0 else self.value[key]
        class SQL:
            def __init__(self,statement):self.statement=statement
            def first(self):
                path=self.statement.split('`')[1];row=tables[path];events.append(('detail',Path(path).name))
                return types.SimpleNamespace(asDict=lambda:{'id':row['uuid'],'location':row.get('location',Path(path).as_uri()),'partitionColumns':[],'properties':row.get('properties',{}),'minReaderVersion':1,'minWriterVersion':2})
            def select(self,*args):return self
            def limit(self,count):return self
            def collect(self):
                path=self.statement.split('`')[1];row=tables[path]
                if self.statement.startswith('ALTER TABLE'):
                    import re
                    row['properties']=dict(re.findall("'([^']+)'='([^']+)'",self.statement))
                    row['history']=[{'version':1,'operation':'SET TBLPROPERTIES','userMetadata':settings['spark.databricks.delta.commitInfo.userMetadata']},{'version':0,'userMetadata':None}]
                    events.append(('native-register',Path(path).name));return []
                return [Row(value)for value in row.get('history',[{'version':0,'userMetadata':None}])]
        conf=types.SimpleNamespace(get=lambda key,default=None:settings.get(key,default),set=lambda key,value:settings.__setitem__(key,value),unset=lambda key:settings.pop(key,None))
        spark=types.SimpleNamespace(createDataFrame=lambda rows,schema:Frame(schema),sql=SQL,read=Frame(),conf=conf)
        @contextmanager
        def native(jars):
            try:yield spark
            finally:
                events.append(('spark-stop',))
                if replace_output:
                    config.output.rename(base/'retained-observations');config.output.mkdir(mode=0o700);(config.output/'foreign').write_bytes(b'untouched')
        actual_transport=owner.LocalDeltaTransport
        def initialize(spark,journal,installation,targets,policy,*,context,capacity):
            self.assertEqual(len(targets),6);self.assertEqual(len({t.uuid for t in targets}),6)
            transport=actual_transport.initialize(spark,journal,installation,targets,policy,context=context,capacity=capacity)
            events.append(('journal-initialize',))
            original_close=transport.close
            def close():events.append(('transport-close',));original_close()
            transport.close=close
            if mutate_uuid:tables[str(config.root/'manifest')]['uuid']=str(uuid.uuid4())
            if registered_drift is not None:registered_drift(tables)
            return transport
        def reopen(*args,**kwargs):
            events.append(('registered-owner-reopen',))
            if reopen_cancel is not None:raise reopen_cancel
            transport=actual_transport(*args,**kwargs);original_close=transport.close
            def close():events.append(('registered-owner-close',));original_close()
            transport.close=close;return transport
        types_module=types.ModuleType('pyspark.sql.types');types_module.StructType=types.SimpleNamespace(fromJson=lambda schema:schema)
        with mock.patch.dict('sys.modules',{'pyspark.sql.types':types_module}),mock.patch.object(owner,'runtime_paths',return_value=[]),mock.patch.object(owner,'original_inputs',return_value=(b'original-model',b'original-graph')),mock.patch.object(owner,'recompute_dataset',return_value={}),mock.patch.object(OriginalCommerceLocalRuntime,'renew',return_value=None),mock.patch.object(owner,'_native_session',side_effect=native),mock.patch.object(owner,'LocalDeltaTransport',side_effect=reopen)as transport_owner:
            transport_owner.initialize.side_effect=initialize
            result=owner.initialize_original_commerce_local(config)
        return result,events,config,base
    def test_real_owner_creates_all_generated_schemas_and_closes_before_handoff(self):
        result,events,config,base=self.run_setup()
        self.assertEqual(len(result['targets']),6);self.assertEqual([e[1]for e in events if e[0]=='create'],['object_current','edge_current','tombstone','whole_source_history','attempts','manifest'])
        self.assertLess(events.index(('transport-close',)),events.index(('spark-stop',)))
        self.assertTrue((config.output/'installation.json').is_file())
        self.assertEqual(sum(event[0]=='native-register'for event in events),6)
        self.assertIn(('registered-owner-reopen',),events);self.assertIn(('registered-owner-close',),events)
        for role in ['object_current','edge_current','tombstone','whole_source_history','attempts','manifest']:
            self.assertGreaterEqual(sum(e==('detail',role)for e in events),3)
    def test_cancel_identity_and_partial_installation_never_becomes_handoff(self):
        cancellation=KeyboardInterrupt('native body cancellation')
        with self.assertRaises(KeyboardInterrupt)as raised:self.run_setup(cancel=cancellation)
        self.assertIs(raised.exception,cancellation)
        config,base,events,tables=self.state
        self.assertFalse((config.output/'installation.json').exists());self.assertEqual(len(tables),3)
        self.assertIn(('spark-stop',),events);self.assertNotIn(('journal-initialize',),events)
    def test_output_replaced_during_spark_shutdown_refuses_and_preserves_foreign(self):
        with self.assertRaises(PermissionError):self.run_setup(replace_output=True)
        config,base,events,tables=self.state
        self.assertEqual((config.output/'foreign').read_bytes(),b'untouched')
        self.assertFalse((config.output/'installation.json').exists())
        self.assertTrue((base/'retained-observations').exists())
    def test_coordination_uuid_drift_closing_refuses_but_all_cleanup_runs(self):
        with self.assertRaises(ValueError):self.run_setup(mutate_uuid=True)
        config,base,events,tables=self.state
        self.assertFalse((config.output/'installation.json').exists())
        self.assertIn(('transport-close',),events);self.assertIn(('spark-stop',),events)
        for role in ['object_current','edge_current','tombstone','whole_source_history','attempts','manifest']:
            self.assertGreaterEqual(sum(e==('detail',role)for e in events),3)

    def test_registered_stage_refuses_exact_native_drift(self):
        def extra_history(tables):next(iter(tables.values()))['history'].insert(0,{'version':2,'operation':'WRITE','userMetadata':None})
        def extra_property(tables):next(iter(tables.values()))['properties']['foreign']='unadmitted'
        def changed_schema(tables):next(iter(tables.values()))['schema']={'type':'struct','fields':[]}
        def changed_location(tables):next(iter(tables.values()))['location']='file:///foreign'
        def nonempty(tables):next(iter(tables.values()))['count']=1
        def bad_metadata(tables):next(iter(tables.values()))['history'][0]['userMetadata']='{}'
        def boolean_version(tables):next(iter(tables.values()))['history'][0]['version']=True
        for drift in (extra_history,extra_property,changed_schema,changed_location,nonempty,bad_metadata,boolean_version):
            with self.subTest(drift=drift),self.assertRaises(ValueError):self.run_setup(registered_drift=drift)
            config,base,events,tables=self.state
            self.assertFalse((config.output/'installation.json').exists())
            self.assertIn(('registered-owner-reopen',),events);self.assertIn(('transport-close',),events);self.assertIn(('spark-stop',),events)
    def test_registered_owner_cancel_keeps_identity_and_attempts_all_six_witnesses(self):
        cancellation=KeyboardInterrupt('owned registration renew cancellation')
        def drift(tables):next(iter(tables.values()))['properties']['unadmitted']='value'
        with self.assertRaises(KeyboardInterrupt)as caught:self.run_setup(registered_drift=drift,reopen_cancel=cancellation)
        self.assertIs(caught.exception,cancellation)
        config,base,events,tables=self.state
        self.assertFalse((config.output/'installation.json').exists());self.assertIn(('transport-close',),events);self.assertIn(('spark-stop',),events)
        opening=events.index(('registered-owner-reopen',))
        self.assertEqual({event[1]for event in events[opening:]if event[0]=='detail'},set(owner_role for owner_role in ('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')))
    def test_actual_registered_owner_refuses_reservation_and_journal_drift(self):
        import sqlite3
        from ashlar_host.delta_custody import LocalDeltaError
        def reservation(tables):
            config,base,events,_=self.state
            (config.root/'manifest/.ashlar-local-installation-reservation.json').write_text('{}')
        def journal(tables):
            config,base,events,_=self.state
            connection=sqlite3.connect(str(config.root/'operations.sqlite'))
            try:
                with connection:connection.execute("UPDATE local_installation SET original='{}'")
            finally:connection.close()
        for drift in (reservation,journal):
            with self.subTest(drift=drift),self.assertRaises(LocalDeltaError):self.run_setup(registered_drift=drift)
            config,base,events,tables=self.state
            self.assertFalse((config.output/'installation.json').exists())
            self.assertIn(('registered-owner-reopen',),events);self.assertIn(('transport-close',),events);self.assertIn(('spark-stop',),events)

class OrdinaryTransportTests(TestCase):
    def selection(self):
        from ashlar_host.ack import AckScope
        from ashlar_host.original_commerce_local_configuration import OriginalCommerceLocalSource
        return OriginalCommerceLocalSource('127.0.0.1',25434,'truss_e2e','isolated-loopback-trust',AckScope('ashlar_ack_original','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002','consumer','private-original-commerce-fixture','original-commerce-v1'),'source_original','a'*64,'writer_original','reader_original','ack_original')
    def test_exact_ordinary_logins_do_not_consult_ambient_authentication(self):
        import types
        from ashlar_host.original_commerce_local_setup import original_commerce_local_source_ports
        calls=[];connection=object();module=types.ModuleType('psycopg');module.connect=lambda **kwargs:(calls.append(kwargs)or connection)
        with mock.patch.dict('sys.modules',{'psycopg':module}),mock.patch.dict(os.environ,{},clear=True):
            ports=original_commerce_local_source_ports(self.selection())
            self.assertIs(ports.writer_connection(object()),connection);self.assertIs(ports.registration.source.connection_factory(object()),connection);self.assertIs(ports.registration.metadata_factory(object()),connection)
        self.assertEqual([c['user']for c in calls],['writer_original','reader_original','ack_original'])
        for call in calls:
            self.assertEqual((call['host'],call['port'],call['dbname']),('127.0.0.1',25434,'truss_e2e'))
            self.assertEqual({k:call[k]for k in ('password','passfile','sslmode','gssencmode')},{'password':'','passfile':'/dev/null','sslmode':'disable','gssencmode':'disable'})
            self.assertEqual(call['hostaddr'],'127.0.0.1')
            self.assertEqual(call['connect_timeout'],5);self.assertFalse(call['autocommit'])
    def test_all_pg_environment_names_refuse_before_import_or_connection(self):
        import builtins
        from ashlar_host.original_commerce_local_setup import original_commerce_local_source_ports
        ports=original_commerce_local_source_ports(self.selection());original_import=builtins.__import__
        def guarded_import(name,*args,**kwargs):
            if name=='psycopg':raise AssertionError('Ambient configuration reached import')
            return original_import(name,*args,**kwargs)
        for name in ('PGHOSTADDR','PGSERVICE','PGSERVICEFILE','PGPASSWORD','PGPASSFILE','PGUNLISTEDFUTUREOPTION'):
            with mock.patch.dict(os.environ,{name:'secret sentinel never consumed'},clear=True),mock.patch('builtins.__import__',side_effect=guarded_import):
                with self.assertRaises(PermissionError)as raised:ports.writer_connection(object())
                self.assertNotIn('sentinel',str(raised.exception));self.assertNotIn(name,str(raised.exception))
    def test_endpoint_authentication_and_role_aliases_refuse(self):
        from dataclasses import replace
        selected=self.selection()
        for change in ({'host':'localhost'},{'host':'192.0.2.1'},{'port':True},{'authentication':'password'},{'database':'production'},{'writer_role':selected.reader_role}):
            with self.assertRaises(ValueError):replace(selected,**change)

class SparkSelectionTests(TestCase):
    def test_actual_session_builder_effective_configuration_and_existing_session_refusal(self):
        import types
        import ashlar_host.original_commerce_local_setup as owner
        configurations={};events=[]
        spark=mock.Mock();spark.version='3.5.3';spark.sparkContext.master='local[1]'
        spark.conf.get.side_effect=lambda key:configurations[key]
        spark.stop.side_effect=lambda:events.append('stop')
        builder=mock.Mock()
        builder.master.return_value=builder;builder.appName.return_value=builder
        def setting(key,value):configurations[key]=value;return builder
        builder.config.side_effect=setting;builder.getOrCreate.return_value=spark
        session=mock.Mock();session.builder=builder;session.getActiveSession.return_value=None
        module=types.ModuleType('pyspark.sql');module.SparkSession=session
        with mock.patch.dict(sys.modules,{'pyspark':types.ModuleType('pyspark'),'pyspark.sql':module}):
            with owner._native_session((Path('/selected.jar'),))as actual:self.assertIs(actual,spark)
            self.assertEqual(events,['stop']);self.assertEqual(configurations['spark.executorEnv.PYTHONDONTWRITEBYTECODE'],'1')
            session.getActiveSession.return_value=object();builder.getOrCreate.reset_mock()
            with self.assertRaises(PermissionError):
                with owner._native_session((Path('/selected.jar'),)):pass
            builder.getOrCreate.assert_not_called()
            session.getActiveSession.return_value=None;spark.sparkContext.master='local[2]'
            with self.assertRaises(PermissionError):
                with owner._native_session((Path('/selected.jar'),)):pass
            self.assertEqual(events,['stop','stop'])


class PublicationIdentityTests(TestCase):
    def test_original_handoff_cannot_adopt_older_source_ack_installation(self):
        import hashlib
        from ashlar_host.config import ProducerConfig
        from ashlar_host.original_commerce_publication import OriginalCommercePublicationConfig
        from ashlar_host.original_commerce_local_configuration import OriginalCommerceLocalPublicationConfig,OriginalCommerceLocalRuntime
        import ashlar_host.original_commerce_local_setup as owner
        selected=OrdinaryTransportTests().selection()
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();handoff=root/'installation.json'
            value={'profile':'ashlar-original-commerce-local-installation/0.1','installation_id':'new-private-installation',
                'journal':str(root/'operations.sqlite'),'targets':[{'table':'local.original.'+role,'path':str(root/role),'uuid':'actual-target-'+role}for role in owner._ROLES],
                'capacity':None,'generated_provenance_sha256':owner._PROVENANCE,'qualification':'local development'}
            raw=json.dumps(value).encode();handoff.write_bytes(raw)
            publication=OriginalCommercePublicationConfig(root/'publication',root/'model',root/'graph',root/'jars',ProducerConfig(root/'producer',root/'bun',root/'git',30,1024,4096))
            runtime=OriginalCommerceLocalRuntime(root/'python','1'*64,root/'java','2'*64,'3'*64)
            config=OriginalCommerceLocalPublicationConfig(handoff,hashlib.sha256(raw).hexdigest(),publication,selected,runtime)
            with mock.patch.object(OriginalCommerceLocalRuntime,'renew'),mock.patch('ashlar_host.original_commerce_publication.OriginalCommercePublicationOperator',side_effect=AssertionError('Operator acquired before installation match')),mock.patch.object(owner,'runtime_paths',side_effect=AssertionError('Native selection before installation match')):
                with self.assertRaisesRegex(ValueError,'installation and protected source scope'):owner.publish_existing_original_commerce_local(config)
            self.assertFalse(publication.output.exists());self.assertFalse((root/'operations.sqlite').exists())
