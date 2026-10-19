# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure setup composition and owned cleanup controls; no container or database."""
from dataclasses import replace
import hashlib
import json
import os
import re
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch,MagicMock
from ashlar_host.ack import AckScope
from ashlar_host.resources import RESOURCE_ROOT
import setup_private_outbox_fixture as owner


def source(label,ordinal):
    return owner.Source(AckScope('ashlar_ack_service_'+label,
        '00000000-0000-0000-0000-'+str(ordinal).zfill(12),
        '10000000-0000-0000-0000-'+str(ordinal).zfill(12),'private-fixture',label,'original-epoch'),
        'ashlar_ack_source_'+label,'ashlar_ack_writer_'+label,'ashlar_ack_reader_'+label,'ashlar_ack_role_'+label)


class Tests(unittest.TestCase):
    def test_packaged_native_source_namespace_predicate_matches_explicit_identifier_owner(self):
        packaged=(RESOURCE_ROOT/'sql/ashlar-source-ack/01-postgresql.sql').read_text()
        original=(Path(__file__).resolve().parents[1]/'sql/ashlar-source-ack/01-postgresql.sql').read_text()
        self.assertEqual(packaged,original)
        rendered=owner.render_ddl('ashlar_ack_medical','medical_ack')
        predicate=re.search(r"IF source_namespace::text !~ '([^']+)' THEN RAISE EXCEPTION 'ACK_SOURCE_NAMESPACE_UNSUPPORTED'",rendered).group(1)
        self.assertEqual(predicate,'^[a-z][a-z0-9_]{0,62}$')
        for name in ('ashlar_medical_source','ashlar_ack_source_old','source','a'*63):
            self.assertIsNotNone(re.fullmatch(predicate,name));owner.identifier(name)
        for name in ('','A','x.y','a;DROP','a"b','a'*64,'x\n','_source'):
            self.assertIsNone(re.fullmatch(predicate,name))
            with self.assertRaises(ValueError):owner.identifier(name)
        # Catalog/physical/append ownership and signature content remain present;
        # safe spelling does not confer registration or ordinary session rights.
        for gate in ('ACK_SOURCE_OWNER_OR_KIND_UNSUPPORTED','ACK_SOURCE_SCHEMA_UNSUPPORTED',
                'pg_get_constraintdef','pg_get_triggerdef','prosecdef','relrowsecurity'):
            self.assertIn(gate,rendered)

    def test_public_ack_renderer_preserves_old_bytes_and_quotes_explicit_roles(self):
        original=(RESOURCE_ROOT/'sql/ashlar-source-ack/01-postgresql.sql').read_text()
        self.assertEqual(owner.render_ddl('ashlar_ack_test','ashlar_ack_role'),
            original.replace('__ACK_SCHEMA__','ashlar_ack_test').replace('__ACK_ROLE__','ashlar_ack_role'))
        actual=owner.render_ddl('ashlar_ack_medical','medical_ack')
        expected=original.replace('__ACK_SCHEMA__','ashlar_ack_medical').replace("'__ACK_ROLE__'","'medical_ack'").replace('__ACK_ROLE__','"medical_ack"')
        self.assertEqual(actual,expected)
        self.assertIn('CREATE ROLE "medical_ack" NOLOGIN;',actual)
        self.assertIn("pg_has_role(session_user,'medical_ack','MEMBER')",actual)
        for namespace,role in [('public','medical_ack'),('ashlar_ack_test','bad;DROP'),('ashlar_ack_test','x"y'),('ashlar_ack_test','A'),('ashlar_ack_test','a'*64)]:
            with self.subTest(namespace=namespace,role=role),self.assertRaises(ValueError):owner.render_ddl(namespace,role)

    def test_one_medical_source_exact_names_uses_existing_privilege_and_inventory_owner(self):
        medical=owner.Source(AckScope('ashlar_ack_medical',
            '00000000-0000-0000-0000-000000000003',
            '10000000-0000-0000-0000-000000000003','original-medical',
            'private-original-medical-fixture','original-medical-v1'),
            'ashlar_medical_source','medical_writer','medical_reader','medical_ack')
        self.settings=replace(self.settings,sources=(medical,),port=25436,database='original_medical')
        result=owner.setup(self.settings,self.runtime)
        self.assertEqual(len(result['sources']),1)
        self.assertEqual(result['sources'][0]['scope'],owner.asdict(medical.scope))
        self.assertEqual(result['sources'][0]['binding']['source_schema'],medical.schema)
        self.assertEqual(len(result['ordinary_sessions']),3)
        self.assertEqual(len(result['ddl_sha256']),3)
        self.assertTrue(self.admin_closed)
        outbox,ack,login=owner.ddl(medical)
        self.assertIn('medical_writer',outbox);self.assertIn('medical_ack',ack)
        self.assertNotIn('GRANT',login)
        self.assertIn('NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',login)

    def test_cardinality_and_explicit_identifier_collisions_refuse_before_effects(self):
        for sources in ((),self.settings.sources+(source('c',3),)):
            with self.subTest(sources=sources),self.assertRaises(ValueError):replace(self.settings,sources=sources)
        for name in ('bad; DROP ROLE x','Capital','x.y','a'*64,''):
            for field in ('schema','writer','reader','ack'):
                with self.subTest(name=name,field=field),self.assertRaises(ValueError):replace(self.settings.sources[0],**{field:name})
        with self.assertRaises(ValueError):replace(self.settings.sources[0],writer=self.settings.sources[0].reader)
        with self.assertRaises(ValueError):replace(self.settings,sources=(self.settings.sources[0],replace(self.settings.sources[1],writer=self.settings.sources[0].writer)))
        self.assertEqual(self.commands,[]);self.assertFalse(self.settings.output.exists())

    def test_public_runtime_connect_selects_local_trust_without_ambient_credentials(self):
        connection=object()
        connect=MagicMock(return_value=connection)
        ambient={'PGPASSWORD':'ambient-password','PGPASSFILE':'/ambient/passfile',
            'PGSSLMODE':'require','PGGSSENCMODE':'require','PGOPTIONS':'-c statement_timeout=0',
            'PGHOST':'remote.example','PGPORT':'9999','PGUSER':'ambient-role','PGDATABASE':'ambient-db'}
        with patch.dict(os.environ,ambient),patch.dict('sys.modules',{'psycopg':SimpleNamespace(connect=connect)}):
            actual=owner.Runtime().connect(self.settings,self.settings.sources[0].reader)
        self.assertIs(actual,connection)
        connect.assert_called_once_with(host='127.0.0.1',port=25432,dbname='private_outbox_test',
            user='ashlar_ack_reader_a',password='',passfile='/dev/null',sslmode='disable',
            gssencmode='disable',connect_timeout=2,
            options='-c statement_timeout=5000 -c lock_timeout=1000')

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        self.settings=owner.Settings(Path('/explicit/docker'),'unix:///explicit/docker.sock',owner.IMAGE,'ashlar-private-outbox-test',25432,
            'private_outbox_test','isolated-loopback-trust',(source('a',1),source('b',2)),self.root/'output')
        self.runtime=owner.Runtime();self.events=[];self.commands=[];self.admin_closed=False
        outer=self
        class Result:
            def __init__(self,value):self.value=value
            def fetchone(self):return self.value
        class Connection:
            autocommit=False
            def __init__(self,role):self.role=role
            def execute(self,sql,parameters=None):
                outer.events.append(('sql',self.role,sql,parameters))
                if 'rolcanlogin' in sql and self.role!='postgres':return Result((self.role,self.role,outer.settings.database,True,False,False,False,False,False))
                if sql=='SELECT current_user,session_user,current_database()':return Result((self.role,self.role,outer.settings.database))
                if 'server_version' in sql:return Result(('17.9',))
                if 'pg_catalog.pg_class' in sql:return Result((0,))
                if '.scope_binding(' in sql and self.role!='postgres':
                    selected=next(s for s in outer.settings.sources if s.ack==self.role)
                    value=owner.asdict(selected.scope);value.pop('service_schema')
                    value.update(source_schema=selected.schema,source_signature_sha256='a'*64)
                    return Result((value,))
                if 'position::text' in sql:return Result(('0',))
                return Result((0,))
            def commit(self):outer.events.append(('commit',self.role))
            def rollback(self):outer.events.append(('rollback',self.role))
            def close(self):
                outer.events.append(('close',self.role))
                if self.role=='postgres':outer.admin_closed=True
        def connect(settings,role):
            if role!='postgres':self.assertTrue(self.admin_closed)
            self.events.append(('connect',role));return Connection(role)
        def command(argv,timeout=30):
            self.commands.append(argv)
            self.assertEqual(argv[:3],['/explicit/docker','--host','unix:///explicit/docker.sock'])
            if argv[3:5]==['image','inspect']:
                return json.dumps({'digests':[owner.IMAGE],'architecture':'arm64','os':'linux'}).encode()
            if argv[3]=='run':return b'c'*64+b'\n'
            if argv[3]=='rm':return b'c'*64+b'\n'
            raise AssertionError('Unexpected fixture command')
        self.runtime.command=command;self.runtime.connect=connect

    def test_packaged_ddl_bytes_only_rename_original_closed_identifiers(self):
        selected=self.settings.sources[0]
        outbox,ack,login=owner.ddl(selected)
        original=(RESOURCE_ROOT/'sql/ashlar-outbox/01-postgresql.sql').read_text()
        self.assertEqual(outbox,original.replace('ashlar_outbox_writer',selected.writer).replace('ashlar_outbox_reader',selected.reader).replace('ashlar_outbox',selected.schema))
        self.assertEqual(ack,owner.render_ddl(selected.scope.service_schema,selected.ack))
        self.assertEqual(login,'\n'.join('ALTER ROLE '+role+' LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;' for role in (selected.writer,selected.reader,selected.ack)))
        self.assertNotIn('GRANT',login)

    def test_complete_setup_closes_admin_before_ordinary_observation_and_retains_owned_handle(self):
        result=owner.setup(self.settings,self.runtime)
        self.assertEqual([s['scope']['feed']for s in result['sources']],['a','b'])
        self.assertEqual(result['cleanup_argv'],['/explicit/docker','--host','unix:///explicit/docker.sock','rm','--force','--volumes','c'*64])
        self.assertEqual(json.loads((self.settings.output/'inventory.json').read_bytes()),result)
        self.assertNotIn('rm',[a[3] for a in self.commands])
        create=self.commands[1]
        self.assertIn('--pull=never',create);self.assertIn('127.0.0.1:25432:5432',create)
        self.assertEqual(create[create.index('--cpus')+1],'1');self.assertEqual(create[create.index('--memory')+1],'512m')
        self.assertEqual(result['docker_host'],'unix:///explicit/docker.sock')
        self.assertNotIn('POSTGRES_PASSWORD',' '.join(create));self.assertNotIn('Config.Env',' '.join(self.commands[0]))
        self.assertTrue(self.admin_closed)
        statements=[e[2] for e in self.events if e[0]=='sql' and e[1]=='postgres']
        for s in self.settings.sources:
            login=owner.ddl(s)[2]
            self.assertLess(statements.index(login),next(i for i,v in enumerate(statements)if v.startswith('SELECT '+s.scope.service_schema+'.register_source(')))
        self.assertEqual(len(result['ddl_sha256']),6)
        self.assertIn('not remotely authenticated',result['qualification'])

    def test_exact_config_refuses_before_runtime_or_directory_effects(self):
        for change in ({'port':True},{'port':25432.0},{'image':'postgres:latest'},
                {'authentication':'trust'},{'docker_host':'tcp://127.0.0.1:2375'},{'docker_host':'ssh://remote'},{'database':'postgres'},{'sources':(self.settings.sources[0],)*2}):
            with self.subTest(change=change),self.assertRaises(ValueError):replace(self.settings,**change)
        self.assertEqual(self.commands,[]);self.assertFalse(self.settings.output.exists())

    def test_existing_output_refuses_without_inspecting_or_mutating_resources(self):
        self.settings.output.mkdir();foreign=self.settings.output/'foreign';foreign.write_bytes(b'original')
        with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertEqual(self.commands,[]);self.assertEqual(foreign.read_bytes(),b'original')

    def test_original_cancellation_survives_admin_close_failure_and_owned_removal(self):
        primary=KeyboardInterrupt();closing=SystemExit()
        class Connection:
            autocommit=False
            def execute(self,*args):raise primary
            def rollback(self):pass
            def close(self):raise closing
        self.runtime.connect=lambda *args:Connection()
        with self.assertRaises(KeyboardInterrupt)as caught:owner.setup(self.settings,self.runtime)
        self.assertIs(caught.exception,primary)
        self.assertEqual(self.commands[-1],['/explicit/docker','--host','unix:///explicit/docker.sock','rm','--force','--volumes','c'*64])
        self.assertFalse((self.settings.output/'inventory.json').exists())
        self.assertEqual(json.loads((self.settings.output/'created-container.json').read_bytes())['container_id'],'c'*64)

    def test_create_failure_cannot_remove_an_existing_named_container(self):
        old=self.runtime.command
        def command(argv,timeout=30):
            if argv[3]=='run':raise ValueError('name already exists')
            return old(argv,timeout)
        self.runtime.command=command
        with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertEqual(len(self.commands),1)
        self.assertFalse((self.settings.output/'created-container.json').exists())

    def test_ordinary_privilege_substitution_refuses_after_admin_closed(self):
        old=self.runtime.connect
        def connect(settings,role):
            c=old(settings,role);execute=c.execute
            def selected(sql,parameters=None):
                if 'rolcanlogin' in sql:
                    class Result:
                        def fetchone(self):return (role,role,settings.database,True,True,False,False,False,False)
                    return Result()
                return execute(sql,parameters)
            c.execute=selected;return c
        self.runtime.connect=connect
        with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertTrue(self.admin_closed)
        self.assertFalse((self.settings.output/'inventory.json').exists())
        self.assertEqual(self.commands[-1][-1],'c'*64)

    def test_output_replacement_preserves_foreign_path_and_refuses_release(self):
        old=self.runtime.connect;moved=self.root/'retained-original'
        def connect(settings,role):
            if role!='postgres' and not moved.exists():
                settings.output.rename(moved);settings.output.mkdir()
                (settings.output/'foreign').write_bytes(b'foreign')
            return old(settings,role)
        self.runtime.connect=connect
        with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertEqual((self.settings.output/'foreign').read_bytes(),b'foreign')
        self.assertFalse((self.settings.output/'inventory.json').exists())
        self.assertTrue((moved/'created-container.json').exists())
        self.assertEqual(self.commands[-1][-1],'c'*64)

    def test_selector_construction_cancellation_reaps_child_and_closes_both_pipes(self):
        primary=KeyboardInterrupt();child=MagicMock();child.poll.return_value=None;child.pid=12345
        child.stdout.close.side_effect=ValueError('ordinary close')
        with patch.object(owner.subprocess,'Popen',return_value=child),patch.object(owner.selectors,'DefaultSelector',side_effect=primary),patch.object(owner.os,'killpg') as kill:
            with self.assertRaises(KeyboardInterrupt) as caught:owner.Runtime().command(['/unused/explicit/docker'])
        self.assertIs(caught.exception,primary)
        kill.assert_called_once_with(12345,owner.signal.SIGKILL)
        child.wait.assert_called_once_with(timeout=5)
        child.stdout.close.assert_called_once();child.stderr.close.assert_called_once()

    def test_image_inspection_parent_replacement_refuses_before_mkdir_or_run(self):
        import shutil
        moved=self.root.parent/(self.root.name+'-original')
        self.addCleanup(lambda:shutil.rmtree(moved) if moved.exists() else None)
        old=self.runtime.command
        def command(argv,timeout=30):
            value=old(argv,timeout)
            if argv[3:5]==['image','inspect']:
                self.root.rename(moved);self.root.mkdir();(self.root/'foreign').write_bytes(b'foreign-parent')
            return value
        self.runtime.command=command
        with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertEqual((self.root/'foreign').read_bytes(),b'foreign-parent')
        self.assertFalse(self.settings.output.exists());self.assertEqual(len(self.commands),1)

    def test_final_inventory_write_replacement_refuses_and_removes_owned_container(self):
        moved=self.root/'retained-final';write=owner.write_inventory
        def retain(directory_fd,name,value):
            write(directory_fd,name,value)
            if name=='inventory.json':
                self.settings.output.rename(moved);self.settings.output.mkdir()
                (self.settings.output/'foreign').write_bytes(b'foreign-final')
        with patch.object(owner,'write_inventory',side_effect=retain):
            with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertEqual((self.settings.output/'foreign').read_bytes(),b'foreign-final')
        self.assertFalse((self.settings.output/'inventory.json').exists())
        self.assertTrue((moved/'inventory.json').is_file())
        self.assertEqual(self.commands[-1],['/explicit/docker','--host','unix:///explicit/docker.sock','rm','--force','--volumes','c'*64])

    def test_final_directory_fsync_parent_replacement_refuses_release(self):
        import shutil
        moved=self.root.parent/(self.root.name+'-final-original')
        self.addCleanup(lambda:shutil.rmtree(moved) if moved.exists() else None)
        fsync=owner.os.fsync
        def sync(fd):
            fsync(fd)
            info=owner.os.fstat(fd)
            if owner.stat.S_ISDIR(info.st_mode) and (self.settings.output/'inventory.json').exists() and not moved.exists():
                self.root.rename(moved);self.root.mkdir();self.settings.output.mkdir()
                (self.settings.output/'foreign').write_bytes(b'foreign-parent-final')
        with patch.object(owner.os,'fsync',side_effect=sync):
            with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertEqual((self.settings.output/'foreign').read_bytes(),b'foreign-parent-final')
        self.assertTrue((moved/'output'/'inventory.json').is_file())
        self.assertEqual(self.commands[-1][-1],'c'*64)

    def test_created_directory_replacement_before_open_cannot_be_adopted(self):
        original_open=owner.os.open;moved=self.root/'retained-created'
        def open_selected(path,flags,*args,**kwargs):
            if path==self.settings.output.name and flags & owner.os.O_DIRECTORY and 'dir_fd' in kwargs:
                self.settings.output.rename(moved);self.settings.output.mkdir(mode=0o700)
                (self.settings.output/'foreign').write_bytes(b'foreign-created')
            return original_open(path,flags,*args,**kwargs)
        with patch.object(owner.os,'open',side_effect=open_selected):
            with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertEqual((self.settings.output/'foreign').read_bytes(),b'foreign-created')
        self.assertTrue(moved.is_dir());self.assertEqual(len(self.commands),1)
        self.assertFalse((self.settings.output/'created-container.json').exists())

    def test_nonempty_database_refuses_and_cleans_only_created_container(self):
        old=self.runtime.connect
        def connect(settings,role):
            c=old(settings,role);execute=c.execute
            def selected(sql,parameters=None):
                if 'pg_catalog.pg_class' in sql:
                    class Result:
                        def fetchone(self):return (1,)
                    return Result()
                return execute(sql,parameters)
            c.execute=selected;return c
        self.runtime.connect=connect
        with self.assertRaises(ValueError):owner.setup(self.settings,self.runtime)
        self.assertTrue(self.admin_closed)
        self.assertEqual(self.commands[-1][-1],'c'*64)
        self.assertFalse((self.settings.output/'inventory.json').exists())
