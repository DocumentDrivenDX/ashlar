# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small existing-owner continuation controls; no native engine/source launch."""
import copy
import hashlib
import json
import sqlite3
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_graph_admission as cases
from ashlar_host.delta_custody import encoded
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_continue import continue_truss_graph


class Tests(unittest.TestCase):
    def test_advancing_empty_interval_uses_durable_progress_publication(self):
        import base64
        from ashlar.apply import empty_state
        from test_truss_feed_sessions import Page,Cursor
        from ashlar_host.schema_rows import fixture_columns
        from ashlar_host.resources import RESOURCE_ROOT
        batch,catalogs=cases.Tests().original();prior=base64.b64decode(batch.cursor_after)
        before=json.loads(prior);after={**before,'nextXid':'50'}
        payload={'interfaceVersion':'truss-native-feed/0.1','prior':before,'next':after,'writerXid':None,
            'configuration':{},'catalogPrerequisites':[],'revisions':[],'changes':[],'sources':[],'reservations':[]}
        content=json.dumps(payload).encode();page=Page(content,hashlib.sha256(content).hexdigest(),Cursor(prior),
            Cursor(json.dumps(after).encode()),writer_xid=None,safe_watermark_xid='50',outcome='empty_interval')
        with TemporaryDirectory()as directory:
            root=Path(directory).resolve();db=sqlite3.connect(':memory:')
            try:
                db.execute('CREATE TABLE local_source_plan(request_digest TEXT,original TEXT)')
                context=object();source=object.__new__(TrussFeedSessions)
                object.__setattr__(source,'page',SimpleNamespace(next=Cursor(prior),consumer_id='consumer',registration_id='registration'))
                object.__setattr__(source,'feed','feed');object.__setattr__(source,'context',context)
                columns=fixture_columns(RESOURCE_ROOT);expected={role:[]for role in columns}
                driver=object.__new__(NativeDriver);driver.context=context;driver.source_admission=TrussGraphAdmission(batch,catalogs)
                driver.lookup_hash_profile='sha256-json-v1';driver.source_sessions=source;driver.ack_sessions=source;driver.columns=columns
                driver.policy=SimpleNamespace(active={'request':{'stream':'feed'},'manifest':{'schema_revisions_json':'{"installation":"3"}'},
                    'publication_id':'old','expected':expected,'progress':{'feed':{'epoch':'epoch','position':base64.b64encode(prior).decode()}}})
                driver.tables={role:'local.truss.'+role for role in (*columns,'attempts','manifest')}
                driver.transport=SimpleNamespace(db=db,journal_path=root/'operations.sqlite',targets={driver.tables['attempts']:SimpleNamespace(table=driver.tables['attempts'],uuid='uuid')})
                @contextmanager
                def writer(*args):yield
                driver.writer=writer
                admitted=SimpleNamespace(changes=(),revisions={},typed=True,parsed=SimpleNamespace(event_bytes=(),edge_order_keys=()),
                    admit=lambda change:None,admit_transition=lambda prior,change:None,current=True,current_truss_version='0.0.50')
                following=object.__new__(NativeDriver);following.transport=driver.transport;following.policy=driver.policy
                def published(backend,feed,original,**kwargs):
                    self.assertEqual(original.batch_id,'empty-interval:'+page.sha256)
                    self.assertEqual(json.loads(kwargs['schema_revisions_json']),{'installation':'3'})
                    self.assertEqual(following.policy.active['steps'],[])
                    self.assertEqual(following.policy.active['expected'],expected)
                    self.assertNotEqual(following.policy.active['progress']['feed']['position'],following.policy.active['progress']['feed']['previous'])
                    return SimpleNamespace(raw={'publication_id':following.policy.active['publication_id']})
                with patch('ashlar_host.truss_continue._ready',return_value=empty_state()),\
                     patch('ashlar_host.truss_continue.read_next_page',return_value=page),\
                     patch('ashlar_host.truss_feed_sessions._sdk',return_value=(None,None,None,None,Page,None)),\
                     patch('ashlar_host.truss_page_archive._types',return_value=(Page,Cursor)),\
                     patch('ashlar_host.truss_continue.TrussEmptyIntervalAdmission',return_value=admitted),\
                     patch.object(NativeDriver,'from_existing_truss',return_value=following),\
                     patch('ashlar_host.truss_continue.publish_truss_transaction',side_effect=published):
                    result=continue_truss_graph(driver,lambda:None)
                self.assertEqual(len(result['publications']),1)
            finally:db.close()

    def test_existing_owner_checks_and_empty_page_no_publication(self):
        batch,catalogs=cases.Tests().original();admission=TrussGraphAdmission(batch,catalogs)
        with TemporaryDirectory()as directory:
            root=Path(directory).resolve();db=sqlite3.connect(':memory:')
            try:
                db.execute('CREATE TABLE local_source_plan(request_digest TEXT,original TEXT)')
                db.execute('CREATE TABLE local_publication_artifact(request_digest TEXT,request TEXT,artifact TEXT)')
                context=object();driver=object.__new__(NativeDriver)
                driver.lookup_hash_profile='sha256-json-v1'
                source=object.__new__(TrussFeedSessions)
                cursor=json.loads(batch.begin)['next'];cursor_raw=json.dumps(cursor).encode()
                object.__setattr__(source,'page',SimpleNamespace(next=SimpleNamespace(content=cursor_raw),consumer_id='consumer',registration_id='registration'))
                object.__setattr__(source,'feed','feed');object.__setattr__(source,'context',context)
                driver.source_sessions=source;driver.ack_sessions=source;driver.source_admission=admission;driver.context=context
                driver.policy=SimpleNamespace(active={'request':{'stream':'feed'},'manifest':{'publication_id':'old','table_versions_json':'{}','schema_revisions_json':'{}'},
                    'expected':{'object_current':[],'edge_current':[],'tombstone':[],'whole_source_history':[]},'progress':{},'publication_id':'old'})
                driver.tables={'attempts':'local.truss.attempts'};pending=[]
                driver.transport=SimpleNamespace(db=db,journal_path=root/'operations.sqlite',targets={},query=lambda *args:SimpleNamespace(rows=pending))
                events=[]
                @contextmanager
                def writer(*args):
                    events.append('open')
                    try:yield
                    finally:events.append('close')
                driver.writer=writer
                page=SimpleNamespace(prior=SimpleNamespace(content=cursor_raw),next=SimpleNamespace(content=cursor_raw),consumer_id='consumer',registration_id='registration',outcome='empty_interval')
                with patch('ashlar_host.truss_continue.manifest_pin_vector',return_value=object()),patch('ashlar_host.truss_continue.admit_truss_ack_attempt'), \
                     patch.object(TrussFeedSessions,'observe_checkpoint',return_value=cursor_raw),patch('ashlar_host.truss_continue.read_next_page',return_value=page)as read, \
                     patch('ashlar_host.truss_continue.publish_truss_transaction')as publish:
                    result=continue_truss_graph(driver,lambda:None)
                    self.assertEqual(result['publications'],[]);publish.assert_not_called()
                    self.assertEqual(events,['open','close']);read.assert_called_once()
                    # Receipt mode cannot inherit a checkpoint advanced by plain ACK.
                    object.__setattr__(source,'destination_receipts',True)
                    driver.policy.active['manifest'].update(profile_version='ashlar-delta/0.3',schema_revisions_json='{}')
                    with patch.object(TrussFeedSessions,'adopt_ack_attempt')as adopt,patch.object(TrussFeedSessions,'reconcile',return_value=None)as reconcile:
                        read.reset_mock()
                        with self.assertRaisesRegex(ValueError,'destination receipt not retained'):
                            continue_truss_graph(driver,lambda:None)
                        read.assert_not_called();adopt.assert_called_once();reconcile.assert_called_once()
                        reconcile.return_value={'receipt_observed':True,'source_receipt_retained':True}
                        self.assertEqual(continue_truss_graph(driver,lambda:None)['publications'],[])
                    object.__setattr__(source,'destination_receipts',False)
                    pending.append({'batch_id':'unfinished'})
                    with self.assertRaisesRegex(ValueError,'Pending original native'):
                        continue_truss_graph(driver,lambda:None)
                    pending.clear()
                    with patch.object(TrussFeedSessions,'observe_checkpoint',return_value=b'foreign'):
                        with self.assertRaisesRegex(ValueError,'predecessor source checkpoint'):
                            continue_truss_graph(driver,lambda:None)
                    self.assertFalse(list(root.iterdir()))
            finally:db.close()

    def test_one_complete_native_page_plans_appends_and_persists_originals(self):
        import base64
        from ashlar.apply import empty_state
        from test_truss_feed_sessions import Page,Cursor
        batch,catalogs=cases.Tests().original()
        prior=base64.b64decode(batch.cursor_before);following=base64.b64decode(batch.cursor_after)
        page=Page(batch.begin,hashlib.sha256(batch.begin).hexdigest(),Cursor(prior),Cursor(following))
        admission=TrussGraphAdmission(batch,catalogs)
        with TemporaryDirectory()as directory:
            root=Path(directory).resolve();db=sqlite3.connect(':memory:')
            try:
                db.execute('CREATE TABLE local_source_plan(request_digest TEXT,original TEXT)')
                context=object();source=object.__new__(TrussFeedSessions)
                object.__setattr__(source,'page',SimpleNamespace(next=Cursor(prior),consumer_id=page.consumer_id,registration_id=page.registration_id))
                object.__setattr__(source,'feed','feed');object.__setattr__(source,'context',context)
                from ashlar_host.schema_rows import fixture_columns
                from ashlar_host.resources import RESOURCE_ROOT
                columns=fixture_columns(RESOURCE_ROOT);expected={role:[]for role in columns}
                driver=object.__new__(NativeDriver);driver.context=context;driver.source_admission=admission;driver.lookup_hash_profile='sha256-json-v1'
                driver.source_sessions=source;driver.ack_sessions=source;driver.columns=columns
                driver.policy=SimpleNamespace(active={'request':{'stream':'feed'},'manifest':{'schema_revisions_json':'{}'},'publication_id':'old','expected':expected,'progress':{}})
                driver.tables={role:'local.truss.'+role for role in (*columns,'attempts','manifest')}
                driver.transport=SimpleNamespace(db=db,journal_path=root/'operations.sqlite',targets={driver.tables['attempts']:SimpleNamespace(table=driver.tables['attempts'],uuid='original-uuid')})
                life=[]
                @contextmanager
                def writer(*args):
                    life.append('open')
                    try:yield
                    finally:life.append('close')
                driver.writer=writer
                captured=[]
                def existing(transport,policy,supplied,tables,changes,selected_columns,**ports):
                    self.assertIs(transport,driver.transport);self.assertIs(supplied,context)
                    self.assertIs(ports['source_sessions'],ports['ack_sessions'])
                    new=object.__new__(NativeDriver);new.context=context;new.transport=transport;new.policy=policy
                    captured.append(new);return new
                def publish(backend,feed,original,**kwargs):
                    self.assertEqual(life,['open','close']);self.assertEqual(original.begin,batch.begin)
                    active=captured[0].policy.active
                    self.assertEqual(len(active['expected']['object_current']),2)
                    self.assertEqual(len(active['expected']['edge_current']),1)
                    self.assertEqual(len(active['expected']['whole_source_history']),3)
                    self.assertEqual(kwargs['predecessor'],'old')
                    return SimpleNamespace(raw={'publication_id':active['publication_id']})
                initial_active=copy.deepcopy(driver.policy.active)
                with patch('ashlar_host.truss_continue._ready',return_value=empty_state()),patch('ashlar_host.truss_continue.read_next_page',return_value=page), \
                     patch('ashlar_host.truss_feed_sessions._sdk',return_value=(None,None,None,None,Page,None)), \
                     patch('ashlar_host.truss_page_archive._types',return_value=(Page,Cursor)), \
                     patch('ashlar_host.truss_catalog_reader.read_graph_catalog_evidence',return_value=catalogs), \
                     patch.object(NativeDriver,'from_existing_truss',side_effect=existing),patch('ashlar_host.truss_continue.publish_truss_transaction',side_effect=publish):
                    report=continue_truss_graph(driver,lambda:None)
                self.assertEqual(len(report['publications']),1)
                selected=report['publications'][0]
                intent=json.loads(Path(selected['publication_intent']['path']).read_bytes())
                self.assertEqual(intent['publication_id'],'truss-graph-'+intent['request_digest'])
                self.assertEqual(hashlib.sha256(Path(selected['source_page']['path']).read_bytes()).hexdigest(),selected['source_page']['sha256'])
                self.assertEqual(Path(selected['source_page']['path']).stat().st_mode&0o777,0o600)
                second=root/'failed';second.mkdir();driver.transport.journal_path=second/'operations.sqlite'
                driver.policy.active=initial_active
                failure=OSError('private native failure')
                with patch('ashlar_host.truss_continue._ready',return_value=empty_state()),patch('ashlar_host.truss_continue.read_next_page',return_value=page), \
                     patch('ashlar_host.truss_feed_sessions._sdk',return_value=(None,None,None,None,Page,None)), \
                     patch('ashlar_host.truss_page_archive._types',return_value=(Page,Cursor)), \
                     patch('ashlar_host.truss_catalog_reader.read_graph_catalog_evidence',return_value=catalogs), \
                     patch.object(NativeDriver,'from_existing_truss',side_effect=existing),patch('ashlar_host.truss_continue.publish_truss_transaction',side_effect=failure):
                    with self.assertRaises(OSError)as raised:continue_truss_graph(driver,lambda:None)
                self.assertIs(raised.exception,failure)
                capture=failure.ashlar_truss_continuation_interruption
                retained=Path(capture['publication_intent']['path'])
                self.assertEqual(hashlib.sha256(retained.read_bytes()).hexdigest(),capture['publication_intent']['sha256'])
                self.assertEqual(capture['outcome'],'unconfirmed')

            finally:db.close()
