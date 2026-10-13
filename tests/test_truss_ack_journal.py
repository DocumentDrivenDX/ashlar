"""Small durable intent/native correspondence control; no source ACK execution."""
import sqlite3
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_feed_sessions as cases
from ashlar_host.delta_custody import encoded
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_ack_journal import retain_truss_ack_attempt


class Tests(unittest.TestCase):
    def test_fresh_owner_adopts_only_original_committed_ack_intent(self):
        source,request,context,*_=cases.Tests().setup_case()
        db=sqlite3.connect(':memory:')
        try:
            db.execute('CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT,digest TEXT)')
            driver=object.__new__(NativeDriver);driver.context=context;driver.held=True
            driver.source_sessions=source;driver.ack_sessions=source;driver.tables={'attempts':'attempts'}
            driver.transport=SimpleNamespace(db=db,targets={'attempts':SimpleNamespace(table='attempts',uuid='uuid')})
            calls=[];driver.source_admit=lambda request:calls.append('source')
            driver.validate_manifest=lambda *args:calls.append('native')
            manifest={'publication_id':'original'}
            vector=SimpleNamespace(authority='private-local-process',scope_kind='manifest',scope_id='original',
                custody_digest='a'*64,targets={'nodes':('uuid',1)})
            record=SimpleNamespace(phase='committed',request_digest=request['request_digest'],
                payload_json=encoded({'request':request,'descriptor_json':encoded(manifest)}))
            @contextmanager
            def session(*args):yield SimpleNamespace(read=lambda *args:[record])
            with patch('ashlar_host.truss_ack_journal.DeltaAttemptStore',return_value=SimpleNamespace(session=session)),patch.object(type(source),'observe_checkpoint',return_value=source.page.next.content):
                with self.assertRaisesRegex(ValueError,'durable Truss ACK attempt'):
                    source.adopt_ack_attempt(driver,request,manifest,vector)
                self.assertIsNone(source._pending_checkpoint)
                retain_truss_ack_attempt(driver,request,manifest,vector)
                source.adopt_ack_attempt(driver,request,manifest,vector)
                self.assertEqual(calls,['source','native'])
                self.assertEqual(source._pending_checkpoint,source.page.next.content)
                with self.assertRaisesRegex(ValueError,'ACK intent differs'):
                    retain_truss_ack_attempt(driver,request,{'publication_id':'changed'},vector)
        finally:db.close()
