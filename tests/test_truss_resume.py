import sqlite3
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_feed_sessions as cases
from ashlar_host.delta_custody import encoded
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_resume import resume_truss_publication


class Tests(unittest.TestCase):
    def test_original_coordinator_after_writer_close_and_advanced_refusal(self):
        source,request,context,events,_,*_=cases.Tests().setup_case()
        db=sqlite3.connect(':memory:')
        try:
            db.execute('CREATE TABLE local_source_plan(request_digest TEXT,original TEXT)')
            db.execute('CREATE TABLE local_publication_artifact(request_digest TEXT)')
            db.execute('INSERT INTO local_source_plan VALUES(?,?)',(request['request_digest'],encoded({'request':request})))
            db.execute('INSERT INTO local_publication_artifact VALUES(?)',(request['request_digest'],))
            driver=object.__new__(NativeDriver);driver.context=context;driver.source_sessions=source;driver.ack_sessions=source
            driver.tables={'attempts':'attempts'};driver.transport=SimpleNamespace(db=db,targets={'attempts':SimpleNamespace(table='attempts',uuid='uuid')})
            active=[]
            @contextmanager
            def writer(*args):
                self.assertFalse(active);active.append(True)
                try:yield
                finally:active.clear()
            driver.writer=writer
            record=SimpleNamespace(phase='committing',request_digest=request['request_digest'],payload_json=encoded({'request':request}))
            @contextmanager
            def session(*args):
                self.assertTrue(active);yield SimpleNamespace(read=lambda *args:[record])
            def restored(*args):self.assertTrue(active);events.append('restore')
            def publish(backend,stream,batch,**kwargs):
                self.assertFalse(active);self.assertEqual(stream,request['stream'])
                self.assertEqual(kwargs['predecessor'],request['predecessor']);events.append('coordinator');return 'descriptor'
            with patch('ashlar_host.truss_resume.DeltaAttemptStore',return_value=SimpleNamespace(session=session)), \
                 patch('ashlar_host.truss_resume.restore_applied_truss_publication',side_effect=restored), \
                 patch('ashlar_host.truss_resume.publish_truss_transaction',side_effect=publish), \
                 patch.object(type(source),'observe_checkpoint',return_value=source.page.prior.content):
                self.assertEqual(resume_truss_publication(driver,request['request_digest']),'descriptor')
                self.assertEqual(events[-2:],['restore','coordinator'])
                record.phase='prepared'
                db.execute('DELETE FROM local_publication_artifact')
                with patch('ashlar_host.truss_resume.restore_prepared_truss_publication',side_effect=restored),patch('ashlar_host.truss_resume.recover_interrupted_truss_apply',side_effect=AssertionError('Prepared must not recover effects')):
                    self.assertEqual(resume_truss_publication(driver,request['request_digest']),'descriptor')
                self.assertEqual(events[-2:],['restore','coordinator'])
                record.phase='committing'
            with patch('ashlar_host.truss_resume.DeltaAttemptStore',return_value=SimpleNamespace(session=session)),patch.object(type(source),'observe_checkpoint',return_value=source.page.next.content):
                with self.assertRaisesRegex(ValueError,'Advanced Truss checkpoint'):
                    resume_truss_publication(driver,request['request_digest'])
        finally:db.close()
