# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual read-only opener/constructor dispatch; lower Spark frames are inert."""
import hashlib,unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_read_only_capacity as fixture
from ashlar_host.delta_custody import LocalDeltaTransport,LocalDeltaError,encoded
from ashlar_host.read_only_transport import ReadOnlyTransport,ReadOnlyDeltaTransport
from ashlar_host.evolution_verification_types import VerificationBounds

class Tests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixture.Tests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.bounds=VerificationBounds(262144,10000,1048576,16777216,1048576,1048576)
        self.calls=[]
    def spark(self):
        def sql(statement):
            target=next(t for t in self.fixture.targets if str(t.path)in statement)
            detail={'id':target.uuid,'properties':{'ashlar.local.installation.id':self.fixture.installation,'ashlar.local.registry.sha256':hashlib.sha256(encoded(self.fixture.body).encode()).hexdigest(),'ashlar.local.journal.sha256':hashlib.sha256(str(self.fixture.journal).encode()).hexdigest()}}
            self.calls.append(target.table)
            return SimpleNamespace(detail=detail,first=lambda:(_ for _ in ()).throw(AssertionError('Unbounded first refused')))
        return SimpleNamespace(conf=SimpleNamespace(get=lambda key:{'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true'}[key]),sql=sql)
    def open(self,bounds):
        f=self.fixture
        return ReadOnlyTransport.open(self.spark(),f.journal,f.installation,f.targets,SimpleNamespace(writer=lambda *a:None,admit=lambda *a:None),capacity=f.body['operation_capacity'],verification_bounds=bounds)
    def test_actual_constructor_all_six_details_bounded_and_read_only(self):
        observed=[];before=self.fixture.journal.read_bytes();bounds=self.bounds
        def capture(owner,frame,*,maximum_rows=None):
            self.assertIs(owner.observation_bounds,bounds);self.assertEqual(maximum_rows,1)
            observed.append(frame.detail['id']);return [frame.detail]
        with patch.object(LocalDeltaTransport,'_bounded_native_rows',capture):transport=self.open(bounds)
        try:
            self.assertIs(type(transport),ReadOnlyDeltaTransport);self.assertIsNone(transport.observation_bounds)
            self.assertEqual(observed,[t.uuid for t in self.fixture.targets])
            with self.assertRaises(LocalDeltaError):transport.mutation(None)
            with self.assertRaises(LocalDeltaError):transport.recover(None)
            with self.assertRaises(Exception):transport.db.execute('CREATE TABLE forbidden(id INTEGER)')
            with transport.bounded_observations(bounds):self.assertIs(transport.observation_bounds,bounds)
            self.assertIsNone(transport.observation_bounds)
        finally:transport.close()
        self.assertEqual(before,self.fixture.journal.read_bytes())
    def test_bound_refusal_and_cancellation_clear_constructor_interval(self):
        for error in [LocalDeltaError('pretransfer limit'),KeyboardInterrupt()]:
            objects=[]
            def capture(owner,frame,*,maximum_rows=None):objects.append(owner);raise error
            with patch.object(LocalDeltaTransport,'_bounded_native_rows',capture):
                with self.assertRaises(type(error))as caught:self.open(self.bounds)
            self.assertIs(caught.exception,error);self.assertEqual(len(self.calls),1);self.calls=[]
            self.assertIsNone(objects[0].observation_bounds)
            self.assertFalse(hasattr(objects[0],'db'))
    def test_wrong_nominal_bounds_refuse_before_native_reads(self):
        for bounds in [object(),SimpleNamespace(**vars(self.bounds))]:
            with self.assertRaises(LocalDeltaError):self.open(bounds)
        self.assertEqual(self.calls,[])

if __name__=='__main__':unittest.main()
