# SPDX-License-Identifier: MIT OR Apache-2.0
import json
import unittest
from types import SimpleNamespace
from types import MappingProxyType
from unittest.mock import patch
from ashlar_host.driver import NativeDriver
from ashlar_host.delta_custody import LocalDeltaEffects, LocalDeltaError
from ashlar_host.truss_backend import TrussPublisherBackend
from ashlar.stored_publisher import StoredPublisherBackend


class Tests(unittest.TestCase):
    def test_coordinator_immutable_request_is_normalized_for_original_json(self):
        request=MappingProxyType({'request_digest':'d','batch_id':'b','stream':'feed',
            'source_checkpoint_json':'{}','schema_revisions_json':'{}','predecessor':'original'})
        received=[]
        def retain(value, context):
            self.assertIs(type(value),dict)
            self.assertEqual(value,dict(request))
            json.dumps(value)
            received.append(value)
        backend=TrussPublisherBackend(None,SimpleNamespace(prepare_truss_apply=retain),None,context=object())
        with patch.object(StoredPublisherBackend,'observe',return_value=None),patch.object(StoredPublisherBackend,'prepare',return_value='prepared'):
            self.assertEqual(backend.prepare('feed',request),'prepared')
        self.assertEqual(len(received),1)

    def test_prepare_precedes_phase_and_prior_drift_refuses(self):
        calls=[];context=object();request={'request_digest':'d','batch_id':'b'}
        driver=object.__new__(NativeDriver)
        driver.require=lambda supplied:None
        driver.admission_facts=lambda:{'profile':'ashlar-truss-scalar-graph-semantic-admission/0.1'}
        driver.policy=SimpleNamespace(active={'request':request,'steps':[],'previous_expected':{'object_current':[]}})
        driver.tables={'object_current':'objects'}
        driver.source_admit=lambda selected:calls.append('source')
        plan={'observed_native_prior':{'objects':{'uuid':'u','version':2}}}
        driver._apply_source_plan=lambda *args:json.dumps(plan)
        driver.transport=SimpleNamespace(targets={'objects':SimpleNamespace(uuid='u')},
            _detail=lambda target:None,original_history=lambda target:[{'version':2}])
        backend=TrussPublisherBackend(None,driver,None,context=context)
        with self.assertRaises(Exception):
            backend.prepare('feed',request)
        self.assertEqual(calls,[])
        with patch.object(StoredPublisherBackend,'observe',return_value=None),patch.object(LocalDeltaEffects,'prepare',side_effect=lambda *args,**kwargs:calls.append('plan')),patch.object(LocalDeltaEffects,'observe',return_value={'states':[]}),patch.object(StoredPublisherBackend,'prepare',side_effect=lambda *args:calls.append('prepared')):
            backend.prepare('feed',request)
            self.assertLess(calls.index('plan'),calls.index('prepared'))
            driver.transport.original_history=lambda target:[{'version':3}]
            with self.assertRaisesRegex(LocalDeltaError,'native prior changed'):
                driver.observe_prepared_truss_apply(request,context)
