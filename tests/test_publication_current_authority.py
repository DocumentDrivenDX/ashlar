# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert boundary lifecycle controls, not source/publication qualification."""
from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.publication_current_authority import open_publication_current_interval


class InertReader:
    def __init__(self):
        self.context=object();self.manifest={'publication_id':'p','profile_version':'ashlar-delta/0.3','table_versions_json':'{"c.s.objects":0}','source_progress_json':'{"source":1}','schema_revisions_json':'{"source":"1"}','validation_report_json':'{"complete":true}','recorded_at':'1'}
        self.request_bytes=b'original';self.model=b'original-model';self.graph=b'original-graph'
        self.admission=object();self.events=[];self.active=False;self.refuse=False
        self.driver=SimpleNamespace(transport=SimpleNamespace(targets={'c.s.objects':SimpleNamespace(uuid='native-id')}),policy=object())
        self.provider=SimpleNamespace(interval=self.interval)
    @contextmanager
    def interval(self,context):
        assert context is self.context;self.events.append('hold');self.active=True
        try:yield
        finally:self.active=False;self.events.append('release')
    def require_current(self,context):
        assert self.active and context is self.context;self.events.append('require-current')
        if self.refuse:raise PermissionError('current source refusal')

    def renew_original_graph_read(self,vector,*,context):
        assert self.active and context is self.context
        self.events.append('renew-original-graph')
        if self.refuse:raise PermissionError('current source refusal')


class PublicationIntervalTests(unittest.TestCase):
    def test_real_owner_type_required_before_acquisition(self):
        with self.assertRaises(TypeError):
            with open_publication_current_interval(object()):pass
    def test_inert_interval_originals_and_closing(self):
        reader=InertReader()
        with patch('ashlar_host.publication_current_authority.OpenedCommerceReader',InertReader):
            with open_publication_current_interval(reader)as held:
                self.assertIs(held.context,reader.context);held.require_current(reader.context)
            self.assertFalse(held.active)
            with self.assertRaises(PermissionError):held.require_current(reader.context)
        self.assertEqual(reader.events,['hold','require-current','require-current','require-current','release'])
    def test_drift_and_primary_failure_preserved(self):
        for primary in (False,True):
            reader=InertReader()
            with patch('ashlar_host.publication_current_authority.OpenedCommerceReader',InertReader):
                with self.assertRaises(LookupError if primary else PermissionError):
                    with open_publication_current_interval(reader)as held:
                        if primary:reader.refuse=True;raise LookupError('primary')
                        reader.manifest['table_versions_json']='changed'
            self.assertFalse(held.active);self.assertFalse(reader.active)

    def test_body_renewal_retains_full_boundaries_and_refuses_drift(self):
        reader=InertReader()
        with patch('ashlar_host.publication_current_authority.OpenedCommerceReader',InertReader):
            with open_publication_current_interval(reader)as held:
                held.renew_current(reader.context)
                held.renew_current(reader.context)
                self.assertEqual(reader.events.count('require-current'),1)
                self.assertEqual(reader.events.count('renew-original-graph'),2)
                reader.driver.transport.targets['c.s.objects'].uuid='changed'
                with self.assertRaises(PermissionError):held.renew_current(reader.context)
                reader.driver.transport.targets['c.s.objects'].uuid='native-id'
            self.assertEqual(reader.events.count('require-current'),2)
        reader=InertReader()
        with patch('ashlar_host.publication_current_authority.OpenedCommerceReader',InertReader):
            with self.assertRaises(LookupError):
                with open_publication_current_interval(reader):
                    reader.refuse=True;raise LookupError('primary')
        self.assertEqual(reader.events.count('require-current'),2)
        self.assertFalse(reader.active)


class PublicationOwnerCompositionTests(unittest.TestCase):
    def test_nominal_stage_same_interval_and_source_closing(self):
        import contextlib
        import sys
        from ashlar_host.publication_current_authority import PublicationCurrentOwner
        class Stage:
            def __init__(self,held):
                self.held_publication=held;self.spark=object();self.original_child=object()
                self.original_binding='original-binding';self.events=[]
            def require_current(self,held,child,binding,context):
                assert held is self.held_publication and child is self.original_child
                assert context is held.context and binding==self.original_binding
                held.renew_current(context);self.events.append('stage-current')
        class Local:
            def __init__(self,spark,child,binding,context):self.original=binding;self.context=context;self.active=False
            @contextlib.contextmanager
            def open_current_query(self,child,context):
                assert context is self.context;self.active=True
                try:yield self
                finally:self.active=False
            def binding(self):assert self.active;return self.original
            def renew(self,child,binding,context):assert self.active and binding==self.original and context is self.context
        reader=InertReader()
        module=SimpleNamespace(PublicationCurrentStage=Stage)
        with patch('ashlar_host.publication_current_authority.OpenedCommerceReader',InertReader), \
             patch.dict(sys.modules,{'ashlar_host.publication_current_stage':module}), \
             patch('ashlar_host.local_current_development.LocalCurrentDevelopmentOwner',Local):
            with open_publication_current_interval(reader)as publication:
                stage=Stage(publication);owner=PublicationCurrentOwner(publication,stage)
                with owner.open_current_query(stage.original_child,reader.context)as query:
                    self.assertEqual(query.binding(),stage.original_binding)
                    query.admit(stage.original_child,stage.original_binding,reader.context)
                    with self.assertRaises(PermissionError):query.renew(stage.original_child,stage.original_binding,object())
                self.assertFalse(query.active);self.assertFalse(owner.local.active)
                with self.assertRaises(TypeError):PublicationCurrentOwner(publication,object())
            self.assertFalse(publication.active)
        self.assertGreaterEqual(len(stage.events),3)
        # Every stage boundary still freshly renews source/ACK/native custody.
        # Full snapshot admission remains at independent interval boundaries.
        self.assertEqual(reader.events.count('require-current'),2)
        self.assertEqual(reader.events.count('renew-original-graph'),len(stage.events))
