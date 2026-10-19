# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed explicit setup selections; no SDK or database operation."""
from pathlib import Path
from unittest import TestCase
from ashlar_host.config import ProducerConfig
from ashlar_host.original_commerce_local_configuration import OriginalCommerceLocalInitializationConfig,OriginalCommerceLocalRuntime
from ashlar_host.delta_custody import LOCAL_OPERATION_CAPACITY_8M

class Tests(TestCase):
    def test_closed_configuration_and_owned_capacity(self):
        producer=ProducerConfig(Path('/producer'),Path('/bun'),Path('/git'),30,1024,4096)
        config=OriginalCommerceLocalInitializationConfig(Path('/private/tmp/native-explicit'),Path('/private/tmp/observed-explicit'),'local-original',Path('/model'),Path('/graph'),Path('/jars'),producer,dict(LOCAL_OPERATION_CAPACITY_8M),OriginalCommerceLocalRuntime(Path('/python'),'1'*64,Path('/java'),'2'*64,'3'*64))
        self.assertEqual(config.publication_inputs().producer,producer)
        with self.assertRaises(TypeError):config.capacity['max_intent_bytes']=1
        for change in ({'installation_id':True},{'root':Path('relative')},{'producer':object()},{'capacity':{'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':True}},{'output':config.root/'observations'}):
            values=dict(config.__dict__);values.update(change)
            with self.assertRaises((ValueError,PermissionError)):OriginalCommerceLocalInitializationConfig(**values)
