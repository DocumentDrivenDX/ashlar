"""Captured ordinary-provider selection controls; no native execution claim."""
import hashlib,tempfile,unittest
from pathlib import Path
from dataclasses import fields
from unittest.mock import patch
import test_implicit_arithmetic_configuration as fixtures
from ashlar_host.config import HostError
from ashlar_host.count_star_configuration import QueryCommerceCountStarConfig
from ashlar_host.implicit_arithmetic_configuration import ExplicitCommercePostgresEndpoint
from ashlar_host.count_star_command import run_count_star_command
from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory

class Tests(unittest.TestCase):
    def test_exact_current_ports_pack_custody_and_preflight(self):
        fixture=fixtures.Tests();fixture.setUp()
        settings={field.name:getattr(fixture.config,field.name)for field in fields(QueryCommerceCountStarConfig)}
        settings['postgres']=ExplicitCommercePostgresEndpoint('chosen','127.0.0.1',25435,'truss_e2e');config=QueryCommerceCountStarConfig(**settings)
        pack_raw=(Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
        code=b'from test_commerce_query_sources import factory\ndef commerce_query_source_factory(config):\n return factory()\nfrom ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory\ndef ordinary_query_connection_factory(config):\n return OrdinaryQueryConnectionFactory(lambda role: None)\n'
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();provider=root/'provider.py';pack=root/'pack.json';provider.write_bytes(code);pack.write_bytes(pack_raw)
            with patch('ashlar_host.count_star_query.query_commerce_count_star',return_value={'complete':True})as query:
                result=run_count_star_command(config,provider_path=provider,provider_sha256=hashlib.sha256(code).hexdigest(),pack_path=pack,selected_case_ids=('expanded-count-having',))
                self.assertEqual(result,{'complete':True});self.assertIs(type(query.call_args.kwargs['ordinary_session']),OrdinaryQueryConnectionFactory)
                self.assertIs(type(query.call_args.kwargs['source_factory']),CommerceQuerySourceFactory)
                self.assertEqual(query.call_args.kwargs['selected_case_ids'],('expanded-count-having',))
                custody=query.call_args.kwargs['invocation_custody'];custody.renew()
                provider.write_bytes(code+b'\n')
                with self.assertRaises(HostError):custody.renew()
            with patch('ashlar_host.count_star_command.load_commerce_query_provider')as load:
                with self.assertRaisesRegex(HostError,'query-selection'):
                    run_count_star_command(config,provider_path=provider,provider_sha256='a'*64,pack_path=pack,selected_case_ids=('unknown',))
                load.assert_not_called()
            with patch('ashlar_host.commerce_query_command.InvocationFile.read')as read:
                with self.assertRaises(HostError):run_count_star_command(config,provider_path=provider,provider_sha256='wrong',pack_path=pack)
                read.assert_not_called()
