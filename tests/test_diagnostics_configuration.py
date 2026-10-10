"""CONTRACT-006 admission/privacy controls; no SDK or receiver qualification."""
from dataclasses import FrozenInstanceError, replace, asdict
from pathlib import Path
import copy
import json
import os
import pickle
import tempfile
import unittest
from unittest.mock import patch

from ashlar_host.config import DiagnosticsConfig, DiagnosticsLimits, SecretText, HostError


class DiagnosticsConfigurationTests(unittest.TestCase):
    def limits(self):
        return DiagnosticsLimits(1024, 16, 4096, 4096, 16384, 100, 8, 100, 1000, 60)

    def configuration(self, **changes):
        limits = self.limits()
        origins = dict.fromkeys(('profile','capture_root','endpoint','headers','tls','ca_file','environment'), 'explicit')
        origins.update({'limits.' + name: 'default-toml' for name in limits.__dataclass_fields__})
        settings = dict(profile='ashlar-host-otel-http/0.1', capture_root=Path('/private/tmp/private-capture'),
                        endpoint=SecretText('http://127.0.0.1:4318/base/'), headers=(),
                        tls='loopback-test', ca_file=None, environment='test', limits=limits, origins=origins)
        settings.update(changes)
        return DiagnosticsConfig(**settings)

    def test_explicit_configuration_ignores_poisoned_ambient_settings(self):
        with patch.dict(os.environ, {'OTEL_EXPORTER_OTLP_ENDPOINT':'http://evil.invalid',
                                    'HTTPS_PROXY':'http://evil.invalid', 'OTEL_RESOURCE_ATTRIBUTES':'secret=sentinel'}):
            config = self.configuration()
        self.assertEqual(config.environment, 'test')
        self.assertEqual(config.endpoint._reveal(), 'http://127.0.0.1:4318/base/')
        with self.assertRaises(FrozenInstanceError): config.tls = 'system'
        with self.assertRaises(TypeError): config.origins['endpoint'] = 'field-default'

    def test_secret_representation_copy_and_serialization(self):
        secret = SecretText('private-sentinel')
        self.assertNotIn('private-sentinel', repr(secret) + str(secret))
        self.assertIs(copy.deepcopy(secret), secret)
        with self.assertRaises(AttributeError): secret.value = 'changed'
        with self.assertRaises(AttributeError): del secret._SecretText__value
        self.assertEqual(secret._reveal(), 'private-sentinel')
        with self.assertRaises(TypeError): pickle.dumps(secret)
        with self.assertRaises(TypeError): json.dumps(secret)
        config = self.configuration(headers=(('Authorization', secret),))
        for sentinel in ('private-sentinel','private-capture','127.0.0.1','Authorization'):
            self.assertNotIn(sentinel, repr(config))
        with self.assertRaises(TypeError): json.dumps(config)
        with self.assertRaises(TypeError): asdict(config)

    def test_origin_snapshot_and_required_operator_sources(self):
        original = dict(self.configuration().origins)
        config = self.configuration(origins=original)
        original['endpoint'] = 'field-default'
        self.assertEqual(config.origins['endpoint'], 'explicit')
        for key in ('capture_root','endpoint','headers','tls','environment'):
            for source in ('default-toml','environment-toml','field-default'):
                origins = dict(config.origins); origins[key] = source
                with self.subTest(key=key, source=source), self.assertRaisesRegex(HostError, '^diagnostics-configuration$'):
                    self.configuration(origins=origins)
        for change in ('missing','unknown','invalid'):
            origins = dict(config.origins)
            if change == 'missing': del origins['endpoint']
            if change == 'unknown': origins['surprise'] = 'explicit'
            if change == 'invalid': origins['endpoint'] = True
            with self.assertRaises(HostError): self.configuration(origins=origins)

    def test_url_and_tls_policy_refusals_are_safe(self):
        invalid = ('https://user:private-sentinel@example.com','https://example.com?','https://example.com#',
                   'https://example.com/base/v1/logs/','https://example.com/v1/traces',
                   'https://example.com/v1/metrics','http://remote.invalid','https:///missing-host',
                   'https://example.com:99999','https://example.com:0','https://example.com\n',
                   'file:///private-sentinel','https://example.com private-sentinel',
                   'https://example.com\\private-sentinel')
        for endpoint in invalid:
            with self.subTest(endpoint=endpoint), self.assertRaisesRegex(HostError, '^diagnostics-configuration$'):
                self.configuration(endpoint=SecretText(endpoint), tls='system')
        for endpoint in ('http://127.0.0.1:4318','http://[::1]:4318','http://localhost:4318'):
            self.configuration(endpoint=SecretText(endpoint))
        self.configuration(endpoint=SecretText('https://example.com/base'), tls='system')
        with self.assertRaises(HostError): self.configuration(endpoint='http://127.0.0.1')
        with self.assertRaises(HostError): self.configuration(endpoint=SecretText('http://192.0.2.1'))

    def test_headers_are_closed_immutable_and_bounded(self):
        invalid = ([('A', SecretText('v'))], (('A', 'raw-secret'),),
                   (('A',SecretText('v')),('a',SecretText('v'))),
                   (('A\n',SecretText('v')),), (('é',SecretText('v')),),
                   (('a'*65,SecretText('v')),), (('A',SecretText('x'*2049)),),
                   (('A',SecretText('\x85')),), tuple((str(i),SecretText('v')) for i in range(9)))
        for headers in invalid:
            with self.subTest(headers_type=type(headers)), self.assertRaisesRegex(HostError, '^diagnostics-configuration$'):
                self.configuration(headers=headers)
        self.configuration(headers=(('Authorization',SecretText('x'*2048)),))

    def test_custom_ca_and_path_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            ca = Path(directory)/'ca.pem';ca.write_bytes(b'fixture certificate bytes')
            self.configuration(endpoint=SecretText('https://example.com'), tls='custom-ca', ca_file=ca)
            origins = dict(self.configuration().origins);origins['ca_file'] = 'field-default'
            with self.assertRaises(HostError):
                self.configuration(endpoint=SecretText('https://example.com'), tls='custom-ca', ca_file=ca, origins=origins)
            for path in (Path('relative'),Path(directory),Path(directory)/'missing'):
                with self.assertRaisesRegex(HostError, '^diagnostics-configuration$'):
                    self.configuration(endpoint=SecretText('https://example.com'), tls='custom-ca', ca_file=path)
            ca.write_bytes(b'x'*(1024*1024+1))
            with self.assertRaises(HostError):
                self.configuration(endpoint=SecretText('https://example.com'), tls='custom-ca', ca_file=ca)
        with self.assertRaises(HostError): self.configuration(capture_root=Path('relative'))
        with self.assertRaises(HostError): self.configuration(ca_file=Path('/private/tmp/ca'))

    def test_contract_budget_boundaries_reject_booleans_and_cross_limit_drift(self):
        axes = {'max_event_bytes':(512,4096),'max_queue_records':(1,128),
                'max_queue_bytes':(4096,524288),'max_segment_bytes':(4096,4194304),
                'max_capture_bytes':(4096,16777216),'max_emissions':(1,10000),
                'max_attempts':(1,64),'export_timeout_ms':(1,1000),
                'shutdown_timeout_ms':(1,2000),'retention_seconds':(1,604800)}
        for name, (minimum,maximum) in axes.items():
            for value in (minimum-1,maximum+1,True,1.5):
                with self.subTest(name=name,value=value), self.assertRaisesRegex(HostError, '^diagnostics-configuration$'):
                    replace(self.limits(), **{name:value})
        with self.assertRaises(HostError): replace(self.limits(), max_segment_bytes=32768)
        self.configuration(limits=replace(self.limits(), max_capture_bytes=16777216))


if __name__ == '__main__': unittest.main()
