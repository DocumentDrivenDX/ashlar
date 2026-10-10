import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from check_weft_paths_corpus_schemas import Config, Refusal, document, regular_read, validate_envelope


class SchemaRoutingTests(unittest.TestCase):
    config = Config(Path('/reviewed/schemas'), 4096, 1024)

    def envelope(self, scope, role, request, response=None):
        return json.dumps({'scope': scope, 'role': role, 'requestHex': request.hex(),
                           'responseHex': json.dumps(response or {'interfaceVersion': 'weft-compile/0.4.0', 'status': 'blocked'}).encode().hex()}).encode()

    def test_malformed_control_reaches_response_validation_without_request_parsing(self):
        calls = []
        validate_envelope(self.envelope('controls', 'invalid', b'{'), self.config,
                          lambda name, value: calls.append(name))
        self.assertEqual(calls, ['compile-response-v0.4.schema.json'])

    def test_original_namespace_versions_select_original_request_schema(self):
        for version, schema in [('0.1', 'compile-request.schema.json'), ('0.2', 'compile-request-v0.2.schema.json'), ('0.3', 'compile-request-v0.3.schema.json')]:
            calls = []
            request = json.dumps({'interfaceVersion': 'weft-compile/' + version + '.0'}).encode()
            validate_envelope(self.envelope('legacy', 'namespace', request), self.config,
                              lambda name, value: calls.append(name))
            self.assertEqual(calls, ['compile-response-v0.4.schema.json', schema])

    def test_valid_paths_check_both_protocol_schemas(self):
        calls = []
        validate_envelope(self.envelope('paths', 'valid', b'{"interfaceVersion":"weft-compile/0.4.0"}'), self.config,
                          lambda name, value: calls.append(name))
        self.assertEqual(calls, ['compile-response-v0.4.schema.json', 'compile-request-v0.4.schema.json'])

    def test_role_cannot_bypass_paths_request_validation(self):
        calls = []
        with self.assertRaises(Refusal):
            validate_envelope(self.envelope('paths', 'invalid', b'{'), self.config,
                              lambda name, value: calls.append(name))
        self.assertEqual(calls, [])

    def test_schema_failure_propagates_and_non_none_verifier_return_refuses(self):
        error = ValueError('schema refusal')
        def fail(name, value):
            raise error
        with self.assertRaises(ValueError) as caught:
            validate_envelope(self.envelope('controls', 'invalid', b'{'), self.config, fail)
        self.assertIs(caught.exception, error)
        with self.assertRaises(Refusal):
            validate_envelope(self.envelope('controls', 'invalid', b'{'), self.config, lambda name, value: False)

    def test_hex_input_and_duplicate_envelope_bounds(self):
        raw = self.envelope('controls', 'invalid', b'x' * 1025)
        with self.assertRaises(Refusal):
            validate_envelope(raw, self.config, lambda name, value: None)
        with self.assertRaises(Refusal):
            validate_envelope(b'{"scope":"controls","scope":"paths"}', self.config, lambda name, value: None)

    def test_valid_role_refuses_legacy_version_and_namespace_refuses_current_version(self):
        for scope, role, version in [('paths', 'valid', '0.3'), ('legacy', 'namespace', '0.4')]:
            with self.assertRaises(Refusal):
                validate_envelope(self.envelope(scope, role, ('{"interfaceVersion":"weft-compile/' + version + '.0"}').encode()), self.config, lambda name, value: None)

    def test_fraction_and_exponent_do_not_round_through_binary_float(self):
        values = document(b'{"fraction":0.123456789012345678901,"exponent":1e999}')
        self.assertEqual(str(values['fraction']), '0.123456789012345678901')
        self.assertTrue(values['exponent'].is_finite())
        with self.assertRaises(Refusal):
            document(b'1.' + b'1' * 257)

    def test_json_bearing_bytes_require_utf8_at_every_boundary(self):
        request = b'{"interfaceVersion":"weft-compile/0.4.0"}'
        for encoding in ('utf-16', 'utf-32'):
            normal = self.envelope('paths', 'valid', request)
            with self.assertRaises(Refusal):
                validate_envelope(normal.decode().encode(encoding), self.config, lambda name, value: None)
            for field in ('requestHex', 'responseHex'):
                envelope = json.loads(normal)
                envelope[field] = bytes.fromhex(envelope[field]).decode().encode(encoding).hex()
                with self.assertRaises(Refusal):
                    validate_envelope(json.dumps(envelope).encode(), self.config, lambda name, value: None)
        validate_envelope(self.envelope('controls', 'invalid', b'\xff'), self.config, lambda name, value: None)

    def test_regular_reader_refuses_fifo_symlink_and_oversize(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fifo = root / 'fifo'
            os.mkfifo(fifo)
            with self.assertRaises(Refusal):
                regular_read(fifo, 4)
            file = root / 'file'
            file.write_bytes(b'exact')
            with self.assertRaises(Refusal):
                regular_read(file, 4)
            self.assertEqual(regular_read(file, 5), b'exact')
            link = root / 'link'
            link.symlink_to(file)
            with self.assertRaises(Refusal):
                regular_read(link, 5)

    def test_reader_cleanup_preserves_original_cancellation(self):
        error = KeyboardInterrupt('cancelled')
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory).resolve() / 'file'
            file.write_bytes(b'x')
            original_close = os.close
            def close_then_fail(fd):
                original_close(fd)
                raise OSError('cleanup')
            with patch('check_weft_paths_corpus_schemas.os.read', side_effect=error), patch('check_weft_paths_corpus_schemas.os.close', side_effect=close_then_fail):
                with self.assertRaises(KeyboardInterrupt) as caught:
                    regular_read(file, 5)
            self.assertIs(caught.exception, error)
            self.assertTrue(error.cleanup_failed)


if __name__ == '__main__':
    unittest.main()
