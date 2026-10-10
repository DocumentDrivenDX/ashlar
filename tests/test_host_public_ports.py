"""Public portable host operations retain existing exact semantics/aliases."""
import hashlib
import json
import unittest
from decimal import Decimal
from ashlar.publication import validate_table_identifier, ResolutionError, _name
from ashlar.native import quote_table_identifier, _quoted
from ashlar.schema import decode_retained_json, SchemaIntakeError, _json
from ashlar.attempt_store import verify_request_digest, AttemptStoreError, _request_digest


class PublicHostPortsTests(unittest.TestCase):
    def test_identifier_and_quoting_share_closed_grammar(self):
        for name in ('catalog.schema.table', '_c.s2.Table_3'):
            self.assertEqual(validate_table_identifier(name), name)
            self.assertEqual(quote_table_identifier(name), '.'.join('`'+p+'`' for p in name.split('.')))
        for bad in ('a.b', 'a.b.c.d', 'a.b.c; DROP TABLE t', 'a.b.`c`', 'a.b.c\x00', 'a.b.c\n', 'a..c', 'é.b.c', 123, None):
            with self.subTest(bad=bad):
                with self.assertRaises(ResolutionError): validate_table_identifier(bad)
                with self.assertRaises(ResolutionError): quote_table_identifier(bad)
        self.assertIs(_name, validate_table_identifier)
        self.assertIs(_quoted, quote_table_identifier)

    def test_json_exact_fraction_unknown_content_and_root_shapes(self):
        value=decode_retained_json(b'{"future":{"wide":9007199254740993,"fraction":1.2300},"null":null}')
        self.assertEqual(value['future']['wide'],9007199254740993)
        self.assertEqual(value['future']['fraction'].as_tuple(),Decimal('1.2300').as_tuple())
        self.assertIsNone(value['null'])
        for raw, expected in ((b'[]', []),(b'"text"','text'),(b'false',False),(b'null',None)):
            self.assertEqual(decode_retained_json(raw),expected)
        self.assertIs(_json,decode_retained_json)

    def test_json_rejects_duplicates_invalid_utf8_and_nonfinite(self):
        for raw in (b'{"nested":{"a":1,"a":2}}',b'\xff',b'NaN',b'Infinity',b'-Infinity',b'{',b'{} trailing'):
            with self.subTest(raw=raw), self.assertRaises(SchemaIntakeError): decode_retained_json(raw)

    def request(self):
        request={k:'retained-original' for k in ('stream','batch_id','predecessor','schema_revisions_json','source_batch_json','source_batch_digest')}
        request['request_digest']=hashlib.sha256(json.dumps(request,separators=(',',':'),sort_keys=True).encode()).hexdigest()
        return request

    def test_request_digest_rejects_mutation_missing_unknown_and_empty(self):
        request=self.request(); saved=dict(request)
        self.assertEqual(verify_request_digest(request),request['request_digest']); self.assertEqual(request,saved)
        self.assertIs(_request_digest,verify_request_digest)
        for operation in ('tamper','missing','unknown','empty','wrongtype','checkpoint'):
            bad=self.request()
            if operation=='tamper':bad['batch_id']='different'
            elif operation=='missing':del bad['stream']
            elif operation=='unknown':bad['future']='opaque'
            elif operation=='empty':bad['stream']=''
            elif operation=='wrongtype':bad['stream']=1
            else:
                bad['source_checkpoint_json']='{}'
                content={k:v for k,v in bad.items() if k!='request_digest'}
                bad['request_digest']=hashlib.sha256(json.dumps(content,separators=(',',':'),sort_keys=True).encode()).hexdigest()
            with self.subTest(operation=operation),self.assertRaises(AttemptStoreError):verify_request_digest(bad)

if __name__=='__main__':unittest.main()
