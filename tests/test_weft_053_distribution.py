# SPDX-License-Identifier: MIT OR Apache-2.0
"""Selected public Weft .53 identity and API contract, without Spark startup."""
import hashlib,unittest
from pathlib import Path
from types import SimpleNamespace
from ashlar_host.count_star_admission import CountStarAdmissionConfig,CountStarSchemaValidation
from ashlar.weft_053_distribution import SCHEMA_SHA256,SOURCE_COMMIT,VERSION,WHEEL_SHA256,Weft053Distribution,Weft053Paths

class Weft053DistributionTests(unittest.TestCase):
    def test_separate_public_release_and_schema_identity(self):
        self.assertEqual(VERSION,'0.4.53')
        self.assertEqual(SOURCE_COMMIT,'726a840e956e25217d6aeea2674fdfcf3f342a24')
        self.assertEqual(WHEEL_SHA256,'4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c')
        self.assertEqual(set(SCHEMA_SHA256),{'compile-request-v0.4.1.schema.json','compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json'})

    def test_optional_page_continuation_none_is_forwarded(self):
        calls=[]; module=SimpleNamespace(check_engine_page_json=lambda *args: calls.append(args) or '{"status":"compiled"}')
        instance=object.__new__(Weft053Distribution); instance.module=module; instance._verify=lambda: True
        self.assertEqual(instance.check_engine_page_json('original','request',None),'{"status":"compiled"}')
        self.assertEqual(calls,[('original','request',None)])
        with self.assertRaises(ValueError): instance.check_engine_page_json(None,'request',None)
        with self.assertRaises(ValueError): instance.check_engine_page_json('original',None,None)
        with self.assertRaises(ValueError): instance.compile_paths_keys_json(None)

    def test_index_digest_format_is_required(self):
        path=Path('/tmp/weft053-index.json')
        self.assertEqual(Weft053Paths(path,'0'*64).index,path)
        with self.assertRaises(ValueError): Weft053Paths(path,'not-a-digest')

    def test_schema_files_are_exact_weft_v053_pins(self):
        root=Path(__file__).parent.parent/'docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024/weft053-schemas'
        self.assertEqual({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.glob('*.json')},SCHEMA_SHA256)
        schemas={p.name:p.read_bytes()for p in root.glob('*.json')}
        validation=CountStarSchemaValidation(schemas,lambda *_:None,profile='count-star-current-053')
        self.assertEqual(CountStarAdmissionConfig(1048576,validation,profile='count-star-current-053').profile,'count-star-current-053')

if __name__=='__main__': unittest.main()
