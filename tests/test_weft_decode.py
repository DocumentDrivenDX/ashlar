import json,unittest
from pathlib import Path
from run_local_example import fixture_inputs
from ashlar.weft_decode import decode_string_column
from ashlar.publication import ResolutionError
ROOT=Path(__file__).resolve().parents[1]
class DecodeTests(unittest.TestCase):
    def test_selected_string_presence_and_refusals(self):
        intake,policy,_=fixture_inputs();columns=json.loads((ROOT/'docs/helix/04-build/evidence/weft-pinned-compiler-20261008.json').read_text())['compiled']['response']['columns']
        decode=lambda c,v:decode_string_column(c,v,policy,intake)
        self.assertEqual(decode(columns[0],'updated'),'updated')
        self.assertEqual(dict(decode(columns[1],'{"state":"absent"}')),{'state':'absent'})
        self.assertEqual(dict(decode(columns[1],'{"state":"value","value":"雪"}')),{'state':'value','value':'雪'})
        for raw in [None,'{"state":"null"}','{"state":"value","value":null}','{"state":"value","value":1}','{"state":"absent","state":"value"}','{"state":"absent","extra":true}']:
            with self.assertRaises(ValueError):decode(columns[1],raw)
        for raw in [None,'bad\x00text','\ud800']:
            with self.assertRaises(ResolutionError):decode(columns[0],raw)
