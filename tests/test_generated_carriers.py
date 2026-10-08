import json,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from generated_carriers import load_generated_carriers

class GeneratedCarriersTests(unittest.TestCase):
    def test_current_package_and_stale_model_refusal(self):
        value,digest=load_generated_carriers(ROOT)
        self.assertEqual(len(value['carriers']),8)
        self.assertEqual(len(digest),64)
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            for relative in [*value['inputs'],'sql/ashlar-delta-v03/runtime-carriers.generated.json']:
                target=root/relative;target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes((ROOT/relative).read_bytes())
            load_generated_carriers(root)
            target=root/'docs/helix/02-design/models/ashlar-delta-runtime/object_current.umf.json'
            target.write_bytes(target.read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError,'Stale generated model'):
                load_generated_carriers(root)

    def test_unexpected_input_cannot_trigger_arbitrary_file_read(self):
        value,_=load_generated_carriers(ROOT)
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);path=root/'sql/ashlar-delta-v03/runtime-carriers.generated.json'
            path.parent.mkdir(parents=True)
            value['inputs']={'../../private-file':'0'*64};path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'Unexpected model input inventory'):
                load_generated_carriers(root)
