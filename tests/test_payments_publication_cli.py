# SPDX-License-Identifier: MIT OR Apache-2.0
"""Publication admission stays separate from unadmitted financial queries."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host import pack_native_cli as selected
import test_pack_native_cli as fixtures

class Tests(unittest.TestCase):
    def test_payments_publish_uses_original_owned_finite_host(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();path,value=fixtures.NativeCliTests().configuration(root,'publish');value['pack']='payments';path.write_text(json.dumps(value))
            with patch.object(selected,'_publish',return_value={'source':{'pack':'payments'},'manifest':{'publication_id':'original'},'qualification':'original owned publication'})as publish:
                selected.run_finite_pack_command(path,'publish')
            self.assertEqual(publish.call_args.args[0].producer.definition.name,'payments')
            self.assertTrue((root/'observations/report.json').is_file())
    def test_payments_financial_query_profile_refuses_before_owner(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();path,value=fixtures.NativeCliTests().configuration(root);value['pack']='payments';path.write_text(json.dumps(value))
            with patch.object(selected,'_query',side_effect=AssertionError('No query owner permitted')):
                with self.assertRaisesRegex(ValueError,'execution profile'):selected.run_finite_pack_command(path,'query')
            self.assertFalse((root/'observations').exists())
