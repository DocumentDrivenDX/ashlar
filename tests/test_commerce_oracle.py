import shutil
import tempfile
import unittest
from pathlib import Path
from commerce_oracle import commerce_oracle

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'examples/domain-packs/commerce/upstream'
INVENTORY=ROOT/'examples/domain-packs/inventory.json'

class CommerceOracleTests(unittest.TestCase):
    def test_original_scenario_results_are_independently_expected(self):
        result=commerce_oracle(SOURCE,INVENTORY)
        self.assertEqual(result['scenarios'],{'partial-return':[[6,2,6]],
            'fulfillment':[['F2']],'settlement':[['PAY1']],'refund':[['RF1']]})
        self.assertEqual(sum(result['table_counts'].values()),11)
        self.assertEqual(result['table_counts']['fulfillments'],2)

    def test_numeric_source_tamper_refuses_before_oracle(self):
        with tempfile.TemporaryDirectory() as folder:
            copy=Path(folder)/'commerce';shutil.copytree(SOURCE,copy)
            p=copy/'data/products.csv';p.write_bytes(p.read_bytes().replace(b'12.50',b'12.500000000000001'))
            with self.assertRaisesRegex(ValueError,'bytes differ'):
                commerce_oracle(copy,INVENTORY)

    def test_inventory_repin_and_symlink_refuse(self):
        with tempfile.TemporaryDirectory() as folder:
            inventory=Path(folder)/'inventory';inventory.write_bytes(INVENTORY.read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError,'inventory'):
                commerce_oracle(SOURCE,inventory)
            link=Path(folder)/'link';link.symlink_to(SOURCE,target_is_directory=True)
            with self.assertRaisesRegex(ValueError,'regular source'):
                commerce_oracle(link,INVENTORY)
