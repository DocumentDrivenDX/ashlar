# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest
from ashlar_host.pack_count_star_compile import select_pack_cases

class Tests(unittest.TestCase):
    def test_selection_preserves_original_order_bytes_and_rejects_bad_case_ids(self):
        original=(('first',b'original SQL first'),('second',b'original SQL second'),('third',b'original SQL third'))
        self.assertEqual(select_pack_cases(original),original)
        selected=select_pack_cases(original,('third','first'))
        self.assertEqual(selected,(original[0],original[2]));self.assertIs(selected[0],original[0])
        for invalid in ((),('first','first'),('unknown',),('',),['first'],(1,)):
            with self.subTest(invalid=invalid),self.assertRaises(ValueError):select_pack_cases(original,invalid)
