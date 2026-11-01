"""Effect-free exact page lookahead shape and value gate."""
import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).with_name('prepare.py')
SPEC = importlib.util.spec_from_file_location('g03_native_prepare_page', SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PageLookaheadTests(unittest.TestCase):
    def setUp(self):
        self.page_one = [['person-1', 'Ada'], ['person-2', '李']]
        self.page_two = [['person-3', '雪']]
        self.after = ['person-2']

    def test_accepts_exact_page_and_full_row_lookahead(self):
        MODULE.assert_page_one_lookahead(self.page_one + self.page_two[:1], self.page_one, self.page_two, self.after)

    def test_rejects_wrong_lookahead_row(self):
        with self.assertRaisesRegex(RuntimeError, 'lookahead differs'):
            MODULE.assert_page_one_lookahead(self.page_one + [['person-3', 'wrong']], self.page_one, self.page_two, self.after)

    def test_requires_exactly_one_lookahead_row(self):
        with self.assertRaisesRegex(RuntimeError, 'cardinality'):
            MODULE.assert_page_one_lookahead(self.page_one, self.page_one, self.page_two, self.after)
        with self.assertRaisesRegex(RuntimeError, 'cardinality'):
            MODULE.assert_page_one_lookahead(self.page_one + self.page_two + [['person-4', 'four']], self.page_one, self.page_two, self.after)

    def test_checks_continuation_key(self):
        with self.assertRaisesRegex(RuntimeError, 'continuation key'):
            MODULE.assert_page_one_lookahead(self.page_one + self.page_two[:1], self.page_one, self.page_two, ['other'])


if __name__ == '__main__':
    unittest.main()
