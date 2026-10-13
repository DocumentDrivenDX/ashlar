import hashlib
import unittest
from unittest.mock import patch
import test_truss_feed_sessions as cases
from ashlar_host.truss_page_archive import encode_truss_page, decode_truss_page


class Tests(unittest.TestCase):
    def test_original_public_page_roundtrip_and_changed_digest(self):
        owner, *_ = cases.Tests().setup_case()
        with patch('ashlar_host.truss_page_archive._types', return_value=(cases.Page, cases.Cursor)):
            raw = encode_truss_page(owner.page, feed=owner.feed)
            self.assertEqual(decode_truss_page(raw, expected_sha256=hashlib.sha256(raw).hexdigest(), feed=owner.feed), owner.page)
            with self.assertRaisesRegex(ValueError, 'digest differs'):
                decode_truss_page(raw+b' ', expected_sha256=hashlib.sha256(raw).hexdigest(), feed=owner.feed)
