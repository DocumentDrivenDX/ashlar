# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import unittest
from unittest.mock import patch
import test_truss_feed_sessions as cases
from ashlar_host.truss_page_archive import encode_truss_page, decode_truss_page


class Tests(unittest.TestCase):
    def test_encoder_refuses_a_page_the_existing_decoder_cannot_retain(self):
        from dataclasses import replace
        from types import SimpleNamespace
        owner, *_ = cases.Tests().setup_case()
        page=replace(owner.page,content=b'x'*(2*1024*1024+1))
        with patch('ashlar_host.truss_page_archive._types',return_value=(cases.Page,cases.Cursor)),patch('ashlar_host.truss_page_archive.native_feed_batch',return_value=SimpleNamespace(batch_id=page.writer_xid)):
            with self.assertRaisesRegex(ValueError,'byte-carrier bounds'):
                encode_truss_page(page,feed=owner.feed)

    def test_original_public_page_roundtrip_and_changed_digest(self):
        owner, *_ = cases.Tests().setup_case()
        with patch('ashlar_host.truss_page_archive._types', return_value=(cases.Page, cases.Cursor)):
            raw = encode_truss_page(owner.page, feed=owner.feed)
            self.assertEqual(decode_truss_page(raw, expected_sha256=hashlib.sha256(raw).hexdigest(), feed=owner.feed), owner.page)
            with self.assertRaisesRegex(ValueError, 'digest differs'):
                decode_truss_page(raw+b' ', expected_sha256=hashlib.sha256(raw).hexdigest(), feed=owner.feed)

    def test_two_mib_original_page_archive_and_staging_roundtrip(self):
        import json
        from dataclasses import replace
        from ashlar.truss_native_feed import MAX_NATIVE_PAGE_BYTES,native_feed_batch,NativeFeedError
        from ashlar.staging import batch_row,batch_from_row
        from ashlar_host.truss_page_archive import MAXIMUM_ARCHIVE_BYTES,MAXIMUM_CONTENT_BASE64_BYTES
        owner,*_=cases.Tests().setup_case()
        value=json.loads(owner.page.content);value['unknown_opaque_padding']=''
        original=json.dumps(value,separators=(',',':')).encode()
        value['unknown_opaque_padding']='x'*(MAX_NATIVE_PAGE_BYTES-len(original))
        content=json.dumps(value,separators=(',',':')).encode();self.assertEqual(len(content),MAX_NATIVE_PAGE_BYTES)
        page=replace(owner.page,content=content,sha256=hashlib.sha256(content).hexdigest())
        with patch('ashlar_host.truss_page_archive._types',return_value=(cases.Page,cases.Cursor)):
            raw=encode_truss_page(page,owner.feed)
            self.assertGreater(len(raw),2*1024*1024);self.assertLess(len(raw),MAXIMUM_ARCHIVE_BYTES)
            self.assertEqual(MAXIMUM_CONTENT_BASE64_BYTES,2796204)
            self.assertEqual(decode_truss_page(raw,hashlib.sha256(raw).hexdigest(),owner.feed),page)
        kwargs={'sha256':page.sha256,'prior':page.prior.content,'next':page.next.content,'feed':owner.feed}
        batch=native_feed_batch(content,**kwargs);self.assertEqual(batch_from_row(batch_row(batch)),batch)
        oversized=content+b' '
        with self.assertRaises(NativeFeedError):native_feed_batch(oversized,**{**kwargs,'sha256':hashlib.sha256(oversized).hexdigest()})
        with self.assertRaises(ValueError):decode_truss_page(b' '*(MAXIMUM_ARCHIVE_BYTES+1),'0'*64,owner.feed)
