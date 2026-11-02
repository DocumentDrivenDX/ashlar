# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure exact-byte checks for detached Truss A/B source evidence."""
import hashlib
import json
from types import SimpleNamespace
import unittest

from ashlar_host.truss_detached_source_codec import (
    DetachedSourceCodecError, DetachedTrussSourcePage,
    decode_detached_source_page, encode_detached_source_page,
)
from ashlar_host.truss_source_custody_selection import (
    TrussSourceCustodyAnchor, TrussSourceCustodyOpenSelection,
    TrussSourceCustodySelection, _ANCHORS, _issue,
)
from ashlar_host.r04_schedule import canonical


class DetachedSourceCodecTests(unittest.TestCase):
    def setUp(self):
        content = b"original exact Truss page bytes"
        self.page = SimpleNamespace(
            content=content, sha256=hashlib.sha256(content).hexdigest(),
            prior=SimpleNamespace(content=b"prior cursor exact bytes"),
            next=SimpleNamespace(content=b"next cursor exact bytes"),
            consumer_id="consumer-a", registration_id="consumer-registration-a",
            writer_xid="91", safe_watermark_xid="91", outcome="transaction")
        self.request = b'{ "request_digest" : "original-bytes" }'
        identity = {"feed": "feed-a", "consumer_id": "consumer-a",
            "registration_id": "consumer-registration-a", "ack_scope": "scope-a",
            "ack_registration_id": "ack-registration-a", "installation_id": "install-a",
            "epoch": "epoch-a"}
        self.anchor = _issue(_ANCHORS, TrussSourceCustodyAnchor(
            schedule_sha256="a" * 64, source="A", identity=identity,
            page=self.page, destination_id="destination-a",
            request_bytes=self.request,
            request_sha256=hashlib.sha256(self.request).hexdigest(),
            destination_registration_bytes=b'{ "registration" : "exact" }',
            ack_scope_bytes=b'{ "scope" : "exact" }',
            maximum_facts=17, maximum_bytes=4096,
            preselected_publication_id="ashlar-current:publication-a"))

    def test_codec_roundtrips_exact_detached_bytes_without_authority(self):
        encoded = encode_detached_source_page(self.anchor)
        detached = decode_detached_source_page(encoded)
        self.assertIs(type(detached), DetachedTrussSourcePage)
        self.assertEqual(detached.request_bytes, self.request)
        self.assertEqual(detached.destination_registration_bytes,
                         self.anchor.destination_registration_bytes)
        self.assertEqual(detached.ack_scope_bytes, self.anchor.ack_scope_bytes)
        self.assertEqual(detached.page_content, self.page.content)
        self.assertEqual(detached.prior_bytes, self.page.prior.content)
        self.assertEqual(detached.next_bytes, self.page.next.content)
        self.assertEqual(detached.page_fields["safe_watermark_xid"], "91")
        self.assertNotIsInstance(detached, (TrussSourceCustodyAnchor,
            TrussSourceCustodyOpenSelection, TrussSourceCustodySelection))
        with self.assertRaises(TypeError):
            detached.identity["installation_id"] = "changed"

    def test_codec_refuses_unissued_anchor(self):
        from dataclasses import replace
        with self.assertRaisesRegex(DetachedSourceCodecError, "Issued pre-publication"):
            encode_detached_source_page(replace(self.anchor))

    def test_decoder_refuses_unknown_duplicate_and_digest_changed_inputs(self):
        encoded = encode_detached_source_page(self.anchor)
        value = json.loads(encoded)
        value["extra"] = "unknown"
        with self.assertRaisesRegex(DetachedSourceCodecError, "closed detached source"):
            decode_detached_source_page(canonical(value))
        duplicate = encoded[:-1] + b',"profile":"second"}'
        with self.assertRaisesRegex(DetachedSourceCodecError, "Closed detached source JSON"):
            decode_detached_source_page(duplicate)
        value = json.loads(encoded)
        value["request_bytes_base64"] = "eA=="
        with self.assertRaisesRegex(DetachedSourceCodecError, "binding digest differs"):
            decode_detached_source_page(canonical(value))

    def test_decoder_checks_page_sha_even_with_recomputed_outer_digest(self):
        value = json.loads(encode_detached_source_page(self.anchor))
        value["page"]["content_base64"] = "Y2hhbmdlZA=="
        value.pop("binding_sha256")
        value["binding_sha256"] = hashlib.sha256(canonical(value)).hexdigest()
        with self.assertRaisesRegex(DetachedSourceCodecError, "page integrity differs"):
            decode_detached_source_page(canonical(value))


if __name__ == "__main__":
    unittest.main()
