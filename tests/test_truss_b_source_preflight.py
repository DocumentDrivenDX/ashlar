# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ashlar_host import truss_b_source_preflight as command


class TrussBSourcePreflightTests(unittest.TestCase):
    def test_authentic_retained_source_and_origin_hashes_preflight_without_native_imports(self):
        source = Path("/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json")
        origin = Path(__file__).resolve().parents[1] / "docs/helix/04-build/evidence/current-upstream-release-20261022/r04-b-source-installation-draft-20261023/b-origin-candidate.json"
        if not source.is_file():
            self.skipTest("retained authentic source is unavailable in this checkout")
        result = command.preflight(source, origin)
        self.assertEqual(result["status"], "source_and_origin_bytes_verified_preflight_only")
        self.assertEqual(result["source"]["sha256"], command.SOURCE_SHA256)
        self.assertEqual(result["origin"]["sha256"], command.ORIGIN_SHA256)
        self.assertIsNone(result["execution_approval"])
        self.assertEqual(result["native_effects"], 0)

    def test_exact_authentic_digests_and_document_identity_are_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            source = root / "source.json"
            origin = root / "origin.json"
            source.write_bytes(json.dumps({"id": command.SOURCE_DOCUMENT_ID, "umf": "0.10.0"}).encode())
            origin.write_bytes(b'{"actor":"ashlar-r04-b-source-acceptance","reason":"independent R04 B source acceptance using the retained original UMF 0.10.0 model bytes"}\n')
            with patch.object(command, "SOURCE_SHA256", hashlib.sha256(source.read_bytes()).hexdigest()), patch.object(command, "ORIGIN_SHA256", hashlib.sha256(origin.read_bytes()).hexdigest()):
                self.assertEqual(command.preflight(source, origin)["native_effects"], 0)
            source.write_bytes(b'{"id":"substitute","umf":"0.10.0"}')
            with patch.object(command, "SOURCE_SHA256", hashlib.sha256(source.read_bytes()).hexdigest()), patch.object(command, "ORIGIN_SHA256", hashlib.sha256(origin.read_bytes()).hexdigest()):
                with self.assertRaisesRegex(ValueError, "identity/version"):
                    command.preflight(source, origin)

    def test_symlinked_or_overlapping_input_is_not_followed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            target = root / "target"
            target.write_bytes(b"{}")
            link = root / "link"
            link.symlink_to(target)
            with self.assertRaisesRegex(ValueError, "non-symlink"):
                command.preflight(link, target)


if __name__ == "__main__":
    unittest.main()
