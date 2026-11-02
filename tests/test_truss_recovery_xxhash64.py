# SPDX-License-Identifier: MIT OR Apache-2.0
"""Applied recovery recomputes the carrier-specific lookup hash oracle."""
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from ashlar_host.truss_recovery import _restored_graph_oracle


class Tests(unittest.TestCase):
    def test_delta04_recovery_uses_native_xxhash64_oracle(self):
        driver = SimpleNamespace(lookup_hash_profile="spark-xxhash64-typed-key/0.1",
            transport=SimpleNamespace(spark=object()), columns={"object_current": "object_current"})
        admission = SimpleNamespace(changes=("change",), batch="batch",
            parsed=SimpleNamespace(event_bytes=b"events", edge_order_keys=("edge-order",)))
        hashes = {"object-current-key": 931}
        expected = {"object_current": [{"lookup_hash": "931"}]}
        with patch("ashlar_host.truss_recovery.native_xxhash64", return_value=hashes) as native, \
             patch("ashlar_host.truss_recovery.graph_expected_rows", return_value=expected) as rows:
            actual = _restored_graph_oracle(driver, {"prior": []}, {"state": []}, admission, "stamp")
        self.assertIs(actual, expected)
        native.assert_called_once_with(driver.transport.spark, admission.changes, {"state": []})
        self.assertEqual(rows.call_args.kwargs["lookup_hashes"], hashes)

    def test_delta03_recovery_keeps_existing_hash_basis(self):
        driver = SimpleNamespace(lookup_hash_profile="sha256-json-v1", transport=SimpleNamespace(spark=object()),
            columns={"object_current": "object_current"})
        admission = SimpleNamespace(changes=(), batch="batch",
            parsed=SimpleNamespace(event_bytes=b"events", edge_order_keys=()))
        with patch("ashlar_host.truss_recovery.native_xxhash64") as native, \
             patch("ashlar_host.truss_recovery.graph_expected_rows", return_value={}) as rows:
            _restored_graph_oracle(driver, {}, {}, admission, "stamp")
        native.assert_not_called()
        self.assertIsNone(rows.call_args.kwargs["lookup_hashes"])


if __name__ == "__main__":
    unittest.main()
