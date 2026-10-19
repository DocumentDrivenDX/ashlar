# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure public receipt ABI controls; native receipt qualification is separate."""

import unittest
from dataclasses import replace

from ashlar_host.delta_atomic import DeltaAtomicReceipt
from ashlar_host.delta_receipt import sdk_delta_receipt


class DeltaReceiptControls(unittest.TestCase):
    def original(self):
        return DeltaAtomicReceipt(
            "11111111-1111-1111-1111-111111111111",
            "original-epoch",
            123,
            "a" * 64,
            '{"original":"metadata"}',
        )

    def test_exact_public_vector_and_native_custody_unchanged(self):
        original = self.original()
        receipt = sdk_delta_receipt(original)
        self.assertEqual(receipt.store, '["11111111-1111-1111-1111-111111111111"]')
        self.assertEqual(
            receipt.version,
            '[{"tableUuid":"11111111-1111-1111-1111-111111111111","version":"123"}]',
        )
        self.assertEqual(receipt.epoch, original.epoch)
        self.assertEqual(original.version, 123)
        self.assertEqual(original.metadata_json, '{"original":"metadata"}')

    def test_non_native_receipt_and_bad_native_identifiers_refuse(self):
        with self.assertRaises(TypeError):
            sdk_delta_receipt(object())
        for changes in (
            {"version": True},
            {"version": -1},
            {"version": "123"},
            {"table_uuid": "label"},
            {"table_uuid": "11111111-1111-1111-1111-11111111111A"},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                sdk_delta_receipt(replace(self.original(), **changes))
