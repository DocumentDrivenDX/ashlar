# SPDX-License-Identifier: MIT OR Apache-2.0
from types import MappingProxyType, SimpleNamespace
import unittest

from ashlar.pins import PinVector
from ashlar_host.managed_a1_local_pin_owner import (
    _A1PinPolicy, ManagedA1LocalPinOwnerError,
)


class ManagedA1LocalPinPolicyTests(unittest.TestCase):
    def setUp(self):
        self.vector = PinVector("managed-current-spark:user", "manifest", "pub",
            "a" * 64, {f"cat.sch.{role}": (f"uuid-{role}", 1) for role in (
                "object_current", "edge_current", "tombstone", "whole_source_history")})
        self.context = object()
        self.selection = SimpleNamespace(
            vector=self.vector,
            expected_source_custody=MappingProxyType({
                name: "abcdef"[index % 6] * 64 for index, name in enumerate((
                    "source_registration", "consumer_registration", "cursor",
                    "destination_registration", "ack_scope", "checkpoint",
                    "current_page", "retained_page"))}),
        )
        self.policy = _A1PinPolicy(self.selection, self.vector, self.context)

    def test_exact_selection_vector_context_registration_and_source_identity(self):
        self.assertIsNone(self.policy.admit_registration(self.vector, self.context))
        self.policy._registered = True
        self.assertIsNone(self.policy.authorize_read(self.vector, self.context))
        self.assertIsNone(self.policy.authorize_source_bound_read(
            self.vector, self.selection, self.context))

    def test_wrong_context_vector_or_source_selection_refuses(self):
        with self.assertRaises(ManagedA1LocalPinOwnerError):
            self.policy.admit_registration(self.vector, object())
        with self.assertRaises(ManagedA1LocalPinOwnerError):
            other = PinVector("other", "manifest", "pub", "b" * 64,
                {key: tuple(value) for key, value in self.vector.targets.items()})
            self.policy.authorize_read(other, self.context)
        with self.assertRaises(ManagedA1LocalPinOwnerError):
            self.policy.authorize_source_bound_read(self.vector, object(), self.context)

    def test_second_registration_and_malformed_custody_refuse(self):
        self.policy._registered = True
        with self.assertRaises(ManagedA1LocalPinOwnerError):
            self.policy.admit_registration(self.vector, self.context)
        self.selection.expected_source_custody = {"missing": "bad"}
        with self.assertRaises(ManagedA1LocalPinOwnerError):
            self.policy.authorize_read(self.vector, self.context)


if __name__ == "__main__":
    unittest.main()
