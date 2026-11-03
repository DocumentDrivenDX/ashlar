# SPDX-License-Identifier: MIT OR Apache-2.0
from types import MappingProxyType, SimpleNamespace
import tempfile
import unittest

from ashlar.pins import PinVector
from ashlar_host.managed_a1_local_pin_owner import (
    _A1PinPolicy, ManagedA1LocalPinOwner, ManagedA1LocalPinOwnerError,
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

    def test_parent_native_pin_helper_opens_exact_one_shot_source_bound_handle(self):
        owner = object.__new__(ManagedA1LocalPinOwner)
        owner._active = True
        owner._policy = SimpleNamespace(_registered=True)
        owner._pins = SimpleNamespace(open_live=lambda vector, selection, **kw:
            (vector, selection, kw))
        owner.vector, owner.selection, owner.context = (
            self.vector, self.selection, self.context)
        owner.native_timeout_seconds = 60
        owner._pin_opened = False
        self.assertEqual(owner.open_native_pin(),
            (self.vector, self.selection, {
                "context": self.context, "bounded_lifetime_seconds": 60}))
        with self.assertRaises(ManagedA1LocalPinOwnerError):
            owner.open_native_pin()

    def test_owner_accepts_concrete_posix_path_without_starting_postgres(self):
        from pathlib import Path
        from ashlar_host.managed_operational_retention_interval import (
            ManagedOperationalRetentionSelection,
        )
        with tempfile.TemporaryDirectory() as temporary:
            selected_path = Path(temporary).resolve() / "fresh-pgdata"
            selection = object.__new__(ManagedOperationalRetentionSelection)
            object.__setattr__(selection, "vector", self.vector)
            owner = ManagedA1LocalPinOwner(selected_path, selection, self.vector)
            self.assertIs(owner.data_directory, selected_path)


if __name__ == "__main__":
    unittest.main()
