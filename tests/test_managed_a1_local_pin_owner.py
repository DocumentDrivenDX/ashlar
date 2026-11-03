# SPDX-License-Identifier: MIT OR Apache-2.0
from types import MappingProxyType, SimpleNamespace
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import patch

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
        owner._phase_handle_opened = False
        owner._phase_released = False
        owner._pin_handle = None
        self.assertEqual(owner.open_native_pin(),
            (self.vector, self.selection, {
                "context": self.context, "bounded_lifetime_seconds": 60}))
        with self.assertRaises(ManagedA1LocalPinOwnerError):
            owner.open_native_pin()

    def test_phase_release_uses_exact_public_vector_and_requires_closed_handle(self):
        owner = object.__new__(ManagedA1LocalPinOwner)
        owner._active = True
        owner._pin_handle = object()
        owner._phase_handle_opened = True
        owner._phase_close_confirmed = False
        owner._phase_released = False
        owner._phase_index = 0
        owner._policy = SimpleNamespace(_registered=True)
        owner.context = self.context
        owner.vector = self.vector
        owner._executor = _ReleaseExecutor(self.vector)
        with self.assertRaisesRegex(ManagedA1LocalPinOwnerError, "Closed registered phase"):
            owner.release_current_registration()
        closed = SimpleNamespace(close=lambda: None)
        owner._pin_handle = closed
        owner.close_native_pin(closed)
        result = owner.release_current_registration()
        self.assertEqual(result["released_rows"], len(self.vector.targets))
        self.assertEqual(result["scope_id"], self.vector.scope_id)
        self.assertTrue(owner._phase_released)
        self.assertEqual(owner._executor.release_parameters,
            [{"id": self.vector.parameters(table)["id"],
              "authority": self.vector.authority,
              "digest": self.vector.custody_digest}
             for table in sorted(self.vector.targets)])

    def test_failed_pin_close_cannot_release_or_advance_phase(self):
        owner = object.__new__(ManagedA1LocalPinOwner)
        owner._active = True
        owner._pin_handle = SimpleNamespace(close=lambda: (_ for _ in ()).throw(
            RuntimeError("close failed")))
        owner._phase_handle_opened = True
        owner._phase_close_confirmed = False
        owner._phase_released = False
        owner._phase_index = 0
        owner._policy = SimpleNamespace(_registered=True)
        owner.context = self.context
        owner.vector = self.vector
        owner._executor = _ReleaseExecutor(self.vector)
        failed = owner._pin_handle
        with self.assertRaisesRegex(RuntimeError, "close failed"):
            owner.close_native_pin(failed)
        self.assertIsNone(owner._pin_handle)
        self.assertFalse(owner._phase_close_confirmed)
        with self.assertRaisesRegex(ManagedA1LocalPinOwnerError, "Closed registered phase"):
            owner.release_current_registration()

    def test_next_phase_requires_released_a0_and_distinct_scope_context(self):
        from ashlar_host.managed_operational_retention_interval import (
            ManagedOperationalRetentionSelection,
        )
        owner = object.__new__(ManagedA1LocalPinOwner)
        owner._active = True
        owner._pin_handle = None
        owner._phase_released = False
        owner._phase_index = 0
        owner._seen_scope_keys = {(self.vector.authority, self.vector.scope_kind,
                                   self.vector.scope_id)}
        owner.deadline_monotonic = None
        owner.context = self.context
        owner.vector, owner.selection = self.vector, self.selection
        with self.assertRaisesRegex(ManagedA1LocalPinOwnerError, "closed and released"):
            owner.select_next_phase(self.selection, self.vector, native_timeout_seconds=60)

        owner._phase_released = True
        next_vector = PinVector(self.vector.authority, self.vector.scope_kind, "pub-a1",
            "b" * 64, dict(self.vector.targets))
        next_selection = object.__new__(ManagedOperationalRetentionSelection)
        object.__setattr__(next_selection, "vector", next_vector)
        owner._connect = lambda: None
        result = owner.select_next_phase(next_selection, next_vector,
                                         native_timeout_seconds=60)
        self.assertEqual(result["phase_index"], 1)
        self.assertIs(owner.selection, next_selection)
        self.assertIsNot(owner.context, self.context)
        self.assertFalse(owner._policy._registered)
        owner._phase_index = 0
        owner._phase_released = True
        with self.assertRaisesRegex(ManagedA1LocalPinOwnerError, "new immutable publication scope"):
            owner.select_next_phase(next_selection, next_vector, native_timeout_seconds=60)

    def test_owner_accepts_concrete_posix_path_without_starting_postgres(self):
        from pathlib import Path
        from ashlar_host.managed_operational_retention_interval import (
            ManagedOperationalRetentionSelection,
        )
        with tempfile.TemporaryDirectory() as temporary:
            selected_path = Path(temporary).resolve() / "fresh-pgdata"
            selection = object.__new__(ManagedOperationalRetentionSelection)
            object.__setattr__(selection, "vector", self.vector)
            with patch.dict("sys.modules", {"truss": SimpleNamespace(
                    LocalPostgres=lambda path: SimpleNamespace(path=path))}):
                owner = ManagedA1LocalPinOwner(selected_path, selection, self.vector)
            self.assertIs(owner.data_directory, selected_path)


class _ReleaseExecutor:
    def __init__(self, vector):
        self.vector = vector
        self.release_parameters = []
        self.rows = [{"table_name": name, "table_uuid": uuid,
                      "version": str(version), "digest": vector.custody_digest,
                      "released": False}
                     for name, (uuid, version) in vector.targets.items()]

    @contextmanager
    def transaction(self, context):
        yield _ReleaseSession(self)


class _ReleaseSession:
    def __init__(self, executor):
        self.executor = executor

    def query(self, sql, parameters):
        if "FROM ashlar_pins.pin" in sql:
            if not self.executor.release_parameters:
                return SimpleNamespace(rows=[dict(row) for row in self.executor.rows
                    if row["released"] is False])
            return SimpleNamespace(rows=[dict(row) for row in self.executor.rows])
        if "ashlar_pins.release" in sql:
            self.executor.release_parameters.append(dict(parameters))
            matched = [table for table in self.executor.vector.targets
                       if self.executor.vector.parameters(table)["id"] == parameters["id"]]
            if len(matched) != 1:
                raise AssertionError("Release must bind exactly one vector row ID")
            for row in self.executor.rows:
                if row["table_name"] == matched[0]:
                    row["released"] = True
            return SimpleNamespace(rows=[])
        raise AssertionError("Unexpected SQL in pin release test")

if __name__ == "__main__":
    unittest.main()
