# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure choreography tests for the managed A1 release bridge.

The ports below are protocol doubles. These tests establish ordering and
failure behavior only; they do not qualify Truss, Unity Catalog, retention,
PostgreSQL pin service, managed runtime, or protected ACK authority.
"""
import unittest
import base64
import json
from types import SimpleNamespace
from unittest.mock import patch

from ashlar.pins import PinVector
from ashlar_host.managed_a1_release_bridge import (
    ManagedA1AckIncomplete,
    ManagedA1AckUncertain,
    ManagedA1BridgeError,
    ManagedA1VerifierTerminalSuccess,
    ManagedA1ReleaseBridge,
    _sha,
    _vector_sha,
)


VECTOR = PinVector("authority", "manifest", "pub-a1", "a" * 64, {
    "catalog.schema.object_current": ("uuid-node", 1),
    "catalog.schema.edge_current": ("uuid-edge", 1),
    "catalog.schema.tombstone": ("uuid-tombstone", 0),
    "catalog.schema.whole_source_history": ("uuid-history", 1),
})


def valid_a1_inputs():
    from tests.test_managed_current_a1_trial_plan import TestManagedCurrentA1TrialPlan as Fixtures
    from ashlar_host.managed_current_a1_trial_plan import prepare_managed_current_a1_trial
    from ashlar_host.managed_current_genesis import ManagedCurrentGenesisPredecessor
    from ashlar_host.managed_current_truss052_genesis_owner import (
        ManagedCurrentTruss052GenesisSelection,
    )

    schedule = Fixtures._schedule()
    batch = Fixtures._truss_batch()
    page = SimpleNamespace(content=batch.begin,
        prior=SimpleNamespace(content=base64.b64decode(batch.cursor_before)),
        next=SimpleNamespace(content=base64.b64decode(batch.cursor_after)))
    admission = SimpleNamespace(batch=batch)
    selection = ManagedCurrentTruss052GenesisSelection(
        schedule.sha256, schedule.raw, {}, {}, {},
        {"install-A": "1", "install-B": "1"},
        {"A": SimpleNamespace(batch=batch, page=page),
         "B": SimpleNamespace(batch=None, page=None)}, admission, batch)
    genesis = {"publication_id": "genesis-manifest"}
    predecessor = ManagedCurrentGenesisPredecessor(genesis, {}, {})
    context = object()
    with patch("ashlar_host.managed_current_genesis.verify_managed_current_genesis_predecessor",
               return_value=predecessor):
        plan = prepare_managed_current_a1_trial(selection, genesis,
            stream="source-stream", materialized_at="1762000000000000", context=context)
    request_bytes = json.dumps(plan.request, sort_keys=True,
        separators=(",", ":")).encode()
    from ashlar_host.driver import progress_union
    checkpoint = json.loads(plan.request["source_checkpoint_json"])
    progress = progress_union(predecessor.source_progress, checkpoint)
    versions = {table: target[1] for table, target in VECTOR.targets.items()}
    report = {"predecessor": plan.request["predecessor"],
              "request_digest": plan.request["request_digest"]}
    manifest = {"publication_id": "a1-manifest",
        "table_versions_json": json.dumps(versions, sort_keys=True, separators=(",", ":")),
        "source_progress_json": json.dumps(progress, sort_keys=True, separators=(",", ":")),
        "validation_report_json": json.dumps(report, sort_keys=True, separators=(",", ":"))}
    manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
    receipt_bytes = encode_truss_destination_receipt(request_bytes, manifest_bytes, VECTOR)
    return plan, request_bytes, manifest_bytes, receipt_bytes, VECTOR, context


_, REQUEST, MANIFEST, RECEIPT, _, _ = valid_a1_inputs()


class Handle:
    def __init__(self, role, events, *, renew_error=None, close_error=None,
                 coverage_values=(60.0,)):
        self.role, self.events = role, events
        self.renew_error, self.close_error = renew_error, close_error
        self.coverage_values = list(coverage_values)
        self.native_pin_coverage_values = [60.0]

    def renew(self):
        self.events.append(self.role + ".renew")
        if self.renew_error:
            raise self.renew_error

    def close(self):
        self.events.append(self.role + ".close")
        if self.close_error:
            raise self.close_error

    def remaining_lifetime_seconds(self):
        if len(self.coverage_values) > 1:
            return self.coverage_values.pop(0)
        return self.coverage_values[0]

    def native_pin_remaining_lifetime_seconds(self):
        if len(self.native_pin_coverage_values) > 1:
            return self.native_pin_coverage_values.pop(0)
        return self.native_pin_coverage_values[0]


def managed_read_pin_port(events, *, context, request_bytes=REQUEST,
        manifest_bytes=MANIFEST, receipt_bytes=RECEIPT, vector=VECTOR,
        renew_error=None, close_error=None, coverage_values=(60.0,),
        native_pin_coverage_values=(60.0,), on_open=None):
    """Use exact production port classes with inert test-only internals."""
    from ashlar_host.managed_operational_read_port import (
        ManagedOperationalReadPort, ManagedOperationalReadPinPort,
    )
    from ashlar_host.managed_operational_retention_interval import (
        ManagedOperationalRetentionSelection,
    )

    selected = object.__new__(ManagedOperationalRetentionSelection)
    for name, value in (("request_bytes", request_bytes), ("manifest_bytes", manifest_bytes),
                        ("receipt_bytes", receipt_bytes), ("vector", vector)):
        object.__setattr__(selected, name, value)
    read_port = object.__new__(ManagedOperationalReadPort)
    read_port.selection = selected
    read_port.context = context
    port = object.__new__(ManagedOperationalReadPinPort)
    port.read_port = read_port
    port.context = context

    def open_port(receipt_bytes, vector, context):
        events.append(("managed.open", receipt_bytes, vector, context))
        if on_open:
            on_open()
        handle = Handle("managed", events, renew_error=renew_error,
                        close_error=close_error, coverage_values=coverage_values)
        handle.native_pin_coverage_values = list(native_pin_coverage_values)
        return handle

    port.open = open_port
    return port


class SourcePort:
    def __init__(self, events, *, renew_error=None, close_error=None,
                 coverage_values=(60.0,)):
        self.events, self.renew_error, self.close_error = events, renew_error, close_error
        self.coverage_values = coverage_values

    def open(self, **kwargs):
        self.events.append(("source.open", kwargs))
        return Handle("source", self.events, renew_error=self.renew_error,
                      close_error=self.close_error,
                      coverage_values=self.coverage_values)


class AckPort:
    def __init__(self, events, *, commit_error=None, readback=None,
                 reconcile_error=None, clock=None, reconcile_elapsed=0):
        self.events = events
        self.commit_error = commit_error
        self.readback = readback
        self.reconcile_error = reconcile_error
        self.clock = clock
        self.reconcile_elapsed = reconcile_elapsed
        self.deadlines = []

    def commit(self, request, manifest, receipt, context, *, deadline_monotonic):
        self.events.append("ack.commit")
        self.deadlines.append(deadline_monotonic)
        if self.commit_error:
            raise self.commit_error

    def reconcile(self, request, manifest, receipt, context, *, deadline_monotonic):
        self.events.append("ack.reconcile")
        self.deadlines.append(deadline_monotonic)
        if self.clock:
            self.clock.value += self.reconcile_elapsed
        if self.reconcile_error:
            raise self.reconcile_error
        return self.readback or (request, manifest, receipt)


class VerifierCompletionPort:
    def __init__(self, events, *, observation=None, error=None):
        self.events, self.observation, self.error = events, observation, error

    def verify_terminal_success(self, **kwargs):
        self.events.append(("verifier.verify", kwargs))
        if self.error:
            raise self.error
        if self.observation is not None:
            return self.observation
        return ManagedA1VerifierTerminalSuccess(
            "ashlar-managed-a1-verifier-terminal-success/0.1", "run-1", "verify-1",
            True, "SUCCEEDED", "durably_verified", _sha(REQUEST), _sha(MANIFEST),
            _sha(RECEIPT), _vector_sha(VECTOR), "b" * 64)


class Clock:
    def __init__(self): self.value = 1.0
    def __call__(self): return self.value


class ManagedA1ReleaseBridgeTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        (self.plan, self.request_bytes, self.manifest_bytes,
         self.receipt_bytes, self.vector, self.context) = valid_a1_inputs()
        self.clock = Clock()

    def make_bridge(self, *, ack=None, managed=None, source=None,
                    completion=None, clock=None):
        return ManagedA1ReleaseBridge(
            managed or managed_read_pin_port(self.events, context=self.context,
                request_bytes=self.request_bytes, manifest_bytes=self.manifest_bytes,
                receipt_bytes=self.receipt_bytes, vector=self.vector),
            source or SourcePort(self.events),
            ack or AckPort(self.events),
            completion or VerifierCompletionPort(self.events),
            clock=clock or self.clock,
        )

    def run_bridge(self, *, ack=None, managed=None, source=None,
                   completion=None, clock=None, request_bytes=REQUEST):
        bridge = self.make_bridge(ack=ack, managed=managed, source=source,
                                  completion=completion, clock=clock)
        with patch("ashlar_host.managed_current_genesis.verify_managed_current_genesis_predecessor",
                   return_value=self.plan.predecessor):
            receipt = bridge.acknowledge(plan=self.plan, request_bytes=request_bytes,
                manifest_bytes=self.manifest_bytes, receipt_bytes=self.receipt_bytes,
                vector=self.vector, context=self.context)
        return bridge, receipt

    def test_renews_both_live_owners_before_one_ack_then_reconciles_and_closes(self):
        _, result = self.run_bridge()
        self.assertEqual(self.events, [
            ("verifier.verify", {
                "selection": self.plan.selection, "request_bytes": self.request_bytes,
                "manifest_bytes": self.manifest_bytes, "receipt_bytes": self.receipt_bytes,
                "vector": self.vector, "context": self.context}),
            ("managed.open", self.receipt_bytes, self.vector, self.context),
            ("source.open", {
                "selection": self.plan.selection, "request_bytes": self.request_bytes,
                "manifest_bytes": self.manifest_bytes, "receipt_bytes": self.receipt_bytes,
                "vector": self.vector, "context": self.context}),
            "managed.renew", "source.renew", "ack.commit", "ack.reconcile",
            "managed.close", "source.close",
        ])
        self.assertEqual(result.source, "A")
        self.assertEqual(result.publication_id, "a1-manifest")
        self.assertEqual(result.outcome, "acknowledged_exactly_reconciled")
        self.assertTrue(result.commit_response_received)

    def test_lost_commit_response_is_reconciled_once_without_retry(self):
        ack = AckPort(self.events, commit_error=OSError("commit response lost"))
        _, result = self.run_bridge(ack=ack)
        self.assertFalse(result.commit_response_received)
        self.assertEqual(self.events.count("ack.commit"), 1)
        self.assertEqual(self.events.count("ack.reconcile"), 1)
        self.assertEqual(ack.deadlines, [31.0, 31.0])
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_mismatched_or_missing_reconciliation_is_uncertain_and_closes(self):
        ack = AckPort(self.events, readback=(REQUEST, MANIFEST, b"other"))
        with self.assertRaises(ManagedA1AckUncertain):
            self.run_bridge(ack=ack)
        self.assertEqual(self.events.count("ack.commit"), 1)
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_pre_ack_renewal_refusal_never_calls_ack_and_closes_both(self):
        managed = managed_read_pin_port(self.events, context=self.context,
            renew_error=RuntimeError("read failed"))
        with self.assertRaisesRegex(RuntimeError, "read failed"):
            self.run_bridge(managed=managed)
        self.assertFalse(any(event in ("ack.commit", "ack.reconcile")
                             for event in self.events))
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_reconciled_ack_with_close_failure_is_incomplete(self):
        source = SourcePort(self.events, close_error=RuntimeError("source close"))
        with self.assertRaises(ManagedA1AckIncomplete):
            self.run_bridge(source=source)
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_interval_expiry_before_ack_refuses_and_closes(self):
        managed = managed_read_pin_port(self.events, context=self.context,
            on_open=lambda: setattr(self.clock, "value", self.clock.value + 901))
        with self.assertRaisesRegex(ManagedA1BridgeError, "interval exceeded"):
            self.run_bridge(managed=managed)
        self.assertFalse(any(event in ("ack.commit", "ack.reconcile")
                             for event in self.events))
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_nonterminal_verifier_result_refuses_before_any_read_interval(self):
        good = ManagedA1VerifierTerminalSuccess(
            "ashlar-managed-a1-verifier-terminal-success/0.1", "run-1", "verify-1",
            True, "RUNNING", "durably_verified", _sha(REQUEST), _sha(MANIFEST),
            _sha(RECEIPT), _vector_sha(VECTOR), "b" * 64)
        completion = VerifierCompletionPort(self.events, observation=good)
        with self.assertRaisesRegex(ManagedA1BridgeError, "terminal verifier success"):
            self.run_bridge(completion=completion)
        self.assertEqual(len(self.events), 1)
        self.assertEqual(self.events[0][0], "verifier.verify")

    def test_original_request_bytes_must_match_managed_selection(self):
        alternate = b'{ "request" : "original" }'
        bridge = self.make_bridge()
        with self.assertRaisesRegex(ManagedA1BridgeError, "Exact original request"):
            bridge.acknowledge(plan=self.plan, request_bytes=alternate,
                manifest_bytes=MANIFEST, receipt_bytes=RECEIPT,
                vector=VECTOR, context=self.context)
        self.assertEqual(self.events, [])

    def test_nonproduction_managed_port_is_rejected_by_type_gate(self):
        bridge = self.make_bridge(managed=SimpleNamespace(open=lambda *_: None))
        with self.assertRaisesRegex(ManagedA1BridgeError, "managed operational read/pin owner"):
            bridge.acknowledge(plan=self.plan, request_bytes=REQUEST,
                manifest_bytes=MANIFEST, receipt_bytes=RECEIPT,
                vector=VECTOR, context=self.context)
        self.assertEqual(self.events, [])

    def test_both_handles_must_cover_ack_and_reconciliation_at_open(self):
        source = SourcePort(self.events, coverage_values=(29.0,))
        with self.assertRaisesRegex(ManagedA1BridgeError, "does not cover"):
            self.run_bridge(source=source)
        self.assertFalse(any(event in ("managed.renew", "source.renew",
                                       "ack.commit", "ack.reconcile")
                             for event in self.events))
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_renewed_coverage_must_still_cover_ack_and_reconciliation(self):
        source = SourcePort(self.events, coverage_values=(60.0, 29.0))
        with self.assertRaisesRegex(ManagedA1BridgeError, "does not cover"):
            self.run_bridge(source=source)
        self.assertIn("managed.renew", self.events)
        self.assertIn("source.renew", self.events)
        self.assertNotIn("ack.commit", self.events)
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_native_pin_transaction_requires_separate_bounded_lifetime(self):
        managed = managed_read_pin_port(self.events, context=self.context,
            native_pin_coverage_values=(29.0,))
        with self.assertRaisesRegex(ManagedA1BridgeError,
                                    "native pin transaction lifetime"):
            self.run_bridge(managed=managed)
        self.assertNotIn("ack.commit", self.events)
        self.assertNotIn("ack.reconcile", self.events)
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_production_composite_does_not_substitute_read_deadline_for_pin(self):
        from ashlar_host.managed_operational_read_port import (
            ManagedOperationalReadPinHandle, ManagedOperationalReadPortError,
        )
        read = SimpleNamespace(remaining_lifetime_seconds=lambda: 900.0)
        native_pin = SimpleNamespace(renew=lambda: None, close=lambda: None)
        handle = ManagedOperationalReadPinHandle(read, native_pin)
        with self.assertRaisesRegex(ManagedOperationalReadPortError,
                                    "does not expose bounded live transaction"):
            handle.native_pin_remaining_lifetime_seconds()

    def test_native_pin_coverage_is_rechecked_after_renewal(self):
        managed = managed_read_pin_port(self.events, context=self.context,
            native_pin_coverage_values=(60.0, 29.0))
        with self.assertRaisesRegex(ManagedA1BridgeError,
                                    "native pin transaction lifetime"):
            self.run_bridge(managed=managed)
        self.assertIn("managed.renew", self.events)
        self.assertNotIn("ack.commit", self.events)
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_ack_reconcile_must_return_within_owner_deadline(self):
        ack = AckPort(self.events, clock=self.clock, reconcile_elapsed=31)
        with self.assertRaises(ManagedA1AckIncomplete):
            self.run_bridge(ack=ack)
        self.assertEqual(ack.deadlines, [31.0, 31.0])
        self.assertEqual(self.events[-2:], ["managed.close", "source.close"])

    def test_plan_and_publication_bytes_must_match_selected_a1_lineage(self):
        from tests.test_managed_current_a1_trial_plan import TestManagedCurrentA1TrialPlan as Fixtures
        from ashlar_host.managed_current_a1_trial_plan import prepare_managed_current_a1_trial
        from ashlar_host.managed_current_genesis import ManagedCurrentGenesisPredecessor
        from ashlar_host.managed_current_truss052_genesis_owner import (
            ManagedCurrentTruss052GenesisSelection,
        )
        from ashlar_host.managed_a1_release_bridge import _validate_plan

        schedule = Fixtures._schedule()
        batch = Fixtures._truss_batch()
        page = SimpleNamespace(content=batch.begin,
            prior=SimpleNamespace(content=__import__("base64").b64decode(batch.cursor_before)),
            next=SimpleNamespace(content=__import__("base64").b64decode(batch.cursor_after)))
        admission = SimpleNamespace(batch=batch)
        selection = ManagedCurrentTruss052GenesisSelection(
            schedule.sha256, schedule.raw, {}, {}, {},
            {"install-A": "1", "install-B": "1"},
            {"A": SimpleNamespace(batch=batch, page=page),
             "B": SimpleNamespace(batch=None, page=None)}, admission, batch)
        genesis = {"publication_id": "genesis-manifest"}
        predecessor = ManagedCurrentGenesisPredecessor(genesis, {}, {})
        context = object()
        with patch("ashlar_host.managed_current_genesis.verify_managed_current_genesis_predecessor",
                   return_value=predecessor):
            plan = prepare_managed_current_a1_trial(selection, genesis,
                stream="source-stream", materialized_at="1762000000000000", context=context)
            request_bytes = json.dumps(plan.request, sort_keys=True,
                separators=(",", ":")).encode()
            manifest = {"publication_id": "a1-manifest",
                "table_versions_json": json.dumps(
                    {table: target[1] for table, target in VECTOR.targets.items()},
                    sort_keys=True, separators=(",", ":")),
                "source_progress_json": "{}"}
            manifest_bytes = json.dumps(manifest, sort_keys=True,
                separators=(",", ":")).encode()
            from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
            receipt_bytes = encode_truss_destination_receipt(request_bytes, manifest_bytes, VECTOR)
            with patch("ashlar_host.driver.validate_union", return_value=None):
                self.assertEqual(_validate_plan(plan, request_bytes, manifest_bytes,
                                                receipt_bytes, VECTOR),
                                 ("A", "a1-manifest"))
                bad_manifest = dict(manifest)
                bad_versions = {table: target[1] for table, target in VECTOR.targets.items()}
                bad_versions["catalog.schema.object_current"] = True
                bad_manifest["table_versions_json"] = json.dumps(
                    bad_versions, sort_keys=True, separators=(",", ":"))
                bad_manifest_bytes = json.dumps(bad_manifest, sort_keys=True,
                    separators=(",", ":")).encode()
                bad_receipt_bytes = encode_truss_destination_receipt(
                    request_bytes, bad_manifest_bytes, VECTOR)
                with self.assertRaisesRegex(ManagedA1BridgeError, "versions differ"):
                    _validate_plan(plan, request_bytes, bad_manifest_bytes,
                                   bad_receipt_bytes, VECTOR)
            with self.assertRaisesRegex(ManagedA1BridgeError, "request bytes differ"):
                _validate_plan(plan, b'{"different":true}', manifest_bytes,
                               receipt_bytes, VECTOR)
            self.assertGreater(len(receipt_bytes), 0)


if __name__ == "__main__":
    unittest.main()
