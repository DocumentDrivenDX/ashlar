# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert owner-controlled authority checks for `.4` transaction one."""
from pathlib import Path
import copy
from types import SimpleNamespace
import json
import tempfile
import unittest
from unittest.mock import patch

from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
from ashlar_host.configured_local_delta4_publication import (
    INSTALLATION_ROLES, admit_delta4_fresh_empty_installation,
    build_delta4_transaction1_effect_plan,
    open_configured_local_delta4_authority,
    plan_configured_local_delta4_transaction1,
)
from ashlar_host.configured_local_runtime import ConfiguredLocalAdmission
from ashlar_host.configured_source_local import ConfiguredLocalBatchSpec, ConfiguredLocalSource
from ashlar_host.delta_custody import (LocalDeltaEffects, original_operation_intent,
                                       sha as custody_sha)
from ashlar_host.local_receipt_codec import encode_local_receipt, original_receipt_chunks
from ashlar_host.configured_local_delta4_publication import materialize_delta4_expected_rows
from ashlar_host.driver import PrivatePolicy
from test_configured_local_delta4_snapshots import CONFIG, FakeTransport


class Tests(unittest.TestCase):
    def setUp(self):
        configuration = load_typed_graph_jsonl_configuration(CONFIG.resolve())
        self.plan = plan_configured_local_delta4_transaction1(
            configuration, stream="gate3-typed-graph", materialized_at_us="1791547200000000")
        self.context = object()

    def build(self, root):
        fake = FakeTransport(self.plan)
        transport = fake.transport()
        transport.installation_id = "delta4-inert-installation"
        transport.registry_sha = "a" * 64
        transport.journal_sha = "b" * 64
        transport.operation_capacity = None
        transport.journal_path = root / "operations.sqlite"
        transport.journal_path.touch(mode=0o600)
        transport.db.execute("DROP TABLE local_operation")
        transport.db.execute("CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)")
        transport.db.execute("DROP TABLE local_plan")
        transport.db.execute("CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)")
        transport.db.execute("DROP TABLE local_source_plan")
        transport.db.execute("CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)")
        transport.policy = PrivatePolicy(self.context, tuple(transport.targets.values()))
        transport.policy.initializing = False
        effect = build_delta4_transaction1_effect_plan(
            self.plan, tables={role: "local.configured." + role
                               for role in INSTALLATION_ROLES},
            installation_profile=fake.profile)
        fresh = admit_delta4_fresh_empty_installation(transport, fake.profile)

        source = object.__new__(ConfiguredLocalSource)
        source.context = self.context
        source.graph = True
        source._active = True
        source.batches = (object(),)
        source._specs = (ConfiguredLocalBatchSpec(
            1, self.plan.request_json, self.plan.publication_id),)
        source.renew = lambda context: self.assertIs(context, self.context)
        source.metadata = lambda context: {
            "profile": "test-source-custody/1",
            "configuration_sha256": self.plan.configuration_sha256,
            "source_sha256": self.plan.source_sha256,
            "model_sha256": self.plan.model_sha256,
            "admission_report_sha256": "f" * 64, "batches": 1,
        }
        admission = object.__new__(ConfiguredLocalAdmission)
        admission.source = source
        admission.context = self.context
        authority = open_configured_local_delta4_authority(
            self.plan, effect, transport, fake.profile, fresh, source,
            admission, self.context, transport.policy)
        return fake, transport, effect, source, admission, authority

    def test_durable_source_scope_is_byte_exact_across_reopen_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, source, _admission, authority = self.build(root)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    row = transport.db.execute(
                        "SELECT request_digest,original FROM local_source_plan").fetchone()
                    self.assertEqual(row[0], self.plan.request["request_digest"])
                    record = json.loads(row[1])
                    self.assertEqual(record["profile"],
                                     "ashlar-configured-local-delta4-source-scope/0.1")
                    self.assertEqual(record["scope"]["request"]["json"],
                                     self.plan.request_json)
                    self.assertEqual(record["scope"]["effect_plan"]["steps_json"],
                                     effect.steps_json)
                    self.assertEqual(record["scope"]["installation_id"],
                                     transport.installation_id)
                    scope_digest = authority.source_scope_sha256

                # A new authority owner over the same retained journal can
                # only replay the exact original source/effect/custody bytes.
                context = object()
                reopened = copy.copy(transport)
                reopened.policy = PrivatePolicy(context, tuple(reopened.targets.values()))
                reopened.policy.initializing = False
                source2 = object.__new__(ConfiguredLocalSource)
                source2.context = context; source2.graph = True; source2._active = True
                source2.batches = (object(),)
                source2._specs = (ConfiguredLocalBatchSpec(1, self.plan.request_json,
                                                           self.plan.publication_id),)
                source2.renew = lambda selected: self.assertIs(selected, context)
                source2.metadata = source.metadata
                admission2 = object.__new__(ConfiguredLocalAdmission)
                admission2.source = source2; admission2.context = context
                fresh2 = admit_delta4_fresh_empty_installation(reopened,
                                                                 reopened.installation_profile)
                authority2 = open_configured_local_delta4_authority(
                    self.plan, effect, reopened, reopened.installation_profile, fresh2,
                    source2, admission2, context, reopened.policy)
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority2.hold():
                        self.assertEqual(authority2.source_scope_sha256, scope_digest)
                        self.assertEqual(transport.db.execute(
                            "SELECT count(*) FROM local_source_plan").fetchone(), (1,))

    def test_source_scope_refuses_changed_custody_missing_or_corrupt_record(self):
        for fault in ("changed", "missing", "corrupt"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); root.chmod(0o700)
                _fake, transport, _effect, source, _admission, authority = self.build(root)
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority.hold():
                        pass
                if fault == "changed":
                    source.metadata = lambda _context: {"profile": "changed"}
                elif fault == "missing":
                    transport.db.execute("DELETE FROM local_source_plan"); transport.db.commit()
                else:
                    transport.db.execute("UPDATE local_source_plan SET original=original || ' '")
                    transport.db.commit()
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with self.assertRaises(ValueError):
                        with authority.hold():
                            self.fail("changed or missing source scope must not be recreated")

    def test_source_scope_rechecks_retained_bytes_during_close(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, transport, _effect, _source, _admission, authority = self.build(root)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with self.assertRaises(ValueError):
                    with authority.hold():
                        transport.db.execute(
                            "UPDATE local_source_plan SET original=original || ' '")
                        transport.db.commit()

    def test_source_scope_refuses_metadata_digest_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, _transport, _effect, source, _admission, authority = self.build(root)
            source.metadata = lambda _context: {
                "configuration_sha256": "0" * 64,
                "source_sha256": self.plan.source_sha256,
                "model_sha256": self.plan.model_sha256,
            }
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with self.assertRaises(ValueError):
                    with authority.hold():
                        self.fail("source custody must match the frozen plan")

    def _seed_committed_prefix(self, fake, transport, effect, prefix):
        operation = "effects:" + effect.intent_digest
        original, plan_digest, identities = LocalDeltaEffects(transport).original_plan(
            operation, effect.intent_digest, effect.steps)
        transport.db.execute("INSERT INTO local_plan VALUES(?,?,?)",
                             (operation, original, plan_digest))
        full_rows = materialize_delta4_expected_rows(self.plan, fake.lookup_hashes)
        fake.expected = {role: (full_rows[role] if role in
                       ["object_current", "edge_current", "whole_source_history"][:prefix]
                       else []) for role in INSTALLATION_ROLES}
        steps = effect.steps
        for index in range(prefix):
            target = transport._target(steps[index]["statement"])
            role = target.table.rsplit(".", 1)[-1]
            key = identities[index]
            intent = original_operation_intent(
                transport.installation_id, key, effect.intent_digest,
                steps[index]["statement"], steps[index]["parameters"], target,
                transport.operation_capacity)
            body_sha = custody_sha(intent)
            before = 1
            version = before + 1
            metadata = json.dumps({"profile": "ashlar-local-delta-commit/0.1",
                "installation_id": transport.installation_id, "operation": key,
                "operation_sha256": body_sha, "request_digest": effect.intent_digest},
                sort_keys=True, separators=(",", ":"))
            fake.histories[role] = [version, before, 0]
            fake.native_metadata[role][version] = metadata
            snapshot = transport._snapshot(target, version)
            receipt = {"profile": "ashlar-local-delta-receipt/0.1",
                "operation": key, "operation_sha256": body_sha,
                "request_digest": effect.intent_digest, "table": target.table,
                "uuid": target.uuid, "before_version": before, "version": version,
                "commit_metadata": metadata, "native_operation": "WRITE",
                "snapshot": snapshot}
            transport.db.execute("INSERT INTO local_operation VALUES(?,?,?,?,?,?)",
                (key, intent, body_sha, before, "committed",
                 encode_local_receipt(json.dumps(receipt, sort_keys=True,
                                                 separators=(",", ":"), ensure_ascii=False,
                                                 allow_nan=False))))
        transport.db.commit()

    def test_readonly_reconciliation_accepts_each_exact_committed_prefix(self):
        for prefix in range(4):
            with self.subTest(prefix=prefix), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); root.chmod(0o700)
                fake, transport, effect, _source, _admission, authority = self.build(root)
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority.hold():
                        pass
                    self._seed_committed_prefix(fake, transport, effect, prefix)
                    result = authority.reconcile_effect_prefix()
                self.assertEqual(result["committed_prefix"], prefix)
                self.assertEqual(result["states"], ["committed"] * prefix
                                 + ["missing"] * (3 - prefix))
                self.assertIsNone(transport.policy.active)

    def test_readonly_reconciliation_refuses_ambiguous_or_changed_prefix(self):
        for fault in ("prepared", "submitted", "missing_plan", "changed_plan", "extra_native",
                      "altered_receipt", "wrong_native_metadata", "noncontiguous",
                      "oversized_plan", "oversized_intent", "oversized_receipt"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); root.chmod(0o700)
                fake, transport, effect, _source, _admission, authority = self.build(root)
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority.hold():
                        pass
                    seeded_prefix = (2 if fault == "noncontiguous" else
                                     1 if fault in ("altered_receipt", "wrong_native_metadata",
                                                    "oversized_intent", "oversized_receipt") else 0)
                    self._seed_committed_prefix(fake, transport, effect, seeded_prefix)
                    if fault in ("prepared", "submitted"):
                        # Prefix zero has no operation rows; add exact first
                        # handle bytes but retain its uncertain state.
                        step = effect.steps[0]; target = transport._target(step["statement"])
                        operation = "effects:" + effect.intent_digest
                        _original, _digest, ids = LocalDeltaEffects(transport).original_plan(
                            operation, effect.intent_digest, effect.steps)
                        key = ids[0]
                        intent = original_operation_intent(
                            transport.installation_id, key, effect.intent_digest,
                            step["statement"], step["parameters"], target,
                            transport.operation_capacity)
                        transport.db.execute("INSERT INTO local_operation VALUES(?,?,?,?,?,NULL)",
                            (key, intent, custody_sha(intent), 1, fault))
                    elif fault == "missing_plan":
                        transport.db.execute("DELETE FROM local_plan")
                    elif fault == "changed_plan":
                        transport.db.execute("UPDATE local_plan SET original=original || ' '")
                    elif fault == "extra_native":
                        role = "object_current"
                        fake.histories[role] = [2, 1, 0]
                    elif fault == "altered_receipt":
                        stored, = transport.db.execute(
                            "SELECT receipt FROM local_operation").fetchone()
                        proof = json.loads(b"".join(original_receipt_chunks(stored)).decode())
                        proof["snapshot"]["row_sha256"] = "0" * 64
                        changed = json.dumps(proof, sort_keys=True, separators=(",", ":"),
                                             ensure_ascii=False, allow_nan=False)
                        transport.db.execute("UPDATE local_operation SET receipt=?",
                                             (encode_local_receipt(changed),))
                    elif fault == "wrong_native_metadata":
                        fake.native_metadata["object_current"][2] = json.dumps({
                            "profile": "ashlar-local-delta-commit/0.1",
                            "installation_id": transport.installation_id,
                            "operation": "effects:" + effect.intent_digest + ":wrong",
                            "operation_sha256": "0" * 64,
                            "request_digest": effect.intent_digest,
                        }, sort_keys=True, separators=(",", ":"))
                    elif fault == "oversized_plan":
                        transport.db.execute(
                            "UPDATE local_plan SET original=CAST(zeroblob(4194305) AS TEXT)")
                    elif fault == "oversized_intent":
                        transport.db.execute(
                            "UPDATE local_operation SET intent=CAST(zeroblob(4194305) AS TEXT)")
                    elif fault == "oversized_receipt":
                        transport.db.execute(
                            "UPDATE local_operation SET receipt=CAST(zeroblob(4194305) AS TEXT)")
                    else:
                        _original, _digest, ids = LocalDeltaEffects(transport).original_plan(
                            "effects:" + effect.intent_digest, effect.intent_digest,
                            effect.steps)
                        transport.db.execute("DELETE FROM local_operation WHERE operation=?",
                                             (ids[0],))
                    transport.db.commit()
                    with self.assertRaises(Exception):
                        authority.reconcile_effect_prefix()
                self.assertIsNone(transport.policy.active)

    def test_source_scope_refuses_duplicate_rows_and_unmarked_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, transport, _effect, _source, _admission, authority = self.build(root)
            transport.db.execute("DROP TABLE local_source_plan")
            transport.db.execute("CREATE TABLE local_source_plan (request_digest TEXT,original TEXT)")
            transport.db.execute("INSERT INTO local_source_plan VALUES ('x','{}'),('y','{}')")
            transport.db.commit()
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with self.assertRaises(ValueError):
                    with authority.hold():
                        self.fail("duplicate source-scope records must refuse")

    def test_effects_remain_denied_inside_source_and_head_hold(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, _source, _admission, authority = self.build(root)
            operation = "effects:" + effect.intent_digest
            steps = effect.steps
            retained, _digest, _identities = LocalDeltaEffects(transport).original_plan(
                operation, effect.intent_digest, steps)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    self.assertIsNone(transport.policy.active)
                    with self.assertRaises(PermissionError):
                        transport.policy.admit(json.loads(retained), self.context)
                    with self.assertRaises(PermissionError):
                        transport.policy.admit({"profile": "ashlar-local-delta-operation/0.1"},
                                               self.context)
            self.assertFalse(authority.held)
            with self.assertRaises(PermissionError):
                transport.policy.admit(json.loads(retained), self.context)

    def test_reentrant_hold_refuses_without_disturbing_outer_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, _transport, _effect, _source, _admission, authority = self.build(root)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    fd = authority._fd
                    with self.assertRaises(ValueError):
                        with authority.hold():
                            self.fail("nested hold must not enter")
                    self.assertIs(authority._fd, fd)
                    self.assertTrue(authority.held)

    def test_closing_native_head_drift_revokes_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, _source, _admission, authority = self.build(root)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with self.assertRaises(ValueError):
                    with authority.hold():
                        self.assertIsNone(transport.policy.active)
                        fake.histories["publication_manifest"] = [2, 1, 0]
            self.assertIsNone(transport.policy.active)
            self.assertFalse(authority.held)

    def test_closing_current_admission_failure_revokes_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, transport, _effect, _source, _admission, authority = self.build(root)
            with patch.object(ConfiguredLocalAdmission, "_admit",
                              side_effect=[None, ValueError("revoked")]):
                with self.assertRaisesRegex(ValueError, "revoked"):
                    with authority.hold():
                        self.assertIsNone(transport.policy.active)
            self.assertIsNone(transport.policy.active)
            self.assertFalse(authority.held)

    def test_source_request_mismatch_refuses_authority_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, transport, effect, source, admission, _authority = self.build(root)
            with patch.object(ConfiguredLocalSource, "spec",
                              return_value=SimpleNamespace(request=lambda: {"request_digest": "0" * 64})):
                with self.assertRaises(ValueError):
                    open_configured_local_delta4_authority(
                        self.plan, effect, transport, transport.installation_profile,
                        admit_delta4_fresh_empty_installation(transport,
                                                              transport.installation_profile),
                        source, admission, self.context, transport.policy)

    def test_existing_dot3_policy_admission_remains_unchanged(self):
        policy = PrivatePolicy(self.context, ())
        request_digest = "a" * 64
        policy.active = {"request": {"request_digest": request_digest}, "steps": []}
        before = policy.active
        policy.admit({"profile": "ashlar-local-delta-effects/0.1",
                      "intent_digest": request_digest, "steps": []}, self.context)
        self.assertIs(policy.active, before)


if __name__ == "__main__":
    unittest.main()
