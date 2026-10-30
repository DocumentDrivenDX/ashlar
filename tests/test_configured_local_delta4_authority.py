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
    rehydrate_delta4_fresh_head_admission,
    retain_delta4_output_completion_receipt,
)
from ashlar_host.configured_local_runtime import ConfiguredLocalAdmission
from ashlar_host.configured_source_local import ConfiguredLocalBatchSpec, ConfiguredLocalSource
from ashlar_host.delta_custody import (LocalDeltaEffects, LocalDeltaError,
                                       original_operation_intent,
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
        for role, target in ((role, transport.targets["local.configured." + role])
                             for role in INSTALLATION_ROLES):
            fake.native_metadata[role][1] = json.dumps({
                "profile": "ashlar-local-delta-installation-commit/0.1",
                "installation_id": transport.installation_id,
                "registry_sha256": transport.registry_sha,
                "journal_sha256": transport.journal_sha,
                "uuid": target.uuid,
            }, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
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

    def reopen_authority(self, transport, effect, source):
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
        fresh2 = rehydrate_delta4_fresh_head_admission(
            self.plan, effect, reopened, reopened.installation_profile, source2, context)
        authority2 = open_configured_local_delta4_authority(
            self.plan, effect, reopened, reopened.installation_profile, fresh2,
            source2, admission2, context, reopened.policy)
        return reopened, source2, fresh2, authority2

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
        retained_plan = transport.db.execute(
            "SELECT original,digest FROM local_plan WHERE operation=?", (operation,)).fetchone()
        if retained_plan is None:
            transport.db.execute("INSERT INTO local_plan VALUES(?,?,?)",
                                 (operation, original, plan_digest))
        else:
            self.assertEqual(retained_plan, (original, plan_digest))
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
            fake.native_operations[role][version] = "WRITE"
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

    def _seed_submitted_step(self, fake, transport, effect, index):
        """Fake one native commit after SQL dispatch but before receipt storage."""
        from ashlar_host.delta_custody import original_operation_intent
        operation = "effects:" + effect.intent_digest
        original, _digest, identities = LocalDeltaEffects(transport).original_plan(
            operation, effect.intent_digest, effect.steps)
        full_rows = materialize_delta4_expected_rows(self.plan, fake.lookup_hashes)
        prefix = index + 1
        roles = ["object_current", "edge_current", "whole_source_history"]
        fake.expected = {role: (full_rows[role] if role in roles[:prefix] else [])
                         for role in INSTALLATION_ROLES}
        step = effect.steps[index]
        target = transport._target(step["statement"])
        key = identities[index]
        intent = original_operation_intent(
            transport.installation_id, key, effect.intent_digest,
            step["statement"], step["parameters"], target,
            transport.operation_capacity)
        body_sha = custody_sha(intent)
        metadata = json.dumps({"profile": "ashlar-local-delta-commit/0.1",
            "installation_id": transport.installation_id, "operation": key,
            "operation_sha256": body_sha, "request_digest": effect.intent_digest},
            sort_keys=True, separators=(",", ":"))
        role = target.table.rsplit(".", 1)[-1]
        fake.histories[role] = [2, 1, 0]
        fake.native_metadata[role][2] = metadata
        fake.native_operations[role][2] = "WRITE"
        snapshot = transport._snapshot(target, 2)
        receipt = {"profile": "ashlar-local-delta-receipt/0.1",
            "operation": key, "operation_sha256": body_sha,
            "request_digest": effect.intent_digest, "table": target.table,
            "uuid": target.uuid, "before_version": 1, "version": 2,
            "commit_metadata": metadata, "native_operation": "WRITE",
            "snapshot": snapshot}
        encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False)
        return (key, intent, body_sha, encoded, {
            "version": "2", "uuid": target.uuid,
            "operation_sha256": body_sha,
            "snapshot_sha256": snapshot["row_sha256"]})

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

    def test_rehydrated_baseline_reconciles_restart_prefixes(self):
        for prefix in (1, 2, 3):
            with self.subTest(prefix=prefix), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); root.chmod(0o700)
                fake, transport, effect, source, _admission, authority = self.build(root)
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority.hold():
                        pass
                    self._seed_committed_prefix(fake, transport, effect, prefix)
                    reopened, _source2, restored, authority2 = self.reopen_authority(
                        transport, effect, source)
                    self.assertEqual(restored.heads, authority.fresh.heads)
                    result = authority2.reconcile_effect_prefix()
                self.assertEqual(result["committed_prefix"], prefix)
                self.assertIsNone(reopened.policy.active)

    def test_baseline_rehydration_refuses_corrupt_scope_and_nonempty_history(self):
        for fault in ("corrupt_scope", "nonempty_baseline", "forged_version_zero"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); root.chmod(0o700)
                fake, transport, effect, source, _admission, authority = self.build(root)
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority.hold():
                        pass
                if fault == "corrupt_scope":
                    transport.db.execute(
                        "UPDATE local_source_plan SET original=original || ' '")
                    transport.db.commit()
                elif fault == "forged_version_zero":
                    import hashlib
                    row_key, original = transport.db.execute(
                        "SELECT request_digest,original FROM local_source_plan").fetchone()
                    envelope = json.loads(original)
                    scope = envelope["scope"]
                    heads = json.loads(scope["fresh_heads_json"])
                    inventory = json.loads(scope["fresh_inventory_json"])
                    for role in INSTALLATION_ROLES:
                        heads[role]["version"] = 0
                        inventory[role]["version"] = 0
                    canonical = lambda value: json.dumps(
                        value, sort_keys=True, separators=(",", ":"),
                        ensure_ascii=False, allow_nan=False)
                    scope["fresh_heads_json"] = canonical(heads)
                    scope["fresh_heads_sha256"] = hashlib.sha256(
                        scope["fresh_heads_json"].encode()).hexdigest()
                    scope["fresh_inventory_json"] = canonical(inventory)
                    scope["fresh_inventory_sha256"] = hashlib.sha256(
                        scope["fresh_inventory_json"].encode()).hexdigest()
                    envelope["scope_sha256"] = hashlib.sha256(
                        canonical(scope).encode()).hexdigest()
                    transport.db.execute(
                        "UPDATE local_source_plan SET original=? WHERE request_digest=?",
                        (canonical(envelope), row_key))
                    transport.db.commit()
                else:
                    fake.expected = materialize_delta4_expected_rows(
                        self.plan, fake.lookup_hashes)
                    fake.baseline_rows["object_current"] = list(
                        fake.expected["object_current"])
                context = object()
                reopened = copy.copy(transport)
                reopened.policy = PrivatePolicy(context, tuple(reopened.targets.values()))
                reopened.policy.initializing = False
                source2 = object.__new__(ConfiguredLocalSource)
                source2.context = context; source2.graph = True; source2._active = True
                source2.batches = (object(),)
                source2._specs = (ConfiguredLocalBatchSpec(1, self.plan.request_json,
                                                           self.plan.publication_id),)
                source2.renew = lambda _selected: None
                source2.metadata = source.metadata
                with self.assertRaises(ValueError):
                    rehydrate_delta4_fresh_head_admission(
                        self.plan, effect, reopened, reopened.installation_profile,
                        source2, context)

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

    def test_delta4_prepare_only_grant_retains_plan_and_denies_operations(self):
        from ashlar_host.delta_custody import LocalDeltaEffects
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, transport, effect, _source, _admission, authority = self.build(root)
            original_prepare = LocalDeltaEffects.prepare
            observed = {}

            def guarded_prepare(effects, operation, intent_digest, steps, *, context):
                expected, _digest, _ids = effects.original_plan(
                    operation, intent_digest, steps)
                transport.policy.admit(json.loads(expected), context)
                observed["plan"] = expected
                with self.assertRaises(PermissionError):
                    transport.policy.admit({
                        "profile": "ashlar-local-delta-operation/0.1",
                        "request_digest": intent_digest,
                    }, context)
                return original_prepare(effects, operation, intent_digest,
                                        steps, context=context)

            with patch.object(LocalDeltaEffects, "prepare", guarded_prepare):
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority.hold():
                        result = authority.prepare_effect_plan()
                        self.assertIsNone(transport.policy._delta4_prepare_owner)
                        self.assertIsNone(transport.policy._delta4_prepare_intent)
                        self.assertEqual(result["intent_digest"], effect.intent_digest)
                        self.assertEqual(transport.db.execute(
                            "SELECT count(*) FROM local_operation").fetchone(), (0,))
                    prefix = authority.reconcile_effect_prefix()
            self.assertEqual(prefix["committed_prefix"], 0)
            self.assertEqual(prefix["states"], ["missing", "missing", "missing"])
            self.assertEqual(transport.db.execute(
                "SELECT operation,digest FROM local_plan").fetchone(),
                ("effects:" + effect.intent_digest,
                 result["plan_sha256"]))
            self.assertIsNotNone(observed.get("plan"))
            self.assertIsNone(transport.policy._delta4_prepare_owner)

    def test_delta4_prepare_refuses_changed_retained_plan_and_revokes_grant(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, transport, effect, _source, _admission, authority = self.build(root)
            operation = "effects:" + effect.intent_digest
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    transport.db.execute("INSERT INTO local_plan VALUES(?,?,?)",
                                         (operation, "{}", "0" * 64))
                    transport.db.commit()
                    with self.assertRaises(LocalDeltaError):
                        authority.prepare_effect_plan()
                    self.assertIsNone(transport.policy._delta4_prepare_owner)
                    self.assertIsNone(transport.policy._delta4_prepare_intent)
            self.assertIsNone(transport.policy._delta4_prepare_owner)

    def test_prepared_tail_observer_rejects_tampered_intent_before_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            _fake, transport, effect, _source, _admission, authority = self.build(root)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    authority.prepare_effect_plan()
                    operation = "effects:" + effect.intent_digest
                    _original, _digest, identities = LocalDeltaEffects(transport).original_plan(
                        operation, effect.intent_digest, effect.steps)
                    step = effect.steps[0]
                    target = transport._target(step["statement"])
                    key = identities[0]
                    intent = original_operation_intent(
                        transport.installation_id, key, effect.intent_digest,
                        step["statement"], step["parameters"], target,
                        transport.operation_capacity)
                    transport.db.execute(
                        "INSERT INTO local_operation VALUES(?,?,?,?,?,NULL)",
                        (key, intent + " ", custody_sha(intent), 1, "prepared"))
                    transport.db.commit()
                    with self.assertRaisesRegex(ValueError, "operation custody"):
                        authority._observe_effect_prefix(allow_prepared=True)

    def _simulate_delta4_effects(self, effects, operation, intent_digest, steps, *, context,
                                 failure=None):
        from ashlar_host.delta_custody import original_operation_intent
        original, _digest, ids = effects.original_plan(operation, intent_digest, steps)
        effects.transport.policy.admit(json.loads(original), context)
        receipts = []
        for index, step in enumerate(steps):
            key = ids[index]
            target = effects.transport._target(step["statement"])
            intent = json.loads(original_operation_intent(
                effects.transport.installation_id, key, intent_digest,
                step["statement"], step["parameters"], target,
                effects.transport.operation_capacity))
            effects.transport.policy.admit(intent, context)
            row = effects.transport.db.execute(
                "SELECT intent,intent_sha,state,receipt FROM local_operation WHERE operation=?",
                (key,)).fetchone()
            if failure == ("before_dispatch", index) and row is None:
                effects.transport.db.execute(
                    "INSERT INTO local_operation VALUES(?,?,?,?,?,NULL)",
                    (key, original_operation_intent(
                        effects.transport.installation_id, key, intent_digest,
                        step["statement"], step["parameters"], target,
                        effects.transport.operation_capacity),
                     custody_sha(original_operation_intent(
                        effects.transport.installation_id, key, intent_digest,
                        step["statement"], step["parameters"], target,
                        effects.transport.operation_capacity)), 1, "prepared"))
                effects.transport.db.commit()
                raise RuntimeError("simulated pre-dispatch stop")
            if row is None or row[2] == "prepared":
                key, raw, body_sha, receipt, result = self._seed_submitted_step(
                    self._active_fake, effects.transport, self._active_effect, index)
                if row is None:
                    effects.transport.db.execute(
                        "INSERT INTO local_operation VALUES(?,?,?,?,?,NULL)",
                        (key, raw, body_sha, 1, "submitted"))
                else:
                    effects.transport.db.execute(
                        "UPDATE local_operation SET state='submitted' WHERE operation=?",
                        (key,))
                effects.transport.db.commit()
                if failure == ("wrong_native_metadata", index):
                    role = target.table.rsplit(".", 1)[-1]
                    fake = self._active_fake
                    fake.native_metadata[role][2] = json.dumps({
                        "profile": "ashlar-local-delta-commit/0.1",
                        "installation_id": effects.transport.installation_id,
                        "operation": key + ":forged",
                        "operation_sha256": body_sha,
                        "request_digest": intent_digest,
                    }, sort_keys=True, separators=(",", ":"))
                effects.transport.policy.admit(intent, context)
                if failure == ("after_native_commit", index):
                    raise RuntimeError("simulated uncertain completion after native commit")
                from ashlar_host.local_receipt_codec import encode_local_receipt
                effects.transport.db.execute(
                    "UPDATE local_operation SET state='committed',receipt=? WHERE operation=?",
                    (encode_local_receipt(receipt), key))
                effects.transport.db.commit()
                receipts.append([result])
            elif row[2] == "committed":
                effects.transport.policy.admit(intent, context)
                proof = json.loads(b"".join(original_receipt_chunks(row[3])).decode())
                receipts.append([{"version": str(proof["version"]),
                                  "uuid": proof["uuid"],
                                  "operation_sha256": proof["operation_sha256"],
                                  "snapshot_sha256": proof["snapshot"]["row_sha256"]}])
                effects.transport.policy.admit(intent, context)
        effects.transport.policy.admit(json.loads(original), context)
        return {"plan_sha256": _digest, "intent_digest": intent_digest,
                "original_native_receipts": receipts}

    def test_delta4_execution_commits_and_advances_closing_vector(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, _source, _admission, authority = self.build(root)
            self._active_fake, self._active_effect = fake, effect
            def run_stub(effects, operation, intent_digest, steps, *, context):
                return self._simulate_delta4_effects(
                    effects, operation, intent_digest, steps, context=context,
                    failure=getattr(self, "_active_failure", None))
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    authority.prepare_effect_plan()
                    with patch.object(LocalDeltaEffects, "run", run_stub):
                        answer = authority.execute_effect_plan()
                    self.assertEqual(answer["observation"]["committed_prefix"], 3)
                    self.assertIsNone(transport.policy._delta4_execution_owner)
                    self.assertEqual(transport.db.execute(
                        "SELECT count(*) FROM local_operation").fetchone(), (3,))
                self.assertEqual(authority._delta4_closing_heads["publication_manifest"]["version"], 1)
            self.assertIsNone(transport.policy._delta4_execution_owner)

    def test_delta4_output_receipt_is_durable_exact_and_does_not_publish_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, _source, _admission, authority = self.build(root)
            self._active_fake, self._active_effect = fake, effect
            def run_stub(effects, operation, intent_digest, steps, *, context):
                return self._simulate_delta4_effects(
                    effects, operation, intent_digest, steps, context=context)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    authority.prepare_effect_plan()
                    with patch.object(LocalDeltaEffects, "run", run_stub):
                        authority.execute_effect_plan()
                    retained = retain_delta4_output_completion_receipt(authority)
                    replay = retain_delta4_output_completion_receipt(authority)
                    self.assertEqual(replay, retained)
                    original = json.loads(retained["original"])
                    body = original["receipt"]
                    self.assertEqual(body["profile"],
                                     "ashlar-configured-local-delta4-output-completion/0.1")
                    self.assertEqual(len(json.loads(body["installation_inventory_json"])), 6)
                    self.assertEqual(len(body["operation_receipts"]), 3)
                    self.assertEqual(fake.histories["publication_manifest"], [1, 0])
                    self.assertEqual(fake.histories["publication_attempt_phase"], [1, 0])
                    transport.db.execute(
                        "UPDATE local_delta4_output_receipt SET original=original || ' '")
                    transport.db.commit()
                    with self.assertRaises(ValueError):
                        retain_delta4_output_completion_receipt(authority)

    def test_delta4_output_receipt_requires_current_hold_completion_witness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, source, _admission, authority = self.build(root)
            self._active_fake, self._active_effect = fake, effect
            def run_stub(effects, operation, intent_digest, steps, *, context):
                return self._simulate_delta4_effects(
                    effects, operation, intent_digest, steps, context=context)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    authority.prepare_effect_plan()
                    with patch.object(LocalDeltaEffects, "run", run_stub):
                        authority.execute_effect_plan()

                reopened, _source2, _fresh2, reopened_authority = self.reopen_authority(
                    transport, effect, source)
                with self.assertRaisesRegex(ValueError, "heads differ"):
                    with reopened_authority.hold():
                        with self.assertRaisesRegex(ValueError, "completion witness"):
                            retain_delta4_output_completion_receipt(reopened_authority)
                self.assertIsNone(reopened.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    ("local_delta4_output_receipt",)).fetchone())

    def test_delta4_same_authority_reentry_resets_witness_until_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, _source, _admission, authority = self.build(root)
            self._active_fake, self._active_effect = fake, effect
            def run_stub(effects, operation, intent_digest, steps, *, context):
                return self._simulate_delta4_effects(
                    effects, operation, intent_digest, steps, context=context)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    authority.prepare_effect_plan()
                    with patch.object(LocalDeltaEffects, "run", run_stub):
                        authority.execute_effect_plan()
                # The prior hold's vector must not authorize receipt creation
                # in a later hold, even though all three effects remain committed.
                with self.assertRaisesRegex(ValueError, "heads differ"):
                    with authority.hold():
                        self.assertIsNone(authority._delta4_closing_heads)
                        with self.assertRaisesRegex(ValueError, "completion witness"):
                            retain_delta4_output_completion_receipt(authority)
                self.assertIsNone(transport.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    ("local_delta4_output_receipt",)).fetchone())

                # Exact reconciliation in this new hold re-establishes the
                # witness without resubmitting committed graph operations.
                with authority.hold():
                    with patch.object(LocalDeltaEffects, "recover", run_stub):
                        observed = authority.execute_effect_plan()
                    self.assertEqual(observed["observation"]["committed_prefix"], 3)
                    retained = retain_delta4_output_completion_receipt(authority)
                    self.assertEqual(retained["request_digest"],
                                     self.plan.request["request_digest"])

    def test_delta4_post_commit_mismatch_quarantines_and_revokes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, _source, _admission, authority = self.build(root)
            self._active_fake, self._active_effect = fake, effect
            self._active_failure = ("wrong_native_metadata", 0)
            def run_stub(effects, operation, intent_digest, steps, *, context):
                return self._simulate_delta4_effects(
                    effects, operation, intent_digest, steps, context=context,
                    failure=self._active_failure)
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with self.assertRaises(ValueError):
                    with authority.hold():
                        authority.prepare_effect_plan()
                        with patch.object(LocalDeltaEffects, "run", run_stub):
                            authority.execute_effect_plan()
            self.assertIsNone(transport.policy._delta4_execution_owner)
            self.assertIsNone(transport.policy._delta4_execution_intent)
            self.assertIsNone(transport.policy._delta4_execution_ids)
            self.assertEqual(transport.db.execute(
                "SELECT state,receipt FROM local_operation").fetchone(),
                ("submitted", None))
            self.assertIsNone(authority._delta4_inflight_index)
            self.assertIsNone(authority._delta4_inflight_state)

    def test_delta4_pre_dispatch_failure_clears_inflight_for_same_owner_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            fake, transport, effect, _source, _admission, authority = self.build(root)
            self._active_fake, self._active_effect = fake, effect
            self._active_failure = ("before_dispatch", 0)
            def run_stub(effects, operation, intent_digest, steps, *, context):
                return self._simulate_delta4_effects(
                    effects, operation, intent_digest, steps, context=context,
                    failure=getattr(self, "_active_failure", None))
            with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                with authority.hold():
                    authority.prepare_effect_plan()
                    with patch.object(LocalDeltaEffects, "run", run_stub), \
                         patch.object(LocalDeltaEffects, "recover", run_stub):
                        with self.assertRaisesRegex(RuntimeError, "pre-dispatch"):
                            authority.execute_effect_plan()
                        self.assertIsNone(authority._delta4_inflight_index)
                        self.assertIsNone(authority._delta4_inflight_state)
                        self._active_failure = None
                        result = authority.execute_effect_plan()
                        self.assertEqual(result["observation"]["committed_prefix"], 3)
            self.assertIsNone(transport.policy._delta4_execution_owner)

    def test_delta4_execution_recovers_committed_prefix_and_quarantines_uncertain(self):
        for fault in (None, "uncertain"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); root.chmod(0o700)
                fake, transport, effect, source, _admission, authority = self.build(root)
                self._active_fake, self._active_effect = fake, effect
                def recover_stub(effects, operation, intent_digest, steps, *, context):
                    return self._simulate_delta4_effects(
                        effects, operation, intent_digest, steps, context=context)
                with patch.object(ConfiguredLocalAdmission, "_admit", return_value=None):
                    with authority.hold():
                        authority.prepare_effect_plan()
                    self._seed_committed_prefix(fake, transport, effect, 1)
                    reopened, _source2, _fresh2, authority2 = self.reopen_authority(
                        transport, effect, source)
                    self._active_fake = fake
                    if fault == "uncertain":
                        # Retain the exact ordinal intent in submitted state with
                        # no native commit. Recovery must refuse and not retry it.
                        key, raw, body_sha, _receipt, _result = self._seed_submitted_step(
                            fake, reopened, effect, 1)
                        fake.histories["edge_current"] = [1, 0]
                        fake.native_metadata["edge_current"].pop(2, None)
                        reopened.db.execute(
                            "INSERT INTO local_operation VALUES(?,?,?,?,?,NULL)",
                            (key, raw, body_sha, 1, "submitted"))
                        reopened.db.commit()
                        with self.assertRaises(ValueError):
                            with authority2.hold():
                                authority2.execute_effect_plan()
                        self.assertIsNone(reopened.policy._delta4_execution_owner)
                        continue
                    with patch.object(LocalDeltaEffects, "recover", recover_stub):
                        with authority2.hold():
                            result = authority2.execute_effect_plan()
                    self.assertEqual(result["observation"]["committed_prefix"], 3)
                    self.assertIsNone(reopened.policy._delta4_execution_owner)


if __name__ == "__main__":
    unittest.main()
