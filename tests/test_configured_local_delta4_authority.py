# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert owner-controlled authority checks for `.4` transaction one."""
from pathlib import Path
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
from ashlar_host.delta_custody import LocalDeltaEffects
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
        transport.operation_capacity = None
        transport.journal_path = root / "operations.sqlite"
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
        admission = object.__new__(ConfiguredLocalAdmission)
        admission.source = source
        admission.context = self.context
        authority = open_configured_local_delta4_authority(
            self.plan, effect, transport, fake.profile, fresh, source,
            admission, self.context, transport.policy)
        return fake, transport, effect, source, admission, authority

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
