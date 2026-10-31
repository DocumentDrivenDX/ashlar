"""Inert R20 proposal/hash/limit checks; no child or native process is started."""
from __future__ import annotations
import hashlib, json, tempfile
from pathlib import Path
import run_supervisor_r20 as supervisor
import r20_pre_migration_custody_worker as prefix
import r20_post_migration_admission as lifecycle

HERE = Path(__file__).resolve().parent
PROPOSAL = HERE / "full-lifecycle-scope-proposal-r20.json"
R16 = HERE.parents[1] / "current-upstream-release-20261024" / "truss051-r16-native-candidate-20261029"

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

class R20ProposalTests(__import__("unittest").TestCase):
    def test_frozen_r16_verifier_does_not_leave_source_on_child_import_path(self):
        scope=json.loads(PROPOSAL.read_bytes())
        previous=list(supervisor.sys.path)
        supervisor._verify_frozen_source_inputs(scope["source"])
        self.assertEqual(supervisor.sys.path,previous)

    def test_exact_worker_scope_and_historical_r16_bindings(self):
        scope=json.loads(PROPOSAL.read_bytes())
        self.assertEqual(scope["proposal_status"], "proposed_not_approved")
        self.assertIs(scope["approval_artifact_created"], False)
        self.assertIs(scope["native_effects_executed"], False)
        self.assertEqual(scope["workers"]["custody_prefix_sha256"], sha(HERE/"r20_pre_migration_custody_worker.py"))
        self.assertEqual(scope["workers"]["migration_readback_sha256"], sha(HERE/"r20_post_migration_admission.py"))
        self.assertEqual(scope["workers"]["supervisor_sha256"], sha(HERE/"run_supervisor_r20.py"))
        self.assertEqual(scope["source_custody"], prefix.FROZEN_SOURCE_CUSTODY)
        self.assertEqual(scope["recovery_source_custody"], prefix.RECOVERY_SOURCE_CUSTODY)
        self.assertEqual(scope["recovery_source_custody"], lifecycle.RECOVERY_SOURCE_CUSTODY)
        prefix.verify_recovery_source_custody(scope)
        lifecycle._verify_recovery_source_custody(scope)
        self.assertFalse((HERE / "approval-r20-exact-one-run.json").exists())
        self.assertFalse(Path(scope["output_root"]).exists())
        self.assertFalse(Path(scope["supervisor_output_directory"]).exists())
        for key, expected in (("selected_discrepancy", "selected_incomplete_pgdata_tree_sha256"),
                              ("cold_backup_copy", "cold_backup_tree_sha256")):
            observed = prefix.tree(Path(scope["recovery_source_custody"][key]["path"]))
            self.assertEqual(observed["tree_sha256"], scope["state_custody"][expected])
            self.assertEqual(observed["file_count"], scope["recovery_source_custody"][key]["file_count"])
            self.assertEqual(observed["bytes"], scope["recovery_source_custody"][key]["bytes"])
        prefix_scope = dict(scope)
        prefix_scope["profile"] = "ashlar-truss-r20-custody-prefix-scope/0.1"
        prefix_scope["authorization"] = "one_run_local_custody_prefix"
        prefix_scope["source"] = dict(scope["source"])
        prefix_scope["source"]["worker_sha256"] = scope["workers"]["custody_prefix_sha256"]
        prefix.verify_scope(prefix_scope)
        self.assertEqual(tuple(scope["migration"]["route"]), lifecycle.MIGRATION_EDGES)
        self.assertEqual(scope["seed_custody"]["semantic_approval_proposal"],
            "Approve this explicit local_truss_acceptance_trial origin meaning for the seven accepted seed operations. It is not historical acceptance-origin evidence and grants no production authority.")
        self.assertEqual(sha(R16/"full-lifecycle-scope-proposal-r16.json"),
            "911cdefe6d9ee68d8a55b14ceff6640dba21384d7b59c3d082c10ae069acb5a0")
        self.assertEqual(sha(R16/"approval-r16-exact-one-run.json"),
            "d2a3b206cc5fdf095f1f74499ddfe431dfadb3f9535bb3bc336efdd5d36d4b64")
        self.assertEqual(sha(R16/"r16-failure-evidence.json"),
            "ab8ee15cdd9a891473f2d910a049cb3166346d239b349ff7ce416ed8cb45627b")

    def test_frozen_helper_environment_and_registry_absence_receipt_are_exact(self):
        scope=json.loads(PROPOSAL.read_bytes())
        self.assertEqual(prefix.FROZEN_SOURCE_HELPER_ENV, "ASHLAR_R16_SOURCE_SRC")
        self.assertEqual(prefix._frozen_source_env(Path("/frozen/src")),
                         {"ASHLAR_R16_SOURCE_SRC": "/frozen/src"})
        state=scope["state_custody"]
        receipt={"profile":"ashlar-r20-registry-absence-before-migration/0.1",
                 "registry_path":state["registry_path"],
                 "exact_creation_rule":state["new_registry_pre_effect_backup_sha256_or_exact_creation_rule"],
                 "registry_created":False}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"registry-pre-effect.json"
            prefix.write_new(path,receipt)
            self.assertEqual(sha(path),state["new_registry_pre_effect_backup_sha256"])
        rule=state["fresh_process_inventory_rule"]
        self.assertIn("no postgres/pg_ctl owner",rule)
        self.assertIn("unrelated command-line path mentions are allowed",rule)

    def test_900_second_ceiling_is_consistent_and_rss_stays_at_two_gib(self):
        scope=json.loads(PROPOSAL.read_bytes())
        self.assertEqual(prefix.MAX_CHILD_DEADLINE_SECONDS, 900)
        self.assertEqual(supervisor.MAX_CHILD_DEADLINE_SECONDS, 900)
        self.assertEqual(lifecycle.MAX_CHILD_DEADLINE_SECONDS, 900)
        self.assertEqual(scope["limits"]["child_deadline_seconds"], 900)
        self.assertEqual(scope["stage_limits"]["preserve_restore_start"]["deadline_seconds"], 900)
        self.assertEqual(scope["limits"]["process_group_rss_bytes_max"], 2*1024**3)
        self.assertIn("15 minutes", scope["planning_estimate"]["cap_interpretation"])
        self.assertIn("not a performance acceptance target", scope["planning_estimate"]["cap_interpretation"])
