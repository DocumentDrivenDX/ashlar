# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused local controller-protocol and uncertain-submit recovery checks."""
import json
import hashlib
import os
import copy
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from tools.run_managed_a1_serverless import (
    _LocalPinStages, _recover_run_after_lost_response, _run_stage, run,
    _put_volume_bytes,
    _job_runs,
    _validate_ack_settlement,
    _verify_terminal_proof,
    _check_artifact_lock,
    _directory_contents,
    _admit_job_readback, _job_settings,
    _write_private_json, DatabricksCLI, _admit_auth_readback_host,
    _phase_clock_after_manifest, _managed_serverless_protocol_profile,
    _admit_owned_uc_table_inventory,
)
from ashlar_host.a1_ack_attempt import A1AckSettlement
from ashlar_host.managed_a1_serverless_tasks import (
    _po_status, parse_predictive_optimization_status,
)


class PhaseClockTests(unittest.TestCase):
    def test_each_phase_clock_follows_exact_predecessor_and_is_stable(self):
        genesis = {"recorded_at": "1793677610661611"}
        a0_clock = _phase_clock_after_manifest("1790000000000000", genesis)
        self.assertEqual(a0_clock, "1793677610661612")
        self.assertEqual(_phase_clock_after_manifest(a0_clock, genesis), a0_clock)
        a0_manifest = {"recorded_at": a0_clock, "publication_id": "a0-id"}
        a1_clock = _phase_clock_after_manifest(a0_clock, a0_manifest)
        self.assertEqual(a1_clock, "1793677610661613")
        self.assertGreater(int(a1_clock), int(a0_manifest["recorded_at"]))
        for bad in ("01", "-1", str(2**63)):
            with self.assertRaises(ValueError):
                _phase_clock_after_manifest(a0_clock, {"recorded_at": bad})
        with self.assertRaises(ValueError):
            _phase_clock_after_manifest(str(2**63 - 1), {"recorded_at": str(2**63 - 1)})

    def test_generated_protocol_wire_decodes_to_exact_serverless_profile(self):
        from ashlar_host.configured_local_runtime import MANAGED_SERVERLESS_PROFILE
        wire = {"identity": MANAGED_SERVERLESS_PROFILE.identity,
            "reader_versions": sorted(MANAGED_SERVERLESS_PROFILE.reader_versions),
            "writer_versions": sorted(MANAGED_SERVERLESS_PROFILE.writer_versions),
            "features": sorted(MANAGED_SERVERLESS_PROFILE.features)}
        self.assertIs(_managed_serverless_protocol_profile(wire), MANAGED_SERVERLESS_PROFILE)
        altered = dict(wire, features=wire["features"] + ["unknownFeature"])
        with self.assertRaisesRegex(ValueError, "Exact closed"):
            _managed_serverless_protocol_profile(altered)

    def test_uc_table_ids_remain_distinct_from_delta_installation_uuids(self):
        roles = ("object_current", "edge_current", "tombstone",
            "whole_source_history", "publication_manifest", "publication_attempt_phase")
        principal = "owner@example.test"
        schema = "trial_schema"
        catalog = "client_dev"
        installation = {role: {"name": f"{catalog}.{schema}.{role}",
            "owner": principal, "uuid": f"00000000-0000-4000-8000-{index:012d}",
            "version": 0} for index, role in enumerate(roles, 1)}
        rows = [{"name": role, "full_name": f"{catalog}.{schema}.{role}",
            "catalog_name": catalog, "schema_name": schema, "table_type": "MANAGED",
            "owner": principal, "table_id": f"10000000-0000-4000-8000-{index:012d}"}
            for index, role in enumerate(roles, 1)]
        admitted = _admit_owned_uc_table_inventory(rows, installation,
            catalog=catalog, schema=schema, principal=principal)
        self.assertNotEqual(admitted[installation[roles[0]]["name"]],
                            installation[roles[0]]["uuid"])
        with self.assertRaisesRegex(ValueError, "exactly six"):
            _admit_owned_uc_table_inventory(rows[:-1], installation,
                catalog=catalog, schema=schema, principal=principal)


class _PinHandle:
    def __init__(self, events): self.events = events
    def renew(self): self.events.append("renew")
    def close(self): self.events.append("close")


class _PinOwner:
    native_timeout_seconds = 60
    def __init__(self): self.events = []
    def open_native_pin(self):
        self.events.append("open")
        return _PinHandle(self.events)


class PredictiveOptimizationTests(unittest.TestCase):
    def test_actual_inherited_describe_value_is_enabled_and_other_states_refuse(self):
        # Exact DESCRIBE TABLE EXTENDED row captured from the owned Run10 tables.
        actual = {"col_name": "Predictive Optimization", "comment": "",
            "data_type": "ENABLE (inherited from METASTORE metastore_centralus)"}

        class Row(dict):
            def asDict(self, recursive=True): return dict(self)

        class Frame:
            def collect(self): return [Row(actual)]

        class Spark:
            def sql(self, statement):
                assert statement == "DESCRIBE TABLE EXTENDED `client_dev`.`trial`.`object_current`"
                return Frame()

        self.assertEqual(_po_status(Spark(),
            "`client_dev`.`trial`.`object_current`"), "ENABLE")
        self.assertEqual(parse_predictive_optimization_status("ENABLE"), "ENABLE")
        self.assertEqual(parse_predictive_optimization_status("ENABLED"), "ENABLE")
        self.assertEqual(parse_predictive_optimization_status(
            "ENABLED (inherited from METASTORE central_us)"), "ENABLE")
        for value in ("DISABLE", "DISABLED", "INHERIT",
                      "DISABLE (inherited from METASTORE central_us)",
                      "ENABLE now", "ENABLE (inherited from CATALOG central_us)",
                      "ENABLE (inherited from METASTORE )", "ENABLE (METASTORE central_us)",
                      None, 1):
            self.assertIsNone(parse_predictive_optimization_status(value), repr(value))

        class DisabledSpark(Spark):
            def sql(self, statement):
                class DisabledFrame:
                    def collect(self):
                        return [Row({"col_name": "Predictive Optimization",
                                     "data_type": "ENABLE (inherited from METASTORE bad/name)"})]
                return DisabledFrame()

        with self.assertRaisesRegex(ValueError, "must be observed enabled"):
            _po_status(DisabledSpark(), "`client_dev`.`trial`.`object_current`")


class _RunsAPI:
    def __init__(self, pages): self.pages = iter(pages); self.cancelled = []; self.rows = []
    def call(self, method, path, body=None):
        if path.startswith("/api/2.2/jobs/runs/list"):
            page = next(self.pages)
            self.rows = page.get("runs", [])
            return page
        if path.startswith("/api/2.2/jobs/runs/get?run_id="):
            run_id = int(path.split("=")[-1])
            row = next(item for item in self.rows if item["run_id"] == run_id)
            return {**row, "state": {"life_cycle_state": "RUNNING"}}
        if path == "/api/2.2/jobs/runs/cancel":
            self.cancelled.append(body["run_id"])
            return {}
        raise AssertionError((method, path, body))


class _LostRunNowAPI:
    def __init__(self, *, visible=True):
        self.deadline = None
        self.visible = visible
        self.settings = None
        self.run_now_count = 0
        self.run_now_payload = None
        self.run_list_count = 0
        self.deleted = False
        self.cancelled = []

    def call(self, method, path, body=None):
        if method == "post" and path == "/api/2.2/jobs/create":
            self.settings = body
            return {"job_id": 17}
        if method == "get" and path == "/api/2.2/jobs/get?job_id=17":
            return {"settings": self.settings,
                    "run_as_user_name": self.settings["run_as"]["user_name"]}
        if method == "get" and path.startswith("/api/2.2/jobs/runs/list?"):
            self.run_list_count += 1
            rows = [] if self.run_list_count == 1 or not self.visible else [{"run_id": 29,
                "job_id": 17, "start_time": int(time.time() * 1000)}]
            return {"runs": rows, "next_page_token": None}
        if method == "post" and path == "/api/2.2/jobs/run-now":
            self.run_now_count += 1
            self.run_now_payload = body
            raise TimeoutError("response lost after server acceptance")
        if method == "get" and path == "/api/2.2/jobs/runs/get?run_id=29":
            return {"run_id": 29, "job_id": 17,
                "state": {"life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS"}, "tasks": []}
        if method == "get" and path == "/api/2.2/jobs/runs/get-output?run_id=29":
            return {"notebook_output": {"result": '{"ok":true}'}}
        if method == "post" and path == "/api/2.2/jobs/delete":
            self.deleted = True
            return {}
        if method == "post" and path == "/api/2.2/jobs/runs/cancel":
            self.cancelled.append(body["run_id"])
            return {}
        raise AssertionError((method, path, body))


class _ForeignRunNowAPI:
    def __init__(self):
        self.deadline = None; self.settings = None; self.deleted = False; self.cancelled = []
    def call(self, method, path, body=None):
        if method == "post" and path == "/api/2.2/jobs/create":
            self.settings = body
            return {"job_id": 17}
        if method == "get" and path == "/api/2.2/jobs/get?job_id=17":
            return {"settings": self.settings,
                    "run_as_user_name": self.settings["run_as"]["user_name"]}
        if method == "get" and path.startswith("/api/2.2/jobs/runs/list?"):
            return {}
        if method == "post" and path == "/api/2.2/jobs/run-now":
            self.token = body["idempotency_token"]
            return {"run_id": 29}
        if method == "get" and path == "/api/2.2/jobs/runs/get?run_id=29":
            return {"run_id": 29, "job_id": 86034381333009,
                "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
                "tasks": []}
        if method == "post" and path == "/api/2.2/jobs/runs/cancel":
            self.cancelled.append(body["run_id"])
            return {}
        if method == "post" and path == "/api/2.2/jobs/delete":
            self.deleted = True
            return {}
        raise AssertionError((method, path, body))


class _MixedOwnershipRunsAPI:
    def __init__(self): self.cancelled = []
    def call(self, method, path, body=None):
        if path.startswith("/api/2.2/jobs/runs/list"):
            return {"runs": [{"run_id": 418, "start_time": 100100},
                             {"run_id": 419, "start_time": 100200}],
                    "next_page_token": None}
        if path == "/api/2.2/jobs/runs/get?run_id=418":
            return {"run_id": 418, "job_id": 7, "state": {"life_cycle_state": "RUNNING"}}
        if path == "/api/2.2/jobs/runs/get?run_id=419":
            return {"run_id": 419, "job_id": 999, "state": {"life_cycle_state": "RUNNING"}}
        if method == "post" and path == "/api/2.2/jobs/runs/cancel":
            self.cancelled.append(body["run_id"])
            return {}
        raise AssertionError((method, path, body))


class ManagedA1ServerlessRunnerTests(unittest.TestCase):
    def test_auth_describe_accepts_sanitized_nested_host_and_legacy_host_only(self):
        from tools.run_managed_a1_serverless import HOST
        captured_path = Path("/private/tmp/ashlar-managed-auth-describe.json")
        if captured_path.is_file():
            raw = json.loads(captured_path.read_text(encoding="utf-8"))
            # Preserve only the public host/status/auth-type shape. The raw
            # configuration and token-storage payload never enter assertions.
            captured_sanitized = {
                "status": raw.get("status"),
                "username": raw.get("username"),
                "details": {"auth_type": raw.get("details", {}).get("auth_type"),
                            "host": raw.get("details", {}).get("host"),
                            "configuration": "<redacted>"},
                "token_storage": "<redacted>",
            }
            self.assertEqual(_admit_auth_readback_host(captured_sanitized, HOST), HOST)
        captured_sanitized = {
            "status": "success", "username": "runner@example.test",
            "details": {"auth_type": "oauth", "host": HOST,
                        "configuration": "<redacted>"},
            "token_storage": "<redacted>",
        }
        self.assertEqual(_admit_auth_readback_host(captured_sanitized, HOST), HOST)
        legacy = {"status": "success", "host": HOST}
        self.assertEqual(_admit_auth_readback_host(legacy, HOST), HOST)

        contradictory = copy.deepcopy(captured_sanitized)
        contradictory["host"] = "https://other.example.test"
        with self.assertRaisesRegex(ValueError, "missing or contradictory"):
            _admit_auth_readback_host(contradictory, HOST)
        malformed = copy.deepcopy(captured_sanitized)
        malformed["details"]["host"] = "adb-7405607548213398.18.azuredatabricks.net"
        with self.assertRaisesRegex(ValueError, "malformed"):
            _admit_auth_readback_host(malformed, HOST)
        failed = copy.deepcopy(captured_sanitized)
        failed["status"] = "failure"
        with self.assertRaisesRegex(ValueError, "did not report success"):
            _admit_auth_readback_host(failed, HOST)

    def test_workspace_exports_use_source_for_notebooks_and_auto_for_files(self):
        calls = []
        def fake_run(argv, **kwargs):
            calls.append(argv)
            return type("Completed", (), {"returncode": 0, "stderr": ""})()
        with patch("tools.run_managed_a1_serverless.subprocess.run", fake_run):
            cli = DatabricksCLI("aidev-cus")
            cli.export_workspace("/Workspace/notebook", "/tmp/notebook.py", notebook=True)
            cli.export_workspace("/Workspace/wheel.whl", "/tmp/wheel.whl", notebook=False)
        self.assertEqual(calls[0][calls[0].index("--format") + 1], "SOURCE")
        self.assertEqual(calls[1][calls[1].index("--format") + 1], "AUTO")

    def test_files_copy_overwrites_only_explicit_local_readback_targets(self):
        calls = []
        def fake_run(argv, **kwargs):
            calls.append(argv)
            return type("Completed", (), {"returncode": 0, "stderr": ""})()
        with patch("tools.run_managed_a1_serverless.subprocess.run", fake_run):
            cli = DatabricksCLI("aidev-cus")
            cli.copy("/tmp/source", "dbfs:/Volumes/client_dev/s/a", overwrite=True)
            cli.copy("/tmp/source", "dbfs:/Volumes/client_dev/s/b")
        self.assertEqual(calls[0][-1], "--overwrite")
        self.assertNotIn("--overwrite", calls[1])

        class FilesCopy:
            def __init__(self): self.remote = {}; self.calls = []
            def copy(self, source, target, *, overwrite=False):
                self.calls.append((source, target, overwrite))
                if source.startswith("dbfs:"):
                    payload = self.remote[source]
                    if Path(target).exists() and not overwrite:
                        return
                    Path(target).write_bytes(payload)
                else:
                    if target in self.remote and not overwrite:
                        return
                    self.remote[target] = Path(source).read_bytes()
        api = FilesCopy()
        payload = b"exact genesis bytes"
        result = _put_volume_bytes(api, "/Volumes/client_dev/ashlar_a1_x/trial/genesis.bin",
                                   payload)
        self.assertEqual(result["sha256"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(api.calls[0][2], False)  # immutable remote upload
        self.assertEqual(api.calls[1][2], True)   # precreated local readback only
        self.assertEqual(api.remote["dbfs:/Volumes/client_dev/ashlar_a1_x/trial/genesis.bin"],
                         payload)

    def test_fresh_job_run_inventory_accepts_omitted_runs_only_as_empty(self):
        class RunsAPI:
            def __init__(self, response): self.response = response; self.path = None
            def call(self, method, path):
                self.path = path
                return self.response
        empty = RunsAPI({})
        self.assertEqual(_job_runs(empty, 745992151311768), [])
        self.assertIn("limit=25", empty.path)
        explicit_null = RunsAPI({"runs": None})
        with self.assertRaisesRegex(ValueError, "complete fresh-job run inventory"):
            _job_runs(explicit_null, 745992151311768)
        continuation = RunsAPI({"runs": [], "next_page_token": "next"})
        with self.assertRaisesRegex(ValueError, "complete fresh-job run inventory"):
            _job_runs(continuation, 745992151311768)

    def test_artifact_lock_fetches_each_remote_dependency_once_per_admission(self):
        notebook = b"notebook bytes"
        artifacts = {"/Volumes/client_dev/shared/ashlar.whl": b"ashlar wheel",
                     "/Volumes/client_dev/shared/runtime.whl": b"runtime wheel"}
        dependencies = [{"path": path,
                         "sha256": hashlib.sha256(raw).hexdigest()}
                        for path, raw in artifacts.items()]
        config = {name: {"notebook_path": "/Workspace/ashlar/" + name,
                         "notebook_sha256": hashlib.sha256(notebook).hexdigest(),
                         "dependencies": copy.deepcopy(dependencies)}
                  for name in ("initializer", "publisher", "verifier")}

        class ArtifactAPI:
            def __init__(self): self.downloads = []; self.notebooks = []
            def export_workspace(self, path, target, *, notebook):
                self.notebooks.append(path)
                Path(target).write_bytes(globals_notebook)
            def copy(self, source, target, *, overwrite=False):
                self.downloads.append(source)
                Path(target).write_bytes(artifacts[source.removeprefix("dbfs:")])

        globals_notebook = notebook
        api = ArtifactAPI()
        _check_artifact_lock(api, config)
        self.assertEqual(len(api.notebooks), 3)
        self.assertEqual(api.downloads, ["dbfs:" + path for path in artifacts])

        conflicting = copy.deepcopy(config)
        conflicting["verifier"]["dependencies"][0]["sha256"] = "0" * 64
        refused = ArtifactAPI()
        with self.assertRaisesRegex(ValueError, "Conflicting dependency SHA-256"):
            _check_artifact_lock(refused, conflicting)
        self.assertEqual(refused.notebooks, [])
        self.assertEqual(refused.downloads, [])

    def test_empty_files_directory_may_omit_contents_but_not_null_or_pagination(self):
        self.assertEqual(_directory_contents({}, maximum_entries=16), [])
        self.assertEqual(_directory_contents({"contents": []}, maximum_entries=16), [])
        with self.assertRaisesRegex(ValueError, "Complete bounded directory inventory"):
            _directory_contents({"contents": None}, maximum_entries=16)
        with self.assertRaisesRegex(ValueError, "Complete bounded directory inventory"):
            _directory_contents({"contents": [], "next_page_token": "next"},
                                maximum_entries=16)

    def test_detached_initializer_mapping_shape_is_admitted_without_attribute_access(self):
        from ashlar_host.managed_a1_detached_admission import admit_progress
        from ashlar_host.managed_a1_serverless_tasks import _model_content_bytes
        from tests.test_managed_current_genesis import _progress
        import base64

        selected = _progress("A")
        evidence = {"progress": {
            "feed": selected.feed, "epoch": selected.epoch,
            "cursor_bytes_base64": base64.b64encode(selected.cursor_bytes).decode("ascii"),
            "position_sha256": selected.position_sha256,
            "catalog_registration_id": selected.catalog_registration_id,
            "catalog_registration_bytes_base64": base64.b64encode(
                selected.catalog_registration_bytes).decode("ascii"),
            "consumer_id": selected.consumer_id,
            "consumer_registration_id": selected.consumer_registration_id,
            "installation_id": selected.installation_id,
        }}
        self.assertEqual(admit_progress("A", evidence), selected)
        model_bytes = b"original selected model"
        self.assertEqual(_model_content_bytes({"pin": {}, "content": model_bytes}),
                         model_bytes)

    def test_actual_provider_probe_readback_admits_only_documented_defaults(self):
        from tools.prepare_a1_serverless_runtime_probe import build_payload
        from pathlib import Path

        submitted_path = Path("/private/tmp/ashlar-runtime-probe-job.json")
        readback_path = Path("/private/tmp/ashlar-runtime-probe-job-readback.json")
        if not submitted_path.is_file() or not readback_path.is_file():
            self.skipTest("Captured actual serverless Jobs probe pair is unavailable")
        submitted = json.loads(submitted_path.read_text(encoding="utf-8"))
        readback = json.loads(readback_path.read_text(encoding="utf-8"))
        protocol = submitted["tasks"][0]["notebook_task"]["base_parameters"]["protocol_identity"]
        current = build_payload(submitted["tasks"][0]["notebook_task"]["notebook_path"],
            Path(__file__).resolve().parents[1] / "notebooks/managed_a1/runtime_probe.py",
            int(submitted["environments"][0]["spec"]["environment_version"]),
            submitted["run_as"]["user_name"], protocol)
        self.assertNotIn("max_retries", current)
        self.assertNotIn("retry_on_timeout", current)
        self.assertEqual(current["tasks"][0]["max_retries"], 0)
        self.assertIs(current["tasks"][0]["retry_on_timeout"], False)
        self.assertIs(current["tasks"][0]["disable_auto_optimization"], True)
        a1_settings = _job_settings({"trial_id": "a1-readback-test",
            "expected_principal": submitted["run_as"]["user_name"],
            "initializer": {"timeout_seconds": 60}}, "initializer", {
                "task": {"task_key": "initializer", "depends_on": [],
                    "timeout_seconds": 60, "max_retries": 0, "retry_on_timeout": False,
                    "disable_auto_optimization": True,
                    "notebook_task": {"notebook_path": "/Workspace/initialize",
                        "source": "WORKSPACE", "base_parameters": {"task_parameters_json": "{}"}},
                    "environment_key": "initializer_env"},
                "environment": {"environment_key": "initializer_env",
                    "spec": {"environment_version": "6", "dependencies": [
                        {"library": {"whl": "/Workspace/ashlar.whl"}}]}}})
        self.assertNotIn("max_retries", a1_settings)
        self.assertNotIn("retry_on_timeout", a1_settings)
        self.assertEqual(a1_settings["tasks"][0]["max_retries"], 0)
        self.assertIs(a1_settings["tasks"][0]["retry_on_timeout"], False)
        self.assertIs(a1_settings["tasks"][0]["disable_auto_optimization"], True)
        expected = copy.deepcopy(current)
        expected["tasks"][0]["notebook_task"]["source"] = "WORKSPACE"
        expected["tags"] = readback["settings"]["tags"]
        # The captured unpaid probe predates the explicit task auto-optimization
        # disable. It must not be admitted for the next probe after a provider
        # retry occurred despite task max_retries=0.
        with self.assertRaisesRegex(ValueError, "pinned serverless artifact lock"):
            _admit_job_readback(readback, expected, None)
        readback = {"settings": copy.deepcopy(expected),
                    "run_as_user_name": expected["run_as"]["user_name"]}
        _admit_job_readback(readback, expected, None)

        changed = copy.deepcopy(readback)
        changed["settings"]["max_retries"] = 1
        with self.assertRaisesRegex(ValueError, "job-level retries"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["tasks"][0]["notebook_task"]["source"] = "GIT"
        with self.assertRaisesRegex(ValueError, "pinned serverless artifact lock"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["tasks"][0]["max_retries"] = 1
        with self.assertRaisesRegex(ValueError, "pinned serverless artifact lock"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["tasks"][0]["existing_cluster_id"] = "shared"
        with self.assertRaisesRegex(ValueError, "unselected task settings"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(readback)
        changed["settings"]["environments"][0]["spec"]["dependencies"] = [
            "/Volumes/client_dev/other.whl"]
        with self.assertRaisesRegex(ValueError, "artifact locks"):
            _admit_job_readback(changed, expected, None)

    def test_captured_borrowed_probe_admits_only_zero_task_retry_interval_default(self):
        from pathlib import Path
        actual_path = Path("/private/tmp/ashlar-runtime-probe-borrowed-job-actual.json")
        expected_path = Path("/private/tmp/ashlar-runtime-probe-borrowed-job-expected.json")
        if not actual_path.is_file() or not expected_path.is_file():
            self.skipTest("Captured borrowed-session Jobs create/readback pair is unavailable")
        actual = json.loads(actual_path.read_text(encoding="utf-8"))
        expected = json.loads(expected_path.read_text(encoding="utf-8"))
        _admit_job_readback(actual, expected, None)
        changed = copy.deepcopy(actual)
        changed["settings"]["tasks"][0]["min_retry_interval_millis"] = 1
        with self.assertRaisesRegex(ValueError, "pinned serverless artifact lock"):
            _admit_job_readback(changed, expected, None)
        changed = copy.deepcopy(actual)
        changed["settings"]["tasks"][0]["min_retry_interval_millis"] = False
        with self.assertRaisesRegex(ValueError, "pinned serverless artifact lock"):
            _admit_job_readback(changed, expected, None)

    def test_real_holder_settlement_matched_is_accepted_and_exact_bytes_are_checked(self):
        request, manifest, snapshot, next_checkpoint, receipt = (
            b"request", b"manifest", b"selection", b"next", b"receipt")
        settlement = A1AckSettlement("matched", next_checkpoint, receipt,
            hashlib.sha256(request).hexdigest(),
            hashlib.sha256(manifest).hexdigest(),
            hashlib.sha256(snapshot).hexdigest())
        _validate_ack_settlement(settlement, request_bytes=request,
            manifest_bytes=manifest, snapshot_bytes=snapshot,
            next_checkpoint_bytes=next_checkpoint, receipt_bytes=receipt)
        with self.assertRaisesRegex(ValueError, "did not reconcile"):
            _validate_ack_settlement(settlement, request_bytes=request,
                manifest_bytes=manifest, snapshot_bytes=snapshot,
                next_checkpoint_bytes=b"different", receipt_bytes=receipt)

    def test_ack_settlement_rejects_old_runner_only_outcome(self):
        settlement = A1AckSettlement("acknowledged_exactly_reconciled", b"next", b"receipt",
            "a", "b", "c")
        with self.assertRaisesRegex(ValueError, "did not reconcile"):
            _validate_ack_settlement(settlement, request_bytes=b"request",
                manifest_bytes=b"manifest", snapshot_bytes=b"selection",
                next_checkpoint_bytes=b"next", receipt_bytes=b"receipt")

    def test_parent_native_pin_survives_verifier_until_source_ack(self):
        owner = _PinOwner()
        stages = _LocalPinStages(owner, lifetime_seconds=60)
        stages("open")
        stages("pre_ack")
        stages("close")
        self.assertEqual(owner.events, ["open", "renew", "renew"])
        self.assertEqual(stages.stages, ["open", "pre_ack", "close"])
        self.assertIsNotNone(stages.handle)
        stages.renew_for_ack()
        self.assertEqual(owner.events[-1], "renew")
        stages.begin_post_ack_observation()
        stages("open")
        stages("pre_ack")
        stages("close")
        stages.renew_after_post_ack_observation()
        stages.close_after_ack()
        self.assertEqual(owner.events[-1], "close")
        self.assertIsNone(stages.handle)

    def test_post_ack_close_requires_its_own_terminal_native_proof(self):
        request, manifest, receipt = b"request", b"manifest", b"receipt"
        vector = {"authority": "managed-current-spark:owner",
            "scope_kind": "publication", "scope_id": "pub-1",
            "custody_digest": "f" * 64,
            "targets": {role: ["uuid-" + role, 1] for role in (
                "object_current", "edge_current", "tombstone", "whole_source_history")}}
        targets = {role: {"uuid": item[0], "version": item[1]}
                   for role, item in vector["targets"].items()}
        observation = {"profile": "ashlar-managed-operational-retention/0.1",
            "interval_id": "interval", "outcome": "observed_available_at_checks",
            "authority": "read_observation_only_no_source_ack",
            "maximum_elapsed_seconds": 60,
            "observations": [{"stage": stage, "predictive_optimization": "ENABLE",
                "session_ownership_profile": "databricks-serverless-provided-task-session/0.1",
                "task_session_scope_id": "same-verifier-task-scope",
                "session_wrapper_id": "wrapper-" + stage,
                "vector_targets": targets} for stage in ("open", "pre_ack", "close")]}
        observation_raw = json.dumps(observation, sort_keys=True,
            separators=(",", ":")).encode()
        output = {"profile": "ashlar-managed-a1-verifier-result/0.1",
            "phase": "a0", "observationPhase": "post_ack_close",
            "runtimeIdentity": {"opening": {}, "closing": {}},
            "predictiveOptimization": "ENABLE", "managedObservation": observation,
            "managedObservationSha256": hashlib.sha256(observation_raw).hexdigest(),
            "requestSha256": hashlib.sha256(request).hexdigest(),
            "manifestSha256": hashlib.sha256(manifest).hexdigest(),
            "destinationReceiptSha256": hashlib.sha256(receipt).hexdigest(),
            "vector": vector}
        proof = _verify_terminal_proof(output, 10, 11, request, manifest, receipt,
            "a" * 64, vector, phase="a0", observation_phase="post_ack_close")
        self.assertEqual(proof["outcome"], "durably_verified")
        with self.assertRaisesRegex(ValueError, "wrong source-ACK observation phase"):
            _verify_terminal_proof(output, 10, 11, request, manifest, receipt,
                "a" * 64, vector, phase="a0", observation_phase="pre_ack")

    def test_lost_run_now_response_recovers_unique_run_from_fresh_job(self):
        api = _RunsAPI([{"runs": [{"run_id": 418, "job_id": 7,
            "start_time": 100100}], "next_page_token": None}])
        state = []
        found = _recover_run_after_lost_response(api, 7, "token", 100000,
            time.monotonic() + 5, set(), state.append)
        self.assertEqual(found, 418)
        self.assertEqual(state[-1]["submission_state"],
                         "run_id_recovered_after_lost_response")
        self.assertEqual(api.cancelled, [])

    def test_ambiguous_runs_are_cancelled_and_recorded_unknown_without_retry(self):
        api = _RunsAPI([{"runs": [
            {"run_id": 418, "job_id": 7, "start_time": 100100},
            {"run_id": 419, "job_id": 7, "start_time": 100200}],
            "next_page_token": None}])
        state = []
        found = _recover_run_after_lost_response(api, 7, "token", 100000,
            time.monotonic() + 5, set(), state.append)
        self.assertIsNone(found)
        self.assertEqual(api.cancelled, [418, 419])
        self.assertEqual(state[-1]["submission_state"],
                         "multiple_runs_unknown_cancel_requested")

    def test_ambiguous_recovery_refuses_all_cancellation_if_any_run_is_foreign(self):
        api = _MixedOwnershipRunsAPI()
        state = []
        found = _recover_run_after_lost_response(api, 7, "token", 100000,
            time.monotonic() + 5, set(), state.append)
        self.assertIsNone(found)
        self.assertEqual(api.cancelled, [])
        self.assertEqual(state[-1]["submission_state"],
                         "multiple_runs_ownership_unknown")

    def test_lost_run_now_response_is_reconciled_without_resubmission(self):
        api = _LostRunNowAPI()
        config = {"trial_id": "a1-regression", "expected_principal": "owner@example.com",
            "initializer": {"timeout_seconds": 60}}
        task_payload = {"task": {"task_key": "initializer", "depends_on": [],
            "timeout_seconds": 60, "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": "/Workspace/initialize",
                              "base_parameters": {"task_parameters_json": "{}"}},
            "environment_key": "initializer_env"},
            "environment": {"environment_key": "initializer_env",
                            "spec": {"environment_version": "1", "dependencies": ["/Volumes/x.whl"]}}}
        recovery = {"jobs": {}, "known_run_ids": {}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700); os.chmod(root, 0o700)
            run_id, task_run_id, snapshot, output = _run_stage(api, config,
                "initializer", task_payload, {"task_parameters_json": "{}"},
                time.monotonic() + 10, cleanup_deadline=time.monotonic() + 15,
                recovery_state=recovery, recovery_path=root / "recovery.json")
        self.assertEqual((run_id, task_run_id), (29, 29))
        self.assertEqual(snapshot["state"]["result_state"], "SUCCESS")
        self.assertEqual(output, {"ok": True})
        self.assertEqual(api.run_now_count, 1)
        self.assertEqual(api.run_now_payload["idempotency_token"],
            hashlib.sha256(b"a1-regression:17:initializer").hexdigest())
        self.assertTrue(api.deleted)
        self.assertEqual(recovery["known_run_ids"]["initializer"], 29)
        self.assertEqual(recovery["jobs"]["initializer"]["run_now_state"],
                         "terminal_success")

    def test_cross_job_run_now_result_is_never_trusted_or_cancelled(self):
        api = _ForeignRunNowAPI()
        config = {"trial_id": "a1-foreign", "expected_principal": "owner@example.com",
            "initializer": {"timeout_seconds": 60}}
        task_payload = {"task": {"task_key": "initializer", "depends_on": [],
            "timeout_seconds": 60, "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": "/Workspace/initialize",
                              "base_parameters": {"task_parameters_json": "{}"}},
            "environment_key": "initializer_env"},
            "environment": {"environment_key": "initializer_env",
                            "spec": {"environment_version": "1",
                                      "dependencies": ["/Volumes/x.whl"]}}}
        recovery = {"jobs": {}, "known_run_ids": {}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700); os.chmod(root, 0o700)
            with self.assertRaisesRegex(ValueError, "not owned by the exact newly-created job"):
                _run_stage(api, config, "initializer", task_payload,
                    {"task_parameters_json": "{}"}, time.monotonic() + 10,
                    cleanup_deadline=time.monotonic() + 15,
                    recovery_state=recovery, recovery_path=root / "recovery.json")
        self.assertEqual(api.cancelled, [])
        self.assertTrue(api.deleted)  # only the new, empty job was removed
        stage = recovery["jobs"]["initializer"]
        self.assertEqual(stage["run_now_state"], "foreign_run_id_refused")
        self.assertEqual(stage["foreign_run_id"], 29)
        self.assertIs(stage["foreign_run_cancelled"], False)

    def test_phase_jobs_reuse_role_timeout_but_get_distinct_job_identity(self):
        api = _LostRunNowAPI()
        config = {"trial_id": "a1-phases", "expected_principal": "owner@example.com",
            "publisher": {"timeout_seconds": 120}}
        task_payload = {"task": {"task_key": "publisher", "depends_on": [],
            "timeout_seconds": 120, "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": "/Workspace/publisher",
                              "base_parameters": {"task_parameters_json": "{}"}},
            "environment_key": "publisher_env"},
            "environment": {"environment_key": "publisher_env",
                            "spec": {"environment_version": "1", "dependencies": ["/Volumes/x.whl"]}}}
        recovery = {"jobs": {}, "known_run_ids": {}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700); os.chmod(root, 0o700)
            _run_stage(api, config, "a0-publisher", task_payload,
                {"task_parameters_json": "{}"}, time.monotonic() + 10,
                cleanup_deadline=time.monotonic() + 15,
                recovery_state=recovery, recovery_path=root / "recovery.json",
                config_key="publisher")
        self.assertEqual(api.settings["name"], "ashlar-a1-a1-phases-a0-publisher")
        self.assertEqual(api.settings["timeout_seconds"], 120)
        self.assertEqual(api.run_now_count, 1)

    def test_unseen_lost_run_now_is_durably_marked_unknown_without_resubmit(self):
        api = _LostRunNowAPI(visible=False)
        config = {"trial_id": "a1-unknown", "expected_principal": "owner@example.com",
            "initializer": {"timeout_seconds": 60}}
        task_payload = {"task": {"task_key": "initializer", "depends_on": [],
            "timeout_seconds": 60, "max_retries": 0, "retry_on_timeout": False,
            "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": "/Workspace/initialize",
                              "base_parameters": {"task_parameters_json": "{}"}},
            "environment_key": "initializer_env"},
            "environment": {"environment_key": "initializer_env",
                            "spec": {"environment_version": "1", "dependencies": ["/Volumes/x.whl"]}}}
        recovery = {"jobs": {}, "known_run_ids": {}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700); os.chmod(root, 0o700)
            path = root / "recovery.json"
            with self.assertRaises(Exception):
                _run_stage(api, config, "initializer", task_payload,
                    {"task_parameters_json": "{}"}, time.monotonic() + 0.5,
                    cleanup_deadline=time.monotonic() + 0.8,
                    recovery_state=recovery, recovery_path=path)
            persisted = json.loads(path.read_text())
        self.assertEqual(api.run_now_count, 1)
        self.assertFalse(api.deleted)
        stage = persisted["jobs"]["initializer"]
        self.assertEqual(stage["submission_state"], "unknown_submission_exposure")
        self.assertEqual(stage["run_now_state"], "unknown_submission_exposure")

    def test_private_recovery_receipt_is_fsynced_owner_only_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "private"
            root.mkdir(mode=0o700)
            os.chmod(root, 0o700)
            path = root / "recovery.json"
            _write_private_json(path, {"phase": "run_now_intent", "run_ids": {}})
            self.assertEqual(json.loads(path.read_text()),
                             {"phase": "run_now_intent", "run_ids": {}})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_outer_wall_deadline_writes_recovery_receipt_on_blocked_operation(self):
        def blocked(*args, **kwargs):
            time.sleep(3)
        with patch("tools.run_managed_a1_serverless._launch_trial", side_effect=blocked):
            with self.assertRaisesRegex(RuntimeError, "private recovery receipt retained") as caught:
                run({"trial_id": "a1-deadline", "host": "https://example.invalid",
                     "maximum_wall_seconds": 1}, {}, "aidev-cus", launch=True)
        path = Path(str(caught.exception).rsplit(" ", 1)[-1])
        receipt = json.loads(path.read_text())
        self.assertEqual(receipt["failure_type"], "TimeoutError")
        self.assertEqual(receipt["phase"], "launch_gate")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        import shutil
        shutil.rmtree(path.parent)


if __name__ == "__main__":
    unittest.main()
