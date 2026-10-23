# SPDX-License-Identifier: MIT OR Apache-2.0
import importlib.util
import json
from pathlib import Path
import signal
import tempfile
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

PACKET = Path(__file__).resolve().parents[1] / "docs/helix/04-build/evidence/current-upstream-release-20261022/r04-b-source-installation-draft-20261023"


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, PACKET / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


worker = load_module("b_install_worker_test", "b_install_worker.py")
supervisor = load_module("b_install_supervisor_test", "b_install_supervisor.py")


class FakeAdapter:
    def __init__(self, fail_commit=None):
        self.fail_commit = fail_commit
        self.phase = 0
        self.calls = []

    def open(self, path, *, port): self.calls.append(("open", port))
    def bootstrap_roles(self, *args): self.calls.append(("bootstrap",))
    def install(self, *args):
        self.calls.append(("install",))
        self.phase = 2
        return {"installation_id": "provider-install", "release_id": "release",
                "release_sha256": "manifest-sha", "layout_profile": {"identity": "truss-layout",
                "version": "1.0", "sha256": "layout-sha"}}
    def read_installation(self):
        return {"installation_id": "provider-install", "release_id": "release",
                "release_sha256": "manifest-sha", "layout_profile": {"identity": "truss-layout",
                "version": "1.0", "sha256": "layout-sha"}}
    def grant_registrar(self, *args): self.calls.append(("grant",))
    def accept(self, prepared, source, origin, key, document):
        self.calls.append(("accept",))
        return {"registration_id": "provider-registration", "revision": 1,
                "source_sha256": worker.sha(source), "report_sha256": worker.sha(b"report")}
    def read_acceptance(self, source, document, document_revision, expected_source_catalog_revision):
        return {"source_sha256": worker.sha(source), "revision": 1,
                "document_revision": document_revision,
                "source_catalog_revision": expected_source_catalog_revision,
                "registration_id": "provider-registration", "report_sha256": worker.sha(b"report")}
    def register_consumer(self, consumer, reader):
        self.calls.append(("consumer",))
        return {"consumer_id": consumer, "registration_id": "provider-consumer-registration",
                "installation_id": "provider-install", "cursor_sha256": "cursor"}
    def commit(self):
        self.calls.append(("commit",))
        self.phase += 1
        if self.fail_commit == self.phase:
            raise OSError("commit transport lost")
    def close(self): self.calls.append(("close",))


def executable_config():
    return {"execution": {"approval": {"test-only": True}},
            "scope": {"arras_window_released": True},
            "bounds": {"worker_deadline_seconds": 600},
            "installation": {"root_path": "/b", "pgdata_path": "/b/pgdata",
                "attempt_path": "/b/attempt", "owner_role": "b_owner",
                "registrar_role": "b_registrar", "registrar_login": "b_reg_login",
                "reader_login": "b_reader_login", "consumer_id": "b_consumer",
                "request_key": "b_req", "exclusive_port_admission": "held",
                "retention_owner": "test", "expected_document_source_revision": 0},
            "source": {"document_id": "source", "document_revision": "source-r1"}}


class TrussBInstallWorkerTests(unittest.TestCase):
    def test_accepted_view_matches_exact_source_definition_inventory(self):
        source = json.dumps({"id": "doc", "modules": [{"id": "m", "elements": [
            {"id": "Person", "kind": "record", "keys": [{"id": "identity"}]},
            {"id": "person_key", "kind": "field"}], "relationships": [
            {"id": "knows"}]}]}, separators=(",", ":")).encode()
        expected = worker.expected_catalog_definition_inventory(source, "doc")
        self.assertEqual(len(expected), 4)

        def entry(item):
            kind, document, module, authored, pointer = item
            return SimpleNamespace(
                reference=SimpleNamespace(kind=kind, owner=SimpleNamespace(
                    document_id=document, module_id=module), authored_identity=authored),
                provenance=SimpleNamespace(accepted_catalog_revision="1",
                    document_revision="r1", accepted_document_sha256=worker.sha(source),
                    authored_pointer=pointer))

        accepted = SimpleNamespace(document_id="doc", content=source,
            document_revision="r1", catalog_revision=1, source_catalog_revision=0,
            registration_id="registration", sha256=worker.sha(source))
        report = b"exact report"
        registration = SimpleNamespace(registration_id="registration", report_bytes=report)

        def view_for(items):
            return SimpleNamespace(catalog_revision="1",
                definitions=tuple(entry(item) for item in items), original_bytes=b"complete view")

        result = worker.validate_accepted_observations(status_revision=1,
            accepted=accepted, report_bytes=report, registration=registration,
            view=view_for(expected), source=source, document_id="doc",
            document_revision="r1", expected_source_catalog_revision=0,
            complete_scope=True)
        self.assertEqual(result["source_sha256"], worker.sha(source))
        with self.assertRaisesRegex(RuntimeError, "accepted source/report/complete view"):
            worker.validate_accepted_observations(status_revision=1,
                accepted=accepted, report_bytes=report, registration=registration,
                view=view_for(set(list(expected)[:-1])), source=source, document_id="doc",
                document_revision="r1", expected_source_catalog_revision=0,
                complete_scope=True)

    def test_packet_preflight_is_pure_and_keeps_approval_null(self):
        config = json.loads((PACKET / "b-install-config.json").read_bytes())
        self.assertIsNone(config["execution"]["approval"])
        self.assertFalse(config["scope"]["arras_window_released"])
        self.assertEqual(config["runtime"]["runtime_manifest_sha256"],
                         "e96705451e82392657aafc956e3e85e2975e5e8b56a2658b3a93d9fd3d2f18ce")
        self.assertEqual(config["runtime"]["catalog_manifest_sha256"],
                         "288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4")
        self.assertIsNone(config["installation"]["retention_owner"])
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            origin_path = Path(temporary) / "b-origin.json"
            origin_path.write_bytes((PACKET / "b-origin-candidate.json").read_bytes())
            config["source"]["path"] = "/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json"
            config["origin"]["path"] = str(origin_path)
            config_path = Path(temporary) / "candidate-config.json"
            config_path.write_text(json.dumps(config))
            loaded, source, origin = worker.load_config(config_path)
            self.assertEqual(worker.sha(source), loaded["source"]["sha256"])
            self.assertEqual(worker.sha(origin), loaded["origin"]["sha256"])

    def test_worker_rejects_packet_relative_origin_path(self):
        config = json.loads((PACKET / "b-install-config.json").read_bytes())
        config["source"]["path"] = "/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json"
        config["origin"]["path"] = "b-origin-candidate.json"
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            config_path = Path(temporary) / "candidate-config.json"
            config["source"]["path"] = "/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json"
            config_path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "external canonical absolute"):
                worker.load_config(config_path)

    def test_selected_r4_lock_matches_exact_b_direct_cohort(self):
        config = json.loads((PACKET / "b-install-config.json").read_bytes())
        self.assertEqual(worker.validate_dependency_lock(config),
                         config["dependency_lock"]["sha256"])

    def test_tampered_b_lock_and_unbound_runtime_receipt_refuse(self):
        config = json.loads((PACKET / "b-install-config.json").read_bytes())
        lock_path = Path(config["dependency_lock"]["path"])
        lock = json.loads(lock_path.read_bytes())
        next(w for w in lock["wheels"] if w["distribution"] == "truss-toolkit")["sha256"] = "0" * 64
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            changed = Path(temporary) / "changed-lock.json"
            raw = json.dumps(lock, sort_keys=True).encode()
            changed.write_bytes(raw)
            config["dependency_lock"]["path"] = str(changed)
            config["dependency_lock"]["sha256"] = worker.sha(raw)
            with self.assertRaisesRegex(ValueError, "exact B direct wheel cohort"):
                worker.validate_dependency_lock(config)
        lock_sha = json.loads((PACKET / "b-install-config.json").read_bytes())["dependency_lock"]["sha256"]
        worker.validate_runtime_receipt_lock({"dependency_lock_sha256": lock_sha}, lock_sha)
        with self.assertRaisesRegex(RuntimeError, "does not bind the selected dependency lock"):
            worker.validate_runtime_receipt_lock({"dependency_lock_sha256": "0" * 64}, lock_sha)

    def test_supervisor_preflight_checks_a_bounded_effect_free_worker_result(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            config = json.loads((PACKET / "b-install-config.json").read_bytes())
            source = root / "source.umf.json"
            source.write_bytes(Path("/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json").read_bytes())
            origin = root / "b-origin.json"
            origin.write_bytes((PACKET / "b-origin-candidate.json").read_bytes())
            config["source"]["path"], config["origin"]["path"] = str(source), str(origin)
            runtime = config["runtime"]
            runtime["venv"] = str(root / "venv")
            python = root / "venv" / "bin" / "python"
            python.parent.mkdir(parents=True)
            python.write_bytes(b"fake-python")
            pg_ctl = root / "venv" / "pg_ctl"
            pg_ctl.write_bytes(b"fake-pg-ctl")
            receipt = root / "runtime-receipt.json"
            receipt.write_bytes(b"fake-receipt")
            runtime["pg_ctl_path"] = str(pg_ctl)
            runtime["pg_ctl_sha256"] = supervisor.large_file_sha(pg_ctl)
            runtime["receipt_path"] = str(receipt)
            config_raw = json.dumps(config, sort_keys=True, indent=2).encode() + b"\n"
            config_path = root / "b-install-config.json"
            config_path.write_bytes(config_raw)
            gate = {"status": "ready", "active_postgres_pids": [],
                    "nominal_5432_socket_files": []}
            gate_hash = supervisor.admission_digest(gate)
            prep = {"pg_ctl_path": runtime["pg_ctl_path"],
                    "pg_ctl_sha256": runtime["pg_ctl_sha256"],
                    "bun_path": config["preparation"]["bun_path"],
                    "bun_sha256": config["preparation"]["bun_sha256"]}
            expected = supervisor.expected_approval(config, config_raw, gate_hash, prep)
            worker_result = {"pg_started": False, "spark_started": False,
                "acceptance_invoked": False, "consumer_registered": False,
                "expected_approval": expected, "preparation": prep}
            class Completed:
                returncode = 0
                stdout = json.dumps(worker_result) + "\n"
            def run(*args, **kwargs): return Completed()
            with patch.object(supervisor, "CONFIG", config_path), patch.object(
                    supervisor, "APPROVAL", root / "execution-approval.json"):
                result = supervisor.preflight(run, gate_reader=lambda **kwargs: gate)
            self.assertEqual(result["status"], "effect_free_preflight_passed")
            self.assertIsNone(result["execution_approval"])
            self.assertEqual(result["native_effects"], 0)

    def test_worker_and_supervisor_approval_payloads_match_independently(self):
        config = json.loads((PACKET / "b-install-config.json").read_bytes())
        source = Path("/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json").read_bytes()
        origin = (PACKET / "b-origin-candidate.json").read_bytes()
        config["source"]["path"] = "/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json"
        config["origin"]["path"] = str((PACKET / "b-origin-candidate.json").resolve())
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            pg_ctl = root / "pg_ctl"
            pg_ctl.write_bytes(b"fake pg_ctl")
            receipt = root / "receipt.json"
            receipt.write_bytes(b"fake receipt")
            config["runtime"]["pg_ctl_path"] = str(pg_ctl)
            config["runtime"]["pg_ctl_sha256"] = supervisor.large_file_sha(pg_ctl)
            config["runtime"]["receipt_path"] = str(receipt)
            raw = json.dumps(config, sort_keys=True, indent=2).encode() + b"\n"
            pins = {"preparation_sha256": config["preparation"]["assessment_sha256"],
                "dependency_lock_sha256": config["dependency_lock"]["sha256"],
                "prepared_input_sha256": config["preparation"]["prepared_input_sha256"],
                "prepared_report_sha256": config["preparation"]["prepared_report_sha256"],
                "runtime_receipt_sha256": worker.sha(receipt.read_bytes()),
                "catalog_manifest_sha256": config["runtime"]["catalog_manifest_sha256"],
                "pg_ctl_path": config["runtime"]["pg_ctl_path"],
                "pg_ctl_sha256": config["runtime"]["pg_ctl_sha256"],
                "bun_path": config["preparation"]["bun_path"],
                "bun_sha256": config["preparation"]["bun_sha256"]}
            gate_hash = "2" * 64
            from_worker = worker.worker_approval_payload(config, raw, source, origin, pins, gate_hash)
            from_supervisor = supervisor.expected_approval(config, raw, gate_hash, pins)
            self.assertEqual(from_worker, from_supervisor)

    def test_supervisor_child_starts_new_group_and_records_success(self):
        class Child:
            pid = 4123
            def __init__(self): self.polls = 0
            def poll(self):
                self.polls += 1
                return None if self.polls == 1 else 0
            def wait(self, timeout=None): return 0
        child = Child()
        captured = {}
        def popen(*args, **kwargs): captured.update(kwargs); return child
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            attempt = root / "attempt"
            attempt.mkdir(mode=0o700)
            result = supervisor.supervise_child(["fake"], root=root,
                logs=(attempt / "stdout", attempt / "stderr"), popen=popen,
                sleep=lambda _: None, reap=lambda *a, **k: [])
            self.assertEqual(result["exit_code"], 0)
            self.assertIsNone(result["stop_cause"])
            self.assertTrue(captured["start_new_session"])
            self.assertTrue(captured["close_fds"])

    def test_supervisor_deadline_sends_sigterm_and_reaps_fake_worker(self):
        class Child:
            pid = 4124
            stopped = False
            def poll(self): return 0 if self.stopped else None
            def wait(self, timeout=None):
                self.stopped = True
                return 0
        child = Child()
        now = [0.0]
        signals = []
        def sleep(seconds): now[0] += seconds
        def killpg(pid, sig): signals.append((pid, sig)); child.stopped = True
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            attempt = root / "attempt"
            attempt.mkdir(mode=0o700)
            result = supervisor.supervise_child(["fake"], root=root,
                logs=(attempt / "stdout", attempt / "stderr"),
                popen=lambda *a, **k: child, clock=lambda: now[0], sleep=sleep,
                killpg=killpg, reap=lambda *a, **k: [], deadline=0.3)
            self.assertEqual(result["stop_cause"], "worker_deadline_exceeded")
            self.assertIn((child.pid, signal.SIGTERM), signals)
            self.assertEqual(result["remaining_process_group"], [])

    def test_supervisor_poll_failure_kills_and_reaps_started_fake_worker(self):
        class Child:
            pid = 4125
            def poll(self): raise OSError("injected poll fault")
            def wait(self, timeout=None): return -signal.SIGKILL
        child = Child()
        signals, reaped = [], []
        def killpg(pid, sig): signals.append((pid, sig))
        def reap(group, **kwargs): reaped.append(group); return []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            attempt = root / "attempt"
            attempt.mkdir(mode=0o700)
            result = supervisor.supervise_child(["fake"], root=root,
                logs=(attempt / "stdout", attempt / "stderr"),
                popen=lambda *a, **k: child, killpg=killpg, reap=reap)
        self.assertEqual(result["stop_cause"], "supervisor_exception:OSError")
        self.assertEqual(result["supervisor_error"], "OSError")
        self.assertIn((child.pid, signal.SIGKILL), signals)
        self.assertEqual(reaped, [child.pid])

    def test_process_port_gate_blocks_postgres_and_nominal_socket_occupancy(self):
        class Completed:
            returncode = 0
            stdout = "777 postgres -D /owned/data\n"
        with tempfile.TemporaryDirectory() as temporary:
            socket_root = Path(temporary)
            socket_file = socket_root / ".s.PGSQL.5432"
            socket_file.touch()
            gate = supervisor.process_port_gate(run=lambda *a, **k: Completed(),
                                                socket_root=socket_root)
        self.assertEqual(gate["status"], "occupied")
        self.assertEqual(gate["active_postgres_pids"], [777])
        self.assertEqual(len(gate["nominal_5432_socket_files"]), 1)

    def test_one_shot_sequence_orders_commits_readbacks_and_stops_without_feed_or_ack(self):
        fake = FakeAdapter()
        marks = []
        result = worker.ordered_attempt(fake, executable_config(), object(), b"source", b"origin",
                                        lambda stage, data: marks.append(stage))
        self.assertEqual(result["status"], "installed_accepted_genesis_registered_stopped")
        self.assertEqual(marks[-1], "terminal_stopped")
        self.assertLess(marks.index("install_committed"), marks.index("install_readback"))
        self.assertLess(marks.index("accept_committed"), marks.index("accept_readback"))
        self.assertNotIn("feed_read", [call[0] for call in fake.calls])
        self.assertNotIn("ack", [call[0] for call in fake.calls])

    def test_uncertain_commit_stops_sequence_and_never_attempts_later_stage(self):
        fake = FakeAdapter(fail_commit=1)
        marks = []
        with self.assertRaises(worker.UncertainAttempt):
            worker.ordered_attempt(fake, executable_config(), object(), b"source", b"origin",
                                   lambda stage, data: marks.append(stage))
        self.assertEqual(marks[-1], "bootstrap_commit_started")
        self.assertNotIn(("install",), fake.calls)

    def test_genesis_cursor_uses_checkpoint_content_and_rejects_duplicate_keys(self):
        raw = json.dumps({"profile": "truss-caller-asserted-feed/0.1",
            "installationId": "i-1", "sourceEpoch": "e-1", "consumerId": "b-consumer",
            "registrationId": "r-1", "nextXid": "0"}, separators=(",", ":")).encode()
        consumer = SimpleNamespace(consumer_id="b-consumer", registration_id="r-1",
            checkpoint=SimpleNamespace(content=raw), durability="pending")
        evidence = worker.genesis_cursor_evidence(consumer, "b-consumer")
        self.assertEqual(evidence["installation_id"], "i-1")
        self.assertEqual(evidence["source_epoch"], "e-1")
        self.assertEqual(evidence["cursor_sha256"], worker.sha(raw))
        consumer.checkpoint.content = raw[:-1] + b',"consumerId":"spoof"}'
        with self.assertRaisesRegex(ValueError, "duplicate"):
            worker.genesis_cursor_evidence(consumer, "b-consumer")

    def test_genesis_cursor_must_match_committed_installation_identity(self):
        raw = json.dumps({"profile": "truss-caller-asserted-feed/0.1",
            "installationId": "wrong-install", "sourceEpoch": "e-1", "consumerId": "b-consumer",
            "registrationId": "r-1", "nextXid": "0"}).encode()
        consumer = SimpleNamespace(consumer_id="b-consumer", registration_id="r-1",
            checkpoint=SimpleNamespace(content=raw), durability="pending")
        with self.assertRaisesRegex(ValueError, "different installation"):
            worker.genesis_cursor_evidence(consumer, "b-consumer", "expected-install")

    def test_installation_readback_binds_provider_id_release_hash_and_layout(self):
        manifest = {"releaseId": "truss-release", "layoutProfile": {
            "identity": "truss-layout", "version": "1.0", "sha256": "layout-sha"}}
        status = SimpleNamespace(installed=True, installation_id="install-b",
            release_id="truss-release", release_sha256="manifest-sha",
            layout_profile=SimpleNamespace(**manifest["layoutProfile"]))
        result = worker.validate_installation_readback(status, manifest,
            "manifest-sha", "install-b")
        self.assertEqual(result["installation_id"], "install-b")
        status.release_sha256 = "wrong"
        with self.assertRaisesRegex(RuntimeError, "pinned runtime manifest"):
            worker.validate_installation_readback(status, manifest,
                "manifest-sha", "install-b")

    def test_consumer_adapter_call_matches_public_feedconsumer_shape_without_database(self):
        adapter = worker.Truss049Adapter()
        raw = json.dumps({"profile": "truss-caller-asserted-feed/0.1",
            "installationId": "i-1", "sourceEpoch": "e-1", "consumerId": "b-consumer",
            "registrationId": "r-1", "nextXid": "0"}).encode()
        public_shape = SimpleNamespace(consumer_id="b-consumer", registration_id="r-1",
            checkpoint=SimpleNamespace(content=raw), durability="pending")
        calls = []
        class Assembly:
            def register_feed_consumer_in_transaction(self, tx, *, consumer_id, reader_role):
                calls.append((consumer_id, reader_role))
                return "public outcome"
        adapter.assembly, adapter.tx = Assembly(), object()
        adapter._pending_installation_id = "i-1"
        with patch.object(adapter, "_begin"), patch.object(adapter, "_value", return_value=public_shape):
            result = adapter.register_consumer("b-consumer", "b-reader")
        self.assertEqual(calls, [("b-consumer", "b-reader")])
        self.assertEqual(result["registration_id"], "r-1")
        self.assertEqual(result["installation_id"], "i-1")

    @unittest.skipUnless(sys.version_info >= (3, 12), "isolated Truss runtime requires Python 3.12")
    def test_adapter_smoke_with_actual_truss_feedconsumer_and_cursor_types(self):
        runtime = "/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv"
        site = Path(runtime) / "lib/python3.12/site-packages"
        sys.path.insert(0, str(site))
        try:
            import truss
            from truss import FeedConsumer, FeedCursor
            raw = json.dumps({"profile": "truss-caller-asserted-feed/0.1",
                "installationId": "sdk-install", "sourceEpoch": "sdk-epoch",
                "consumerId": "b-consumer", "registrationId": "sdk-registration",
                "nextXid": "0"}).encode()
            public_result = FeedConsumer(consumer_id="b-consumer",
                registration_id="sdk-registration", checkpoint=FeedCursor(content=raw))
            adapter = worker.Truss049Adapter()
            class Assembly:
                def register_feed_consumer_in_transaction(self, tx, *, consumer_id, reader_role):
                    return "ignored mocked Outcome"
            adapter.assembly, adapter.tx = Assembly(), object()
            adapter._pending_installation_id = "sdk-install"
            with patch.object(adapter, "_begin"), patch.object(adapter, "_value", return_value=public_result):
                result = adapter.register_consumer("b-consumer", "b-reader")
            self.assertEqual(result["registration_id"], "sdk-registration")
            self.assertEqual(result["installation_id"], "sdk-install")
            self.assertFalse(hasattr(public_result, "cursor"))
            self.assertTrue(hasattr(public_result, "checkpoint"))
        finally:
            sys.path.remove(str(site))

    def test_acceptance_readback_binds_exact_source_report_cas_and_view_provenance(self):
        source = b'{"id":"source","umf":"0.10.0"}'
        report = b'{"revision":1}'
        provenance = SimpleNamespace(accepted_catalog_revision="1",
            document_revision="source-r1", accepted_document_sha256=worker.sha(source))
        view = SimpleNamespace(catalog_revision="1", definitions=(
            SimpleNamespace(provenance=provenance),), original_bytes=b'{"view":1}')
        accepted = SimpleNamespace(document_id="source", content=source, document_revision="source-r1",
            catalog_revision=1, source_catalog_revision=0, registration_id="reg-1",
            sha256=worker.sha(source))
        registration = SimpleNamespace(registration_id="reg-1", report_bytes=report)
        result = worker.validate_accepted_observations(status_revision=1,
            accepted=accepted, report_bytes=report, registration=registration,
            view=view, source=source, document_id="source", document_revision="source-r1",
            expected_source_catalog_revision=0, complete_scope=True)
        self.assertEqual(result["complete_view_sha256"], worker.sha(b'{"view":1}'))
        accepted.content = b'{"different":true}'
        with self.assertRaisesRegex(RuntimeError, "differs from frozen pins"):
            worker.validate_accepted_observations(status_revision=1,
                accepted=accepted, report_bytes=report, registration=registration,
                view=view, source=source, document_id="source", document_revision="source-r1",
                expected_source_catalog_revision=0, complete_scope=True)

    def test_begin_uses_matching_readonly_transaction_and_public_host_admission(self):
        calls = []
        class Ok:
            def __init__(self, value): self.value = value
        class Connection:
            def execute(self, statement): calls.append(("sql", statement))
        class Host:
            def adopt_transaction(self, *, access_mode):
                calls.append(("access", access_mode))
                return Ok("tx")
            def detach(self): return True
        fake_truss = SimpleNamespace(Ok=Ok, CatalogRuntimeConfiguration=lambda: "cfg",
            adopt_psycopg_host=lambda conn, **kwargs: Host(),
            create_reference_assembly=lambda cfg, host: Ok(SimpleNamespace(dispose=lambda: None)))
        fake_psycopg = SimpleNamespace(connect=lambda **kwargs: Connection())
        adapter = worker.Truss049Adapter()
        adapter._params = {}
        with patch.dict("sys.modules", {"truss": fake_truss, "psycopg": fake_psycopg}):
            adapter._begin(readonly=True)
        self.assertEqual(calls[0], ("sql", "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY"))
        self.assertEqual(calls[2], ("access", "read_only"))
        self.assertFalse(any("BEGIN" in statement for kind, statement in calls if kind == "sql"))

    def test_execute_refuses_null_approval_before_creating_paths_or_processes(self):
        with patch.object(supervisor, "preflight", return_value={"gate": {"status": "ready"},
                "admission_gates_sha256": "0" * 64, "expected_approval": {}}):
            with self.assertRaisesRegex(PermissionError, "private mode-0700 B root"):
                supervisor.execute(popen=lambda *a, **k: self.fail("child must not start"))


if __name__ == "__main__":
    unittest.main()
