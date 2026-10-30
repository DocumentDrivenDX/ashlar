# SPDX-License-Identifier: MIT OR Apache-2.0
"""Isolated `.4` metadata-operation custody; no Spark or native execution."""
from contextlib import contextmanager
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace

from ashlar.attempt_store import verify_request_digest
from ashlar.native import SQLResult, quote_table_identifier
from ashlar_host.delta_custody import (
    LocalDeltaError, LocalDeltaUncertain, LocalDeltaTransport, encoded, sha,
)
from ashlar_host.local_receipt_codec import original_receipt_chunks
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
from ashlar_host.configured_local_delta4_publication import (
    plan_configured_local_delta4_transaction1,
)

CONFIG = Path(__file__).resolve().parents[1] / "examples/end-to-end/configured-typed-graph-source.json"


class FakePolicy:
    def __init__(self, context):
        self.context = context
        self.expected = None
        self.calls = 0
        self.writer_calls = 0
        self.writer_depth = 0

    @contextmanager
    def writer(self, *_args):
        self.writer_calls += 1
        self.writer_depth += 1
        try:
            yield None
        finally:
            self.writer_depth -= 1

    def admit(self, intent, context):
        if context is not self.context or intent != self.expected:
            raise PermissionError("exact fake metadata intent required")
        self.calls += 1
        return None


class FakeTransport:
    """Fake only the transport/native ports consumed by the metadata executor."""

    def __init__(self, root, request, context):
        self.installation_id = "delta4-metadata-test"
        self.context = context
        self.profile = {
            "profile": "ashlar-local-current-installation-profile/0.1",
            "physical_profile": "ashlar-delta/0.4-xxhash64",
            "roles": {"publication_attempt_phase": {
                "table": "local.configured.publication_attempt_phase",
                "path": str(root), "uuid": "attempt-uuid"}},
        }
        self.installation_profile = self.profile
        self.target = SimpleNamespace(table="local.configured.publication_attempt_phase",
                                      path=root, uuid="attempt-uuid")
        self.targets = {self.target.table: self.target}
        carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
        fields = carriers.ordered_fields("publication_attempt_phase")
        self.profile["roles"]["publication_attempt_phase"].update({
            "fields": [list(field) for field in fields],
            "protocol": {"uuid": self.target.uuid,
                "profile": "ashlar-configured-local-delta/0.1",
                "reader_version": 1, "writer_version": 2, "features": []}})
        self.operation_capacity = None
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)")
        self.db.execute("CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)")
        self.policy = FakePolicy(context)
        self.history = [{"version": 1, "operation": "SET TBLPROPERTIES"},
                        {"version": 0, "operation": "CREATE TABLE"}]
        self.native_commit_metadata = {}
        self.phase_rows = []
        self.dispatches = 0
        self.dispatch_mode = "commit"
        self.current_metadata = None
        self.request = request
        self.spark = SimpleNamespace(conf=_FakeConf(self), sql=self._metadata_sql,
                                      read=_FakeReader(self))

    def _parameters(self, parameters):
        if type(parameters) is not dict or any(type(k) is not str or type(v) is not str
                                               for k, v in parameters.items()):
            raise LocalDeltaError("exact fake parameters")

    def _target(self, _sql):
        return self.target

    def _execute_metadata(self, intent, reservation, context, *, recovery):
        return LocalDeltaTransport._execute_metadata(
            self, intent, reservation, context, recovery=recovery)

    def _require_delta4_metadata_reservation(self, reservation):
        return LocalDeltaTransport._require_delta4_metadata_reservation(self, reservation)

    def delta4_metadata_reservations(self):
        return LocalDeltaTransport.delta4_metadata_reservations(self)

    def _observe_delta4_metadata_target(self, version, *, allow_later=False):
        return LocalDeltaTransport._observe_delta4_metadata_target(
            self, version, allow_later=allow_later)

    def _verify_delta4_prepared_phase_readback(self, target, row, context):
        return LocalDeltaTransport._verify_delta4_prepared_phase_readback(
            self, target, row, context)

    def observe_delta4_metadata_commit(self, reservation, *, context, allow_later=False):
        return LocalDeltaTransport.observe_delta4_metadata_commit(
            self, reservation, context=context, allow_later=allow_later)

    def _profile(self):
        return None

    @contextmanager
    def _lock(self):
        yield

    def _detail(self, target, *, full=False):
        if target.uuid != self.target.uuid or target.path != self.target.path:
            raise LocalDeltaError("changed fake target")
        return {"id": target.uuid, "location": str(target.path)}

    def original_history(self, _target):
        return [dict(row, **({"userMetadata": self.native_commit_metadata[row["version"]]}
                              if row["version"] in self.native_commit_metadata else {}))
                for row in self.history]

    def _render(self, sql):
        return sql

    def _snapshot(self, _target, version):
        rows = ([row for row in self.phase_rows
                 if row["phase"] == "prepared" or version >= 3]
                if version >= 2 else [])
        original = json.dumps(rows, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"), allow_nan=False)
        return {"schema": [[key, "string"] for key in
                            ("stream", "batch_id", "phase", "request_digest",
                             "payload_json", "payload_digest")],
                "rows": rows, "row_sha256": hashlib.sha256(original.encode()).hexdigest()}

    def query(self, sql, parameters):
        if sql.startswith("DESCRIBE DETAIL"):
            return SQLResult([{"id": self.target.uuid, "format": "delta",
                "location": str(self.target.path), "minReaderVersion": 1,
                "minWriterVersion": 2, "tableFeatures": []}])
        if sql.startswith("SELECT phase,request_digest,payload_json,payload_digest"):
            rows = [row for row in self.phase_rows
                    if row["stream"] == parameters["stream"]
                    and row["batch_id"] == parameters["batch"]]
            order = {name: index for index, name in enumerate(
                ("prepared", "applying", "applied", "committing", "committed"))}
            rows.sort(key=lambda row: order[row["phase"]])
            return SQLResult([{key: row[key] for key in
                               ("phase", "request_digest", "payload_json", "payload_digest")}
                              for row in rows])
        raise AssertionError("unexpected fake SQL read: " + sql)

    def _metadata_sql(self, _sql, args):
        self.dispatches += 1
        if self.dispatch_mode == "raise_before_commit":
            raise RuntimeError("fake dispatch failed")
        row = json.loads(args["payload"])
        if self.dispatch_mode == "commit":
            self.phase_rows.append(row)
            self.current_metadata = self.spark.conf.value
            version = int(self.history[0]["version"]) + 1
            self.native_commit_metadata[version] = self.current_metadata
            self.history.insert(0, {"version": version, "operation": "MERGE"})
        return SimpleNamespace(collect=lambda: [])


class _FakeConf:
    def __init__(self, owner):
        self.owner = owner
        self.value = None

    def get(self, _key, default=None):
        return self.value if self.value is not None else default

    def set(self, _key, value):
        self.value = value

    def unset(self, _key):
        self.value = None


class _FakeReader:
    def __init__(self, owner):
        self.owner = owner

    def format(self, _value):
        return self

    def option(self, _key, _value):
        return self

    def load(self, _path):
        fields = self.owner.profile["roles"]["publication_attempt_phase"]["fields"]
        types = {"STRING": "string", "BIGINT": "long", "TIMESTAMP": "timestamp"}
        schema = SimpleNamespace(fields=[SimpleNamespace(
            name=name, nullable=nullable,
            dataType=SimpleNamespace(typeName=lambda kind=types[kind]: kind))
            for name, kind, nullable in fields])
        return SimpleNamespace(schema=schema)


def reservation_for(transport, request):
    verified = dict(request)
    verify_request_digest(verified)
    phase_payload = encoded({"request": verified, "result_json": None,
                             "descriptor_json": None, "plan_json": "{}"})
    target = transport.target
    columns = ("stream", "batch_id", "phase", "request_digest",
               "payload_json", "payload_digest")
    definition = "STRUCT<" + ",".join(column + ":STRING" for column in columns) + ">"
    parity = " AND ".join("t." + column + " IS NOT DISTINCT FROM s." + column
                          for column in columns)
    scope_key = ""
    for key in ("profile", "feed", "epoch"):
        scope_key += (" AND get_json_object(get_json_object(t.payload_json,"
                      "'$.request.source_checkpoint_json'),'$." + key + "')="
                      "get_json_object(get_json_object(s.payload_json,"
                      "'$.request.source_checkpoint_json'),'$." + key + "')")
    statement = ("MERGE INTO " + quote_table_identifier(target.table)
        + " t USING (SELECT r.* FROM (SELECT from_json(:payload,'" + definition
        + "') r)) s ON t.stream=s.stream AND t.batch_id=s.batch_id AND t.phase=s.phase"
        + scope_key + " WHEN MATCHED AND NOT (" + parity + ") THEN UPDATE SET "
        + "payload_json=cast(raise_error('ATTEMPT_PHASE_CONFLICT') AS STRING) "
        + "WHEN NOT MATCHED THEN INSERT *")
    reservations = []
    for phase, before in (("prepared", 1), ("applying", 2)):
        row = {"stream": verified["stream"], "batch_id": verified["batch_id"],
               "phase": phase, "request_digest": verified["request_digest"],
               "payload_json": phase_payload, "payload_digest": sha(phase_payload)}
        parameters = {"payload": json.dumps(row, ensure_ascii=False, separators=(",", ":"))}
        operation = "delta4-metadata:attempts:" + phase + ":" + sha(encoded({
            "statement": statement, "parameters": parameters}))
        intent = {"profile": "ashlar-local-delta-metadata-operation/0.2",
            "lane": "publication_attempt_phase", "phase": phase,
            "installation_id": transport.installation_id,
            "operation": operation, "request_digest": verified["request_digest"],
            "statement": statement, "parameters": parameters,
            "table": target.table, "path": str(target.path), "uuid": target.uuid,
            "before_version": before, "source_scope_sha256": "a" * 64,
            "effect_plan_sha256": "b" * 64, "graph_plan_sha256": "c" * 64,
            "phase_payload_sha256": row["payload_digest"]}
        original = encoded({"profile": "ashlar-configured-local-delta4-metadata-lane/0.2",
                            "intent": intent})
        reservations.append({"profile": "ashlar-configured-local-delta4-metadata-lane/0.2",
            "operation": operation, "original": original, "digest": sha(original),
            "phase_row": row})
    transport.db.execute("CREATE TABLE local_delta4_metadata_lane_reservation(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)")
    transport.db.executemany("INSERT INTO local_delta4_metadata_lane_reservation VALUES(?,?,?)",
        [(item["operation"], item["original"], item["digest"])
         for item in reservations])
    transport.policy.expected = json.loads(reservations[0]["original"])["intent"]
    return reservations[0]


class Tests(unittest.TestCase):
    def setUp(self):
        configuration = load_typed_graph_jsonl_configuration(CONFIG)
        self.plan = plan_configured_local_delta4_transaction1(
            configuration, stream="gate3-typed-graph", materialized_at_us="1791547200000000")

    def test_reserved_metadata_lane_commits_once_and_exactly_reopens(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            context = object()
            transport = FakeTransport(root, self.plan.request, context)
            reservation = reservation_for(transport, self.plan.request)
            result = LocalDeltaTransport.metadata_mutation(transport, reservation,
                                                           context=context)
            self.assertEqual(result.rows[0]["version"], "2")
            self.assertEqual(transport.dispatches, 1)
            journal = transport.db.execute(
                "SELECT operation,lane,intent_sha,before_version,state,receipt "
                "FROM local_delta4_metadata_operation").fetchone()
            intent = json.loads(reservation["original"])["intent"]
            self.assertEqual(journal[:5], (reservation["operation"],
                "publication_attempt_phase", sha(encoded(intent)), 1, "committed"))
            proof = json.loads(b"".join(original_receipt_chunks(journal[5])).decode())
            self.assertEqual(proof["lane"], "publication_attempt_phase")
            self.assertEqual(proof["phase_row_sha256"], reservation["phase_row"]["payload_digest"])
            observed = transport.observe_delta4_metadata_commit(reservation,
                                                                 context=context)
            self.assertEqual(observed, proof)
            self.assertEqual(observed["version"], 2)
            reopened = copy.copy(transport)
            reopened.policy = FakePolicy(context)
            reopened.policy.expected = json.loads(reservation["original"])["intent"]
            replay = LocalDeltaTransport.metadata_recover(reopened, reservation,
                                                          context=context)
            self.assertEqual(replay, result)
            reopened_proof = reopened.observe_delta4_metadata_commit(
                reservation, context=context)
            self.assertEqual(reopened_proof, observed)
            self.assertEqual(reopened.dispatches, 1)
            self.assertEqual(reopened.policy.calls, 2)
            self.assertEqual(reopened.policy.writer_calls, 3)
            self.assertEqual(reopened.policy.writer_depth, 0)
            self.assertEqual(reopened.db.execute(
                "SELECT count(*) FROM local_operation").fetchone(), (0,))

    def test_native_phase_readback_tamper_refuses_without_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            context = object()
            transport = FakeTransport(root, self.plan.request, context)
            reservation = reservation_for(transport, self.plan.request)
            LocalDeltaTransport.metadata_mutation(transport, reservation,
                                                  context=context)
            transport.phase_rows[0]["payload_json"] = "{}"
            with self.assertRaises(ValueError):
                LocalDeltaTransport.metadata_recover(transport, reservation,
                                                     context=context)
            self.assertEqual(transport.dispatches, 1)
            self.assertEqual(transport.db.execute(
                "SELECT state FROM local_delta4_metadata_operation").fetchone(),
                ("committed",))

    def test_applying_phase_is_a_separate_committed_v3_operation_and_reopens(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            context = object()
            transport = FakeTransport(root, self.plan.request, context)
            prepared = reservation_for(transport, self.plan.request)
            prepared_result = LocalDeltaTransport.metadata_mutation(
                transport, prepared, context=context)
            applying = transport.delta4_metadata_reservations()[1]
            transport.policy.expected = json.loads(applying["original"])["intent"]
            applying_result = LocalDeltaTransport.metadata_mutation(
                transport, applying, context=context)
            self.assertEqual(prepared_result.rows[0]["version"], "2")
            self.assertEqual(applying_result.rows[0]["version"], "3")
            self.assertEqual([row["phase"] for row in transport.phase_rows],
                             ["prepared", "applying"])
            self.assertEqual(transport.db.execute(
                "SELECT state FROM local_delta4_metadata_operation "
                "ORDER BY before_version").fetchall(),
                [("committed",), ("committed",)])
            proof = LocalDeltaTransport.observe_delta4_metadata_commit(
                transport, applying, context=context)
            self.assertEqual(proof["version"], 3)
            reopened = copy.copy(transport)
            reopened.policy = FakePolicy(context)
            reopened.policy.expected = json.loads(applying["original"])["intent"]
            recovered = LocalDeltaTransport.metadata_recover(
                reopened, applying, context=context)
            self.assertEqual(recovered, applying_result)
            self.assertEqual(reopened.dispatches, 2)
            reopened.db.execute(
                "UPDATE local_delta4_metadata_operation SET receipt='corrupt' "
                "WHERE before_version=1")
            reopened.db.commit()
            with self.assertRaises(LocalDeltaError):
                reopened.observe_delta4_metadata_commit(applying, context=context)

    def test_allow_later_accepts_only_the_reserved_v2_v3_phase_heads(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            context = object()
            transport = FakeTransport(root, self.plan.request, context)
            prepared = reservation_for(transport, self.plan.request)
            LocalDeltaTransport.metadata_mutation(transport, prepared, context=context)
            applying = transport.delta4_metadata_reservations()[1]
            transport.policy.expected = json.loads(applying["original"])["intent"]
            LocalDeltaTransport.metadata_mutation(transport, applying, context=context)

            # The earlier prepared receipt is valid while the exact later
            # applying phase is also committed at v3.
            prepared_proof = LocalDeltaTransport.observe_delta4_metadata_commit(
                transport, prepared, context=context, allow_later=True)
            self.assertEqual(prepared_proof["version"], 2)
            self.assertEqual(transport.original_history(transport.target)[0]["version"], 3)

            # A later, unreserved write must not be accepted as an arbitrary
            # extension of allow_later.
            transport.history.insert(0, {"version": 4, "operation": "UNRELATED"})
            with self.assertRaises(LocalDeltaError):
                LocalDeltaTransport.observe_delta4_metadata_commit(
                    transport, prepared, context=context, allow_later=True)

    def test_submitted_without_commit_is_quarantined_and_later_exact_commit_recovers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            context = object()
            transport = FakeTransport(root, self.plan.request, context)
            reservation = reservation_for(transport, self.plan.request)
            transport.dispatch_mode = "raise_before_commit"
            with self.assertRaisesRegex(RuntimeError, "fake dispatch"):
                LocalDeltaTransport.metadata_mutation(transport, reservation,
                                                      context=context)
            row = transport.db.execute(
                "SELECT state,receipt FROM local_delta4_metadata_operation").fetchone()
            self.assertEqual(row, ("submitted", None))
            with self.assertRaises(LocalDeltaUncertain):
                LocalDeltaTransport.metadata_recover(transport, reservation,
                                                     context=context)
            self.assertEqual(transport.dispatches, 1)
            transport.dispatch_mode = "commit"
            intent = json.loads(reservation["original"])["intent"]
            transport.current_metadata = encoded({
                "profile": "ashlar-local-delta-metadata-commit/0.2",
                "installation_id": transport.installation_id,
                "lane": intent["lane"], "operation": intent["operation"],
                "operation_sha256": sha(encoded(intent)),
                "request_digest": intent["request_digest"]})
            transport.spark.conf.value = transport.current_metadata
            transport._metadata_sql(reservation["original"], intent["parameters"])
            recovered = LocalDeltaTransport.metadata_recover(transport, reservation,
                                                             context=context)
            self.assertEqual(recovered.rows[0]["version"], "2")
            self.assertEqual(transport.dispatches, 2)  # the second is external delayed commit injection

    def test_applying_submitted_without_commit_is_quarantined_then_exact_commit_recovers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            context = object()
            transport = FakeTransport(root, self.plan.request, context)
            prepared = reservation_for(transport, self.plan.request)
            LocalDeltaTransport.metadata_mutation(transport, prepared, context=context)
            applying = transport.delta4_metadata_reservations()[1]
            transport.policy.expected = json.loads(applying["original"])["intent"]
            transport.dispatch_mode = "raise_before_commit"
            with self.assertRaisesRegex(RuntimeError, "fake dispatch"):
                LocalDeltaTransport.metadata_mutation(transport, applying, context=context)
            self.assertEqual(transport.db.execute(
                "SELECT state,receipt FROM local_delta4_metadata_operation "
                "ORDER BY before_version").fetchall(),
                [("committed", transport.db.execute(
                    "SELECT receipt FROM local_delta4_metadata_operation "
                    "WHERE before_version=1").fetchone()[0]), ("submitted", None)])
            with self.assertRaises(LocalDeltaUncertain):
                LocalDeltaTransport.metadata_recover(transport, applying, context=context)
            self.assertEqual(transport.dispatches, 2)  # no retry of submitted applying

            # Simulate the original named Delta commit arriving after the
            # client lost its response; recovery reads it and persists only
            # the local receipt, without another metadata SQL submission.
            intent = json.loads(applying["original"])["intent"]
            metadata = encoded({
                "profile": "ashlar-local-delta-metadata-commit/0.2",
                "installation_id": transport.installation_id,
                "lane": intent["lane"], "operation": intent["operation"],
                "operation_sha256": sha(encoded(intent)),
                "request_digest": intent["request_digest"]})
            transport.spark.conf.value = metadata
            transport.dispatch_mode = "commit"
            transport._metadata_sql(intent["statement"], intent["parameters"])
            recovered = LocalDeltaTransport.metadata_recover(
                transport, applying, context=context)
            self.assertEqual(recovered.rows[0]["version"], "3")
            self.assertEqual(transport.dispatches, 3)  # only external delayed commit injection
            self.assertEqual(transport.db.execute(
                "SELECT state FROM local_delta4_metadata_operation "
                "ORDER BY before_version").fetchall(),
                [("committed",), ("committed",)])

    def test_changed_reservation_and_wrong_metadata_lane_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            context = object()
            transport = FakeTransport(root, self.plan.request, context)
            reservation = reservation_for(transport, self.plan.request)
            changed = dict(reservation, original=reservation["original"] + " ")
            with self.assertRaises(LocalDeltaError):
                LocalDeltaTransport.metadata_mutation(transport, changed, context=context)
            intent = json.loads(reservation["original"])["intent"]
            intent["lane"] = "graph"
            wrong_lane = dict(reservation, original=encoded({
                "profile": reservation["profile"], "intent": intent}))
            wrong_lane["digest"] = sha(wrong_lane["original"])
            with self.assertRaises(LocalDeltaError):
                LocalDeltaTransport.metadata_mutation(transport, wrong_lane,
                                                      context=context)

    def test_delta4_journal_preflight_allows_only_known_profile_extensions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            target_path = root / "attempts"
            target_path.mkdir()
            from ashlar_host.delta_custody import DeltaTarget, local_journal_definitions
            target = DeltaTarget("local.configured.publication_attempt_phase",
                                 target_path, "attempt-uuid")
            profile = {"profile": "ashlar-local-current-installation-profile/0.1",
                       "physical_profile": "ashlar-delta/0.4-xxhash64"}
            registry = encoded({"installation": "test", "targets": [{
                "table": target.table, "path": str(target.path), "uuid": target.uuid}],
                "installation_profile": profile})
            path = root / "operations.sqlite"
            db = sqlite3.connect(path)
            for statement in local_journal_definitions().values():
                db.execute(statement)
            db.execute("INSERT INTO local_installation VALUES(1,?)", (registry,))
            db.execute("CREATE TABLE local_delta4_metadata_operation(operation TEXT PRIMARY KEY,lane TEXT NOT NULL,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)")
            db.commit(); db.close()
            LocalDeltaTransport.require_existing_journal(
                path, "test", (target,), installation_profile=profile)
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE arbitrary_extension(value TEXT)")
            db.commit(); db.close()
            with self.assertRaises(LocalDeltaError):
                LocalDeltaTransport.require_existing_journal(
                    path, "test", (target,), installation_profile=profile)


if __name__ == "__main__":
    unittest.main()
