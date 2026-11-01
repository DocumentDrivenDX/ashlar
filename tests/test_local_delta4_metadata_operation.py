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
import uuid
from types import SimpleNamespace

from ashlar.attempt_store import verify_request_digest
from ashlar.native import SQLResult, quote_table_identifier
from ashlar_host.delta_custody import (
    LocalDeltaError, LocalDeltaUncertain, LocalDeltaTransport, _retained_installation_profile,
    encoded, sha,
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
    def sdk0101_profile(self, root, targets, *, table_prefix='local.configured'):
        from ashlar.typed_graph_source_sdk0101 import GRAPH_CHECK_PIN
        from ashlar_host.configured_local_delta4_publication import (
            INSTALLATION_ROLES, LOCAL_ROLE_BY_CARRIER,
        )
        from ashlar_host.local_current_delta4_umf_sdk0101 import (
            BRIDGE_SHA256, BUILD_INPUTS_SHA256, LOCAL_TARGET_PROFILE, MANIFEST_SHA256,
            SOURCE_POLICY_SHA256, UMF_REALIZATION_PROFILE,
        )
        roles = {}
        for role in INSTALLATION_ROLES:
            alias = LOCAL_ROLE_BY_CARRIER[role]
            target = targets[table_prefix + "." + alias]
            roles[alias] = {"carrier_role": role, "table": target.table,
                            "path": str(target.path), "uuid": target.uuid}
        return {
            "profile": "ashlar-local-current-installation-profile/0.4",
            "physical_profile": "ashlar-delta/0.4-xxhash64",
            "lookup_hash_profile": "spark-xxhash64-typed-key/0.1",
            "generated_carriers_sha256": "cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa",
            "umf_revision": "0.10.1", "local_realization": LOCAL_TARGET_PROFILE,
            "umf_realization_profile": UMF_REALIZATION_PROFILE,
            "umf_candidate_commit": GRAPH_CHECK_PIN,
            "umf_selected_tag_commit": GRAPH_CHECK_PIN,
            "umf_archive_sha256": "3d95fff822f4f44b3d1a8e56185c3697e94ee66f5315cda7389f5301f3d16d26",
            "umf_source_policy_sha256": SOURCE_POLICY_SHA256,
            "umf_realization_manifest_sha256": MANIFEST_SHA256,
            "umf_bridge_sha256": BRIDGE_SHA256,
            "umf_build_inputs_sha256": BUILD_INPUTS_SHA256,
            "source_policy_revision": "ashlar-local-current-delta4-sdk0101-source-policy/0.1",
            "typed_graph_source_revision": GRAPH_CHECK_PIN,
            "writer_settings": [{"key": "spark.sql.parquet.compression.codec", "value": "zstd"}],
            "roles": roles,
        }

    def test_sdk0101_profile_is_exact_and_bound_to_all_six_transport_targets(self):
        from ashlar_host.delta_custody import DeltaTarget
        from ashlar_host.configured_local_delta4_publication import (
            INSTALLATION_ROLES, LOCAL_ROLE_BY_CARRIER,
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            targets = {}
            for role in INSTALLATION_ROLES:
                alias = LOCAL_ROLE_BY_CARRIER[role]
                path = root / alias
                path.mkdir()
                table = "local.configured." + alias
                targets[table] = DeltaTarget(
                    table, path, str(uuid.uuid5(uuid.NAMESPACE_URL, "sdk0101-" + role)))
            profile = self.sdk0101_profile(root, targets)
            self.assertEqual(_retained_installation_profile(
                profile, targets=targets.values()), profile)

            changed = dict(profile, typed_graph_source_revision="0" * 40)
            with self.assertRaisesRegex(LocalDeltaError, "profile"):
                _retained_installation_profile(changed, targets=targets.values())
            with_extra = dict(profile, unrecognized=True)
            with self.assertRaisesRegex(LocalDeltaError, "profile"):
                _retained_installation_profile(with_extra, targets=targets.values())
            wrong_target = dict(targets)
            first = next(iter(wrong_target.values()))
            wrong_target[first.table] = DeltaTarget(first.table, first.path, str(uuid.uuid4()))
            with self.assertRaisesRegex(LocalDeltaError, "native target"):
                _retained_installation_profile(profile, targets=wrong_target.values())

            registry = {"installation": "sdk0101-test",
                        "targets": [{"table": target.table, "path": str(target.path),
                                     "uuid": target.uuid}
                                    for target in sorted(targets.values(), key=lambda item: item.table)],
                        "installation_profile": profile}
            transport = object.__new__(LocalDeltaTransport)
            transport.journal_path = root / "operations.sqlite"
            transport.targets = targets
            transport._journal(encoded(registry), create=True)
            transport.db.execute("CREATE TABLE local_delta4_attempt_phase(request_digest TEXT NOT NULL,phase TEXT NOT NULL,payload_json TEXT NOT NULL,payload_sha256 TEXT NOT NULL,PRIMARY KEY(request_digest,phase))")
            transport.db.execute("CREATE TABLE local_delta4_metadata_lane_reservation(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)")
            transport.db.execute("CREATE TABLE local_delta4_output_receipt(request_digest TEXT PRIMARY KEY,original TEXT NOT NULL,receipt_sha256 TEXT NOT NULL)")
            transport.db.execute("CREATE TABLE local_delta4_metadata_operation(operation TEXT PRIMARY KEY,lane TEXT NOT NULL,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)")
            transport.db.commit()
            transport.db.close()
            LocalDeltaTransport.require_existing_journal(
                transport.journal_path, "sdk0101-test", tuple(targets.values()),
                installation_profile=profile)

    def test_sdk0101_profile_accepts_only_complete_configured_or_trussgraph_namespaces(self):
        from ashlar_host.delta_custody import DeltaTarget
        from ashlar_host.configured_local_delta4_publication import (
            INSTALLATION_ROLES, LOCAL_ROLE_BY_CARRIER, _delta4_profile_role,
            _delta4_profile_table_prefix, _delta4_target,
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            for prefix in ('local.configured', 'local.trussgraph'):
                targets = {}
                for role in INSTALLATION_ROLES:
                    alias = LOCAL_ROLE_BY_CARRIER[role]
                    path = root / prefix.rsplit('.', 1)[-1] / alias
                    path.mkdir(parents=True)
                    table = prefix + '.' + alias
                    targets[table] = DeltaTarget(
                        table, path, str(uuid.uuid5(uuid.NAMESPACE_URL, prefix + '-' + role)))
                profile = self.sdk0101_profile(root, targets, table_prefix=prefix)
                self.assertEqual(_delta4_profile_table_prefix(profile), prefix)
                retained = _retained_installation_profile(profile, targets=targets.values())
                self.assertEqual(retained, profile)
                self.assertEqual(_delta4_profile_role(
                    profile, 'publication_attempt_phase', table_prefix=prefix)['table'],
                    prefix + '.attempts')
                transport = SimpleNamespace(installation_profile=profile, targets=targets)
                self.assertIs(_delta4_target(
                    transport, 'publication_attempt_phase'),
                    targets[prefix + '.attempts'])

            graph_targets = {}
            graph_prefix = 'local.trussgraph'
            for role in INSTALLATION_ROLES:
                alias = LOCAL_ROLE_BY_CARRIER[role]
                path = root / 'mixed' / alias
                path.mkdir(parents=True)
                table = graph_prefix + '.' + alias
                graph_targets[table] = DeltaTarget(
                    table, path, str(uuid.uuid5(uuid.NAMESPACE_URL, 'mixed-' + role)))
            graph_profile = self.sdk0101_profile(root, graph_targets, table_prefix=graph_prefix)
            mixed = json.loads(json.dumps(graph_profile))
            mixed['roles']['manifest']['table'] = 'local.configured.manifest'
            with self.assertRaises(LocalDeltaError):
                _retained_installation_profile(mixed, targets=graph_targets.values())
            duplicate = json.loads(json.dumps(graph_profile))
            duplicate['roles']['manifest']['uuid'] = duplicate['roles']['attempts']['uuid']
            with self.assertRaises(LocalDeltaError):
                _retained_installation_profile(duplicate, targets=graph_targets.values())

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
