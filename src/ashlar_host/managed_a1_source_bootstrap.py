# SPDX-License-Identifier: MIT OR Apache-2.0
"""Create two fresh public Truss sources and freeze/execute the managed A1 seed."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
from copy import deepcopy
from contextlib import ExitStack

from .r04_schedule import PROFILE as R04_PROFILE, R04Schedule, SLOT_ORDER, STAGES


MODEL_ID = "ashlar-local-dev-typed-graph-r21-20261101"
MODEL_REVISION = "r1"
MODEL_SHA256 = "b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2"
PUBLIC_SETUP_HELPER_SHA256 = "e8898eb892e42f8826cd334b8aa823d95604f432287c713cec20e06748367b0a"
MODEL_RELATIVE_PATH = ("docs/helix/04-build/evidence/current-upstream-release-20261101/"
                       "r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json")


def candidate_seed_group(source: str) -> dict:
    """Return the accepted seven-operation R28 seed as a public group config."""
    _need(source in ("A", "B"), "A/B source slot required")
    lower = source.lower()
    records = [
        {"alias": "person_a", "module_id": "m", "record_id": "Person",
         "values": {"person_key": f"{lower}-ada", "person_name": "Ada"}},
        {"alias": "person_b", "module_id": "m", "record_id": "Person",
         "values": {"person_key": f"{lower}-lin", "person_name": "Lin"}},
        {"alias": "place_a", "module_id": "m", "record_id": "Place",
         "values": {"place_key": f"{lower}-nyc", "place_name": "New York"}},
        {"alias": "place_b", "module_id": "m", "record_id": "Place",
         "values": {"place_key": f"{lower}-paris", "place_name": "Paris"}},
    ]
    def endpoint(alias):
        return {"state": "alias", "alias": alias}
    edges = [
        {"alias": "knows_ab", "module_id": "m", "relationship_id": "knows",
         "source": endpoint("person_a"), "target": endpoint("person_b"),
         "values": {}, "order_key": {"state": "null"}},
        {"alias": "located_a", "module_id": "m", "relationship_id": "located",
         "source": endpoint("person_a"), "target": endpoint("place_a"),
         "values": {}, "order_key": {"state": "null"}},
        {"alias": "located_b", "module_id": "m", "relationship_id": "located",
         "source": endpoint("person_b"), "target": endpoint("place_b"),
         "values": {}, "order_key": {"state": "null"}},
    ]
    return {"profile": "ashlar-truss-record-group/0.1",
            "records": records, "edges": edges}


def public_group_seed(seed: dict) -> dict:
    """Encode plain schedule semantics as the committed Truss scalar carrier."""
    _need(type(seed) is dict and set(seed) == {"profile", "records", "edges"}
          and seed["profile"] == "ashlar-truss-record-group/0.1",
          "Exact plain R28 seed semantics required")
    encoded = deepcopy(seed)
    for record in encoded["records"]:
        _need(type(record["values"]) is dict
              and all(type(value) is str for value in record["values"].values()),
              "R28 group source authored values must be exact strings")
        record["values"] = {key: {"kind": "string", "text": value}
                            for key, value in record["values"].items()}
    return encoded


def example_private_config(*, root: Path, bun_path: Path) -> dict:
    """Create a concrete local-development config skeleton with no credentials."""
    from importlib.resources import files
    repo = Path(__file__).resolve().parents[2]
    model_path = repo / MODEL_RELATIVE_PATH
    initdb = Path(str(files("pgserver"))) / "pginstall" / "bin" / "initdb"
    return {
        "profile": "ashlar-managed-a1-truss053-source-bootstrap/0.1",
        "root": str(Path(root)), "model_path": str(model_path),
        "model_sha256": MODEL_SHA256, "bun_path": str(Path(bun_path)),
        "pg18_initdb": str(initdb),
        "source_ids": {
            "A": {"feed": "ashlar-managed-a1-a-feed",
                  "consumer_id": "ashlar-managed-a1-a-consumer",
                  "ack_scope": "ashlar-managed-a1-a-ack-scope"},
            "B": {"feed": "ashlar-managed-a1-b-feed",
                  "consumer_id": "ashlar-managed-a1-b-consumer",
                  "ack_scope": "ashlar-managed-a1-b-ack-scope"}},
        "a1_group": candidate_seed_group("A"),
    }


def _need(condition, message):
    if not condition:
        raise ValueError(message)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def decode_canonical_json(raw: bytes, label: str):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"Duplicate {label} JSON member")
            result[key] = value
        return result
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    _need(canonical_json(value) == raw, f"Canonical original {label} bytes required")
    return value


def decode_public_json(raw: bytes, label: str):
    """Decode exact public Truss JSON without imposing Ashlar's byte encoding."""
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"Duplicate {label} JSON member")
            result[key] = value
        return result
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    _need(type(value) is dict, f"Public {label} JSON object required")
    return value


def validate_catalog_only_page(page_bytes: bytes, writer_xid: str):
    """Admit the first public acceptance transaction as zero graph effects."""
    value = decode_public_json(page_bytes, "catalog acceptance page")
    _need(type(writer_xid) is str and writer_xid.isdecimal()
          and value.get("interfaceVersion") == "truss-native-feed/0.1"
          and value.get("writerXid") == writer_xid
          and type(value.get("revisions")) is list and len(value["revisions"]) == 1
          and type(value.get("catalogPrerequisites")) is list
          and len(value["catalogPrerequisites"]) == 2
          and value.get("changes") == [] and value.get("sources") == []
          and value.get("reservations") == [],
          "Exact catalog-only public transaction with no graph effects required")
    return value


def candidate_r04_schedule(source_bindings: dict) -> R04Schedule:
    """Freeze all eight A/B semantics against actual public registration IDs.

    A1 is the previously authorized seven-operation graph seed: four objects
    (two Person and two Place) plus knows(Person,Person) and two
    located(Person,Place) edges. A2/A3 operate on the first A Person; B slots
    mirror the same progression against B's independent source. A4/B4 are
    explicit additive-candidate slots, not claims of accepted schema changes.
    """
    _need(type(source_bindings) is dict and set(source_bindings) == {"A", "B"},
          "Two actual public A/B source registration readbacks required")
    required = {"feed", "epoch", "installation_id", "consumer_id",
                "registration_id", "ack_scope", "ack_registration_id"}
    bindings = {}
    for source in ("A", "B"):
        raw = source_bindings[source]
        _need(type(raw) is dict and required.issubset(raw)
              and all(type(raw[k]) is str and raw[k] and "\0" not in raw[k]
                      for k in required),
              f"Complete actual {source} source, consumer and ACK scope readback required")
        bindings[source] = {k: raw[k] for k in sorted(required)}
    _need(len({bindings[s]["feed"] for s in ("A", "B")}) == 2
          and len({bindings[s]["installation_id"] for s in ("A", "B")}) == 2
          and len({bindings[s]["registration_id"] for s in ("A", "B")}) == 2
          and len({bindings[s]["ack_scope"] for s in ("A", "B")}) == 2
          and len({bindings[s]["ack_registration_id"] for s in ("A", "B")}) == 2,
          "Independent A/B source, installation, consumer and ACK registrations required")
    aliases = {s: {"person_a": f"person-{s.lower()}-a",
                   "person_b": f"person-{s.lower()}-b",
                   "place_a": f"place-{s.lower()}-a",
                   "place_b": f"place-{s.lower()}-b",
                   "knows_ab": f"knows-{s.lower()}-ab",
                   "located_a": f"located-{s.lower()}-a",
                   "located_b": f"located-{s.lower()}-b"} for s in ("A", "B")}
    slots = []
    for ordinal, (slot_name, stage) in enumerate(zip(SLOT_ORDER, STAGES)):
        source = slot_name[0]
        first_person = aliases[source]["person_a"]
        if stage == "create":
            entity_ref = first_person
            seed = candidate_seed_group(source)
            operations = ([{"operation": "create_object", "alias": row["alias"],
                "module": row["module_id"], "record": row["record_id"],
                "values": row["values"]} for row in seed["records"]] +
                [{"operation": "create_edge", "alias": row["alias"],
                    "module": row["module_id"], "relationship": row["relationship_id"],
                    "source": row["source"]["alias"], "target": row["target"]["alias"],
                    "order_key": None} for row in seed["edges"]])
            mutation = {"operations": operations}
            expected = {"outcome": "created", "operation_count": len(operations),
                        "entity_ref": entity_ref}
        elif stage == "update":
            entity_ref = first_person
            mutation = {"operation": "update_object", "target": first_person,
                        "values": {"person_name": f"{source} updated"}}
            expected = {"outcome": "updated", "identity_preserved": True,
                        "entity_ref": first_person}
        elif stage == "delete":
            entity_ref = first_person
            incident = ["knows_ab", "located_a"]
            mutation = {"operations": [
                {"operation": "delete_edge", "target": aliases[source][alias]}
                for alias in incident] +
                [{"operation": "delete_object", "target": first_person}]}
            expected = {"outcome": "deleted", "identity_preserved": True,
                        "deleted_aliases": [aliases[source][x] for x in incident] + [first_person]}
        else:
            entity_ref = f"candidate:{source}:schema-addition"
            mutation = {"operation": "additive_schema_candidate",
                        "model": MODEL_ID, "requires_new_accepted_model_revision": True}
            expected = {"outcome": "candidate_only", "no_source_effect_in_a1": True}
        slots.append({"ordinal": ordinal, "slot": slot_name, "source": source,
            "stage": stage, "entity_ref": entity_ref,
            "predecessor_slot": ordinal - 2 if stage in ("update", "delete") else None,
            "mutation": mutation, "expected_transition": expected})
    pins = {s: {"documentId": MODEL_ID, "documentRevision": MODEL_REVISION,
                "modelSha256": MODEL_SHA256, "modelPath": MODEL_RELATIVE_PATH}
            for s in ("A", "B")}
    state = {s: {"objects": {}, "edges": {}, "tombstones": [], "history": []}
             for s in ("A", "B")}
    oracle = []
    for prefix in range(9):
        counts = {s: sum(slot["source"] == s for slot in slots[:prefix]) for s in ("A", "B")}
        oracle.append({"prefix": prefix, "applied_slots": [x["slot"] for x in slots[:prefix]],
            "source_slot_counts": counts,
            "expected_current": {s: {"objects": deepcopy(state[s]["objects"]),
                                     "edges": deepcopy(state[s]["edges"])}
                                 for s in ("A", "B")},
            "expected_tombstones": {s: list(state[s]["tombstones"]) for s in ("A", "B")},
            "expected_history": {s: deepcopy(state[s]["history"]) for s in ("A", "B")},
            "expected_history_row_count": {s: len(state[s]["history"]) for s in ("A", "B")},
            "candidate_schema_state": {s: "unchanged" for s in ("A", "B")}})
        if prefix == 8:
            break
        slot = slots[prefix]
        source = slot["source"]
        current = state[source]
        stage = slot["stage"]
        if stage == "create":
            for operation in slot["mutation"]["operations"]:
                if operation["operation"] == "create_object":
                    current["objects"][operation["alias"]] = {
                        "module": operation["module"], "record": operation["record"],
                        "values": deepcopy(operation["values"]),
                        "presence": sorted(operation["values"])}
                else:
                    current["edges"][operation["alias"]] = {
                        "module": operation["module"], "relationship": operation["relationship"],
                        "source": operation["source"], "target": operation["target"],
                        "order_key": None, "values": {}, "presence": []}
        elif stage == "update":
            current["objects"]["person_a"]["values"].update(slot["mutation"]["values"])
            current["objects"]["person_a"]["presence"] = sorted(current["objects"]["person_a"]["values"])
        elif stage == "delete":
            for name in ("knows_ab", "located_a"):
                current["edges"].pop(name, None)
                current["tombstones"].append(aliases[source][name])
            current["objects"].pop("person_a", None)
            current["tombstones"].append(aliases[source]["person_a"])
        if stage != "additive-candidate":
            operations = slot["mutation"].get("operations", [slot["mutation"]])
            current["history"].extend({"slot": slot["slot"],
                "operation": deepcopy(operation),
                "expected_transition": deepcopy(slot["expected_transition"])}
                for operation in operations)
    return R04Schedule.from_document({"profile": R04_PROFILE,
        "source_bindings": bindings, "accepted_model_pins": pins,
        "slots": slots, "prefix_oracle": oracle})


def validate_private_bootstrap_config(raw: bytes) -> dict:
    """Pure validation for the owner-private runner configuration bytes."""
    _need(type(raw) is bytes and 0 < len(raw) <= 4 * 1024 * 1024,
          "Bounded private A/B bootstrap JSON required")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate A/B bootstrap config member")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("Closed private A/B bootstrap JSON required") from error
    _need(type(value) is dict and canonical_json(value) == raw,
          "Canonical duplicate-free private A/B bootstrap JSON required")
    required = {"profile", "root", "model_path", "model_sha256", "bun_path",
                "pg18_initdb", "source_ids", "a1_group"}
    _need(type(value) is dict and set(value) == required
          and value["profile"] == "ashlar-managed-a1-truss053-source-bootstrap/0.1",
          "Closed managed A1 source-bootstrap config required")
    for key in ("root", "model_path", "bun_path", "pg18_initdb"):
        path = Path(value[key])
        _need(type(value[key]) is str and path.is_absolute() and path.resolve() == path,
              f"Canonical absolute {key} required")
    _need(value["model_sha256"] == MODEL_SHA256,
          "Exact committed R28 UMF 0.10 model digest required")
    source_fields = {"feed", "consumer_id", "ack_scope"}
    _need(type(value["source_ids"]) is dict and set(value["source_ids"]) == {"A", "B"}
            and all(type(value["source_ids"][source]) is dict
              and set(value["source_ids"][source]) == source_fields
              and all(type(value["source_ids"][source][k]) is str
                      and value["source_ids"][source][k]
                      for k in source_fields) for source in ("A", "B"))
          and type(value["a1_group"]) is dict,
          "A/B selected feed identities and closed A1 group selection required")
    _need(value["source_ids"]["A"]["consumer_id"]
          != value["source_ids"]["B"]["consumer_id"]
          and value["source_ids"]["A"]["feed"] != value["source_ids"]["B"]["feed"]
          and value["source_ids"]["A"]["ack_scope"] != value["source_ids"]["B"]["ack_scope"],
          "Distinct A/B feed, consumer and ACK scope selections required")
    return value


def bootstrap_from_private_config(config_bytes: bytes):
    """Construct the live holder from a closed private run configuration.

    Native effects are deliberately behind this explicit worker entry point;
    its caller is the staged managed trial runner, never import-time code.
    """
    config = validate_private_bootstrap_config(config_bytes)
    root = Path(config["root"])
    model_path = Path(config["model_path"])
    bun = Path(config["bun_path"])
    initdb = Path(config["pg18_initdb"])
    model = model_path.read_bytes()
    expected_model_path = Path(__file__).resolve().parents[2] / MODEL_RELATIVE_PATH
    _need(model_path == expected_model_path and not model_path.is_symlink(),
          "Exact committed R28 UMF model path required")
    _need(hashlib.sha256(model).hexdigest() == MODEL_SHA256,
          "Committed R28 model bytes changed")
    parsed_model = json.loads(model)
    _need(parsed_model.get("id") == MODEL_ID and parsed_model.get("umf") == "0.10.0",
          "Declared UMF 0.10 R28 model required")
    _need(root.is_dir() and not root.is_symlink()
          and root.stat().st_uid == os.getuid() and stat.S_IMODE(root.stat().st_mode) == 0o700,
          "Owner-private supervisor run root required")
    _need(initdb.is_file() and os.access(initdb, os.X_OK) and bun.is_file()
          and os.access(bun, os.X_OK), "Selected PostgreSQL 18 initdb and Bun required")
    _need(config["a1_group"] == candidate_seed_group("A"),
          "Exact frozen R28 A1 seed group required before source setup")
    from importlib.resources import files
    selected_initdb = Path(str(files("pgserver"))) / "pginstall" / "bin" / "initdb"
    _need(initdb == selected_initdb and not initdb.is_symlink(),
          "Exact bundled PostgreSQL 18 initdb required")

    setup_path = Path("/private/tmp/ashlar-r61-fresh-cohort-plan/tools/r61_fresh_truss_setup_stage.py")
    _need(setup_path.is_file() and not setup_path.is_symlink(),
          "Exact reviewed public Truss SDK setup helper required")
    _need(hashlib.sha256(setup_path.read_bytes()).hexdigest() == PUBLIC_SETUP_HELPER_SHA256,
          "Exact public Truss SDK setup helper bytes required")
    spec = importlib.util.spec_from_file_location("_ashlar_r61_public_truss_setup", setup_path)
    _need(spec is not None and spec.loader is not None,
          "Exact public Truss SDK setup helper import required")
    setup_helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(setup_helper)
    import truss.local_runtime as local_runtime
    _need(hashlib.sha256(Path(local_runtime.__file__).read_bytes()).hexdigest()
          == setup_helper.LOCAL_RUNTIME_SHA256,
          "Exact selected public Truss PostgreSQL 18 runtime source required")
    from truss import LocalPostgres
    import psycopg
    from .truss_note_publication import read_page, register_note_consumer
    from .truss_record_group import run_create_truss_record_group_command
    from ashlar.truss_native_feed import native_feed_batch, typed_scalar_entity_changes
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053SourceSelection,
    )
    from .truss053_source_custody import Truss053A1SourceHolder
    from .truss_current_catalog import installed_fresh_truss_runtime_pins

    installed_fresh_truss_runtime_pins()

    origin = b'{"operator":"ashlar-managed-current-a1-fresh-local-source"}\n'
    data_directories = {source: root / f"pgdata-{source}" for source in ("A", "B")}
    setups = {}
    for source in ("A", "B"):
        pgdata = data_directories[source]
        _need(not pgdata.exists() and not pgdata.is_symlink(),
              f"Fresh independent {source} PostgreSQL 18 cluster required")
        pgdata.mkdir(mode=0o700)
        initialized = subprocess.run([str(initdb), "--no-sync", "-D", str(pgdata),
            "--auth=trust", "--auth-local=trust", "--encoding=UTF8", "-U", "postgres"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=90, check=False)
        _need(initialized.returncode == 0 and (pgdata / "PG_VERSION").read_text().strip() == "18",
              f"Fresh PostgreSQL 18 {source} cluster initialization failed")
        origin_path = root / f"origin-{source}.json"
        fd = os.open(origin_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(origin)
            stream.flush()
            os.fsync(stream.fileno())
        setups[source] = setup_helper._run_public_truss_setup(
            config={"document_id": MODEL_ID, "document_revision": MODEL_REVISION,
                    "request_key": f"ashlar-managed-a1-{source.lower()}-accept-r1"},
            paths={"data_directory": pgdata, "bun": bun}, source=model,
            origin=origin, module_ids=tuple(item["id"] for item in parsed_model["modules"]))

    selections = {}
    with ExitStack() as stack:
        runtimes = {source: stack.enter_context(LocalPostgres(data_directories[source]))
                    for source in ("A", "B")}
        factories = {}
        for source in ("A", "B"):
            runtime = runtimes[source]
            _need(runtime.info.data_directory.resolve() == data_directories[source]
                  and runtime.info.server_version.split(".", 1)[0] == "18",
                  f"Original owned PostgreSQL 18 {source} runtime required")
            with psycopg.connect(runtime.info.connection_uri, autocommit=True,
                                 connect_timeout=5) as health:
                _need(health.execute("SHOW fsync").fetchone()[0] == "on",
                      "PostgreSQL fsync must remain enabled")
            parameters = psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
            admin_uri = runtime.info.connection_uri
            ordinary_uri = psycopg.conninfo.make_conninfo(
                **dict(parameters, user=setups[source]["roles"]["consumer"]))
            def admin_factory(uri=admin_uri):
                return psycopg.connect(uri, autocommit=False, connect_timeout=5)
            def ordinary_factory(uri=ordinary_uri):
                return psycopg.connect(uri, autocommit=False, connect_timeout=5)
            registration = register_note_consumer(admin_factory,
                config["source_ids"][source]["consumer_id"], setups[source]["roles"]["reader"])
            selections[source] = {"runtime": runtime, "admin_factory": admin_factory,
                "ordinary_factory": ordinary_factory, "registration": registration,
                "admin_uri": admin_uri, "ordinary_uri": ordinary_uri}

        a_ids = config["source_ids"]["A"]
        b_ids = config["source_ids"]["B"]
        selected_cursors = {}
        for source in ("A", "B"):
            selection = selections[source]
            cursor_page = read_page(selection["ordinary_factory"],
                config["source_ids"][source]["consumer_id"], maximum_bytes=1048576)
            try:
                prior = decode_public_json(cursor_page.prior.content,
                                              f"{source} genesis cursor")
                following = decode_public_json(cursor_page.next.content,
                                                  f"{source} next genesis cursor")
            except (UnicodeError, ValueError) as error:
                raise ValueError("Public registered-consumer genesis cursor JSON required") from error
            _need(type(prior) is dict
                  and prior.get("profile") == "truss-caller-asserted-feed/0.1"
                  and prior.get("installationId") == setups[source]["installation"]["installation_id"]
                  and prior.get("consumerId") == selection["registration"].consumer_id
                  and prior.get("registrationId") == selection["registration"].registration_id
                  and prior.get("nextXid") == "0",
                  f"Actual public {source} registered-consumer genesis cursor required")
            _need(cursor_page.outcome == "transaction"
                  and type(following) is dict
                  and all(following.get(key) == prior.get(key) for key in (
                      "profile", "installationId", "sourceEpoch", "consumerId",
                      "registrationId"))
                  and type(following.get("nextXid")) is str
                  and following["nextXid"].isdecimal()
                  and str(int(following["nextXid"])) == following["nextXid"]
                  and int(following["nextXid"]) > 0,
                  f"Actual committed {source} catalog acceptance transaction required")
            validate_catalog_only_page(cursor_page.content, cursor_page.writer_xid)
            selected_cursors[source] = {"epoch": prior.get("sourceEpoch"),
                "cursor_bytes": cursor_page.prior.content,
                "consumer_id": prior.get("consumerId"),
                "registration_id": prior.get("registrationId"),
                "installation_id": prior.get("installationId")}
            _need(type(selected_cursors[source]["epoch"]) is str
                  and selected_cursors[source]["epoch"],
                  f"Actual public {source} source epoch required")
        bindings = {}
        for source, selected in (("A", a_ids), ("B", b_ids)):
            result = setups[source]
            consumer_registration = selections[source]["registration"]
            bindings[source] = {
                "feed": selected["feed"], "epoch": selected_cursors[source]["epoch"],
                "installation_id": result["installation"]["installation_id"],
                "consumer_id": consumer_registration.consumer_id,
                "registration_id": consumer_registration.registration_id,
                "ack_scope": selected["ack_scope"],
                "ack_registration_id": consumer_registration.registration_id,
                "catalog_registration_id": result["registration_id"],
                "catalog_revision": str(result["revision"]),
            }
            cursor_path = root / f"genesis-cursor-{source}.json"
            fd = os.open(cursor_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(selected_cursors[source]["cursor_bytes"])
                stream.flush()
                os.fsync(stream.fileno())
        schedule = candidate_r04_schedule(bindings)
        schedule_path = root / "r04-schedule.json"
        fd = os.open(schedule_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(schedule.raw)
            stream.flush()
            os.fsync(stream.fileno())

        a1 = config["a1_group"]
        public_a1 = public_group_seed(a1)
        group_config = {"profile": "ashlar-truss-record-group/0.1",
            "connection_uri": selections["A"]["ordinary_uri"], "bun": str(bun),
            "document_path": str(model_path), "document_revision": MODEL_REVISION,
            "document_id": MODEL_ID, "source_version": "0.10.0",
            "origin_path": str(root / "origin-A.json"),
            "request_key": "ashlar-managed-a1-a1-seven-operation-group",
            "records": public_a1["records"], "edges": public_a1["edges"]}
        group_path = root / "a1-group.json"
        fd = os.open(group_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical_json(group_config))
            stream.flush()
            os.fsync(fd := stream.fileno())
        context = object()
        for source in ("A", "B"):
            bound = bindings[source]
            selection = ManagedCurrentTruss053SourceSelection(
                source, bound["feed"], bound["epoch"], bound["installation_id"],
                bound["consumer_id"], bound["registration_id"],
                bound["catalog_registration_id"], bound["catalog_revision"],
                selections[source]["ordinary_factory"])
            selections[source] = selection
        holder = Truss053A1SourceHolder(schedule=schedule,
            source_selections={source: selections[source] for source in ("A", "B")},
            context=context)
        holder.phase = "a0"

        def apply_a1_after_a0(checkpoint_bytes):
            current_a = read_page(selections["A"].connection_factory,
                a_ids["consumer_id"], maximum_bytes=1048576)
            current_b = read_page(selections["B"].connection_factory,
                b_ids["consumer_id"], maximum_bytes=1048576)
            _need(current_a.prior.content == checkpoint_bytes
                  and current_a.outcome == "empty_interval"
                  and current_b.prior.content == selected_cursors["B"]["cursor_bytes"],
                  "A0 ACK checkpoint and untouched B genesis cursor must remain exact")
            result_path = root / "a1-public-result.json"
            freeze_path = root / "a1-truss-request-freeze.json"
            run_create_truss_record_group_command(group_path, result_path,
                require_fresh=True, freeze_request_path=freeze_path,
                schedule_sha256=schedule.sha256, slot="A1")
            page = read_page(selections["A"].connection_factory,
                a_ids["consumer_id"], maximum_bytes=1048576)
            _need(page.outcome == "transaction"
                  and page.registration_id == bindings["A"]["registration_id"]
                  and page.prior.content == checkpoint_bytes,
                  "Exact public A1 transaction page must continue the matched A0 cursor")
            batch = native_feed_batch(page.content, sha256=page.sha256,
                prior=page.prior.content, next=page.next.content, feed=a_ids["feed"])
            parsed_batch = typed_scalar_entity_changes(page.content, sha256=page.sha256,
                prior=page.prior.content, next=page.next.content, feed=a_ids["feed"])
            _need(len(parsed_batch.changes) == 7 and batch.epoch == bindings["A"]["epoch"],
                  "Exact public A1 seven-change typed feed page required")
            holder._a0_page_next = checkpoint_bytes
            holder.genesis = None
            holder.genesis_bytes = None

        holder._advance_a1_callback = apply_a1_after_a0
        holder._bootstrap_runtimes = stack.pop_all()
        return holder
