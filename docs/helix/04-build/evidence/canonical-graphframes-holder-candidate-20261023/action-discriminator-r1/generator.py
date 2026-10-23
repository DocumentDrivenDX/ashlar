"""Frozen R05 GraphFrames identity discriminator source (pure, no Spark start)."""
import json


PROFILE = "ashlar-canonical-synthetic-pinned-delta/0.1"
GENERATOR_REVISION = "canonical-graphframes-r05-identity-discriminator/0.1"
SOURCE_A = "ashlar-r05-source-a"
SOURCE_B = "ashlar-r05-source-b"
SCHEMA_REVISION = "canonical-r05-discriminator-r1"
SOURCE_FEED = "synthetic-canonical-r05-discriminator"
EPOCH = "1"
PUBLISHED_AT = "2026-10-23T00:00:00.123456Z"
SEED = [SOURCE_A, 1, 101]
CARRIER_DEFINITION_SHA256 = "cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa"
UMF_REVISION = "2ede4c879bea5db13794a6fa30e67c821306c17f"
RUNTIME = {"spark": "4.2.0", "delta": "4.4.1",
           "graphframes": "0.12.3", "jdk": "21.0.13"}


def _node(source, identity, position, name):
    return {
        "source_system": source, "type_id": 1, "id": identity,
        "logical_key_json": "[]", "schema_revision": SCHEMA_REVISION,
        "entity_version": 1, "props_json": json.dumps({"name": name}, separators=(",", ":")),
        "retained_json": "{}", "root_id": None, "source_feed": SOURCE_FEED,
        "source_epoch": EPOCH, "source_position": position,
        "published_at": PUBLISHED_AT, "apply_batch_id": "fixture-nodes-r1",
        "source_cursor_json": "{}", "source_delivery_id": f"node-delivery-{source}-{identity}",
    }


def _edge(identity, source, source_id, target_id, position, ordinal):
    return {
        "source_system": source, "rel_type_id": 7, "id": identity,
        "source_type": 1, "source_id": source_id, "target_type": 1,
        "target_id": target_id, "schema_revision": SCHEMA_REVISION,
        "entity_version": 1, "props_json": json.dumps({"ordinal": ordinal}, separators=(",", ":")),
        "retained_json": "{}", "order_key": None, "source_feed": SOURCE_FEED,
        "source_epoch": EPOCH, "source_position": position,
        "published_at": PUBLISHED_AT, "apply_batch_id": "fixture-edges-r1",
        "source_cursor_json": "{}", "source_delivery_id": f"edge-delivery-{identity}",
    }


def source_document():
    """Return a closed tiny .4 source whose seed action detects key collapse."""
    nodes = [
        _node(SOURCE_A, 101, 1, "source-a-seed"),
        _node(SOURCE_B, 101, 2, "source-b-overlap"),
        _node(SOURCE_A, 202, 3, "source-a-target"),
        _node(SOURCE_B, 303, 4, "source-b-target"),
        _node(SOURCE_A, 404, 5, "source-a-isolated"),
    ]
    edges = [
        _edge(9001, SOURCE_A, 101, 202, 1, 1),
        _edge(9002, SOURCE_A, 101, 202, 2, 2),
        _edge(9003, SOURCE_A, 101, 101, 3, 3),
        # If source_system is dropped from vertex identity, this foreign-source
        # edge spuriously appears in the selected source-A seed's one-hop bag.
        _edge(9004, SOURCE_B, 101, 303, 4, 4),
    ]
    return {
        "profile": PROFILE, "fixtureId": "canonical-graphframes-r05-identity-discriminator/0.1",
        "generatorRevision": GENERATOR_REVISION, "carrierProfile": "ashlar-delta/0.4-xxhash64",
        "carrierDefinitionSha256": CARRIER_DEFINITION_SHA256,
        "umfRevision": UMF_REVISION, "runtime": RUNTIME,
        "rowCounts": {"object_current": 5, "edge_current": 4},
        "selectedSeed": SEED,
        "formula": {
            "overlappingLocalIds": "source A and source B both contain (type_id,id)=(1,101)",
            "sourceQualifiedVertices": "vertex identity is (source_system,type_id,id)",
            "edgeIdentity": "(source_system,rel_type_id,id), edge ids 9001..9004",
            "endpoints": "9001/9002: A(1,101)->A(1,202); 9003: A(1,101)->A(1,101); 9004: B(1,101)->B(1,303)",
            "isolatedNode": "A(1,404) has no incident edge",
            "lookupHash": "Spark SQL xxhash64(CAST(source_system AS STRING), CAST(type_id_or_rel_type_id AS BIGINT), CAST(id AS BIGINT)); seed 42",
            "sourceFields": "source positions 1..count; fixed UTC timestamp " + PUBLISHED_AT,
            "nullPayload": "only object_current.root_id and edge_current.order_key are null",
        },
        "tables": {"object_current": nodes, "edge_current": edges},
    }


def source_bytes():
    return json.dumps(source_document(), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def write_fixture_tables(spark, fixture_root):
    """Fail closed until an admitted UMF `.4` table creator is available.

    Bare ``DataFrameWriter.save`` creates a table without the selected carrier's
    checked UMF DDL/clustering and writer-7 protocol contract. Writing such a
    table here would make the fixture look canonical while it is not. The
    frozen source/oracle and mocked holder path remain useful inert controls;
    this helper deliberately performs no filesystem or Spark operation.
    """
    del spark, fixture_root
    raise PermissionError(
        "R05 materialization blocked: use admitted UMF-generated `.4` DDL and "
        "verify the selected writer-7 protocol before exposing fixture paths"
    )
