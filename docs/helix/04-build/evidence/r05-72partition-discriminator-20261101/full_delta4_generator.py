"""Offline deterministic R05 `.4` carrier SQL plan using admitted UMF DDL.

No Spark session is imported or started. Building a plan calls only the pinned
UMF V8 bridge to realize and admit both DDL proposals; callers decide whether
to execute those proposals in a separately authorized runner.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from ashlar_host.local_current_delta4_umf import LocalUmfDelta4Bridge, pinned_resources
from ashlar_host.managed_current_xxhash64_carriers import (
    load_current_managed_xxhash64_carriers,
)

PROFILE = "ashlar-r05-full-delta4-generator/0.1"
SOURCE_SYSTEM = "r05-full-scale-source"
SCHEMA_REVISION = "r05-full-delta4-v1"
PUBLISHED_AT = "2026-10-23T00:00:00.000Z"
NODE_ROWS = 1_000_000_000
EDGE_ROWS = 5_000_000_000
ROLES = ("object_current", "edge_current")


@dataclass(frozen=True)
class RolePlan:
    role: str
    path: str
    ddl: str
    insert_sql: str
    schema_sha256: str
    layout: dict
    writer_settings: tuple


@dataclass(frozen=True)
class GenerationPlan:
    profile: str
    node_rows: int
    edge_rows: int
    roles: tuple[RolePlan, RolePlan]
    endpoint_oracle: dict


def endpoint_oracle(edge_ordinal: int, node_rows: int = NODE_ROWS) -> tuple[int, int]:
    if type(edge_ordinal) is not int or type(node_rows) is not int or node_rows <= 0:
        raise ValueError("Integer edge ordinal and positive node count required")
    if not 0 <= edge_ordinal < node_rows * 5:
        raise ValueError("Edge ordinal outside the five-occurrences-per-node domain")
    source = edge_ordinal // 5
    target = (source + (edge_ordinal % 5) // 2) % node_rows
    return source, target


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _path_relation(path: str) -> str:
    if (type(path) is not str or not path.startswith("/") or "`" in path
            or "\x00" in path or "\n" in path or "\r" in path
            or len(path.encode("utf-8")) > 4096):
        raise ValueError("Absolute bounded Delta path required")
    return "delta.`" + path + "`"


def _expressions(role: str, edge_rows: int) -> dict[str, str]:
    identity_value = 1 if role == "object_current" else 7
    common = {
        "source_system": _literal(SOURCE_SYSTEM),
        "schema_revision": _literal(SCHEMA_REVISION),
        "entity_version": "CAST(1 AS BIGINT)",
        "props_json": _literal("{}"),
        "retained_json": _literal("{}"),
        "source_feed": _literal("r05-local-synthetic"),
        "source_epoch": _literal("1"),
        "source_position": "CAST(id AS BIGINT)",
        "published_at": f"TIMESTAMP {_literal(PUBLISHED_AT)}",
        "apply_batch_id": _literal(SCHEMA_REVISION),
        "source_cursor_json": _literal("{}"),
        "source_delivery_id": "CAST(id AS STRING)",
        "id": "CAST(id AS BIGINT)",
        "lookup_hash": (
            f"xxhash64(CAST({_literal(SOURCE_SYSTEM)} AS STRING), "
            f"CAST({identity_value} AS BIGINT), CAST(id AS BIGINT))"
        ),
    }
    if role == "object_current":
        common.update({
            "type_id": "CAST(1 AS BIGINT)",
            "logical_key_json": _literal("[]"),
            "root_id": "CAST(NULL AS BIGINT)",
        })
    elif role == "edge_current":
        common.update({
            "rel_type_id": "CAST(7 AS BIGINT)",
            "source_type": "CAST(1 AS BIGINT)",
            "source_id": "CAST(floor(id / 5) AS BIGINT)",
            "target_type": "CAST(1 AS BIGINT)",
            "target_id": f"CAST(pmod(floor(id / 5) + floor(pmod(id, 5) / 2), {edge_rows // 5}) AS BIGINT)",
            "order_key": "CAST(NULL AS STRING)",
        })
    else:
        raise ValueError("Current carrier role required")
    return common


def _insert_sql(role: str, path: str, fields: tuple, node_rows: int, edge_rows: int) -> str:
    expected = _expressions(role, edge_rows)
    names = tuple(item[0] for item in fields)
    if len(names) != len(set(names)) or set(names) != set(expected):
        raise ValueError(f"UMF `.4` {role} schema differs from the frozen row-value oracle")
    projection = ",\n  ".join(f"{expected[name]} AS `{name}`" for name in names)
    count = node_rows if role == "object_current" else edge_rows
    return f"INSERT INTO {_path_relation(path)}\nSELECT\n  {projection}\nFROM range({count})"


def validate_offline_oracle(node_rows: int = NODE_ROWS, edge_rows: int = EDGE_ROWS) -> None:
    """Validate selected carrier schemas and independently frozen row formulas."""
    if (type(node_rows) is not int or type(edge_rows) is not int
            or node_rows <= 0 or edge_rows != node_rows * 5):
        raise ValueError("Positive node count and exactly five edge occurrences per node required")
    carriers = load_current_managed_xxhash64_carriers()
    expected_fields = {
        "object_current": {
            "source_system", "type_id", "id", "logical_key_json", "schema_revision",
            "entity_version", "props_json", "retained_json", "root_id", "source_feed",
            "source_epoch", "source_position", "published_at", "lookup_hash",
            "apply_batch_id", "source_cursor_json", "source_delivery_id",
        },
        "edge_current": {
            "source_system", "rel_type_id", "id", "source_type", "source_id",
            "target_type", "target_id", "schema_revision", "entity_version", "props_json",
            "retained_json", "order_key", "source_feed", "source_epoch", "source_position",
            "published_at", "lookup_hash", "apply_batch_id", "source_cursor_json",
            "source_delivery_id",
        },
    }
    for role in ROLES:
        fields = carriers.ordered_fields(role)
        expression_fields = set(_expressions(role, edge_rows))
        if ({item[0] for item in fields} != expected_fields[role]
                or expression_fields != expected_fields[role]
                or len(fields) != len(expected_fields[role])):
            raise ValueError(f"Selected `.4` carrier schema/value oracle mismatch for {role}")
    if (endpoint_oracle(0, node_rows) != (0, 0)
            or endpoint_oracle(1, node_rows) != (0, 0)
            or endpoint_oracle(2, node_rows) != (0, 1)
            or endpoint_oracle(edge_rows - 1, node_rows) != (node_rows - 1, 1)):
        raise AssertionError("Independent endpoint oracle boundary mismatch")


def build_generation_plan(
    node_path: str,
    edge_path: str,
    *,
    node_rows: int = NODE_ROWS,
    edge_rows: int = EDGE_ROWS,
) -> GenerationPlan:
    """Realize UMF DDL and deterministic insert SQL without executing either."""
    validate_offline_oracle(node_rows, edge_rows)
    paths = {"object_current": node_path, "edge_current": edge_path}
    if node_path == edge_path:
        raise ValueError("Distinct exact Delta paths required")
    for path in paths.values():
        _path_relation(path)

    carriers = load_current_managed_xxhash64_carriers()
    _manifest, _bundle, sources, _semantics = pinned_resources()
    generated = json.loads(carriers.generated_bytes)
    if generated.get("profile") != "ashlar-managed-current-carriers/0.4-xxhash64":
        raise ValueError("Pinned generated `.4-xxhash64` carrier required")

    roles = []
    with LocalUmfDelta4Bridge.open() as bridge:
        for role in ROLES:
            admitted = bridge.realize(sources[role + ".umf.json"], paths[role], role)
            proposal = admitted["proposal"]
            fields = admitted["fields"]
            carrier_fields = carriers.ordered_fields(role)
            if fields != carrier_fields:
                raise ValueError(f"UMF bridge schema differs from generated carrier for {role}")
            roles.append(RolePlan(
                role=role,
                path=paths[role],
                ddl=proposal["sql"],
                insert_sql=_insert_sql(role, paths[role], fields, node_rows, edge_rows),
                schema_sha256=proposal["schemaSha256"],
                layout=admitted["layout"],
                writer_settings=tuple(proposal["writerSettings"]),
            ))

    boundaries = {
        "first": endpoint_oracle(0, node_rows),
        "parallelEdgePair": (endpoint_oracle(0, node_rows), endpoint_oracle(1, node_rows)),
        "last": endpoint_oracle(edge_rows - 1, node_rows),
        "sourceType": 1,
        "targetType": 1,
        "edgeRows": edge_rows,
        "invalidEndpointsByFormula": 0,
    }
    if boundaries["last"] != (node_rows - 1, 1):
        raise AssertionError("Endpoint boundary oracle mismatch")
    return GenerationPlan(PROFILE, node_rows, edge_rows, tuple(roles), boundaries)


def main() -> int:
    validate_offline_oracle()
    plan = build_generation_plan("/offline/r05/object_current", "/offline/r05/edge_current")
    assert plan.endpoint_oracle["first"] == (0, 0)
    assert plan.endpoint_oracle["parallelEdgePair"] == ((0, 0), (0, 0))
    assert plan.endpoint_oracle["last"] == (NODE_ROWS - 1, 1)
    assert all("CREATE TABLE delta.`" in role.ddl for role in plan.roles)
    assert all(f"FROM range({plan.node_rows if role.role == 'object_current' else plan.edge_rows})"
               in role.insert_sql for role in plan.roles)
    print(f"profile={plan.profile}; roles=2; node_rows={plan.node_rows}; edge_rows={plan.edge_rows}")
    print("ddl=UMF bridge proposals admitted; SQL plans built only; Spark execution=none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
