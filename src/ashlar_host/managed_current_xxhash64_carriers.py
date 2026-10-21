# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pinned UMF 0.10 carrier proposal for ashlar-delta/0.4-xxhash64."""
from dataclasses import dataclass
import hashlib
import json
import re
from importlib.resources import files

from .managed_current_carriers import NATIVE_TYPES, ROLES, _json, _require

PROFILE = "ashlar-delta/0.4-xxhash64"
RESOURCE_ROOT = "managed-current-core-010-xxhash64"
GENERATED_SHA256 = "cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa"
UMF_REVISION = "2ede4c879bea5db13794a6fa30e67c821306c17f"


@dataclass(frozen=True)
class CurrentManagedXxhash64Carriers:
    generated_bytes: bytes
    inputs: tuple

    def __post_init__(self):
        _require(type(self.generated_bytes) is bytes
                 and hashlib.sha256(self.generated_bytes).hexdigest() == GENERATED_SHA256)
        generated = _json(self.generated_bytes)
        _require(generated.get("profile") == "ashlar-managed-current-carriers/0.4-xxhash64"
                 and generated.get("physicalProfile") == PROFILE
                 and generated.get("generator") == {
                     "project": "UMF", "revision": UMF_REVISION,
                     "extension": "umf.delta.definition/0.1.0"}
                 and set(generated.get("carriers", {})) == set(ROLES))
        originals = dict(self.inputs)
        _require(len(originals) == 7 and set(originals) == {
            "index.json", *(role + ".umf.json" for role in ROLES)})
        root = "src/ashlar_host/resources/" + RESOURCE_ROOT + "/"
        _require(set(generated.get("inputs", {})) == {root + name for name in originals})
        for name, raw in self.inputs:
            _require(hashlib.sha256(raw).hexdigest() == generated["inputs"][root + name])
        index = _json(originals["index.json"])
        _require(index.get("profile") == "ashlar-managed-current-carrier-layout/0.4-xxhash64"
                 and index.get("physicalProfile") == PROFILE
                 and index.get("umfCore") == "0.10.0"
                 and index.get("generatorRevision") == UMF_REVISION
                 and index.get("roles") == list(ROLES)
                 and index.get("migration", {}).get("seed") == 42)
        for role in ROLES:
            model = _json(originals[role + ".umf.json"])
            _require(model.get("umf") == "0.10.0"
                     and model.get("vocabularies", {}).get("umf.delta", {}).get("version") == "0.1.0"
                     and model.get("vocabularies", {}).get("umf.delta.definition", {}).get("version") == "0.1.0")
            carrier = generated["carriers"][role]
            columns = carrier.get("columns")
            _require(type(columns) is list and bool(columns)
                     and len({field["name"] for field in columns}) == len(columns)
                     and all(field["deltaType"] in NATIVE_TYPES
                             and type(field["nullable"]) is bool for field in columns))
            if role in ("object_current", "edge_current"):
                typed = "type_id" if role == "object_current" else "rel_type_id"
                lookup = next((field for field in columns if field["name"] == "lookup_hash"), None)
                stats = carrier["layout"].get("properties", {}).get("delta.dataSkippingStatsColumns")
                _require(lookup == {"name": "lookup_hash", "deltaType": "long", "nullable": False}
                         and carrier["layout"].get("clusterBy") == ["lookup_hash"]
                         and stats == f"lookup_hash,source_system,{typed},id"
                         and "BIGINT NOT NULL" in carrier["sql"])

    physical_profile = PROFILE
    lookup_hash_profile = "spark-xxhash64-typed-key/0.1"

    def ordered_fields(self, role):
        if role not in ROLES:
            raise ValueError("Exact current managed role required")
        generated = _json(self.generated_bytes)
        return tuple((field["name"], NATIVE_TYPES[field["deltaType"]], field["nullable"])
                     for field in generated["carriers"][role]["columns"])

    def create_sql(self, role, qualified_name):
        if role not in ROLES or type(qualified_name) is not tuple or len(qualified_name) != 3:
            raise ValueError("Exact current managed role/name required")
        if any(type(part) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", part) is None
               for part in qualified_name):
            raise ValueError("Portable qualified managed identifier required")
        generated = _json(self.generated_bytes)
        sql = generated["carriers"][role]["sql"]
        quoted = ".".join("`" + part.replace("`", "``") + "`" for part in qualified_name)
        return sql.replace("CREATE TABLE `" + role + "`", "CREATE TABLE " + quoted, 1)


def load_current_managed_xxhash64_carriers():
    root = files("ashlar_host").joinpath("resources", RESOURCE_ROOT)

    def read(name):
        with root.joinpath(name).open("rb") as stream:
            raw = stream.read(16777217)
        _require(0 < len(raw) <= 16777216)
        return raw

    names = ("index.json", *(role + ".umf.json" for role in ROLES))
    return CurrentManagedXxhash64Carriers(read("runtime-carriers.generated.json"),
                                          tuple((name, read(name)) for name in names))
