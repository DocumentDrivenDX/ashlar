# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original typed-source allocations for explicit non-temporal Weft candidates.

This builder retains source/IDs; the original compiler and held host still own
semantic admission, native integrity, publication and authorization.
"""

from __future__ import annotations

import hashlib
import json
import re

from .primitive_field_binding import PrimitiveFieldBinding
from .publication import Descriptor
from .schema import SchemaIntake, _json
from .typed_source_policy import TypedSourcePolicy
from .weft_binding import LAYOUT_SHA256, WeftBindingError


def primitive_compile_request(
    sql,
    intake,
    policy,
    descriptor,
    *,
    schema_alias,
    table_uuids,
    manifest_uuid,
    layout_sha256,
    backend_version,
    target_profile,
):
    """Build original required-scalar candidate input without allocating IDs.

    The existing STRING-only API remains closed. The selected original compiler
    must refuse meanings outside its declared profile; successful construction
    does not prove native execution or discharge any host obligation.
    """
    if (
        type(intake) is not SchemaIntake
        or type(policy) not in (TypedSourcePolicy, PrimitiveFieldBinding)
        or type(descriptor) is not Descriptor
    ):
        raise WeftBindingError("Original typed source, intake and descriptor required")
    recovered = SchemaIntake.read(
        intake.artifact,
        intake.document_revision,
        trusted_validator_revision=intake.validator_revision,
    )
    if (
        recovered != intake
        or policy.source != intake.source
        or policy.source_sha256 != intake.source_sha256
    ):
        raise WeftBindingError("Original typed-source byte custody differs")
    if (
        descriptor.profile != "ashlar-delta/0.3"
        or layout_sha256 != LAYOUT_SHA256
        or descriptor.revisions.get(schema_alias) != intake.document_revision
    ):
        raise WeftBindingError("Original layout and model revision required")
    if (backend_version, target_profile) not in (
        ("0.1.0-local-spark4-candidate", "spark4.0.1-delta4.0.0-candidate"),
        (
            "0.1.0-local-spark4-fields-candidate",
            "spark4.0.1-delta4.0.0-fields-candidate",
        ),
        ("0.1.0-local-spark4-pages-candidate", "spark4.0.1-delta4.0.0-candidate"),
    ):
        raise WeftBindingError("Explicit original local candidate profile required")
    if type(sql) is not str or not sql or len(sql.encode("utf-8")) > 262144:
        raise WeftBindingError("Bounded original query required")
    if type(policy) is PrimitiveFieldBinding:
        # Field checks confer no Key, filtering, aggregation or paging authority.
        identifier = r'(?:"[A-Za-z_][A-Za-z0-9_]*"|[A-Za-z_][A-Za-z0-9_]*)'
        direct = identifier + r"\." + identifier
        selected = direct + r"(?: AS " + identifier + r")?"
        closed = (
            r"SELECT "
            + selected
            + r"(?:,\s*"
            + selected
            + r")* FROM "
            + direct
            + r"(?: AS)? "
            + identifier
            + r" ORDER BY "
            + direct
            + r"(?: ASC)?(?: NULLS LAST)? LIMIT [1-9][0-9]{0,3}"
        )
        if (
            backend_version.endswith("-pages-candidate")
            or re.fullmatch(closed, sql) is None
        ):
            raise WeftBindingError(
                "Field-only profile refuses non-direct query semantics"
            )
    uuid = re.compile("[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}").fullmatch
    if (
        type(manifest_uuid) is not str
        or not uuid(manifest_uuid)
        or type(table_uuids) is not dict
        or set(table_uuids) != set(descriptor.versions)
        or any(type(v) is not str or not uuid(v) for v in table_uuids.values())
    ):
        raise WeftBindingError("Complete native UUID inventory required")
    tables = []
    for name, version in sorted(descriptor.versions.items()):
        parts = name.split(".")
        if (
            len(parts) != 3
            or any(re.fullmatch("[A-Za-z_][A-Za-z0-9_]*", p) is None for p in parts)
            or type(version) is not int
            or not 0 <= version < 2**63
        ):
            raise WeftBindingError("Exact qualified native version required")
        tables.append({"name": parts, "uuid": table_uuids[name], "version": version})
    objects = [
        i for i, table in enumerate(tables) if table["name"][-1] == "object_current"
    ]
    if len(objects) != 1:
        raise WeftBindingError("One original object carrier required")
    model = _json(intake.source)
    elements = {(m["id"], e["id"]): e for m in model["modules"] for e in m["elements"]}
    logical = lambda module, element: {
        "documentId": intake.document_id,
        "revision": intake.document_revision,
        "module": module,
        "element": element,
    }
    records = []
    for type_id, binding in sorted(policy.bindings.items()):
        record = elements.get(binding["record"])
        if record is None or record.get("kind") != "record":
            raise WeftBindingError("Original authored Record required")
        refs = {(v[0], v[1]) for v in binding["fields"].values()}
        if refs != {(m["module"], m["element"]) for m in record["members"]} or len(
            refs
        ) != len(binding["fields"]):
            raise WeftBindingError(
                "Complete unique original property allocation required"
            )
        properties = []
        for pid, (module, element, family) in sorted(binding["fields"].items()):
            field = elements.get((module, element))
            if (
                field is None
                or field.get("kind") != "field"
                or field.get("scalarType") != family
                or family not in ("string", "integer", "decimal", "boolean")
            ):
                raise WeftBindingError(
                    "Original non-temporal scalar Field correspondence required"
                )
            properties.append(
                {
                    "logical": logical(module, element),
                    "home": {"kind": "props", "propertyId": pid},
                }
            )
        records.append(
            {
                "logical": logical(*binding["record"]),
                "table": objects[0],
                "kind": "object",
                "sourceSystem": policy.source_system,
                "typeId": str(type_id),
                "schemaRevision": policy.schema_revision,
                "properties": properties,
            }
        )
    pin = {
        "documentId": intake.document_id,
        "revision": intake.document_revision,
        "umfVersion": model["umf"],
        "sha256": intake.source_sha256,
    }
    native = {
        "profile": "ashlar-databricks-candidate/0.1.0",
        "layoutRevision": "ashlar-delta/0.3",
        "layoutSha256": layout_sha256,
        "modelPins": [pin],
        "publication": {
            "id": descriptor.publication_id,
            "manifestUuid": manifest_uuid,
            "tables": tables,
        },
        "records": records,
    }
    text = json.dumps(native, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return {
        "interfaceVersion": "weft-compile/0.2.2"
        if backend_version.endswith("-pages-candidate")
        else "weft-compile/0.2.0",
        "dialect": "weft-sql/0.2.0",
        "sql": sql,
        "parameters": {},
        "modules": [
            {
                "documentJson": intake.source.decode("utf-8"),
                "pin": pin,
                "selectedModuleIds": sorted({r["logical"]["module"] for r in records}),
            }
        ],
        "target": {
            "backendId": "ashlar.databricks",
            "backendVersion": backend_version,
            "targetProfile": target_profile,
            "bindingJson": text,
            "bindingSha256": hashlib.sha256(text.encode()).hexdigest(),
        },
        "options": {"allowCandidate": True},
    }


def decode_primitive_column(column, value, artifact, policy, intake):
    """Decode one original direct required Field, retaining its exact carrier.

    The host must first authenticate/recompile the complete original artifact
    and hold all publication, scalar-integrity and authorization obligations.
    This helper checks correspondence; it grants none of those authorities.
    """
    from .publication import ResolutionError
    from .weft_decode import decode_exact_scalar

    def require(condition, message):
        if not condition:
            raise ResolutionError(message)

    require(
        type(policy) in (TypedSourcePolicy, PrimitiveFieldBinding)
        and type(intake) is SchemaIntake,
        "Original typed source policy and intake required",
    )
    require(
        policy.source == intake.source and policy.source_sha256 == intake.source_sha256,
        "Original typed source differs",
    )
    require(
        type(artifact) is dict and type(column) is dict,
        "Original compiler artifact and column required",
    )
    columns = artifact.get("columns")
    position = column.get("position")
    require(
        type(columns) is list
        and type(position) is int
        and 1 <= position <= len(columns)
        and all(
            type(c) is dict and type(c.get("position")) is int and c["position"] == i
            for i, c in enumerate(columns, 1)
        )
        and columns[position - 1] == column,
        "Complete original ordered columns required",
    )
    pin = {
        "documentId": intake.document_id,
        "revision": intake.document_revision,
        "sha256": intake.source_sha256,
        "umfVersion": _json(intake.source)["umf"],
    }
    require(artifact.get("modelPins") == [pin], "Original model pin differs")
    plan = artifact.get("logicalPlan")
    require(
        type(plan) is dict and type(plan.get("outputs")) is list,
        "Original logical output required",
    )
    outputs = [
        item for item in plan["outputs"] if item.get("name") == column.get("outputName")
    ]
    require(len(outputs) == 1, "Unique original output required")
    expression = outputs[0].get("expression")
    require(
        type(expression) is dict
        and set(expression) == {"op", "identity", "scan"}
        and expression["op"] == "field"
        and type(expression["scan"]) is str,
        "Direct original scalar Field output required",
    )
    identity = expression["identity"]
    require(
        type(identity) is dict
        and set(identity) == {"documentId", "revision", "module", "element"}
        and identity["documentId"] == intake.document_id
        and identity["revision"] == intake.document_revision
        and column.get("sourceIdentities") == [identity],
        "Original Field identity differs",
    )
    model = _json(intake.source)
    authored = [
        field
        for module in model["modules"]
        if module["id"] == identity["module"]
        for field in module["elements"]
        if field["id"] == identity["element"]
    ]
    require(
        len(authored) == 1
        and authored[0].get("kind") == "field"
        and authored[0].get("nullability") == "required"
        and authored[0].get("cardinality") == "one",
        "Original required singleton Field declaration required",
    )
    original_type = {
        "family": authored[0].get("scalarType"),
        "facets": authored[0].get("facets", {}),
        "nullable": False,
    }
    descriptors = [
        item for item in plan.get("typeGraph", []) if item.get("identity") == identity
    ]
    require(
        len(descriptors) == 1
        and descriptors[0].get("kind") == "scalar"
        and descriptors[0].get("availability") == "required",
        "Required original scalar descriptor required",
    )
    representation = column.get("representation")
    require(
        type(representation) is dict
        and representation.get("logicalType")
        == descriptors[0].get("type")
        == original_type
        and column.get("nullable") is False,
        "Original output type differs",
    )
    # Identity allocation is from the actual original ingestion policy. Native
    # source/scan correspondence remains the original compiler/host's duty.
    fields = [
        (module, element, family)
        for binding in policy.bindings.values()
        for module, element, family in binding["fields"].values()
        if (module, element) == (identity["module"], identity["element"])
    ]
    require(
        fields
        and len(set(fields)) == 1
        and fields[0][2] == representation["logicalType"].get("family"),
        "Original accepted primitive Field allocation required",
    )
    return decode_exact_scalar(representation, value)
