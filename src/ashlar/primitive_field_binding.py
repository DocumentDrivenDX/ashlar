# SPDX-License-Identifier: MIT OR Apache-2.0
"""Field-only source custody; deliberately not a Record/ingestion policy.

The host authenticates the original producer and catalog allocations separately.
Original incomplete Record observations remain unchanged in the receipt.
"""

from __future__ import annotations

import base64
import hashlib
from types import MappingProxyType

from .schema import _json
from .typed_source_policy import (
    RECORD_CHECK_PIN,
    TypedSourceError,
    _complete_validation,
)


class PrimitiveFieldBinding:
    """Bind required primitive Fields to existing explicit property IDs only."""

    def __init__(
        self,
        source,
        receipt,
        bindings,
        *,
        source_system,
        schema_revision,
        trusted_producer_revision,
    ):
        def require(condition, message):
            if not condition:
                raise TypedSourceError(message)

        require(
            type(source) is bytes
            and type(receipt) is bytes
            and trusted_producer_revision == RECORD_CHECK_PIN,
            "Pinned original Field producer required",
        )
        model, report = _json(source), _json(receipt)
        require(
            model.get("umf") == "0.8.0"
            and report.get("format") == "ashlar-primitive-field-check/0.1"
            and report.get("producerRevision") == RECORD_CHECK_PIN,
            "Original Field-only profile required",
        )
        try:
            original = base64.b64decode(report["sourceBase64"], validate=True)
            request = base64.b64decode(report["requestBase64"], validate=True)
        except (KeyError, ValueError, TypeError) as error:
            raise TypedSourceError("Original Field byte custody required") from error
        require(
            original == source
            and report.get("sourceSha256") == hashlib.sha256(source).hexdigest()
            and report.get("requestSha256") == hashlib.sha256(request).hexdigest(),
            "Original source/request bytes differ",
        )
        require(
            type(source_system) is str
            and bool(source_system)
            and type(schema_revision) is str
            and bool(schema_revision),
            "Explicit source and revision required",
        )
        require(
            model.get("extensions", {}) == {}
            and set(model.get("vocabularies", {})) <= {"umf.access"}
            and model.get("vocabularies", {}).get("umf.access", {"version": "0.1.0"})
            == {"version": "0.1.0"},
            "Unknown document meaning is not admitted",
        )
        elements = {}
        for module in model["modules"]:
            require(
                not module.get("relationships")
                and set(module.get("extensions", {})) <= {"umf.access"},
                "No relationship or unknown module authority",
            )
            access = module.get("extensions", {}).get("umf.access")
            if access is not None:
                require(
                    type(access) is dict
                    and set(access) <= {"views", "columns", "search", "searchBindings"}
                    and not access.get("columns")
                    and not access.get("search")
                    and not access.get("searchBindings")
                    and type(access.get("views")) is list,
                    "Only retained known access row declarations are admitted",
                )
                for view in access["views"]:
                    require(
                        type(view) is dict
                        and set(view) <= {"record", "fields", "selectedKey"}
                        and type(view.get("fields")) is list,
                        "Unknown access view semantics",
                    )
                    for item in view["fields"]:
                        require(
                            type(item) is dict
                            and set(item)
                            == {"field", "row", "create", "mutable", "generated"}
                            and item["row"] == {"missing": False, "null": False}
                            and all(type(v) is bool for v in item["row"].values())
                            and item["generated"] is False
                            and type(item["mutable"]) is bool
                            and type(item["create"]) is dict
                            and set(item["create"]) == {"missing", "null"}
                            and all(type(v) is bool for v in item["create"].values()),
                            "Only original required non-generated row Fields are admitted",
                        )
            for element in module["elements"]:
                identity = (module["id"], element["id"])
                require(identity not in elements, "Duplicate original element")
                elements[identity] = element
        result = {}
        for tid, binding in bindings.items():
            require(
                type(tid) is int
                and 0 < tid < 2**31
                and type(binding) is dict
                and set(binding) == {"record", "fields"},
                "Explicit original allocation required",
            )
            record_ref = tuple(binding["record"])
            record = elements.get(record_ref, {})
            require(
                record.get("kind") == "record"
                and record.get("extensions") == {}
                and set(record)
                <= {
                    "id",
                    "name",
                    "kind",
                    "members",
                    "keys",
                    "extensions",
                    "title",
                    "description",
                    "aliases",
                    "examples",
                },
                "Unsupported original Record semantics",
            )
            fields = {}
            for pid, identity in binding["fields"].items():
                require(
                    type(pid) is str
                    and pid.isascii()
                    and pid.isdecimal()
                    and str(int(pid)) == pid
                    and 0 < int(pid) < 2**31,
                    "Exact positive property ID required",
                )
                ref = tuple(identity)
                require(
                    len(ref) == 3
                    and ref[2] in ("string", "integer", "decimal", "boolean"),
                    "Original primitive identity required",
                )
                field = elements.get(ref[:2], {})
                require(
                    field.get("kind") == "field"
                    and field.get("scalarType") == ref[2]
                    and field.get("cardinality") == "one"
                    and field.get("nullability") == "required"
                    and field.get("extensions") == {}
                    and set(field)
                    <= {
                        "id",
                        "name",
                        "kind",
                        "scalarType",
                        "cardinality",
                        "nullability",
                        "facets",
                        "extensions",
                        "title",
                        "description",
                        "aliases",
                        "examples",
                    },
                    "Only original direct required primitive Fields are admitted",
                )
                facets = field.get("facets", {})
                if ref[2] in ("string", "boolean"):
                    require(facets == {}, "Unsupported primitive facets")
                elif ref[2] == "integer":
                    width = facets.get("integerWidth", {})
                    require(
                        set(facets) == {"integerWidth"}
                        and set(width) == {"bits", "signed"}
                        and type(width["bits"]) is int
                        and 1 <= width["bits"] <= 64
                        and type(width["signed"]) is bool,
                        "Exact integer facets required",
                    )
                else:
                    require(
                        set(facets) == {"precision", "scale"}
                        and type(facets["precision"]) is int
                        and 1 <= facets["precision"] <= 38
                        and type(facets["scale"]) is int
                        and 0 <= facets["scale"] <= facets["precision"],
                        "Exact Decimal facets required",
                    )
                fields[pid] = ref
            require(
                len({v[:2] for v in fields.values()}) == len(fields)
                and {v[:2] for v in fields.values()}
                == {(m["module"], m["element"]) for m in record["members"]}
                and len(fields) == len(record["members"]),
                "Complete original required member allocation required",
            )
            require(
                record_ref not in [b["record"] for b in result.values()],
                "Duplicate original Record binding",
            )
            result[tid] = MappingProxyType(
                {"record": record_ref, "fields": MappingProxyType(fields)}
            )
        req = _json(request)
        require(
            type(req) is dict
            and set(req) == {"records"}
            and type(req["records"]) is list
            and type(report.get("records")) is list
            and len(req["records"]) == len(report["records"])
            and bool(req["records"]),
            "Original ordered Field requests required",
        )
        for wanted, observed in zip(req["records"], report["records"], strict=True):
            require(
                type(observed) is dict
                and set(observed) == {"request", "result", "fields"}
                and observed["request"] == wanted,
                "Original ordered request differs",
            )
            binding = result.get(wanted.get("typeId"))
            require(
                binding is not None
                and wanted.get("identity")
                == dict(zip(("module", "element"), binding["record"], strict=True)),
                "Original requested Record identity differs",
            )
            original_result = observed["result"]
            require(
                original_result.get("operation") == "validate-core-record-values"
                and original_result.get("version") == "1.0.0"
                and original_result.get("source") == model
                and original_result.get("identity") == wanted["identity"]
                and original_result.get("values") == wanted["values"],
                "Original Record observation differs",
            )
            validation = original_result.get("validation", {})
            diagnostics = validation.get("diagnostics")
            require(
                validation.get("valid") is True
                and type(validation.get("complete")) is bool
                and type(diagnostics) is list
                and validation["complete"] is (not diagnostics)
                and all(
                    (
                        (
                            d.get("code") == "UNKNOWN_EXTENSION"
                            and d.get("path") == "/vocabularies/umf.access"
                        )
                        or (
                            d.get("code") == "RECORD_KEY_CONTEXT_REQUIRED"
                            and d.get("path") == "/identity"
                        )
                    )
                    and d.get("severity") == "warning"
                    for d in diagnostics
                ),
                "Unqualified Record meaning is not admitted",
            )
            values = wanted["values"]
            require(
                type(values) is list
                and len(values) == len(binding["fields"])
                and {(v["field"]["module"], v["field"]["element"]) for v in values}
                == {v[:2] for v in binding["fields"].values()},
                "Exact required singleton population required",
            )
            require(
                len(observed["fields"]) == len(values),
                "Complete original Field observations required",
            )
            for value, check in zip(values, observed["fields"], strict=True):
                require(
                    value.get("state") == "present"
                    and set(check) == {"field", "value", "validation"}
                    and check["field"] == value["field"]
                    and check["value"] == value["value"]
                    and _complete_validation(check["validation"]),
                    "Complete original Field validation required",
                )
        self.source, self.receipt = source, receipt
        self.source_sha256 = hashlib.sha256(source).hexdigest()
        self.receipt_sha256 = hashlib.sha256(receipt).hexdigest()
        self.source_system, self.schema_revision = source_system, schema_revision
        self.bindings = MappingProxyType(result)
