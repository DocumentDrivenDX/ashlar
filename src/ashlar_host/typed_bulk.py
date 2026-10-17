"""One original Delta native transaction for bounded set-based scalar bulk SET.

Preview is advisory. Execution reselects within its own registered whole-table
snapshot and commits data, counts, exact Keys and markers together. No manifest,
seal, durable submit or Truss authority is supplied by this native profile.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from time import monotonic
from typing import Annotated, Any, cast

from pydantic import TypeAdapter
from umf_access.adoption import TargetIdentity
from umf_access.bulk import AtomicBulkPlan, BulkOutcome, BulkPreview
from umf_access.commands import CommandVariant
from umf_access.native_actions import NativeScope, _literal, native_input_value
from umf_access.native_program import NativeProgram, canonical_json
from umf_access.protocols import (
    ActionConflict,
    ActionDenied,
    ActionFailed,
    ActionIndeterminate,
    ActionRefused,
    ActionTimedOut,
    NativeOutcomeDetails,
)
from umf_access.targets import binding_digest

from ashlar.execution_scope import preserve_body_failure
from ashlar_host.delta_atomic import DeltaAtomicConflict
from ashlar_host.delta_receipt import delta_store, sdk_delta_receipt


@dataclass(frozen=True)
class _Executor:
    program: NativeProgram
    action: Any
    inputs: Mapping[str, object]
    admission: Any
    default_timeout: timedelta

    def _run(self, preview, timeout):
        self.program.bind(self.action)
        program = self.program.checked()
        if program["profile"] != "umf.actions.native-delta-bulk/0.1":
            raise ActionRefused("ORIGINAL_DELTA_BULK_PROFILE_REQUIRED")
        if set(self.inputs) != {p["name"] for p in program["parameters"]}:
            raise ActionRefused("DELTA_BULK_PARAMETER_INVENTORY")
        carriers = {"string": str, "boolean": bool, "integer": int, "decimal": Decimal}
        for p in program["parameters"]:
            carrier = carriers[p["carrier"]]
            TypeAdapter(carrier | None if p["nullable"] else carrier).validate_python(
                self.inputs[p["name"]], strict=True
            )
        for source, name in program["actionInputs"].items():
            field = self.action.input_type.model_fields[
                next(
                    python
                    for original, python in self.action.input_fields
                    if original == source
                )
            ]
            TypeAdapter(
                cast(
                    Any,
                    Annotated[(field.annotation, *field.metadata)]
                    if field.metadata
                    else field.annotation,
                )
            ).validate_python(self.inputs[name], strict=True)
        budget = self.default_timeout if timeout is None else timeout
        if type(budget) is not timedelta or not 0 < budget.total_seconds() <= 60:
            raise ActionRefused("FINITE_DELTA_BULK_DEADLINE_REQUIRED")
        until = monotonic() + budget.total_seconds()
        request = canonical_json(
            {
                "program": program,
                "inputDigest": self.program.bundle_input_digest,
                "parameters": {
                    name: native_input_value(value)
                    for name, value in self.inputs.items()
                },
            }
        )
        submitted = False
        scope = None
        sdk_receipt = None
        markers = []
        changes = []
        verification = []

        def details(state):
            return NativeOutcomeDetails(
                self.action,
                program["profile"],
                canonical_json(markers),
                not changes if state == "committed" else None,
                canonical_json(verification),
                canonical_json(changes),
                None if scope is None else scope.attribution_json,
                sdk_receipt,
                canonical_json({"state": state}),
                request,
                scope,
            )

        try:
            with preserve_body_failure(
                self.admission.hold(program, request, deadline=until)
            ) as native:
                scope = NativeScope(
                    delta_store(native.atomic.table_uuid),
                    native.atomic.epoch,
                    native.authenticated,
                    native.effective,
                    native,
                    native.attribution_json,
                )
                if not native.allowed:
                    raise ActionDenied("ROLE_DENIED", details("rolled-back"))
                native.stage(native.atomic.preimage)

                def statement(path):
                    value = program[path]
                    values = []
                    for p in value["parameters"]:
                        source = p["source"]
                        if set(source) == {"bulkInput"}:
                            values.append(self.inputs[source["bulkInput"]])
                        elif set(source) == {"literal"}:
                            values.append(_literal(source["literal"]))
                        else:
                            raise ActionRefused("DELTA_BULK_PARAMETER_SOURCE")
                    return value["sql"], tuple(values)

                if native.query(*statement("keyCheck")) != [(0,)]:
                    raise ActionRefused("DELTA_NATIVE_KEY_INTEGRITY")
                verification.append({"check": "complete-key", "violations": 0})
                sql, values = statement("selection")
                native.recheck()
                frame = native.spark.sql(
                    sql, args={"p" + str(i): v for i, v in enumerate(values, 1)}
                )
                frame = frame.localCheckpoint(eager=True)
                native.cached.append(frame)
                selected = [
                    tuple(row) for row in frame.limit(program["maxItems"] + 2).collect()
                ]
                if len(selected) > program["maxItems"]:
                    raise ActionRefused("BULK_LIMIT")
                fields = program["selectionFields"]
                entity = program["entity"]
                key_fields = entity["key"]["fields"]
                if any(len(row) != len(fields) for row in selected):
                    raise ActionRefused("DELTA_BULK_ROW_SHAPE")
                key_ref = entity["key"]["reference"]
                key_type = next(
                    k.model
                    for k in self.action.key_types
                    if (k.module, k.element, k.key)
                    == (key_ref["module"], key_ref["element"], key_ref["key"])
                )
                positions = [
                    next(
                        i
                        for i, f in enumerate(fields)
                        if f["reference"] == k["reference"]
                    )
                    for k in key_fields
                ]

                def decode(row):
                    values = {
                        (f["reference"]["module"], f["reference"]["element"]): row[p]
                        for f, p in zip(key_fields, positions, strict=True)
                    }
                    return key_type.model_validate(
                        {
                            name: values[(module, field)]
                            for module, field, name in key_type.UMF_FIELDS
                        },
                        strict=True,
                    )

                keys = tuple(decode(row) for row in selected)
                if len({k.model_dump_json() for k in keys}) != len(keys):
                    raise ActionRefused("DELTA_BULK_DUPLICATE_KEY")
                frame.createOrReplaceTempView("__umf_delta_selected")
                if preview:
                    return BulkPreview(
                        keys, "one bounded source-bound Delta SET; preview is advisory"
                    )
                changed = []
                for row, key in zip(selected, keys, strict=True):
                    different = False
                    for field, source in program["values"]:
                        index = next(
                            i
                            for i, f in enumerate(fields)
                            if f["reference"] == field["reference"]
                        )
                        value = (
                            self.inputs[program["actionInputs"][source["input"]]]
                            if "input" in source
                            else _literal(source["literal"])
                        )
                        different = different or row[index] != value
                    if different:
                        changed.append(key)
                native.candidate(*statement("update"))
                if native.query(*statement("keyCheck")) != [(0,)]:
                    raise ActionRefused("DELTA_NATIVE_KEY_INTEGRITY")
                verification.append({"check": "complete-key", "violations": 0})
                if native.query(*statement("verification")) != [(0,)]:
                    raise ActionFailed(
                        "DELTA_BULK_POSTCONDITION",
                        ValueError(
                            "complete original pre/post candidate verification failed"
                        ),
                        details("rolled-back"),
                    )
                verification.append({"check": "complete-candidate", "violations": 0})
                native.recheck()
                markers = [{"selected": len(keys), "changed": len(changed)}]
                changes = [
                    {"kind": "updated", "key": key.model_dump_json()} for key in changed
                ]
                original = canonical_json(
                    {
                        "programSha256": hashlib.sha256(
                            canonical_json(program).encode()
                        ).hexdigest(),
                        "requestSha256": hashlib.sha256(request.encode()).hexdigest(),
                        "counts": markers,
                        "verification": verification,
                        "selectedKeys": [key.model_dump_json() for key in keys],
                        "changedKeys": [key.model_dump_json() for key in changed],
                        "noOp": not changed,
                    }
                )
                if monotonic() >= until:
                    raise TimeoutError("original Delta bulk deadline expired")
                submitted = True
                receipt = native.atomic.commit(native.frame, markers_json=original)
                sdk_receipt = sdk_delta_receipt(receipt)
                native.recheck()
                result = BulkOutcome(
                    len(keys), len(changed), sdk_receipt, details("committed")
                )
            return result
        except DeltaAtomicConflict as error:
            raise ActionConflict(
                "SERIALIZABLE_CONFLICT", False, details("rolled-back")
            ) from error
        except BaseException as error:
            if submitted and scope is not None and scope.custody.atomic.submitted:
                uncertain = ActionIndeterminate(
                    "DELTA_BULK_INDETERMINATE", details("indeterminate")
                )
                try:
                    self.admission.quarantine(scope, error)
                except BaseException as cleanup_error:  # noqa: BLE001 - retain original submission and failed quarantine together.
                    uncertain.settlement_failures = (error, cleanup_error)
                raise uncertain from error
            if (
                isinstance(error, TimeoutError)
                or isinstance(error, Exception)
                and monotonic() >= until
            ):
                raise ActionTimedOut("TIMED_OUT", details("rolled-back")) from error
            raise

    def preview(self, *, timeout: timedelta | None = None) -> BulkPreview:
        return self._run(True, timeout)

    def execute(self, *, timeout: timedelta | None = None) -> BulkOutcome:
        return self._run(False, timeout)


class DeltaBulkTarget:
    def __init__(self, admission, *, binding, default_timeout: timedelta):
        self.admission, self.binding, self.default_timeout = (
            admission,
            binding,
            default_timeout,
        )

    def identity(self) -> TargetIdentity:
        return TargetIdentity(
            "spark-delta",
            "weft-databricks",
            "raw-read/0.1",
            "weft.raw-read/0.1",
            binding_digest(self.binding),
            (("spark", "4.0.1"), ("delta", "4.0.0")),
        )

    def plan(
        self,
        entry: CommandVariant,
        params: Mapping[str, object],
        *,
        timeout: timedelta | None = None,
    ) -> AtomicBulkPlan:
        program = entry.program.checked()
        if (
            program["profile"] != "umf.actions.native-delta-bulk/0.1"
            or program["binding"] != self.binding
        ):
            raise ActionRefused("ORIGINAL_DELTA_BULK_TARGET_REQUIRED")
        return AtomicBulkPlan(
            _Executor(
                entry.program,
                entry.action,
                dict(params),
                self.admission,
                self.default_timeout if timeout is None else timeout,
            )
        )
