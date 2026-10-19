# SPDX-License-Identifier: MIT OR Apache-2.0
"""Candidate bounded native managed bulk SET; no publication/Truss authority.

All SQL is the original managed compiler program. Preview is advisory. Execution
reselects and verifies the complete original population in one admitted native
transaction, with the same original receipt/settlement requirements as actions.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from time import monotonic
from types import MappingProxyType
from typing import Annotated, Any, cast

from pydantic import TypeAdapter
from umf_access.bulk import AtomicBulkPlan, BulkOutcome, BulkPreview
from umf_access.native_actions import NativeScope, _literal, native_input_value
from umf_access.native_program import NativeProgram, canonical_json
from umf_access.protocols import (
    ActionDenied,
    ActionFailed,
    ActionIndeterminate,
    ActionRefused,
    NativeOutcomeDetails,
)

from ashlar.execution_scope import preserve_body_failure


@dataclass(frozen=True)
class ManagedAtomicBulkExecutor:
    program: NativeProgram
    action: Any
    inputs: Mapping[str, object]
    admission: Any
    default_timeout: timedelta

    def __post_init__(self):
        object.__setattr__(self, "inputs", MappingProxyType(dict(self.inputs)))

    def _run(self, preview, timeout):
        self.program.bind(self.action)
        program = self.program.checked()
        if program["profile"] != "umf.actions.managed-delta-bulk/0.1":
            raise ActionRefused("ORIGINAL_MANAGED_BULK_PROFILE_REQUIRED")
        if set(self.inputs) != {p["name"] for p in program["parameters"]}:
            raise ActionRefused("MANAGED_BULK_PARAMETER_INVENTORY")
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
            annotation = (
                Annotated[(field.annotation, *field.metadata)]
                if field.metadata
                else field.annotation
            )
            TypeAdapter(cast(Any, annotation)).validate_python(
                self.inputs[name], strict=True
            )
        budget = self.default_timeout if timeout is None else timeout
        if type(budget) is not timedelta or not 0 < budget.total_seconds() <= 60:
            raise ActionRefused("FINITE_MANAGED_BULK_DEADLINE_REQUIRED")
        deadline = monotonic() + budget.total_seconds()
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
        scope = native = receipt = None
        markers, changes, verification = [], [], []

        def details(state):
            return NativeOutcomeDetails(
                self.action,
                program["profile"],
                canonical_json(markers),
                not changes if state == "committed" else None,
                canonical_json(verification),
                canonical_json(changes),
                None if scope is None else scope.attribution_json,
                receipt,
                canonical_json({"state": state}),
                request,
                scope,
            )

        def statement(path):
            value = program[path]
            values = []
            for parameter in value["parameters"]:
                source = parameter["source"]
                if set(source) == {"bulkInput"}:
                    values.append(self.inputs[source["bulkInput"]])
                elif set(source) == {"literal"}:
                    values.append(_literal(source["literal"]))
                else:
                    raise ActionRefused("MANAGED_BULK_PARAMETER_SOURCE")
            return value["sql"], tuple(values)

        try:
            with preserve_body_failure(
                self.admission.hold(program, request, deadline=deadline)
            ) as native:
                scope = NativeScope(
                    native.store,
                    native.epoch,
                    native.authenticated,
                    native.effective,
                    native,
                    native.attribution_json,
                )
                if not native.allowed:
                    raise ActionDenied("ROLE_DENIED", details("rolled-back"))
                if native.query(*statement("keyCheck")) != [(0,)]:
                    raise ActionRefused("MANAGED_BULK_NATIVE_KEY_INTEGRITY")
                fields = program["selectionFields"]
                key_fields = program["entity"]["key"]["fields"]
                key_ref = program["entity"]["key"]["reference"]
                key_types = [
                    key.model
                    for key in self.action.key_types
                    if (key.module, key.element, key.key)
                    == (key_ref["module"], key_ref["element"], key_ref["key"])
                ]
                if len(key_types) != 1:
                    raise ActionRefused("MANAGED_BULK_ORIGINAL_KEY_TYPE_REQUIRED")
                key_type = key_types[0]
                positions = [
                    next(
                        i
                        for i, field in enumerate(fields)
                        if field["reference"] == key["reference"]
                    )
                    for key in key_fields
                ]
                selected = native.query(*statement("selection"))
                if len(selected) > program["maxItems"]:
                    raise ActionRefused("MANAGED_BULK_MAX_ITEMS")
                if any(len(row) != len(fields) for row in selected):
                    raise ActionRefused("MANAGED_BULK_ORIGINAL_PROJECTION")
                keys = []
                for row in selected:
                    values = {
                        (
                            field["reference"]["module"],
                            field["reference"]["element"],
                        ): row[position]
                        for field, position in zip(key_fields, positions, strict=True)
                    }
                    keys.append(
                        key_type.model_validate(
                            {
                                name: values[(module, field)]
                                for module, field, name in key_type.UMF_FIELDS
                            },
                            strict=True,
                        )
                    )
                if len({key.model_dump_json() for key in keys}) != len(keys):
                    raise ActionRefused("MANAGED_BULK_DUPLICATE_KEY")
                if preview:
                    return BulkPreview(
                        tuple(keys),
                        "original bounded managed transaction selection; preview is advisory",
                    )
                table = next(
                    table
                    for table in program["binding"]["tables"]
                    if table["logical"] == program["entity"]["key"]["table"]
                )
                columns = table["columns"]
                key_positions = [
                    next(
                        i
                        for i, column in enumerate(columns)
                        if column["logical"] == key["logical"]
                    )
                    for key in key_fields
                ]

                def population(rows):
                    result = {}
                    for row in rows:
                        if len(row) != len(columns):
                            raise ActionRefused(
                                "MANAGED_BULK_COMPLETE_POPULATION_SHAPE"
                            )
                        key = tuple(row[position] for position in key_positions)
                        if key in result:
                            raise ActionRefused("MANAGED_BULK_COMPLETE_POPULATION_KEY")
                        result[key] = tuple(row)
                    return result

                before = population(native.query(*statement("before")))
                if len(before) > 10000:
                    raise ActionRefused("MANAGED_BULK_COMPLETE_POPULATION_BOUND")
                expected = dict(before)
                changed = []
                for selected_row, key_object in zip(selected, keys, strict=True):
                    key = tuple(selected_row[position] for position in positions)
                    if key not in expected:
                        raise ActionRefused(
                            "MANAGED_BULK_SELECTION_POPULATION_MISMATCH"
                        )
                    row = list(expected[key])
                    for field, source in program["values"]:
                        position = next(
                            i
                            for i, column in enumerate(columns)
                            if column["logical"] == field["logical"]
                        )
                        row[position] = (
                            self.inputs[program["actionInputs"][source["input"]]]
                            if "input" in source
                            else _literal(source["literal"])
                        )
                    expected[key] = tuple(row)
                    if expected[key] != before[key]:
                        changed.append(key_object)
                native.recheck()
                native.execute(*statement("update"))
                if native.query(*statement("keyCheck")) != [(0,)]:
                    raise ActionRefused("MANAGED_BULK_NATIVE_KEY_INTEGRITY")
                if population(native.query(*statement("after"))) != expected:
                    raise ActionFailed(
                        "MANAGED_BULK_FRAME_POSTCONDITION",
                        ValueError("original whole-population Key/Field frame differs"),
                        details("rolled-back"),
                    )
                verification.extend(
                    [
                        {"check": "complete-key", "violations": 0},
                        {"check": "complete-population-frame", "violations": 0},
                    ]
                )
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
                native.recheck()
                receipt = native.commit(markers_json=original)
                result = BulkOutcome(
                    len(keys), len(changed), receipt, details("committed")
                )
            return result
        except BaseException as original:
            if native is not None and (
                native.commit_submitted
                or native.write_submitted
                and not native.rollback_confirmed
            ):
                uncertain = ActionIndeterminate(
                    "MANAGED_BULK_INDETERMINATE", details("indeterminate")
                )
                try:
                    self.admission.quarantine(scope, original)
                except BaseException as cleanup_error:  # noqa: BLE001 - retain original settlement and quarantine failure together
                    uncertain.settlement_failures = (original, cleanup_error)
                raise uncertain from original
            raise

    def preview(self, *, timeout=None):
        return self._run(True, timeout)

    def execute(self, *, timeout=None):
        return self._run(False, timeout)

    def plan(self):
        return AtomicBulkPlan(self)
