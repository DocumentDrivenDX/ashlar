"""Local Delta action execution using original Weft programs and native admission.

admission.hold owns the native actor/program/fence interval and supplies a pinned
DeltaAtomic transaction. No successful authority or connection default exists.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import timedelta
from time import monotonic
from typing import Any, cast

from umf_access.native_actions import NativeScope, _State
from umf_access.native_program import NativeProgram, canonical_json
from umf_access.protocols import (
    ActionFailed,
    ActionOutcome,
    ActionRefused,
    Committed,
    Conflict,
    Denied,
    Indeterminate,
    Rejected,
    TimedOut,
    ValidationFailure,
    ValidationResult,
)
from umf_access.types import ActionRef

from ashlar.execution_scope import preserve_body_failure
from ashlar_host.delta_atomic import DeltaAlreadySubmitted, DeltaAtomicConflict
from ashlar_host.delta_receipt import delta_store, sdk_delta_receipt


class DeltaActionExecutor:
    def __init__(
        self,
        programs: tuple[NativeProgram, ...],
        admission,
        *,
        default_timeout: timedelta,
    ):
        if (
            type(default_timeout) is not timedelta
            or not 0 < default_timeout.total_seconds() <= 60
        ):
            raise ActionRefused("FINITE_DELTA_DEADLINE_REQUIRED")
        self.programs, self.admission, self.default_timeout = (
            programs,
            admission,
            default_timeout,
        )
        self.quarantined = False

    def _state(self, action, inputs):
        if self.quarantined:
            raise ActionRefused("DELTA_EXECUTOR_QUARANTINED")
        matching = [
            p
            for p in self.programs
            if p.checked().get("profile") == "umf.actions.native-delta/0.1"
            and p.checked()["source"]["documentSha256"]
            == getattr(action.source, "document_sha256", None)
            and p.checked()["source"]["action"] == action.action
        ]
        if len(matching) != 1:
            raise ActionRefused("DELTA_ACTION_NOT_COMPILED")
        return _State(action, matching[0], inputs)

    @staticmethod
    def _drive(generator, scope):
        result = None
        while True:
            try:
                operation = generator.send(result)
            except StopIteration as done:
                return done.value
            if operation[0] != "query":
                raise ActionRefused("DELTA_NATIVE_STATE_OPERATION")
            result = scope.query(operation[1], operation[2])

    @staticmethod
    def _net(state):
        created = []
        changed = []
        combined = {}
        for index, effect in enumerate(state.program["effects"]):
            identity = canonical_json(
                [
                    effect["entity"]["key"]["reference"],
                    [
                        {"carrier": f["carrier"], "value": str(state.resolve(v))}
                        for f, v in zip(
                            effect["entity"]["key"]["fields"],
                            effect["entity"]["components"],
                            strict=True,
                        )
                    ],
                ]
            )
            before, after = (
                state.pre["effect:" + str(index)],
                state.post["effect:" + str(index)],
            )
            if identity not in combined:
                combined[identity] = {
                    "entity": effect["entity"],
                    "before": None if before is None else {},
                    "after": None if after is None else {},
                }
            net = combined[identity]
            for phase, row in (("before", before), ("after", after)):
                if (net[phase] is None) != (row is None):
                    raise ActionRefused("DELTA_ENTITY_IMAGE_DISAGREEMENT")
                if row is not None:
                    net[phase].update(row)
        for identity, net in combined.items():
            before, after = net["before"], net["after"]
            if before == after:
                continue
            key = state.key(net["entity"])
            kind = (
                "created"
                if before is None and after is not None
                else "deleted"
                if after is None
                else "updated"
            )
            state.changes.append({"kind": kind, "key": identity})
            if kind == "created":
                created.append(key)
            elif kind == "updated":
                changed.append(key)
        return tuple(created), tuple(changed)

    def _run(self, action, inputs, timeout, validate):
        state = self._state(action, inputs)
        budget = self.default_timeout if timeout is None else timeout
        if type(budget) is not timedelta or not 0 < budget.total_seconds() <= 60:
            raise ActionRefused("FINITE_DELTA_DEADLINE_REQUIRED")
        until = monotonic() + budget.total_seconds()
        submitted = False
        committed = None
        try:
            with preserve_body_failure(
                self.admission.hold(state.program, state.request, deadline=until)
            ) as scope:
                state.scope = NativeScope(
                    delta_store(scope.atomic.table_uuid),
                    scope.atomic.epoch,
                    scope.authenticated,
                    scope.effective,
                    scope,
                    scope.attribution_json,
                )
                if not scope.allowed:
                    return (
                        ValidationResult(
                            False, (ValidationFailure("ROLE_DENIED"),), True
                        )
                        if validate
                        else Denied("ROLE_DENIED", state.details("rolled-back"))
                    )

                def time():
                    if monotonic() >= until:
                        raise TimeoutError(
                            "original Delta whole-operation deadline expired"
                        )

                def key_check():
                    time()
                    rows = scope.query(*state.statement("/keyCheck"))
                    if rows != [(0,)]:
                        raise ActionRefused("DELTA_NATIVE_KEY_INTEGRITY")

                scope.stage(scope.atomic.preimage)
                key_check()
                self._drive(state.images("pre"), scope)
                missing = [
                    name
                    for name, entity in state.program["entities"].items()
                    if entity["requiredExisting"]
                    and state.pre["entity:" + name] is None
                ]
                failures = (
                    [ValidationFailure("ENTITY_MISSING")]
                    if missing
                    else self._drive(
                        state.conditions("pre", collect_all=validate), scope
                    )
                )
                if failures or validate:
                    if validate:
                        return ValidationResult(
                            not failures,
                            tuple(failures),
                            not missing or not state.program["preconditions"],
                        )
                    condition = next(
                        (
                            c
                            for c in state.program["preconditions"]
                            if c["code"] == failures[0].code
                        ),
                        None,
                    )
                    return Rejected(
                        failures[0].code,
                        False if condition is None else condition["retryable"],
                        state.details("rolled-back"),
                    )
                for index, effect in enumerate(state.program["effects"]):
                    time()
                    before = state.pre["effect:" + str(index)]
                    if effect["kind"] == "create" and before is not None:
                        raise ActionFailed(
                            "KEY_EXISTS",
                            ValueError("original Delta create Key already exists"),
                            state.details("rolled-back"),
                        )
                    if effect["kind"] != "create" and before is None:
                        raise ActionFailed(
                            "ENTITY_MISSING",
                            ValueError("original Delta effect entity absent"),
                            state.details("rolled-back"),
                        )
                    if effect["guard"] is not None:
                        guard = scope.query(
                            *state.statement("/effects/" + str(index) + "/guard")
                        )
                        if (
                            len(guard) != 1
                            or len(guard[0]) != 1
                            or type(guard[0][0]) is not bool
                        ):
                            raise ActionRefused("DELTA_NATIVE_EFFECT_MARKER")
                        if guard[0][0] is False:
                            state.markers.append({"id": effect["id"], "changed": False})
                            continue
                    scope.candidate(
                        *state.statement("/effects/" + str(index) + "/statement")
                    )
                    state.markers.append({"id": effect["id"], "changed": True})
                key_check()
                self._drive(state.images("post"), scope)
                failures = self._drive(state.conditions("post"), scope)
                if failures:
                    raise ActionFailed(
                        "POSTCONDITION_" + failures[0].code,
                        ValueError("original Delta postcondition failed"),
                        state.details("rolled-back"),
                    )
                created, changed = self._net(state)
                time()
                markers = canonical_json(
                    {
                        "programSha256": hashlib.sha256(
                            canonical_json(state.program).encode()
                        ).hexdigest(),
                        "requestSha256": hashlib.sha256(
                            state.request.encode()
                        ).hexdigest(),
                        "verification": state.verification,
                        "effects": state.markers,
                        "changes": state.changes,
                        "noOp": not state.changes,
                    }
                )
                scope.recheck()
                time()
                submitted = True
                receipt = scope.atomic.commit(scope.frame, markers_json=markers)
                state.receipt = sdk_delta_receipt(receipt)
                committed = Committed(
                    None, state.receipt, created, changed, state.details("committed")
                )
                scope.recheck()
                time()
            return committed
        except DeltaAtomicConflict:
            return Conflict(
                False, "SERIALIZABLE_CONFLICT", state.details("rolled-back")
            )
        except DeltaAlreadySubmitted as error:
            raise ActionRefused(
                "DELTA_ORIGINAL_SUBMISSION_REQUIRES_RECONCILIATION"
            ) from error
        except BaseException as error:
            if (
                submitted
                and state.scope is not None
                and state.scope.custody.atomic.submitted
            ):
                self.quarantined = True
                try:
                    self.admission.quarantine(state.scope, error)
                except BaseException as cleanup_error:  # noqa: BLE001 - retain both original submission and quarantine failures.
                    self.quarantine_failure = (error, cleanup_error)
                return Indeterminate(state.details("indeterminate"))
            if (
                isinstance(error, TimeoutError)
                or isinstance(error, Exception)
                and monotonic() >= until
            ):
                return TimedOut(state.details("rolled-back"))
            raise

    def invoke(
        self,
        action: ActionRef[Any, Any],
        input: Mapping[str, object],
        *,
        timeout: timedelta | None = None,
    ) -> ActionOutcome:
        return cast(ActionOutcome, self._run(action, input, timeout, False))

    def validate(
        self,
        action: ActionRef[Any, Any],
        input: Mapping[str, object],
        *,
        timeout: timedelta | None = None,
    ) -> ValidationResult:
        return cast(ValidationResult, self._run(action, input, timeout, True))
