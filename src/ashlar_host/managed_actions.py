"""Managed raw Delta execution of original compiler programs.

This is separate from Ashlar publication targets. The caller supplies a qualified
native admission/transaction interval; no connection or authority is invented.
"""

from __future__ import annotations

import hashlib
from datetime import timedelta
from time import monotonic

from umf_access.native_actions import NativeScope, _State
from umf_access.native_program import canonical_json
from umf_access.protocols import (
    ActionFailed,
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

from ashlar.execution_scope import preserve_body_failure
from ashlar_host.typed_actions import DeltaActionExecutor


class ManagedNativeConflict(Exception):
    """Only an independently verified original native serialization conflict."""

    def __init__(self, original):
        self.original = original
        super().__init__("Original managed native conflict; no retry")


class ManagedDeltaActionExecutor:
    def __init__(self, programs, admission, *, default_timeout: timedelta):
        if (
            type(default_timeout) is not timedelta
            or not 0 < default_timeout.total_seconds() <= 60
        ):
            raise ActionRefused("FINITE_MANAGED_DELTA_DEADLINE_REQUIRED")
        self.programs, self.admission, self.default_timeout = (
            programs,
            admission,
            default_timeout,
        )
        self.quarantined = False

    def _state(self, action, inputs):
        if self.quarantined:
            raise ActionRefused("MANAGED_DELTA_EXECUTOR_QUARANTINED")
        matching = [
            p
            for p in self.programs
            if p.checked().get("profile") == "umf.actions.managed-delta/0.1"
            and p.checked()["source"]["documentSha256"]
            == getattr(action.source, "document_sha256", None)
            and p.checked()["source"]["action"] == action.action
        ]
        if len(matching) != 1:
            raise ActionRefused("MANAGED_DELTA_ACTION_NOT_COMPILED")
        return _State(action, matching[0], inputs)

    def _run(self, action, inputs, timeout, validate):
        state = self._state(action, inputs)
        budget = self.default_timeout if timeout is None else timeout
        if type(budget) is not timedelta or not 0 < budget.total_seconds() <= 60:
            raise ActionRefused("FINITE_MANAGED_DELTA_DEADLINE_REQUIRED")
        deadline = monotonic() + budget.total_seconds()
        native = None
        try:
            with preserve_body_failure(
                self.admission.hold(state.program, state.request, deadline=deadline)
            ) as native:
                # The owner proves these native identities and holds their current
                # source/role/identity fence through the original commit interval.
                state.scope = NativeScope(
                    native.store,
                    native.epoch,
                    native.authenticated,
                    native.effective,
                    native,
                    native.attribution_json,
                )
                if not native.allowed:
                    return (
                        ValidationResult(
                            False, (ValidationFailure("ROLE_DENIED"),), True
                        )
                        if validate
                        else Denied("ROLE_DENIED", state.details("rolled-back"))
                    )

                def check_time():
                    if monotonic() >= deadline:
                        raise TimeoutError("original managed action deadline expired")

                def key_check():
                    check_time()
                    if native.query(*state.statement("/keyCheck")) != [(0,)]:
                        raise ActionRefused("MANAGED_DELTA_NATIVE_KEY_INTEGRITY")

                key_check()
                DeltaActionExecutor._drive(state.images("pre"), native)
                missing = [
                    name
                    for name, entity in state.program["entities"].items()
                    if entity["requiredExisting"]
                    and state.pre["entity:" + name] is None
                ]
                failures = (
                    [ValidationFailure("ENTITY_MISSING")]
                    if missing
                    else DeltaActionExecutor._drive(
                        state.conditions("pre", collect_all=validate), native
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
                    check_time()
                    before = state.pre["effect:" + str(index)]
                    if effect["kind"] == "create" and before is not None:
                        raise ActionFailed(
                            "KEY_EXISTS",
                            ValueError("original managed CREATE Key already exists"),
                            state.details("rolled-back"),
                        )
                    if effect["kind"] != "create" and before is None:
                        raise ActionFailed(
                            "ENTITY_MISSING",
                            ValueError("original managed effect entity absent"),
                            state.details("rolled-back"),
                        )
                    if effect["guard"] is not None:
                        rows = native.query(
                            *state.statement("/effects/" + str(index) + "/guard")
                        )
                        if (
                            len(rows) != 1
                            or len(rows[0]) != 1
                            or type(rows[0][0]) is not bool
                        ):
                            raise ActionRefused("MANAGED_DELTA_EFFECT_MARKER")
                        if not rows[0][0]:
                            state.markers.append({"id": effect["id"], "changed": False})
                            continue
                    native.execute(
                        *state.statement("/effects/" + str(index) + "/statement")
                    )
                    state.markers.append({"id": effect["id"], "changed": True})
                key_check()
                DeltaActionExecutor._drive(state.images("post"), native)
                failures = DeltaActionExecutor._drive(state.conditions("post"), native)
                if failures:
                    raise ActionFailed(
                        "POSTCONDITION_" + failures[0].code,
                        ValueError("original managed postcondition failed"),
                        state.details("rolled-back"),
                    )
                created, changed = DeltaActionExecutor._net(state)
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
                native.recheck()
                check_time()
                # Owner commits data + these exact markers + original request in
                # one native transaction and returns its exact retained receipt.
                state.receipt = native.commit(markers_json=markers)
                result = Committed(
                    None, state.receipt, created, changed, state.details("committed")
                )
                native.recheck()
            return result
        except ManagedNativeConflict:
            if native is None or not native.rollback_confirmed:
                raise ActionRefused("MANAGED_CONFLICT_SETTLEMENT_UNCONFIRMED")
            return Conflict(
                False, "SERIALIZABLE_CONFLICT", state.details("rolled-back")
            )
        except BaseException as error:
            if native is not None and (
                native.commit_submitted
                or (native.write_submitted and not native.rollback_confirmed)
            ):
                self.quarantined = True
                try:
                    self.admission.quarantine(state.scope, error)
                except BaseException as cleanup_error:  # noqa: BLE001 - retain both original and failed quarantine custody
                    self.quarantine_failure = (error, cleanup_error)
                return Indeterminate(state.details("indeterminate"))
            if (
                isinstance(error, TimeoutError)
                and native is not None
                and native.rollback_confirmed
            ):
                return TimedOut(state.details("rolled-back"))
            raise

    def invoke(self, action, input, *, timeout=None):
        return self._run(action, input, timeout, False)

    def validate(self, action, input, *, timeout=None):
        return self._run(action, input, timeout, True)
