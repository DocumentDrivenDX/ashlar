"""Explicit caller-owned managed SQL transaction custody, never publication writes.

Enrollment rows and tables must already exist under native UC administration.
This module cannot enroll programs, grant roles, create a connection factory or
replace missing original custody. The owner supplies one private connection per
invocation and separately qualified source/lifecycle fencing.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass
from time import monotonic

from umf_access.native_program import canonical_json
from umf_access.protocols import ActionRefused, Receipt

from ashlar_host.managed_connector import execute_original, qualified_table_name

_qualified = qualified_table_name


@dataclass(frozen=True)
class ManagedEnrollment:
    # Original host enrollment, not permission derived from this data structure.
    store: str
    epoch: str
    data_table: tuple[str, str, str]
    data_uuid: str
    gate_table: tuple[str, str, str]
    gate_uuid: str
    receipt_table: tuple[str, str, str]
    receipt_uuid: str
    principal: str
    program_sha256: str


class ManagedCommitUncertain(RuntimeError):
    def __init__(self, message, *, originals=()):
        self.originals = tuple(originals)
        super().__init__(message)


class ManagedAdmission:
    def __init__(self, connection_factory, enrollment, lifecycle_fence):
        if not callable(connection_factory) or not callable(
            getattr(lifecycle_fence, "hold", None)
        ):
            raise ActionRefused(
                "ORIGINAL_MANAGED_CONNECTION_AND_NATIVE_LIFECYCLE_FENCE_REQUIRED"
            )
        if not isinstance(enrollment, ManagedEnrollment):
            raise ActionRefused("ORIGINAL_MANAGED_ENROLLMENT_REQUIRED")
        if enrollment.store != enrollment.receipt_uuid:
            raise ActionRefused("MANAGED_STORE_IS_ORIGINAL_NATIVE_LEDGER_UUID")
        self.connection_factory, self.enrollment, self.lifecycle_fence = (
            connection_factory,
            enrollment,
            lifecycle_fence,
        )
        self.quarantined = False
        self.pending = None

    @contextmanager
    def hold(self, program, request, *, deadline):
        if self.quarantined:
            raise ActionRefused("MANAGED_ORIGINAL_OPERATION_QUARANTINED")
        if (
            hashlib.sha256(canonical_json(program).encode()).hexdigest()
            != self.enrollment.program_sha256
        ):
            raise ActionRefused("MANAGED_ORIGINAL_PROGRAM_NOT_ENROLLED")
        scope = None
        # The required native owner fence covers metadata/identity lifecycle. It
        # must not be a process-local lock pretending to exclude remote DDL.
        with self.lifecycle_fence.hold(
            self.enrollment, program, request, deadline=deadline
        ):
            connection = self.connection_factory(deadline=deadline)
            body_error = None
            try:
                scope = ManagedScope(
                    connection, self.enrollment, program, request, deadline
                )
                scope.open()
                scope.receipt_reader = lambda: self._read_original_receipt(scope)
                yield scope
            except BaseException as original:
                body_error = original
                raise
            finally:
                try:
                    if scope is not None and not scope.commit_submitted:
                        try:
                            scope.deadline = max(scope.deadline, monotonic() + 10)
                            scope._execute("ROLLBACK")
                            scope.rollback_confirmed = True
                        except BaseException as rollback_error:
                            # Opening can already have written native control
                            # state before the caller receives a scope. Retain
                            # that exact original even when context entry fails.
                            self.quarantined = True
                            self.pending = (scope, rollback_error)
                            raise ManagedCommitUncertain(
                                "Original managed rollback settlement unknown",
                                originals=tuple(
                                    error
                                    for error in (body_error, rollback_error)
                                    if error is not None
                                ),
                            ) from rollback_error
                finally:
                    connection.close()

    def quarantine(self, native_scope, original):
        self.quarantined = True
        self.pending = (native_scope, original)

    def _read_original_receipt(self, original):
        if (
            original.enrollment is not self.enrollment
            or not original.commit_submitted
            or original.ledger_before is None
        ):
            raise ActionRefused("ORIGINAL_MANAGED_SUBMISSION_CUSTODY_REQUIRED")
        connection = self.connection_factory(deadline=original.deadline)
        reader = ManagedScope(
            connection,
            self.enrollment,
            original.program,
            original.request,
            original.deadline,
        )
        e = self.enrollment
        try:
            if reader.query("SET AUTOCOMMIT") != [("true",)]:
                raise ActionRefused("ORIGINAL_RECEIPT_READER_AUTOCOMMIT_REQUIRED")
            for table, uuid in (
                (e.data_table, e.data_uuid),
                (e.gate_table, e.gate_uuid),
                (e.receipt_table, e.receipt_uuid),
            ):
                reader._identity(table, uuid)
            if reader.query("SELECT current_user()") != [(e.principal,)]:
                raise ActionRefused("MANAGED_RECOVERY_PRINCIPAL_CHANGED")
            gate = _qualified(e.gate_table)
            if reader.query(
                "SELECT count(*) FROM "
                + gate
                + " WHERE store=:p1 AND epoch=:p2 AND actor=current_user() AND enabled=true AND program_sha256=:p3 AND data_uuid=:p4 AND receipt_uuid=:p5",
                (e.store, e.epoch, e.program_sha256, e.data_uuid, e.receipt_uuid),
            ) != [(1,)]:
                raise ActionRefused("MANAGED_CURRENT_RECOVERY_AUTHORITY_REQUIRED")
            history = reader.history_version()
            if (
                len(history) != 1
                or type(history[0][0]) is not int
                or not original.ledger_before
                < history[0][0]
                <= original.ledger_before + 100
            ):
                raise ManagedCommitUncertain(
                    "No bounded retained original native commit interval"
                )
            # CDF searches a bounded interval for this exact original request;
            # tip is a bound only, never selected as the receipt version.
            table_literal = ".".join(e.receipt_table).replace("'", "''")
            sql = (
                "SELECT request_sha256,request_json,program_sha256,markers_json,attribution_json,epoch,_commit_version FROM table_changes('"
                + table_literal
                + "',"
                + str(original.ledger_before + 1)
                + ","
                + str(history[0][0])
                + ") WHERE _change_type='insert' AND request_sha256=:p1"
            )
            rows = reader.query(sql, (original.request_sha256,))
            if (
                len(rows) != 1
                or tuple(rows[0][:6])
                != (
                    original.request_sha256,
                    original.request,
                    e.program_sha256,
                    original.markers_json,
                    original.attribution_json,
                    e.epoch,
                )
                or type(rows[0][6]) is not int
            ):
                raise ManagedCommitUncertain(
                    "No unique exact original native receipt; retain uncertainty"
                )
            for table, uuid in (
                (e.data_table, e.data_uuid),
                (e.gate_table, e.gate_uuid),
                (e.receipt_table, e.receipt_uuid),
            ):
                reader._identity(table, uuid)
            original.trace.extend(reader.trace)
            return Receipt(e.store, e.epoch, str(rows[0][6]))
        finally:
            connection.close()

    def reconcile(self, native_scope, *, deadline):
        if (
            not self.quarantined
            or self.pending is None
            or self.pending[0] is not native_scope
        ):
            raise ActionRefused("SAME_ORIGINAL_MANAGED_RECOVERY_CONTEXT_REQUIRED")
        original = native_scope.custody
        original.deadline = deadline
        # Reconciliation never reopens a mutation or clears quarantine.
        return self._read_original_receipt(original)


class ManagedScope:
    def __init__(self, connection, enrollment, program, request, deadline):
        self.connection, self.enrollment, self.program, self.request = (
            connection,
            enrollment,
            program,
            request,
        )
        self.deadline = deadline
        self.store, self.epoch = enrollment.store, enrollment.epoch
        self.authenticated = self.effective = None
        self.attribution_json = None
        self.allowed = False
        self.commit_submitted = self.rollback_confirmed = False
        self.write_submitted = False
        self.trace = []
        self.original_commit = None
        self.request_sha256 = hashlib.sha256(request.encode()).hexdigest()
        self.cursor = connection.cursor()
        self.ledger_before = None
        self.markers_json = None

    def _execute(self, sql, values=()):
        if monotonic() >= self.deadline:
            raise TimeoutError("original managed native interval expired")
        parameters = {"p" + str(i): v for i, v in enumerate(values, 1)}
        execute_original(self.cursor, sql, parameters, deadline=self.deadline)
        # Keep the original native cursor/result objects in owner custody. The
        # SDK never substitutes a fabricated SQL response or query identifier.
        self.trace.append((sql, parameters, self.cursor.query_id))
        return self.cursor

    def query(self, sql, values=()):
        cursor = self._execute(sql, values)
        rows = cursor.fetchmany(10002)
        if len(rows) > 10001:
            raise ActionRefused("MANAGED_COMPLETE_RESULT_BOUND")
        return [tuple(row) for row in rows]

    def execute(self, sql, values=()):
        self.write_submitted = True
        self._execute(sql, values)

    def _identity(self, table, expected):
        cursor = self._execute("DESCRIBE DETAIL " + _qualified(table))
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchmany(2)
        if len(rows) != 1:
            raise ActionRefused("MANAGED_NATIVE_TABLE_METADATA")
        row = dict(zip(names, rows[0], strict=True))
        features = row.get("tableFeatures")
        if isinstance(features, str):
            features = json.loads(features)
        properties = row.get("properties")
        if isinstance(properties, str):
            properties = json.loads(properties)
        if (
            row.get("id") != expected
            or row.get("format") != "delta"
            or not isinstance(features, list)
            or "catalogManaged" not in features
        ):
            raise ActionRefused("MANAGED_NATIVE_TABLE_IDENTITY")
        if (
            not isinstance(properties, dict)
            or properties.get("delta.isolationLevel") != "Serializable"
        ):
            raise ActionRefused("MANAGED_NATIVE_SERIALIZABLE_PROFILE")

    def history_version(self):
        cursor = self._execute(
            "DESCRIBE HISTORY " + _qualified(self.enrollment.receipt_table) + " LIMIT 1"
        )
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchmany(2)
        if (
            names.count("version") != 1
            or len(names) != len(set(names))
            or len(rows) != 1
            or len(rows[0]) != len(names)
        ):
            raise ActionRefused("MANAGED_ORIGINAL_LEDGER_VERSION_REQUIRED")
        version = rows[0][names.index("version")]
        if type(version) is not int or version < 0:
            raise ActionRefused("MANAGED_ORIGINAL_LEDGER_VERSION_REQUIRED")
        return [(version,)]

    def open(self):
        if self.connection.ignore_transactions is not False:
            raise ActionRefused("NATIVE_CONNECTOR_TRANSACTIONS_MUST_BE_ENABLED")
        if self.query("SET AUTOCOMMIT") != [("true",)]:
            raise ActionRefused("ORIGINAL_UNUSED_AUTOCOMMIT_CONNECTION_REQUIRED")
        e = self.enrollment
        for table, uuid in (
            (e.data_table, e.data_uuid),
            (e.gate_table, e.gate_uuid),
            (e.receipt_table, e.receipt_uuid),
        ):
            self._identity(table, uuid)
        rows = self.query("SELECT current_user(), current_catalog(), current_schema()")
        if len(rows) != 1 or rows[0][0] != e.principal:
            raise ActionRefused("MANAGED_NATIVE_PRINCIPAL")
        self.authenticated = self.effective = rows[0][0]
        self.attribution_json = canonical_json(
            {
                "authenticated": rows[0][0],
                "effective": rows[0][0],
                "catalog": rows[0][1],
                "schema": rows[0][2],
            }
        )
        history = self.history_version()
        if len(history) != 1 or type(history[0][0]) is not int:
            raise ActionRefused("MANAGED_ORIGINAL_LEDGER_VERSION_REQUIRED")
        self.ledger_before = history[0][0]
        self._execute("SET AUTOCOMMIT = FALSE")
        roles = self.program["action"]["authorization"]["roles"]
        groups = [self.program["roleMappings"][role] for role in roles]
        if not groups or len(groups) > 64:
            raise ActionRefused("MANAGED_NATIVE_ROLE_INVENTORY")
        memberships = self.query(
            "SELECT "
            + ",".join(
                "is_account_group_member(:p" + str(i) + ")" for i in range(1, len(groups) + 1)
            ),
            tuple(groups),
        )
        if len(memberships) != 1 or any(
            type(value) is not bool for value in memberships[0]
        ):
            raise ActionRefused("MANAGED_NATIVE_ROLE_MARKER")
        if not any(memberships[0]):
            return
        # Native gate is read and written in this transaction: role/source
        # revocation competes on the same native row, rather than remaining a
        # read-only policy observation outside conflict detection.
        gate = _qualified(e.gate_table)
        predicate = "store=:p1 AND epoch=:p2 AND actor=current_user() AND program_sha256=:p3 AND data_uuid=:p4 AND receipt_uuid=:p5 AND enabled=true"
        values = (e.store, e.epoch, e.program_sha256, e.data_uuid, e.receipt_uuid)
        rows = self.query(
            "SELECT count(*) FROM " + gate + " WHERE " + predicate, values
        )
        if rows != [(1,)]:
            return
        self.execute("UPDATE " + gate + " SET fence=fence+1 WHERE " + predicate, values)
        if self.query(
            "SELECT count(*) FROM "
            + _qualified(e.receipt_table)
            + " WHERE request_sha256=:p1",
            (self.request_sha256,),
        ) != [(0,)]:
            raise ActionRefused("MANAGED_ORIGINAL_REQUEST_REQUIRES_RECONCILIATION")
        # Capture the original table in the native transactional snapshot before
        # any source images or effects. Metadata lifecycle remains owner fenced.
        if self.query("SELECT count(*) FROM " + _qualified(e.data_table))[0][0] > 10000:
            raise ActionRefused("MANAGED_WHOLE_TABLE_BOUND")
        self.allowed = True

    def recheck(self):
        if self.query("SELECT current_user()") != [(self.authenticated,)]:
            raise ActionRefused("MANAGED_NATIVE_PRINCIPAL_CHANGED")

    def commit(self, *, markers_json):
        if self.commit_submitted:
            raise ActionRefused("MANAGED_TRANSACTION_SINGLE_USE")
        e = self.enrollment
        # Full original request/program markers and attribution share data's
        # native transaction. The owner verifies the exact native ledger commit
        # separately; this method cannot infer a version from latest history.
        self.execute(
            "INSERT INTO "
            + _qualified(e.receipt_table)
            + " (request_sha256, request_json, program_sha256, markers_json, attribution_json, epoch) VALUES (:p1,:p2,:p3,:p4,:p5,:p6)",
            (
                self.request_sha256,
                self.request,
                e.program_sha256,
                markers_json,
                self.attribution_json,
                e.epoch,
            ),
        )
        self.commit_submitted = True
        self.markers_json = markers_json
        self.original_commit = self._execute("COMMIT")
        return self.receipt_reader()
