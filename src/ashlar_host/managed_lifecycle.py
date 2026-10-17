"""Fail-closed, native persistent serialization for a source-owned UC deployment.

The private coordinator connection is an owner capability, never an action
connection. Every normal deployment and invocation must consume this same lease.
Administrative bypass remains outside that host boundary. This is a candidate
adapter: native ownership/permission and concurrency witnesses are mandatory.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from time import monotonic
from uuid import uuid4

from umf_access.protocols import ActionRefused

from ashlar_host.managed_connector import (
    describe_native_table,
    execute_original,
    qualified_table_name,
)


@dataclass(frozen=True)
class ManagedLifecycleEnrollment:
    table: tuple[str, str, str]
    table_uuid: str
    store: str
    epoch: str
    coordinator_principal: str


@dataclass
class OriginalManagedLease:
    token: str
    purpose: str
    enrollment: ManagedLifecycleEnrollment
    committed: bool = False
    released: bool = False
    body_entered: bool = False
    body_completed: bool = False
    release_submitted: bool = False
    original_error: BaseException | None = None


class ManagedDeploymentLifecycle:
    """Private owner port shared by invocations and all qualified deployments.

    No timeout steals a lease. An interrupted/uncertain operation keeps the native
    row occupied until a separately qualified original-operation recovery proves
    that releasing it is safe. This adapter deliberately provides no force-unlock.
    """

    def __init__(self, coordinator_factory, enrollment):
        if not callable(coordinator_factory) or not isinstance(
            enrollment, ManagedLifecycleEnrollment
        ):
            raise ActionRefused("ORIGINAL_NATIVE_LIFECYCLE_ENROLLMENT_REQUIRED")
        self._factory = coordinator_factory
        self.enrollment = enrollment
        self.pending = None
        self.trace = []

    def _query(self, connection, statement, parameters, deadline):
        cursor = connection.cursor()
        try:
            execute_original(cursor, statement, parameters, deadline=deadline)
            self.trace.append((statement, parameters, cursor.query_id))
            rows = cursor.fetchmany(3) if cursor.description else []
            return [tuple(row) for row in rows]
        finally:
            cursor.close()

    def _connection(self, deadline):
        connection = self._factory(deadline=deadline)
        try:
            if connection.ignore_transactions is not False or self._query(
                connection, "SET AUTOCOMMIT", None, deadline
            ) != [("true",)]:
                raise ActionRefused("ORIGINAL_NATIVE_COORDINATOR_CONNECTION_REQUIRED")
            detail = describe_native_table(
                connection, self.enrollment.table, deadline=deadline
            )
            if (
                detail.get("id") != self.enrollment.table_uuid
                or detail.get("format") != "delta"
            ):
                raise ActionRefused("NATIVE_LIFECYCLE_TABLE_IDENTITY")
            properties = detail.get("properties")
            features = detail.get("tableFeatures")
            if (
                not isinstance(properties, dict)
                or properties.get("delta.isolationLevel") != "Serializable"
                or not isinstance(features, list)
                or "catalogManaged" not in features
            ):
                raise ActionRefused("NATIVE_LIFECYCLE_SERIALIZABLE_PROFILE")
            if self._query(connection, "SELECT current_user()", None, deadline) != [
                (self.enrollment.coordinator_principal,)
            ]:
                raise ActionRefused("ORIGINAL_NATIVE_COORDINATOR_PRINCIPAL")
            return connection
        except BaseException:
            connection.close()
            raise

    @contextmanager
    def _hold(self, purpose, *, deadline):
        if self.pending is not None:
            raise ActionRefused("ORIGINAL_NATIVE_LIFECYCLE_RECOVERY_REQUIRED")
        if not monotonic() < deadline:
            raise TimeoutError("native lifecycle deadline expired")
        e = self.enrollment
        lease = OriginalManagedLease(uuid4().hex, purpose, e)
        self.pending = lease
        table = qualified_table_name(e.table)
        connection = None
        try:
            connection = self._connection(deadline)
            self._query(connection, "SET AUTOCOMMIT = FALSE", None, deadline)
            # A single pre-existing row is read and written. Competing original
            # transactions conflict under Serializable; neither retries.
            rows = self._query(
                connection, "SELECT store,epoch,token FROM " + table, None, deadline
            )
            if rows != [(e.store, e.epoch, None)]:
                raise ActionRefused("NATIVE_LIFECYCLE_NOT_EXCLUSIVELY_AVAILABLE")
            self._query(
                connection,
                "UPDATE "
                + table
                + " SET token=:token,purpose=:purpose WHERE store=:store AND epoch=:epoch AND token IS NULL",
                {
                    "token": lease.token,
                    "purpose": purpose,
                    "store": e.store,
                    "epoch": e.epoch,
                },
                deadline,
            )
            if self._query(
                connection, "SELECT store,epoch,token FROM " + table, None, deadline
            ) != [(e.store, e.epoch, lease.token)]:
                raise ActionRefused("NATIVE_LIFECYCLE_ACQUISITION_MARKER")
            self._query(connection, "COMMIT", None, deadline)
            lease.committed = True
            connection.close()
            connection = None
            lease.body_entered = True
            yield lease
            lease.body_completed = True
            # The admitted action scope has settled/closed before this point.
            connection = self._connection(deadline)
            self._query(connection, "SET AUTOCOMMIT = FALSE", None, deadline)
            if self._query(
                connection, "SELECT store,epoch,token FROM " + table, None, deadline
            ) != [(e.store, e.epoch, lease.token)]:
                raise ActionRefused("ORIGINAL_NATIVE_LIFECYCLE_CUSTODY_LOST")
            self._query(
                connection,
                "UPDATE "
                + table
                + " SET token=NULL,purpose=NULL WHERE store=:store AND epoch=:epoch AND token=:token",
                {"store": e.store, "epoch": e.epoch, "token": lease.token},
                deadline,
            )
            lease.release_submitted = True
            self._query(connection, "COMMIT", None, deadline)
            lease.released = True
            self.pending = None
        except BaseException as original:
            lease.original_error = original
            # Never release or reuse on unknown acquisition, body or release.
            raise
        finally:
            if connection is not None:
                connection.close()

    def recover_unused_or_completed(self, original, *, deadline):
        """Release only exact custody whose business interval is provably absent/completed.

        An entered but uncertain business interval is deliberately excluded. Its
        original native action recovery must first prove settlement; reading a
        free lease or waiting out a deadline never supplies that evidence.
        """
        if self.pending is not original or original.enrollment is not self.enrollment:
            raise ActionRefused("SAME_ORIGINAL_NATIVE_LEASE_REQUIRED")
        if original.body_entered and not original.body_completed:
            raise ActionRefused("ORIGINAL_BUSINESS_SETTLEMENT_REQUIRED")
        connection = self._connection(deadline)
        table = qualified_table_name(self.enrollment.table)
        e = self.enrollment
        try:
            self._query(connection, "SET AUTOCOMMIT = FALSE", None, deadline)
            rows = self._query(
                connection, "SELECT store,epoch,token FROM " + table, None, deadline
            )
            if (
                rows == [(e.store, e.epoch, None)]
                and original.release_submitted
                and original.body_completed
            ):
                self._query(connection, "ROLLBACK", None, deadline)
                original.released = True
                self.pending = None
                return original
            if rows != [(e.store, e.epoch, original.token)]:
                raise ActionRefused("ORIGINAL_NATIVE_LEASE_SETTLEMENT_UNPROVEN")
            self._query(
                connection,
                "UPDATE "
                + table
                + " SET token=NULL,purpose=NULL WHERE store=:store AND epoch=:epoch AND token=:token",
                {"store": e.store, "epoch": e.epoch, "token": original.token},
                deadline,
            )
            original.release_submitted = True
            self._query(connection, "COMMIT", None, deadline)
            original.released = True
            self.pending = None
            return original
        finally:
            connection.close()

    def hold(self, enrollment, program, request, *, deadline):
        if (enrollment.store, enrollment.epoch) != (
            self.enrollment.store,
            self.enrollment.epoch,
        ):
            raise ActionRefused("NATIVE_LIFECYCLE_STORE_INCARCATION_MISMATCH")
        return self._hold("action", deadline=deadline)

    @contextmanager
    def deployment(self, *, deadline):
        """Only source-owned deployment entrypoint; supplies private owner SQL.

        Caller must use this connection for every relevant DDL/authority change,
        update enrollment only after completion, and retain uncertain custody.
        No credential or connection is supplied to the action principal.
        """
        with self._hold("deployment", deadline=deadline):
            connection = self._connection(deadline)
            try:
                yield connection
            finally:
                connection.close()
