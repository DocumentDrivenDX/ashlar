# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh private PostgreSQL 18 owner for one managed A1 native pin vector.

This owner stores Ashlar pin rows in an owned local PostgreSQL cluster. The
transaction guards pin identity only; it is not a Delta file lease or a UC
retention observer.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
from importlib.resources import as_file, files
from pathlib import Path
import re
import subprocess
from collections.abc import Mapping
import time

from ashlar.pins import PinVector, PostgresPins


class ManagedA1LocalPinOwnerError(RuntimeError):
    """The selected local pin schema, transaction, or A1 binding refused."""


class _A1PinPolicy:
    def __init__(self, selection, vector, context):
        self.selection, self.vector, self.context = selection, vector, context
        self._registered = False

    def _admit(self, vector, context):
        if (vector is not self.vector or context is not self.context
                or type(vector) is not PinVector or vector != self.vector):
            raise ManagedA1LocalPinOwnerError("Exact selected four-role vector and local owner context required")
        custody = getattr(self.selection, "expected_source_custody", None)
        if (getattr(self.selection, "vector", None) != vector
                or not isinstance(custody, Mapping)
                or len(custody) != 8
                or any(type(value) is not str
                       or re.fullmatch(r"[0-9a-f]{64}", value) is None
                       for value in custody.values())):
            raise ManagedA1LocalPinOwnerError("Exact managed A1 retention selection required")
        return None

    def admit_registration(self, vector, context):
        if self._registered:
            raise ManagedA1LocalPinOwnerError("One original pin-phase registration required")
        return self._admit(vector, context)

    def authorize_read(self, vector, context):
        return self._admit(vector, context)

    def authorize_source_bound_read(self, vector, source_selection, context):
        if source_selection is not self.selection:
            raise ManagedA1LocalPinOwnerError("Source-bound pin selection differs from the exact A1 read selection")
        return self._admit(vector, context)


class _PsycopgPinExecutor:
    """One fresh RW transaction per pin owner operation on the private cluster."""
    def __init__(self, connection_factory, context):
        self.connection_factory, self.context = connection_factory, context

    @contextmanager
    def transaction(self, context):
        if context is not self.context:
            raise ManagedA1LocalPinOwnerError("Original local pin owner context required")
        connection = None
        session = None
        primary = None
        try:
            import psycopg
            connection = self.connection_factory()
            if (not isinstance(connection, psycopg.Connection)
                    or connection.autocommit
                    or connection.info.transaction_status != 0):
                raise ManagedA1LocalPinOwnerError("Fresh idle non-autocommit local pin connection required")
            # PostgreSQL requires the access mode in the first transaction
            # statement. No table mutation occurs in this pin check transaction.
            connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE")
            connection.execute("SET LOCAL statement_timeout = '5000ms'")
            connection.execute("SET LOCAL lock_timeout = '2000ms'")
            from .postgres import Session
            session = Session(connection)
            yield session
        except BaseException as error:
            primary = error
            raise
        finally:
            failures = []
            if connection is not None:
                if primary is None:
                    try:
                        connection.commit()
                    except BaseException as error:
                        failures.append(error)
                        try:
                            connection.rollback()
                        except BaseException as cleanup:
                            failures.append(cleanup)
                else:
                    try:
                        connection.rollback()
                    except BaseException as error:
                        failures.append(error)
                if session is not None:
                    session.active = False
                try:
                    connection.close()
                except BaseException as error:
                    failures.append(error)
            if primary is None and failures:
                raise ManagedA1LocalPinOwnerError("Local pin transaction cleanup was incomplete") from failures[0]
            if primary is not None and failures:
                add_note = getattr(primary, "add_note", None)
                if callable(add_note):
                    add_note("Local pin cleanup also failed: " + type(failures[0]).__name__)


class ManagedA1LocalPinOwner:
    """Own one fresh PG18 registry for exact sequential A0/A1 pin scopes."""

    def __init__(self, data_directory, selection, vector, *, native_timeout_seconds=900,
                 deadline_monotonic=None):
        from truss import LocalPostgres
        if (not isinstance(data_directory, Path) or not data_directory.is_absolute()
                or data_directory.resolve() != data_directory
                or data_directory.exists() or data_directory.is_symlink()
                or type(selection) is not __import__(
                    "ashlar_host.managed_operational_retention_interval",
                    fromlist=["ManagedOperationalRetentionSelection"]).ManagedOperationalRetentionSelection
                or type(vector) is not PinVector or selection.vector != vector
                or type(native_timeout_seconds) is not int
                or not 31 <= native_timeout_seconds <= 900
                or (deadline_monotonic is not None
                    and (type(deadline_monotonic) not in (int, float)
                         or deadline_monotonic <= time.monotonic()))):
            raise ManagedA1LocalPinOwnerError("Fresh PG18 path and exact A1 retention/vector inputs required")
        self.data_directory = data_directory
        self.selection, self.vector = selection, vector
        self.native_timeout_seconds = native_timeout_seconds
        self.deadline_monotonic = deadline_monotonic
        self.context = object()
        self._runtime = LocalPostgres(data_directory)
        self._active = False
        self._policy = _A1PinPolicy(selection, vector, self.context)
        self._pins = None
        self._executor = None
        self._pin_handle = None
        self._phase_handle_opened = False
        self._phase_close_confirmed = False
        self._phase_released = False
        self._phase_index = 0
        self._seen_scope_keys = {(vector.authority, vector.scope_kind, vector.scope_id)}

    def __enter__(self):
        if self._active:
            raise ManagedA1LocalPinOwnerError("Local pin owner cannot be reentered")
        try:
            self._initialize_fresh_pg18_no_sync()
            if (self.deadline_monotonic is not None
                    and time.monotonic() >= self.deadline_monotonic):
                raise ManagedA1LocalPinOwnerError("A1 deadline expired before local PG18 startup")
            self._runtime.__enter__()
            self._active = True
            if (self.deadline_monotonic is not None
                    and time.monotonic() >= self.deadline_monotonic):
                raise ManagedA1LocalPinOwnerError("A1 deadline expired during local PG18 startup")
            self._install_schema()
            self._executor = _PsycopgPinExecutor(self._connect, self.context)
            self._pins = PostgresPins(self._executor, self._policy)
        except BaseException as error:
            if self._active:
                try:
                    self._runtime.__exit__(type(error), error, error.__traceback__)
                except BaseException as cleanup:
                    add_note = getattr(error, "add_note", None)
                    if callable(add_note):
                        add_note("Local PostgreSQL startup cleanup also failed: "
                                 + type(cleanup).__name__)
                finally:
                    self._active = False
            raise
        return self

    def _initialize_fresh_pg18_no_sync(self):
        """Use exact bundled PostgreSQL 18 initdb, avoiding its startup sync.

        Server fsync stays enabled. This is only initdb's disposable-directory
        speed option, bounded independently; Truss LocalPostgres subsequently
        validates and starts this exact PGDATA with its own pinned runtime.
        """
        if self.data_directory.exists() or self.data_directory.is_symlink():
            raise ManagedA1LocalPinOwnerError("Fresh empty PG18 data directory required")
        binary_directory = Path(str(files("pgserver"))) / "pginstall" / "bin"
        initdb = binary_directory / "initdb"
        if not initdb.is_file():
            raise ManagedA1LocalPinOwnerError("Exact bundled PostgreSQL 18 initdb required")
        self.data_directory.parent.mkdir(parents=True, exist_ok=True)
        self.data_directory.mkdir(mode=0o700)
        timeout = 90
        if self.deadline_monotonic is not None:
            timeout = min(timeout, int(self.deadline_monotonic - time.monotonic()))
        if timeout < 1:
            raise ManagedA1LocalPinOwnerError("A1 deadline expired before local PG18 initialization")
        result = subprocess.run(
            [str(initdb), "--no-sync", "-D", str(self.data_directory),
             "--auth=trust", "--auth-local=trust", "--encoding=UTF8", "-U", "postgres"],
            capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0 or not (self.data_directory / "PG_VERSION").is_file():
            raise ManagedA1LocalPinOwnerError(
                "Fresh bundled PostgreSQL 18 no-sync initialization failed")

    def _connect(self):
        if not self._active:
            raise ManagedA1LocalPinOwnerError("Fresh active local PostgreSQL owner required")
        import psycopg
        return psycopg.connect(self._runtime.info.connection_uri,
            autocommit=False, connect_timeout=5,
            options="-c statement_timeout=5000 -c lock_timeout=2000")

    def _install_schema(self):
        migration_root = files("ashlar_host").joinpath(
            "resources", "sql", "ashlar-pins")
        migration_names = (
            "01-postgresql.sql", "03-read-guard.sql", "04-table-guard.sql",
            "05-maintenance-quarantine.sql", "06-availability-guard.sql")
        migrations = tuple(migration_root.joinpath(name) for name in migration_names)
        if any(not resource.is_file() for resource in migrations):
            raise ManagedA1LocalPinOwnerError("Committed Ashlar pin migrations required")
        for resource in migrations:
            with as_file(resource) as path:
                if path.is_symlink() or not path.is_file():
                    raise ManagedA1LocalPinOwnerError("Packaged pin migration must be a regular file")
                timeout = 15
                if self.deadline_monotonic is not None:
                    timeout = min(timeout, int(self.deadline_monotonic - time.monotonic()))
                if timeout < 1:
                    raise ManagedA1LocalPinOwnerError("A1 deadline expired during local pin migration")
                result = subprocess.run([str(self._runtime.psql_path),
                    self._runtime.info.connection_uri, "-X", "-v", "ON_ERROR_STOP=1",
                    "-f", str(path)], capture_output=True, text=True, timeout=timeout)
                if result.returncode != 0:
                    raise ManagedA1LocalPinOwnerError(
                        f"Local pin migration refused: {path.name} ({result.returncode})")

    def register(self):
        if not self._active or self._pins is None or self._policy._registered:
            raise ManagedA1LocalPinOwnerError("Active one-shot local pin phase required")
        try:
            self._pins.register(self.vector, context=self.context)
        except BaseException as error:
            raise ManagedA1LocalPinOwnerError("Exact local A1 vector registration refused") from error
        self._policy._registered = True
        return {"profile": "ashlar-a1-local-postgres-pin-owner/0.1",
            "server_version": self._runtime.info.server_version,
            "runtime_version": self._runtime.info.runtime_version,
            "data_directory": str(self.data_directory),
            "vector_sha256": hashlib.sha256(json_bytes(self.vector)).hexdigest(),
            "phase_index": self._phase_index,
            "native_pin_transaction_timeout_seconds": self.native_timeout_seconds,
            "authority": "fresh owned local PostgreSQL pin registry; no Delta file lease"}

    def close_native_pin(self, handle):
        """Close only this phase's exact live handle while retaining PG18."""
        if (not self._active or self._pin_handle is None
                or handle is not self._pin_handle):
            raise ManagedA1LocalPinOwnerError("Original live phase pin handle required")
        try:
            handle.close()
        except BaseException:
            # Do not allow a failed close to authorize registry release or a
            # subsequent phase. Clear the reference only so bounded owner
            # shutdown can reap the local server during failure cleanup.
            self._pin_handle = None
            self._phase_close_confirmed = False
            raise
        self._pin_handle = None
        self._phase_close_confirmed = True

    @staticmethod
    def _phase_inventory(session, vector, *, released):
        rows = session.query(
            "SELECT table_name,table_uuid,version::text AS version,"
            "encode(custody_digest,'hex') AS digest,released "
            "FROM ashlar_pins.pin WHERE authority=:authority "
            "AND scope_kind=:kind AND scope_id=:scope LIMIT 129",
            {"authority": vector.authority, "kind": vector.scope_kind,
             "scope": vector.scope_id}).rows
        expected = {table: (uuid, str(version))
            for table, (uuid, version) in vector.targets.items()}
        if (type(rows) is not list or len(rows) != len(expected)
                or {row.get("table_name") for row in rows} != set(expected)
                or any((row.get("table_uuid"), row.get("version"))
                    != expected.get(row.get("table_name"))
                    or row.get("digest") != vector.custody_digest
                    or row.get("released") is not released for row in rows)):
            raise ManagedA1LocalPinOwnerError(
                "Exact phase pin inventory readback required")

    def release_current_registration(self):
        """Release the closed phase's exact vector, retaining immutable rows."""
        if (not self._active or self._pin_handle is not None
                or not self._phase_handle_opened or self._phase_released
                or not self._phase_close_confirmed
                or not self._policy._registered or self._executor is None):
            raise ManagedA1LocalPinOwnerError(
                "Closed registered phase pin required before exact release")
        vector = self.vector
        try:
            with self._executor.transaction(self.context) as session:
                self._phase_inventory(session, vector, released=False)
                for table in sorted(vector.targets):
                    parameters = vector.parameters(table)
                    session.query(
                        "SELECT ashlar_pins.release(:id,:authority,decode(:digest,'hex'))",
                        {"id": parameters["id"], "authority": vector.authority,
                         "digest": vector.custody_digest})
                self._phase_inventory(session, vector, released=True)
        except BaseException as error:
            if isinstance(error, ManagedA1LocalPinOwnerError):
                raise
            raise ManagedA1LocalPinOwnerError(
                "Exact original phase pin release was not confirmed") from error
        self._phase_released = True
        return {"profile": "ashlar-a1-local-pin-release/0.1",
            "phase_index": self._phase_index,
            "vector_sha256": hashlib.sha256(json_bytes(vector)).hexdigest(),
            "scope_id": vector.scope_id, "released_rows": len(vector.targets),
            "state": "released_exact_vector_readback_confirmed"}

    def select_next_phase(self, selection, vector, *, native_timeout_seconds):
        """Bind a distinct next phase after prior handle close and pin release."""
        from .managed_operational_retention_interval import (
            ManagedOperationalRetentionSelection,
        )
        if (not self._active or self._pin_handle is not None
                or not self._phase_released or self._phase_index != 0
                or type(selection) is not ManagedOperationalRetentionSelection
                or type(vector) is not PinVector or selection.vector != vector
                or type(native_timeout_seconds) is not int
                or not 31 <= native_timeout_seconds <= 900
                or (self.deadline_monotonic is not None
                    and time.monotonic() >= self.deadline_monotonic)):
            raise ManagedA1LocalPinOwnerError(
                "Distinct next phase requires a closed and released prior pin scope")
        scope_key = (vector.authority, vector.scope_kind, vector.scope_id)
        if scope_key in self._seen_scope_keys:
            raise ManagedA1LocalPinOwnerError(
                "A1 pin phase must use a new immutable publication scope")
        self._seen_scope_keys.add(scope_key)
        self.selection, self.vector = selection, vector
        self.native_timeout_seconds = native_timeout_seconds
        self.context = object()
        self._policy = _A1PinPolicy(selection, vector, self.context)
        self._executor = _PsycopgPinExecutor(self._connect, self.context)
        self._pins = PostgresPins(self._executor, self._policy)
        self._phase_handle_opened = False
        self._phase_close_confirmed = False
        self._phase_released = False
        self._phase_index += 1
        return {"profile": "ashlar-a1-local-pin-phase-selection/0.1",
            "phase_index": self._phase_index,
            "vector_sha256": hashlib.sha256(json_bytes(vector)).hexdigest(),
            "scope_id": vector.scope_id,
            "native_pin_transaction_timeout_seconds": native_timeout_seconds}

    def operational_pin_port(self, read_port):
        if (not self._active or not self._policy._registered
                or type(getattr(read_port, "selection", None)) is not type(self.selection)
                or read_port.selection is not self.selection):
            raise ManagedA1LocalPinOwnerError("Exact registered A1 read owner required")
        from .managed_operational_read_port import ManagedOperationalReadPinPort
        return ManagedOperationalReadPinPort(read_port, self._pins,
            context=self.context,
            native_pin_transaction_timeout_seconds=self.native_timeout_seconds)

    def open_native_pin(self):
        """Open one parent-held pin around remote managed observation RPCs.

        The returned handle guards native Ashlar pin identity only. A caller
        composing verifier task observations must renew before pre-ACK and
        close after the verifier's closing observation; this is not a managed
        read or Delta file-retention lease.
        """
        if (not self._active or not self._policy._registered
                or self._pins is None or self._phase_handle_opened
                or self._pin_handle is not None or self._phase_released):
            raise ManagedA1LocalPinOwnerError("Registered one-shot phase pin required")
        try:
            handle = self._pins.open_live(self.vector, self.selection,
                context=self.context,
                bounded_lifetime_seconds=self.native_timeout_seconds)
        except BaseException:
            raise
        self._phase_handle_opened = True
        self._phase_close_confirmed = False
        self._pin_handle = handle
        return handle

    def __exit__(self, exc_type, exc, traceback):
        if not self._active:
            return False
        if self._pin_handle is not None:
            raise ManagedA1LocalPinOwnerError(
                "Live phase pin must be closed before its PostgreSQL owner")
        self._active = False
        return self._runtime.__exit__(exc_type, exc, traceback)


def json_bytes(vector):
    import json
    return json.dumps({"authority": vector.authority, "scope_kind": vector.scope_kind,
        "scope_id": vector.scope_id, "custody_digest": vector.custody_digest,
        "targets": {key: list(value) for key, value in vector.targets.items()}},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
