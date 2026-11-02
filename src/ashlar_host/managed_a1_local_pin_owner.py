# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh private PostgreSQL 18 owner for one managed A1 native pin vector.

This owner stores Ashlar pin rows in an owned local PostgreSQL cluster. The
transaction guards pin identity only; it is not a Delta file lease or a UC
retention observer.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
from importlib.resources import files
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
            raise ManagedA1LocalPinOwnerError("One original A1 pin registration required")
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
    """Own one fresh PG18 data directory and exact A1 four-role pin registry."""

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
        self._pin_opened = False

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
            self._pins = PostgresPins(
                _PsycopgPinExecutor(self._connect, self.context), self._policy)
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
        timeout = 45
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
        migration_root = Path(__file__).resolve().parents[2] / "sql" / "ashlar-pins"
        migrations = tuple(migration_root / name for name in (
            "01-postgresql.sql", "03-read-guard.sql", "04-table-guard.sql",
            "05-maintenance-quarantine.sql", "06-availability-guard.sql"))
        if any(not path.is_file() or path.is_symlink() for path in migrations):
            raise ManagedA1LocalPinOwnerError("Committed Ashlar pin migrations required")
        for path in migrations:
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
            raise ManagedA1LocalPinOwnerError("Active one-shot local A1 pin owner required")
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
            "native_pin_transaction_timeout_seconds": self.native_timeout_seconds,
            "authority": "fresh owned local PostgreSQL pin registry; no Delta file lease"}

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
                or self._pins is None or self._pin_opened):
            raise ManagedA1LocalPinOwnerError("Registered one-shot A1 pin owner required")
        self._pin_opened = True
        try:
            return self._pins.open_live(self.vector, self.selection,
                context=self.context,
                bounded_lifetime_seconds=self.native_timeout_seconds)
        except BaseException:
            self._pin_opened = False
            raise

    def __exit__(self, exc_type, exc, traceback):
        if not self._active:
            return False
        self._active = False
        return self._runtime.__exit__(exc_type, exc, traceback)


def json_bytes(vector):
    import json
    return json.dumps({"authority": vector.authority, "scope_kind": vector.scope_kind,
        "scope_id": vector.scope_id, "custody_digest": vector.custody_digest,
        "targets": {key: list(value) for key, value in vector.targets.items()}},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
