#!/usr/bin/env python3
"""One-use B install/accept/genesis-registration worker (execution is gated)."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "b-install-config.json"
MAX_SOURCE = 1_048_576
MAX_ORIGIN = 65_536


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def strict_json_object(raw: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON object member")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs)
    if type(value) is not dict:
        raise ValueError("JSON object required")
    return value


def genesis_cursor_evidence(consumer, expected_consumer_id: str,
                           expected_installation_id: str | None = None) -> dict:
    """Validate the public FeedConsumer.checkpoint.content bytes exactly."""
    if consumer.consumer_id != expected_consumer_id or consumer.durability != "pending":
        raise ValueError("public consumer identity/durability differs")
    raw = consumer.checkpoint.content
    if type(raw) is not bytes or len(raw) > 16_384:
        raise ValueError("bounded exact FeedCursor.content bytes required")
    cursor = strict_json_object(raw)
    required = {"profile", "installationId", "sourceEpoch", "consumerId", "registrationId", "nextXid"}
    if set(cursor) != required or cursor["profile"] != "truss-caller-asserted-feed/0.1":
        raise ValueError("exact Truss feed cursor schema required")
    if (cursor["consumerId"] != expected_consumer_id
            or cursor["registrationId"] != consumer.registration_id
            or cursor["nextXid"] != "0"):
        raise ValueError("feed cursor identity or genesis checkpoint differs")
    if (expected_installation_id is not None
            and cursor["installationId"] != expected_installation_id):
        raise ValueError("genesis cursor belongs to a different installation")
    if any(type(cursor[k]) is not str or not cursor[k]
           for k in ("installationId", "sourceEpoch", "consumerId", "registrationId", "nextXid")):
        raise ValueError("feed cursor identity fields must be nonempty strings")
    return {"consumer_id": consumer.consumer_id,
            "registration_id": consumer.registration_id,
            "installation_id": cursor["installationId"],
            "source_epoch": cursor["sourceEpoch"],
            "cursor_sha256": sha(raw), "cursor_bytes": raw.decode("utf-8")}


def validate_accepted_observations(*, status_revision, accepted, report_bytes,
                                   registration, view, source, document_id, document_revision,
                                   expected_source_catalog_revision, complete_scope):
    expected_sha = sha(source)
    if (status_revision != 1 or accepted.document_id != document_id
            or accepted.content != source
            or accepted.document_revision != document_revision
            or accepted.catalog_revision != 1
            or accepted.source_catalog_revision != expected_source_catalog_revision
            or accepted.registration_id != registration.registration_id
            or accepted.sha256 != expected_sha
            or registration.report_bytes != report_bytes
            or not complete_scope or str(view.catalog_revision) != "1"
            or not view.definitions):
        raise RuntimeError("accepted source/report/complete view differs from frozen pins")
    for definition in view.definitions:
        provenance = definition.provenance
        if (provenance.accepted_catalog_revision != "1"
                or provenance.document_revision != document_revision
                or provenance.accepted_document_sha256 != expected_sha):
            raise RuntimeError("catalog view provenance differs from frozen source pins")
    return {"revision": 1, "source_sha256": accepted.sha256,
            "document_revision": accepted.document_revision,
            "source_catalog_revision": accepted.source_catalog_revision,
            "registration_id": accepted.registration_id,
            "report_sha256": sha(report_bytes),
            "complete_view_sha256": sha(view.original_bytes)}


def validate_installation_readback(status, manifest: dict, manifest_sha256: str,
                                   expected_installation_id: str) -> dict:
    actual_layout = {"identity": status.layout_profile.identity,
                     "version": status.layout_profile.version,
                     "sha256": status.layout_profile.sha256}
    result = {"installation_id": status.installation_id,
              "release_id": status.release_id,
              "release_sha256": status.release_sha256,
              "layout_profile": actual_layout}
    if (not status.installed or result["installation_id"] != expected_installation_id
            or result["release_id"] != manifest["releaseId"]
            or result["release_sha256"] != manifest_sha256
            or actual_layout != manifest["layoutProfile"]):
        raise RuntimeError("installation status differs from B identity or pinned runtime manifest")
    return result


def _read(path: Path, limit: int) -> bytes:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        raise ValueError("canonical non-symlink absolute path required")
    with path.open("rb") as f:
        raw = f.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("input byte bound exceeded")
    return raw


def load_config(path: Path = CONFIG) -> tuple[dict, bytes, bytes]:
    config = json.loads(_read(path, 65_536))
    if type(config) is not dict or config.get("profile") != "ashlar-r04-b-install-attempt/0.1":
        raise ValueError("unknown B attempt profile")
    source_cfg, origin_cfg = config["source"], config["origin"]
    source = _read(Path(source_cfg["path"]), MAX_SOURCE)
    origin_path = HERE / origin_cfg["path"]
    origin = _read(origin_path.resolve(), MAX_ORIGIN)
    if origin_path.resolve() != origin_path.absolute() or origin_path.is_symlink():
        raise ValueError("origin must be packet-local canonical file")
    if sha(source) != source_cfg["sha256"] or sha(origin) != origin_cfg["sha256"]:
        raise ValueError("pinned source/origin digest mismatch")
    source_doc, origin_doc = json.loads(source), json.loads(origin)
    if (source_doc.get("id") != source_cfg["document_id"]
            or source_doc.get("umf") != source_cfg["umf_version"]):
        raise ValueError("source identity/version mismatch")
    if "revision" in source_doc and source_doc["revision"] != source_cfg["document_revision"]:
        raise ValueError("source revision disagrees with external revision pin")
    if origin_doc != {"actor": "ashlar-r04-b-source-acceptance",
                      "reason": "independent R04 B source acceptance using the retained original UMF 0.10.0 model bytes"}:
        raise ValueError("origin bytes do not match the exact B candidate")
    return config, source, origin


def prepare(config: dict, source: bytes):
    """Pure public preparation; does not create a database or attempt files."""
    runtime = Path(config["runtime"]["venv"]).resolve()
    if Path(sys.executable).resolve() != (runtime / "bin" / "python").resolve():
        raise RuntimeError("selected isolated Truss runtime interpreter required")
    import importlib.metadata as md
    if md.version("truss-toolkit") != "0.0.49":
        raise RuntimeError("Truss 0.0.49 required")
    from truss import CatalogDocument, prepare_catalog
    doc = CatalogDocument(document_id=config["source"]["document_id"],
                          document_revision=config["source"]["document_revision"],
                          content=source)
    return prepare_catalog((doc,), source_version=config["runtime"]["umf_core_version"])


class Truss049Adapter:
    """Thin caller-owned transaction adapter over Truss 0.0.49 public APIs."""
    def __init__(self, config=None):
        self.runtime = self.connection = self.host = self.assembly = self.tx = None
        self._pending_registration = None
        self.config = config

    @staticmethod
    def _value(outcome):
        from truss import Ok
        if not isinstance(outcome, Ok):
            raise RuntimeError("public Truss operation did not return Ok")
        return outcome.value

    def open(self, pgdata, *, port):
        import importlib.resources as resources
        from truss import LocalPostgres
        import psycopg
        if port != 5432:
            raise ValueError("Truss 0.0.49 LocalPostgres fixes port 5432")
        self.runtime = LocalPostgres(pgdata)
        self.runtime.__enter__()
        info = self.runtime.info
        if info.port != 5432 or not info.server_version.startswith("18.6"):
            raise RuntimeError("unexpected local PostgreSQL runtime profile")
        params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
        self._params = params
        manifest_raw = resources.files("truss").joinpath("_catalog_runtime", "manifest.json").read_bytes()
        expected_manifest = (self.config or {}).get("runtime", {}).get("catalog_manifest_sha256")
        if expected_manifest is None or sha(manifest_raw) != expected_manifest:
            raise RuntimeError("installed catalog manifest differs from the exact B runtime pin")
        self._manifest = json.loads(manifest_raw)
        self._manifest_sha256 = sha(manifest_raw)
        self.connection = psycopg.connect(**params, autocommit=True,
                                          connect_timeout=5,
                                          options="-c statement_timeout=5000")

    def _begin(self, user=None, *, readonly=False):
        from truss import CatalogRuntimeConfiguration, adopt_psycopg_host, create_reference_assembly
        import psycopg
        params = dict(self._params)
        if user:
            params["user"] = user
        params.update(autocommit=False, connect_timeout=5,
                      options="-c statement_timeout=5000")
        self.connection = psycopg.connect(**params)
        self.connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED " +
                                ("READ ONLY" if readonly else "READ WRITE"))
        self.connection.execute("SET LOCAL statement_timeout='5000ms'")
        self.host = adopt_psycopg_host(self.connection, statement_timeout_ms=5000,
                                       operation_timeout_ms=30000)
        self.tx = self._value(self.host.adopt_transaction(
            access_mode="read_only" if readonly else "read_write"))
        self.assembly = self._value(create_reference_assembly(
            CatalogRuntimeConfiguration(), self.host))

    def _finish(self, commit):
        primary = None
        try:
            if self.assembly is not None:
                self.assembly.dispose()
            if self.host is not None and not self.host.detach():
                raise RuntimeError("public Truss host detach refused")
            if commit:
                self.connection.commit()
            else:
                self.connection.rollback()
        except BaseException as error:
            primary = error
            try: self.connection.rollback()
            except BaseException: pass
        finally:
            try: self.connection.close()
            except BaseException: pass
            self.host = self.assembly = self.tx = None
        if primary is not None:
            raise primary

    def bootstrap_roles(self, registrar_login, reader_login):
        import re
        from psycopg import sql
        if any(re.fullmatch(r"[a-z][a-z0-9_]{2,62}", n) is None
               for n in (registrar_login, reader_login)) or registrar_login == reader_login:
            raise ValueError("two distinct bounded B login role names required")
        self.connection.autocommit = False
        self.connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE")
        self.connection.execute("SET LOCAL statement_timeout='5000ms'")
        self.connection.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(registrar_login)))
        self.connection.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(reader_login)))
        self._registrar_login = registrar_login

    def commit(self):
        self._finish(True)

    def install(self, owner, registrar):
        from truss import PendingCatalogInstallation
        self._begin()
        status = self._value(self.assembly.status_in_transaction(self.tx))
        if status.installed:
            raise RuntimeError("installation already exists; one-use attempt refuses replay")
        pending = self._value(self.assembly.install_in_transaction(
            self.tx, owner_role=owner, registrar_role=registrar))
        if not isinstance(pending, PendingCatalogInstallation):
            raise RuntimeError("public install did not return PendingCatalogInstallation")
        self._pending_installation_id = pending.installation_id
        return {"installation_id": pending.installation_id,
                "release_id": pending.release_id,
                "release_sha256": self._manifest_sha256,
                "layout_profile": self._manifest["layoutProfile"]}

    def read_installation(self):
        self._begin(readonly=True)
        status = self._value(self.assembly.status_in_transaction(self.tx))
        value = validate_installation_readback(status, self._manifest,
                                               self._manifest_sha256,
                                               self._pending_installation_id)
        self._finish(False)
        return value

    def grant_registrar(self, role):
        from psycopg import sql
        self._begin()
        self.connection.execute(sql.SQL("GRANT {} TO {} WITH SET FALSE").format(
            sql.Identifier(role), sql.Identifier(self._registrar_login)))

    def accept(self, prepared, source, origin, request_key, document_id):
        from truss import CatalogDocumentExpectation, CatalogRegistration
        self._begin(user=self._registrar_login)
        registration = self._value(self.assembly.accept_in_transaction(
            self.tx, prepared, request_key=request_key, origin_bytes=origin,
            expected_revision=0,
            expected_documents=(CatalogDocumentExpectation(
                document_id=document_id, source_catalog_revision=0),)))
        if not isinstance(registration, CatalogRegistration) or registration.replayed or registration.revision != 1:
            raise RuntimeError("public source acceptance did not create exactly revision 1")
        self._pending_registration = registration
        self._accepted_source_bytes = source
        return {"registration_id": registration.registration_id,
                "revision": registration.revision,
                "source_sha256": sha(source),
                "report_sha256": sha(registration.report_bytes)}

    def read_acceptance(self, source, document_id, document_revision, expected_source_catalog_revision):
        from dataclasses import replace
        from truss import CatalogViewAvailable, CompleteCatalogScope, HistoricalCatalogSelection
        self._begin(user=self._registrar_login, readonly=True)
        reg = self._pending_registration
        status = self._value(self.assembly.status_in_transaction(self.tx))
        report = self._value(self.assembly.report_in_transaction(self.tx, 1))
        accepted = self._value(self.assembly.accepted_document_in_transaction(
            self.tx, document_id=document_id, maximum_bytes=MAX_SOURCE))
        request = replace(self.assembly.catalog_view_request(
            definition_count="65536", encoded_bytes="16777216"),
            selection=HistoricalCatalogSelection(catalog_revision="1"))
        view = self._value(self.assembly.catalog_view_in_transaction(self.tx, request))
        if (not isinstance(view, CatalogViewAvailable)
                or not isinstance(view.view.scope, CompleteCatalogScope)
                or str(view.view.catalog_revision) != "1"):
            raise RuntimeError("complete catalog-view readback unavailable")
        if accepted.content != self._accepted_source_bytes:
            raise RuntimeError("accepted bytes differ from originally prepared source")
        result = validate_accepted_observations(
            status_revision=status.revision, accepted=accepted,
            report_bytes=report.original_bytes, registration=reg, view=view.view,
            source=source, document_id=document_id, document_revision=document_revision,
            expected_source_catalog_revision=expected_source_catalog_revision,
            complete_scope=isinstance(view.view.scope, CompleteCatalogScope))
        self._finish(False)
        return result

    def register_consumer(self, consumer_id, reader_role):
        self._begin()
        result = self._value(self.assembly.register_feed_consumer_in_transaction(
            self.tx, consumer_id=consumer_id, reader_role=reader_role))
        return genesis_cursor_evidence(result, consumer_id,
                                       self._pending_installation_id)

    def close(self):
        cleanup_error = None
        # On any pre-commit failure detach the borrowed host before stopping the
        # LocalPostgres owner. Roll back the caller transaction while the host is
        # still alive, and preserve PGDATA regardless of cleanup outcome.
        if self.assembly is not None:
            try: self.assembly.dispose()
            except BaseException as error: cleanup_error = error
            self.assembly = None
        if self.host is not None:
            try:
                if not self.host.detach():
                    raise RuntimeError("public Truss host detach refused during cleanup")
            except BaseException as error:
                cleanup_error = cleanup_error or error
            self.host = self.tx = None
        if self.connection is not None:
            try:
                try: self.connection.rollback()
                except BaseException: pass
                self.connection.close()
            except BaseException as error:
                cleanup_error = cleanup_error or error
            finally: self.connection = None
        if self.runtime is not None:
            runtime, self.runtime = self.runtime, None
            try: runtime.__exit__(None, None, None)
            except BaseException as error: cleanup_error = cleanup_error or error
        if cleanup_error is not None:
            raise cleanup_error


class UncertainAttempt(RuntimeError):
    """A commit may have settled; caller must preserve PGDATA and never retry."""


def _ordered_attempt(adapter, config: dict, prepared, source: bytes, origin: bytes,
                     mark, *, clock=time.monotonic):
    """Run one ordered attempt through a narrow host adapter; never retries.

    Adapter methods own only public Truss calls/transaction boundaries. Every
    write is preceded by an fsynced marker. Any commit exception is uncertain.
    """
    started = clock()
    deadline = started + config["bounds"]["worker_deadline_seconds"]
    def check():
        if clock() >= deadline:
            raise TimeoutError("B attempt deadline exceeded")
    if config["execution"]["approval"] is None:
        raise PermissionError("execution approval is null")
    install = config["installation"]
    required = ("root_path", "pgdata_path", "attempt_path", "owner_role", "registrar_role",
                "registrar_login", "reader_login", "consumer_id", "request_key",
                "exclusive_port_admission", "retention_owner")
    if any(install.get(k) is None for k in required) or not config["scope"]["arras_window_released"]:
        raise PermissionError("B-owned execution pins or exclusive 5432 admission are incomplete")
    check()
    mark("preparation_verified", {"source_sha256": sha(source), "origin_sha256": sha(origin)})
    if isinstance(adapter, Truss049Adapter):
        adapter.config = config
    adapter.open(install["pgdata_path"], port=5432)
    mark("postgres_started", {"port": 5432, "pgdata_path": install["pgdata_path"]})
    check()
    mark("bootstrap_commit_started", {})
    try:
        adapter.bootstrap_roles(install["registrar_login"], install["reader_login"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("bootstrap transaction settlement uncertain") from exc
    mark("bootstrap_committed", {})
    check()
    mark("install_commit_started", {})
    try:
        install_result = adapter.install(install["owner_role"], install["registrar_role"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("catalog install settlement uncertain") from exc
    mark("install_committed", {"installation_id": install_result["installation_id"]})
    observed_install = adapter.read_installation()
    if observed_install != install_result:
        raise RuntimeError("installation readback differs; retain PGDATA")
    mark("install_readback", observed_install)
    mark("registrar_grant_commit_started", {})
    try:
        adapter.grant_registrar(install["registrar_role"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("registrar grant settlement uncertain") from exc
    mark("registrar_grant_committed", {})
    check()
    mark("accept_commit_started", {"request_key": install["request_key"]})
    try:
        acceptance = adapter.accept(prepared, source, origin, install["request_key"],
                                    config["source"]["document_id"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("source acceptance settlement uncertain") from exc
    mark("accept_committed", acceptance)
    accepted = adapter.read_acceptance(
        source, config["source"]["document_id"], config["source"]["document_revision"],
        install["expected_document_source_revision"])
    if accepted.get("source_sha256") != sha(source) or accepted.get("revision") != 1:
        raise RuntimeError("accepted source readback mismatch; retain PGDATA")
    if (accepted.get("document_revision") != config["source"]["document_revision"]
            or accepted.get("source_catalog_revision") != install["expected_document_source_revision"]
            or accepted.get("registration_id") != acceptance.get("registration_id")
            or accepted.get("report_sha256") != acceptance.get("report_sha256")):
        raise RuntimeError("accepted source/report readback does not match frozen pins")
    mark("accept_readback", accepted)
    check()
    mark("consumer_commit_started", {"consumer_id": install["consumer_id"]})
    try:
        consumer = adapter.register_consumer(install["consumer_id"], install["reader_login"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("consumer registration settlement uncertain") from exc
    # Public Truss 0.0.49 returns the provider registration ID and exact genesis
    # cursor, but has no independent consumer-registration readback operation.
    if consumer.get("installation_id") != install_result.get("installation_id"):
        raise RuntimeError("genesis cursor installation identity differs from installed B")
    mark("consumer_registration_committed", consumer)
    check()
    adapter.close()
    mark("terminal_stopped", {"pgdata_retained": True, "feed_reads": 0, "acks": 0})
    return {"status": "installed_accepted_genesis_registered_stopped", "consumer": consumer}


def ordered_attempt(adapter, config: dict, prepared, source: bytes, origin: bytes,
                    mark, *, clock=time.monotonic):
    """Ensure any opened host is closed after failure; never delete its PGDATA."""
    try:
        return _ordered_attempt(adapter, config, prepared, source, origin, mark, clock=clock)
    except BaseException:
        try:
            adapter.close()
        except BaseException:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.preflight == args.execute:
        parser.error("select exactly one mode")
    config, source, origin = load_config()
    if args.preflight:
        print(json.dumps({"profile": "ashlar-r04-b-worker-preflight/0.1",
                          "source_sha256": sha(source), "origin_sha256": sha(origin),
                          "pg_started": False, "spark_started": False,
                          "acceptance_invoked": False, "consumer_registered": False,
                          "execution_approval": config["execution"]["approval"]}, sort_keys=True))
        return 0
    raise PermissionError("No executable attempt adapter is enabled by this draft packet")


if __name__ == "__main__":
    raise SystemExit(main())
