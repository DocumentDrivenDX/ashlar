#!/usr/bin/env python3
"""Source-only corrected helper candidate for the one-view R15 diagnostic."""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path

CLASSIFIER = Path('/Users/erik/Projects/ashlar/docs/helix/04-build/evidence/current-upstream-release-20261024/truss050-r15-selected-source-lifecycle-20261024/diagnostic-candidate/catalog_view_diagnostic.py')
CLASSIFIER_SHA256 = '04a90b13550a757a2e62c1204cd1a73938c136daee1ad962e19f9c21262f0e1a'
REGISTRAR = 'ashlar048_accept_consumer_r1'
EXPECTED_DISTRIBUTION = 'truss-toolkit'
EXPECTED_VERSION = '0.0.48'


def load_classifier():
    if hashlib.sha256(CLASSIFIER.read_bytes()).hexdigest() != CLASSIFIER_SHA256:
        raise RuntimeError('Pinned diagnostic classifier changed')
    spec = importlib.util.spec_from_file_location('ashlar_catalog_view_diagnostic', CLASSIFIER)
    if spec is None or spec.loader is None:
        raise RuntimeError('Pinned diagnostic classifier cannot be loaded')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.require_catalog_view_bytes


def verify_runtime() -> None:
    if importlib.metadata.version(EXPECTED_DISTRIBUTION) != EXPECTED_VERSION:
        raise RuntimeError('Exact Truss 0.0.48 distribution required')


def safe_result(value) -> dict[str, object]:
    from truss import CatalogViewAvailable, CatalogViewUnavailable

    require_catalog_view_bytes = load_classifier()
    if isinstance(value, CatalogViewUnavailable):
        return {'outcome': 'unavailable', 'reason': value.reason}
    if isinstance(value, CatalogViewAvailable):
        raw = require_catalog_view_bytes(value)
        return {'outcome': 'available', 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
    raise RuntimeError('Unexpected Truss catalog-view result type')


def safe_error(error: BaseException) -> dict[str, str]:
    return {'outcome': 'error', 'error_type': type(error).__name__}


def capture(connection_factory=None) -> dict[str, object]:
    import psycopg
    from truss import (
        CatalogRuntimeConfiguration,
        Ok,
        adopt_psycopg_host,
        create_reference_assembly,
    )

    verify_runtime()
    conninfo = os.environ.get('ASHLAR_LOCAL_PG_CONNINFO')
    if not conninfo:
        raise RuntimeError('Local PostgreSQL connection information is required')
    params = psycopg.conninfo.conninfo_to_dict(conninfo)
    if 'password' in params:
        raise RuntimeError('Passwordless retained local connection required')
    params['user'] = REGISTRAR
    connect = connection_factory or psycopg.connect
    connection = connect(**params, connect_timeout=5, options='-c statement_timeout=2000')
    host = assembly = None
    try:
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')
        connection.execute("SET LOCAL statement_timeout='2000ms'")
        host = adopt_psycopg_host(connection)
        transaction = host.adopt_transaction(access_mode='read_only')
        built = create_reference_assembly(CatalogRuntimeConfiguration(), host)
        if not isinstance(transaction, Ok) or not isinstance(built, Ok):
            raise RuntimeError('Read-only Truss catalog assembly refused')
        assembly = built.value
        request = assembly.catalog_view_request(
            definition_count='65536', encoded_bytes='16777216'
        )
        result = assembly.catalog_view_in_transaction(transaction.value, request)
        if not isinstance(result, Ok):
            raise RuntimeError('Read-only catalog-view operation refused')
        return safe_result(result.value)
    finally:
        try:
            if assembly is not None:
                assembly.dispose()
        finally:
            try:
                if host is not None and not host.detach():
                    raise RuntimeError('Read-only Truss host detach refused')
            finally:
                try:
                    connection.rollback()
                finally:
                    connection.close()


if __name__ == '__main__':
    try:
        output = capture()
    except BaseException as error:
        output = safe_error(error)
        print(json.dumps(output, sort_keys=True, separators=(',', ':')))
        raise SystemExit(1)
    print(json.dumps(output, sort_keys=True, separators=(',', ':')))
