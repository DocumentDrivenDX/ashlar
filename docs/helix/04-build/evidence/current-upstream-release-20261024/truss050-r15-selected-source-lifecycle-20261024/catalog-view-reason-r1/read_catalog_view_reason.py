#!/usr/bin/env python3
"""Read one retained .48 catalog view and emit only availability/reason metadata."""
from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
import sys
from pathlib import Path

CLASSIFIER = Path('/Users/erik/Projects/ashlar/docs/helix/04-build/evidence/current-upstream-release-20261024/truss050-r15-selected-source-lifecycle-20261024/diagnostic-candidate/catalog_view_diagnostic.py')
CLASSIFIER_SHA256 = '04a90b13550a757a2e62c1204cd1a73938c136daee1ad962e19f9c21262f0e1a'
REGISTRAR = 'ashlar048_accept_consumer_r1'


def load_classifier():
    digest = hashlib.sha256(CLASSIFIER.read_bytes()).hexdigest()
    if digest != CLASSIFIER_SHA256:
        raise RuntimeError('Pinned diagnostic classifier changed')
    spec = importlib.util.spec_from_file_location('ashlar_catalog_view_diagnostic', CLASSIFIER)
    if spec is None or spec.loader is None:
        raise RuntimeError('Pinned diagnostic classifier cannot be loaded')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.require_catalog_view_bytes


def capture() -> dict[str, object]:
    require_catalog_view_bytes = load_classifier()
    import psycopg
    from truss import (
        CatalogRuntimeConfiguration,
        CatalogViewAvailable,
        CatalogViewUnavailable,
        Ok,
        adopt_psycopg_host,
        create_reference_assembly,
    )
    if importlib.metadata.version('truss') != '0.0.48':
        raise RuntimeError('Exact Truss 0.0.48 helper runtime required')

    conninfo = os.environ.get('ASHLAR_LOCAL_PG_CONNINFO')
    if not conninfo:
        raise RuntimeError('Local PostgreSQL connection information is required')
    params = psycopg.conninfo.conninfo_to_dict(conninfo)
    if 'password' in params:
        raise RuntimeError('Passwordless retained local connection required')
    params['user'] = REGISTRAR
    connection = psycopg.connect(
        **params, connect_timeout=5, options='-c statement_timeout=2000'
    )
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
        if isinstance(result.value, CatalogViewUnavailable):
            try:
                require_catalog_view_bytes(result.value)
            except RuntimeError as error:
                return {
                    'outcome': 'unavailable',
                    'reason': result.value.reason,
                    'diagnostic': str(error),
                }
            raise RuntimeError('Unavailable catalog result unexpectedly produced bytes')
        if isinstance(result.value, CatalogViewAvailable):
            raw = require_catalog_view_bytes(result.value)
            return {
                'outcome': 'available',
                'bytes': len(raw),
                'sha256': hashlib.sha256(raw).hexdigest(),
            }
        raise RuntimeError('Unexpected Truss catalog-view result type')
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
        result = capture()
    except BaseException as error:
        result = {'outcome': 'error', 'error_type': type(error).__name__}
        print(json.dumps(result, sort_keys=True, separators=(',', ':')))
        raise SystemExit(1)
    print(json.dumps(result, sort_keys=True, separators=(',', ':')))
