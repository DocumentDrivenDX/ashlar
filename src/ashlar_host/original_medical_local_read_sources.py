# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit ordinary medical SELECT registrations; no source writer port."""
import os
from dataclasses import replace
from .original_medical_local_configuration import OriginalMedicalLocalSource
from .source_sessions import OutboxSourceRegistration
from .existing_outbox_authority import OutboxAuthorityRegistration
from .medical_read_sources import MedicalReadSourceFactory


def original_medical_local_read_sources(selection):
    if type(selection) is not OriginalMedicalLocalSource:
        raise ValueError('Exact isolated ordinary medical source selection required')
    selection.__post_init__()
    original=replace(selection,scope=replace(selection.scope))

    def connect(role):
        selection.__post_init__()
        if selection!=original:
            raise PermissionError('Original ordinary medical read selection changed')
        if role not in (original.reader_role,original.ack_role):
            raise PermissionError('Read-only ordinary role required')
        if any(name.startswith('PG') for name in os.environ):
            raise PermissionError('Ambient PostgreSQL settings refused for isolated ordinary login')
        import psycopg
        return psycopg.connect(host=selection.host,hostaddr=selection.host,port=selection.port,
            dbname=selection.database,user=role,password='',passfile='/dev/null',
            sslmode='disable',gssencmode='disable',options=('-c statement_timeout='+str(selection.statement_ms)
                +' -c lock_timeout='+str(selection.lock_ms)),connect_timeout=selection.connect_seconds,autocommit=False)

    source=OutboxSourceRegistration(original.scope,original.source_schema,original.source_signature_sha256,
        lambda context:connect(original.reader_role))
    return MedicalReadSourceFactory(OutboxAuthorityRegistration(source,original.reader_role,
        original.ack_role,original.database,lambda context:connect(original.ack_role)))
