# SPDX-License-Identifier: MIT OR Apache-2.0
"""Compose explicit ordinary medical ports without opening or provisioning sessions."""
import os
from dataclasses import replace
from .original_medical_local_configuration import OriginalMedicalLocalSource
from .medical_publication_sources import OriginalMedicalSourcePorts
from .source_sessions import OutboxSourceRegistration
from .existing_outbox_authority import OutboxAuthorityRegistration


def original_medical_local_source_ports(selection):
    """Return inert ordinary transports; acquired session owners verify authority."""
    if type(selection) is not OriginalMedicalLocalSource:
        raise ValueError('Exact isolated ordinary medical source selection required')
    selection.__post_init__()
    original = replace(selection, scope=replace(selection.scope))

    def connect(role):
        selection.__post_init__()
        if selection != original:
            raise PermissionError('Original ordinary medical connection selection changed')
        if any(name.startswith('PG') for name in os.environ):
            raise PermissionError('Ambient PostgreSQL settings refused for isolated ordinary login')
        import psycopg
        return psycopg.connect(
            host=selection.host, hostaddr=selection.host, port=selection.port,
            dbname=selection.database, user=role, password='', passfile='/dev/null',
            sslmode='disable', gssencmode='disable',
            options=('-c statement_timeout='+str(selection.statement_ms)
                     +' -c lock_timeout='+str(selection.lock_ms)),
            connect_timeout=selection.connect_seconds, autocommit=False)

    source = OutboxSourceRegistration(
        selection.scope, selection.source_schema, selection.source_signature_sha256,
        lambda context: connect(selection.reader_role))
    registration = OutboxAuthorityRegistration(
        source, selection.reader_role, selection.ack_role, selection.database,
        lambda context: connect(selection.ack_role))
    return OriginalMedicalSourcePorts(
        registration, selection.writer_role, lambda context: connect(selection.writer_role))
