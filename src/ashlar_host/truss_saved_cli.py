"""Explicit first publication of an archived selection without a retained attempt."""
from .lifecycle import owned_context
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import run_selected_truss_command


def run_publish_saved_truss_graph_command(configuration,publication_intent,publication_intent_sha256,output):
    def publish(driver,config):
        import psycopg
        from .truss_saved_publication import publish_saved_truss_selection
        from .truss_catalog_reader import parse_archived_acceptance_files
        with owned_context(driver.writer(driver.source_sessions.batch().feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        def connection():
            return psycopg.connect(config['connection_uri'],autocommit=False,connect_timeout=5)
        descriptor=publish_saved_truss_selection(driver,connection,publication_intent,publication_intent_sha256,
            archives=parse_archived_acceptance_files(config.get('archived_acceptance_files',[])))
        return {'manifest':dict(descriptor.raw),'qualification':'First publication of the exact saved Truss page after original predecessor and current source admission; caller-asserted delivery ACK, no protected outbox or Unity Catalog qualification.'}
    return run_selected_truss_command(configuration,output,publish)
