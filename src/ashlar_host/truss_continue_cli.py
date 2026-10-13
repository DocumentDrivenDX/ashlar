"""Continue a selected Truss publication using existing local custody."""
from .lifecycle import owned_context
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import run_selected_truss_command


def run_continue_truss_graph_command(configuration,output,*,maximum_pages=1,wait_seconds=0):
    if (type(maximum_pages)is not int or not 1<=maximum_pages<=8
            or type(wait_seconds)is not int or not 0<=wait_seconds<=60):
        raise ValueError('Explicit bounded continuation pages and wait required')
    def advance(driver,config):
        import psycopg
        from .truss_continue import continue_truss_graph
        with owned_context(driver.writer(driver.source_sessions.batch().feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        def connection():
            return psycopg.connect(config['connection_uri'],autocommit=False,connect_timeout=5)
        from .truss_catalog_reader import parse_archived_acceptance_files
        return continue_truss_graph(driver,connection,maximum_pages=maximum_pages,
            wait_seconds=wait_seconds,archives=parse_archived_acceptance_files(config.get('archived_acceptance_files',[])))
    return run_selected_truss_command(configuration,output,advance)
