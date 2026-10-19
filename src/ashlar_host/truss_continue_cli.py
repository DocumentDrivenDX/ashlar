# SPDX-License-Identifier: MIT OR Apache-2.0
"""Continue a selected Truss publication using existing local custody."""
from .lifecycle import owned_context,finish
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import run_selected_truss_command


def run_continue_truss_graph_command(configuration,output,*,maximum_pages=1,wait_seconds=0,maximum_page_bytes=1048576):
    if (type(maximum_pages)is not int or not 1<=maximum_pages<=8
            or type(wait_seconds)is not int or not 0<=wait_seconds<=60
            or type(maximum_page_bytes)is not int or not 1<=maximum_page_bytes<=2097152):
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
            wait_seconds=wait_seconds,maximum_page_bytes=maximum_page_bytes,archives=parse_archived_acceptance_files(config.get('archived_acceptance_files',[])))
    try:
        return run_selected_truss_command(configuration,output,advance)
    except BaseException as error:
        capture=getattr(error,'ashlar_truss_continuation_interruption',None)
        if capture is not None:
            capture={**capture,'owners_cleanup_failed':getattr(error,'cleanup_failed',False)is True}
            # The shared owner has attempted cleanup. Only an actual body failure
            # carrying its saved selection gets a private interruption report.
            def report():
                import os
                from .delta_custody import encoded
                raw=(encoded(capture)+'\n').encode()
                descriptor=os.open(output,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                with os.fdopen(descriptor,'wb')as stream:
                    if stream.write(raw)!=len(raw):raise OSError('Complete private interruption report required')
                    stream.flush();os.fsync(stream.fileno())
            finish(error,[report])
        raise

