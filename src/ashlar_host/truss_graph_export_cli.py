"""Export an explicitly selected existing Truss graph after owned resource close."""
import hashlib
import json
import os
from pathlib import Path
from .delta_custody import encoded
from .lifecycle import owned_context
from .truss_graph_export import read_original_truss_graph_export
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import parse_resume_truss_configuration,run_selected_truss_command


def _write_original(path,payload):
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(payload)!=len(payload):raise OSError('Complete original graph export write required')
        stream.flush();os.fsync(stream.fileno())


def run_export_truss_graph_command(configuration,output):
    """Capture an immutable pair; never ACK, publish, or erase partial output.

    Companion files retain exact original bytes. The success report is written
    last, after transport, source intervals, Spark and lease closure.
    """
    destination=Path(output)
    release_path=destination.with_name(destination.name+'-release.json')
    custody_path=destination.with_name(destination.name+'-custody.json')
    for path in (destination,release_path,custody_path):
        parse_resume_truss_configuration(configuration,str(path))
    def capture(driver,config):
        with owned_context(driver.writer(driver.source_sessions.batch().feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        return read_original_truss_graph_export(driver,with_custody=True)
    def write(pair,selected):
        if selected!=destination:raise ValueError('Original selected graph export output differs')
        release,custody=pair
        if type(release.payload)is not bytes or type(custody)is not bytes:
            raise ValueError('Original immutable graph export bytes required')
        _write_original(release_path,release.payload)
        _write_original(custody_path,custody)
        graph=json.loads(release.payload)
        report={'publication_id':graph['publication']['publication_id'],
            'release':{'path':str(release_path),'sha256':hashlib.sha256(release.payload).hexdigest()},
            'custody':{'path':str(custody_path),'sha256':hashlib.sha256(custody).hexdigest()},
            'nodes':len(graph['nodes']),'edges':len(graph['edges']),
            'qualification':'Immutable original Truss graph captured under live source admission and complete publication pins; caller-asserted checkpoint qualification, no protected outbox ACK or external engine execution.'}
        _write_original(destination,(encoded(report)+'\n').encode())
        return report
    return run_selected_truss_command(configuration,output,capture,report_writer=write)
