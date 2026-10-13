"""Configure existing GraphFrames export inputs without loading an engine."""
import hashlib,os
from pathlib import Path
from .delta_custody import encoded
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes,require_custody_digest
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export
from .lifecycle import owned_context


def run_configure_graphframes_export_command(export_report,export_report_sha256,output,*,jars,java_home):
    """Preserve an explicitly selected report's digests; never infer native grants."""
    require_custody_digest(export_report_sha256)
    def path(value,directory=False):
        if type(value)is not str:raise ValueError('Canonical existing input path required')
        selected=Path(value)
        if (not selected.is_absolute()or selected.resolve()!=selected or not(selected.is_dir()if directory else selected.is_file())
                or any(char in value for char in ('\0','\n','\r',','))):raise ValueError('Canonical existing input path required')
        return selected
    report_path=path(export_report);raw=snapshot(report_path,1048576)
    if hashlib.sha256(raw).hexdigest()!=export_report_sha256:raise ValueError('Selected original export report digest differs')
    report=decode_custody_bytes(raw)
    if type(report)is not dict:raise ValueError('Original graph export report required')
    if type(report.get('release'))is dict and type(report.get('custody'))is dict:
        release=report['release'];custody=report['custody']
        if set(release)!={'path','sha256'}or set(custody)!={'path','sha256'}:raise ValueError('Exact original graph pair selectors required')
        release_path=path(release['path']);custody_path=path(custody['path']);release_sha=release['sha256'];custody_sha=custody['sha256']
    elif {'release','release_sha256','custody','custody_sha256'}<=set(report):
        release_path=path(report['release']);custody_path=path(report['custody']);release_sha=report['release_sha256'];custody_sha=report['custody_sha256']
    else:raise ValueError('Standalone Truss or finite graph export report required')
    require_custody_digest(release_sha);require_custody_digest(custody_sha)
    if type(jars)not in (tuple,list)or len(jars)!=2:raise ValueError('Two explicit existing GraphFrames JARs required')
    selected_jars=tuple(path(value)for value in jars);java=path(java_home,True)
    if {item.name for item in selected_jars}!={'io.graphframes_graphframes-spark3_2.12-0.12.3.jar','io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar'}:
        raise ValueError('GraphFrames0.12.3 Spark3 JARs required')
    destination=Path(output)
    if (not destination.is_absolute()or not destination.parent.is_dir()or destination.parent.resolve()!=destination.parent
            or destination.exists()or destination.is_symlink()):raise ValueError('Fresh canonical graph configuration output required')
    protected=(report_path,release_path,custody_path,java,*selected_jars)
    if any(destination==item or destination in item.parents or item in destination.parents for item in protected):
        raise ValueError('Graph configuration must be separate from original inputs')
    selection=LocalGraphExportSelection(release_path,custody_path,release_sha,custody_sha)
    with owned_context(open_local_graph_export(selection))as owner:
        with owned_context(owner.writer(owner.context)):
            owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
    value={'release_path':str(release_path),'custody_path':str(custody_path),'release_sha256':release_sha,'custody_sha256':custody_sha,
        'jars':[str(item)for item in selected_jars],'java_home':str(java)}
    payload=(encoded(value)+'\n').encode();descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(payload)!=len(payload):raise OSError('Complete private graph configuration write required')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination)}
