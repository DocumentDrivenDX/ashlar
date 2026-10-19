# SPDX-License-Identifier: MIT OR Apache-2.0
"""Configure existing GraphFrames export inputs without loading an engine."""
import os
from pathlib import Path
from .delta_custody import encoded
from .private_graph_custody import require_custody_digest
from .local_graph_export import open_local_graph_export
from .lifecycle import owned_context,finish
from .graph_export_setup import graph_export_selection_from_report,GraphExportReportFile


def run_configure_graphframes_export_command(export_report,export_report_sha256,output,*,jars,java_home):
    """Preserve an explicitly selected report's digests; never infer native grants."""
    require_custody_digest(export_report_sha256)
    def path(value,directory=False):
        if type(value)is not str:raise ValueError('Canonical existing input path required')
        selected=Path(value)
        if (not selected.is_absolute()or selected.resolve()!=selected or not(selected.is_dir()if directory else selected.is_file())
                or any(char in value for char in ('\0','\n','\r',','))):raise ValueError('Canonical existing input path required')
        return selected
    report_path=path(export_report);report_file=GraphExportReportFile.read(report_path)
    selection=graph_export_selection_from_report(report_file.raw,export_report_sha256)
    release_path=selection.release_path;custody_path=selection.custody_path
    release_sha=selection.release_sha256;custody_sha=selection.custody_sha256
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
    primary=None
    try:
        report_file.renew()
        with owned_context(open_local_graph_export(selection))as owner:
            with owned_context(owner.writer(owner.context)):
                owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
    except BaseException as error:primary=error
    finish(primary,[lambda:report_file.renew()])
    value={'release_path':str(release_path),'custody_path':str(custody_path),'release_sha256':release_sha,'custody_sha256':custody_sha,
        'jars':[str(item)for item in selected_jars],'java_home':str(java)}
    payload=(encoded(value)+'\n').encode();descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(payload)!=len(payload):raise OSError('Complete private graph configuration write required')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination)}
