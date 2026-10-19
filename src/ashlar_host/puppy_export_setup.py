# SPDX-License-Identifier: MIT OR Apache-2.0
"""Configure the existing immutable PuppyGraph command without an engine."""
import json,os,stat
from pathlib import Path
from .evolution_cli import InvocationFile
from .graph_export_setup import GraphExportReportFile,graph_export_selection_from_report
from .local_graph_export import open_local_graph_export
from .lifecycle import owned_context,finish
from .private_graph_custody import decode_custody_bytes
from .puppy_export_cli import PROFILE,GREMLIN_PROFILE,parse_puppy_export_configuration


def run_configure_puppy_export_command(export_report,export_report_sha256,operator_configuration,output):
    """Preserve original report selectors and explicit private operator choices."""
    report=GraphExportReportFile.read(export_report)
    selection=graph_export_selection_from_report(report.raw,export_report_sha256)
    operator=InvocationFile.read(operator_configuration,65536)
    if stat.S_IMODE(operator.path.stat().st_mode)&0o077:raise ValueError('Private operator configuration required')
    value=decode_custody_bytes(operator.raw)
    keys={'profile','output','docker','docker_host','image','name','username','password','http_port','bolt_port','deadline_seconds'}
    gremlin=type(value)is dict and value.get('profile')==GREMLIN_PROFILE
    if type(value)is not dict or set(value)!=keys|({'language','gremlin_port'}if gremlin else set())or value.get('profile')not in (PROFILE,GREMLIN_PROFILE):
        raise ValueError('Closed explicit operator PuppyGraph configuration required')
    merged={**value,'release_path':str(selection.release_path),'custody_path':str(selection.custody_path),
        'release_sha256':selection.release_sha256,'custody_sha256':selection.custody_sha256}
    payload=(json.dumps(merged,ensure_ascii=False,separators=(',',':'))+'\n').encode()
    _,runtime_output,_=parse_puppy_export_configuration(payload)
    destination=Path(output)
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink():
        raise ValueError('Fresh canonical private configuration output required')
    protected=(report.path,operator.path,selection.release_path,selection.custody_path,runtime_output)
    if any(destination==path or destination in path.parents or path in destination.parents for path in protected):
        raise ValueError('Configuration output must be separate from original inputs and runtime output')
    primary=None
    try:
        report.renew();operator.renew(65536)
        with owned_context(open_local_graph_export(selection))as owner:
            with owned_context(owner.writer(owner.context)):
                owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
    except BaseException as error:primary=error
    finish(primary,[report.renew,lambda:operator.renew(65536)])
    descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(payload)!=len(payload):raise OSError('Complete private PuppyGraph configuration write required')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination)}
