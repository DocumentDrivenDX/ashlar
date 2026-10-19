# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact carrier-cell comparison of two independently selected local exports."""
import json,os
from pathlib import Path
from .evolution_cli import InvocationFile
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export
from .private_graph_custody import decode_custody_bytes,require_custody_digest
from .lifecycle import owned_context,finish

MAX_REPORT_BYTES=16777216


def parse_graph_export_selection(raw):
    if type(raw)is not bytes or not 0<len(raw)<=65536:raise ValueError('Bounded export selection configuration required')
    value=decode_custody_bytes(raw)
    if type(value)is not dict or set(value)!={'release_path','custody_path','release_sha256','custody_sha256'}:
        raise ValueError('Exact four-field local export selection required')
    for key in ('release_sha256','custody_sha256'):require_custody_digest(value[key])
    paths=[]
    for key in ('release_path','custody_path'):
        if type(value[key])is not str:raise ValueError('Explicit canonical export path required')
        path=Path(value[key])
        if not path.is_absolute()or path.resolve()!=path:raise ValueError('Explicit canonical export path required')
        paths.append(path)
    return LocalGraphExportSelection(*paths,value['release_sha256'],value['custody_sha256'])


def _bytes(value):
    result=bytearray()
    for part in json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).iterencode(value):
        block=part.encode('utf8')
        if len(result)+len(block)+1>MAX_REPORT_BYTES:raise ValueError('Complete export comparison report exceeds16MiB')
        result.extend(block)
    result.extend(b'\n');return bytes(result)


def _changes(first,second):
    def inventory(rows):
        if type(rows)is not list or len(rows)>10000:raise ValueError('Finite complete export row inventory required')
        result={}
        for row in rows:
            if type(row)is not dict or type(row.get('graph_id'))is not str or not row['graph_id']or row['graph_id']in result:
                raise ValueError('Distinct canonical export graph IDs required')
            if any(type(cell)is not str and cell is not None for cell in row.values()):raise ValueError('Original STRING/null carriers required')
            result[row['graph_id']]=row
        return result
    left=inventory(first);right=inventory(second);changed=[]
    for identity in sorted(left.keys()&right.keys()):
        before=left[identity];after=right[identity]
        columns=[name for name in sorted(before.keys()|after.keys())if name not in before or name not in after or before[name]!=after[name]]
        if columns:changed.append({'graph_id':identity,'changed_columns':columns,'before':dict(before),'after':dict(after)})
    return {'added':[dict(right[key])for key in sorted(right.keys()-left.keys())],
        'removed':[dict(left[key])for key in sorted(left.keys()-right.keys())],'changed':changed}


def compare_graph_exports(first,second):
    """Close both actual file-use owners before returning a complete comparison."""
    if type(first)is not LocalGraphExportSelection or type(second)is not LocalGraphExportSelection:
        raise ValueError('Two explicit nominal local export selections required')
    with owned_context(open_local_graph_export(first))as left:
        with owned_context(open_local_graph_export(second))as right:
            with owned_context(left.writer(left.context)):
                with owned_context(right.writer(right.context)):
                    def admit(owner):owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
                    primary=None;result=None
                    try:
                        admit(left);admit(right)
                        before=decode_custody_bytes(left.release.payload);after=decode_custody_bytes(right.release.payload)
                        result={'profile':'ashlar-local-graph-export-diff/0.1','first_publication_id':before['publication']['publication_id'],
                            'second_publication_id':after['publication']['publication_id'],
                            'first_release_sha256':first.release_sha256,'second_release_sha256':second.release_sha256,
                            'nodes':_changes(before['nodes'],after['nodes']),'edges':_changes(before['edges'],after['edges']),
                            'qualification':'Exact graph-ID and opaque STRING/null carrier comparison of two selected immutable local export pairs. No property-value interpretation, ancestry guarantee, current source, native engine or release authority.'}
                        _bytes(result)
                    except BaseException as error:primary=error
                    finish(primary,[lambda:admit(left),lambda:admit(right)])
    return result


def run_compare_graph_exports_command(first_configuration,second_configuration,output):
    files=(InvocationFile.read(first_configuration,65536),InvocationFile.read(second_configuration,65536))
    selections=tuple(parse_graph_export_selection(file.raw)for file in files)
    destination=Path(output)
    if (not destination.is_absolute()or not destination.parent.is_dir()or destination.parent.resolve()!=destination.parent
            or destination.exists()or destination.is_symlink()):raise ValueError('Fresh canonical private comparison report required')
    protected=tuple(file.path for file in files)+tuple(path for item in selections for path in (item.release_path,item.custody_path))
    if any(destination==path or destination in path.parents or path in destination.parents for path in protected):
        raise ValueError('Comparison output must be separate from selected inputs')
    primary=None;result=None
    try:
        for file in files:file.renew(65536)
        result=compare_graph_exports(*selections)
    except BaseException as error:primary=error
    finish(primary,[lambda file=file:file.renew(65536)for file in files])
    raw=_bytes(result);descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(raw)!=len(raw):raise OSError('Complete comparison report write required')
        stream.flush();os.fsync(stream.fileno())
    return result
