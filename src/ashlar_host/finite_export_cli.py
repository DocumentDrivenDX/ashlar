# SPDX-License-Identifier: MIT OR Apache-2.0
"""Export an existing actual finite publication; no compiler or graph-engine execution."""
import hashlib,json,re,tempfile
from pathlib import Path
from .evolution_cli import InvocationFile
from .pack_native_cli import parse_finite_pack_invocation,CONFIG_LIMIT,PROFILE as INVOCATION_PROFILE
from .finite_native import open_finite_pack_publication
from .finite_graph_export import read_original_finite_graph_export
from .finite_graph_custody import build_finite_graph_custody
from .lifecycle import owned_context
from .evolution_producer import snapshot

EXPORT_PROFILE='ashlar-original-finite-pack-export-configuration/0.1'


def parse_finite_export_configuration(raw,output):
    """Select export resources directly; retain old query-configuration compatibility."""
    if type(raw)is not bytes or not 0<len(raw)<=CONFIG_LIMIT:raise ValueError('Bounded original finite export configuration required')
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate finite export configuration member')
            value[key]=item
        return value
    def integer(token):
        if len(token)>8 or re.fullmatch('0|[1-9][0-9]*',token)is None:raise ValueError('Canonical bounded export integer required')
        return int(token)
    def refuse(token):raise ValueError('Exact finite export configuration scalars required')
    value=json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)
    if type(value)is not dict:raise ValueError('Original finite export configuration object required')
    if value.get('profile')!=EXPORT_PROFILE:return parse_finite_pack_invocation(raw,'query')
    if set(value)!={'profile','pack','publication','jars','model','graph','pack_file','producer','operation_capacity'}:raise ValueError('Closed dedicated finite export configuration required')
    # Reuse resource validation only. No publication/compiler operation runs, and
    # the original selected configuration remains separately held and renewed.
    selected={**value,'profile':INVOCATION_PROFILE,'mode':'publish','output':str(output)}
    return parse_finite_pack_invocation(json.dumps(selected,separators=(',',':')).encode(),'publish')


def run_export_original_finite_pack_command(configuration,output):
    invocation=InvocationFile.read(configuration,CONFIG_LIMIT);config=parse_finite_export_configuration(invocation.raw,output);output=Path(output)
    protected=(invocation.path,config.native.output,config.native.model,config.native.graph,config.native.pack,config.native.jars,config.native.producer.source,config.native.producer.bun,config.native.producer.git,*(()if config.query is None else tuple(config.query[key]for key in ('index','installation'))))
    if not output.is_absolute()or output.parent.resolve()!=output.parent or not output.parent.is_dir()or output.exists()or output.is_symlink()or any(output==path or output in path.parents or path in output.parents for path in protected):raise ValueError('Fresh separate canonical private export output required')
    invocation.renew(CONFIG_LIMIT)
    with tempfile.TemporaryDirectory(prefix='ashlar-finite-export-',dir=output.parent)as temporary:
        with owned_context(open_finite_pack_publication(config.native,Path(temporary)/'public-dataset.json'))as driver:
            release,captured=read_original_finite_graph_export(driver,snapshot(config.native.pack,1048576))
        invocation.renew(CONFIG_LIMIT)
    # All native/source/Spark/temporary owners closed before custody/files escape.
    custody=build_finite_graph_custody(release,captured);invocation.renew(CONFIG_LIMIT)
    output.mkdir(mode=0o700)
    for name,payload in (('release.graph.json',release.payload),('custody.json',custody)):
        with (output/name).open('xb')as stream:
            if stream.write(payload)!=len(payload):raise OSError('Complete immutable export file write required')
        (output/name).chmod(0o444)
    return {'release':str(output/'release.graph.json'),'release_sha256':release.sha256,'custody':str(output/'custody.json'),'custody_sha256':hashlib.sha256(custody).hexdigest(),'scope':'Immutable finite-file export only; no protected ACK, native availability or graph engine grant.'}
