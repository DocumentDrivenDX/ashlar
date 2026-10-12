"""Concrete local-development rematerialization of an explicitly selected export."""
from dataclasses import dataclass
from pathlib import Path
import json,tempfile
from .commerce_export_cli import open_commerce_export_spark
from .graph_release_delta import materialize_private_graph_release_delta
from .lifecycle import owned_context
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export,PROFILE
from .supply_chain_native import finite_runtime


def _local(value):
    path=Path(value)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical absolute local paths required')
    return path


def _overlap(a,b):return a==b or a in b.parents or b in a.parents

@dataclass(frozen=True)
class LocalGraphMaterializationRuntime:
    jars:Path
    output:Path
    def __post_init__(self):
        _local(self.jars);_local(self.output)
        if not self.jars.is_dir():raise ValueError('Existing selected Delta JAR directory required')
        if self.output.exists():raise FileExistsError('Fresh materialization report required')


def run_materialize_graph_export_command(release,custody,release_sha256,custody_sha256,node_path,edge_path,jars,output):
    release=_local(release);custody=_local(custody);nodes=_local(node_path);edges=_local(edge_path);jars=_local(jars);output=_local(output)
    if _overlap(nodes,edges)or any(_overlap(destination,input)for destination in (nodes,edges,output)for input in (release,custody,jars))or any(_overlap(output,path)for path in(nodes,edges)):
        raise ValueError('Separate local destinations, inputs and report required')
    if nodes.exists()or edges.exists():raise FileExistsError('Fresh local Delta destinations required')
    runtime=LocalGraphMaterializationRuntime(jars,output);runtime.__post_init__()
    paths=finite_runtime(runtime)
    selection=LocalGraphExportSelection(release,custody,release_sha256,custody_sha256)
    with owned_context(open_local_graph_export(selection))as session:
        with tempfile.TemporaryDirectory(prefix='ashlar-materialize-export-',dir=output.parent)as work:
            with owned_context(open_commerce_export_spark(paths,Path(work)/'warehouse'))as spark:
                result=materialize_private_graph_release_delta(spark,session.release.payload,session.release.sha256,
                    custody_payload=session.custody_bytes,trusted_custody_sha256=session.custody_sha256,
                    source=session.source,node_path=nodes,edge_path=edges,policy=session.policy,context=session.context)
    result={**result,'export_use_profile':PROFILE}
    raw=(json.dumps(result,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode('utf-8')
    if len(raw)>65536:raise ValueError('Bounded complete materialization report required')
    with output.open('xb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Incomplete materialization report write')
    return {'report':str(output),'release_sha256':release_sha256,'custody_sha256':custody_sha256,
        'node_path':str(nodes),'edge_path':str(edges),'scope':'Local-development retained-export rematerialization only; no live source, canonical publication or Fabric authority.'}
