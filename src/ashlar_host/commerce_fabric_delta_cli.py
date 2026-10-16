"""Installed local projection from an existing original publication and source ACK."""
from pathlib import Path
import hashlib,importlib.metadata,json,os,sys,tempfile
from .commerce_export_cli import (CommerceExportConfiguration,parse_commerce_export_configuration,
    open_commerce_export_spark,open_registered_commerce_reader)
from .commerce_fabric_delta import materialize_commerce_fabric_delta
from .commerce_query_sources import CommerceQuerySourceFactory
from .evolution_cli import InvocationFile
from .lifecycle import finish,owned_context
from .ordinary_query_connection import OrdinaryQueryConnectionFactory
from .original_commerce_local_setup import original_commerce_local_source_ports
from .commerce import QUERY_JARS
from .fabric_pack_delta import destination
from .evolution_producer import snapshot


def _local(value):
    path=Path(value)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical absolute local paths required')
    return path


def _overlap(a,b):return a==b or a in b.parents or b in a.parents


def commerce_fabric_runtime(jars):
    """Qualify the selected current local SDK and exact Delta JAR inventory."""
    if sys.version_info[:2]not in ((3,11),(3,12))or any(importlib.metadata.version(k)!=v for k,v in {'pyspark':'4.0.1','delta-spark':'4.0.0'}.items()):
        raise ValueError('Selected Python3.11/3.12 Spark4.0.1 Delta4.0.0 required')
    if jars.is_symlink()or not jars.is_dir()or {p.name for p in jars.iterdir()}!=set(QUERY_JARS):
        raise ValueError('Exact selected Delta4 JAR inventory required')
    paths=[]
    for name,digest in sorted(QUERY_JARS.items()):
        path=jars/name
        if path.is_symlink()or hashlib.sha256(snapshot(path,32*1024*1024)).hexdigest()!=digest:
            raise ValueError('Selected Delta JAR bytes differ')
        paths.append(path)
    return paths


def run_materialize_commerce_fabric_command(configuration,jars,nodes,edges,output):
    captured=InvocationFile.read(configuration,262144)
    config=parse_commerce_export_configuration(captured.raw)
    if type(config)is not CommerceExportConfiguration:
        raise ValueError('Closed existing-commerce export-only configuration required')
    jars,output=map(_local,(jars,output))
    nodes,edges=map(destination,(nodes,edges))
    if nodes==edges or nodes.startswith(edges.rstrip('/')+'/')or edges.startswith(nodes.rstrip('/')+'/'):
        raise ValueError('Separate node and edge destinations required')
    destinations=tuple(Path(path)for path in (nodes,edges)if path.startswith('/'))+(output,)
    protected=(captured.path,config.publication,jars)
    if any(_overlap(a,b)for index,a in enumerate(destinations)for b in destinations[index+1:])or any(_overlap(a,b)for a in destinations for b in protected):
        raise ValueError('Separate destinations, original inputs and report required')
    if any(path.exists()for path in destinations):raise FileExistsError('Fresh Delta destinations and report required')
    paths=commerce_fabric_runtime(jars)
    ports=original_commerce_local_source_ports(config.source);registration=ports.registration
    def connection(role):
        if role==registration.source_role:return registration.source.connection_factory(None)
        if role==registration.ack_role:return registration.metadata_factory(None)
        raise PermissionError('Unselected ordinary query role refused')
    ordinary=OrdinaryQueryConnectionFactory(connection)
    source=CommerceQuerySourceFactory(registration)
    primary=None;result=None
    try:
        captured.renew(262144)
        with tempfile.TemporaryDirectory(prefix='ashlar-commerce-fabric-',dir=output.parent)as work:
            with owned_context(open_commerce_export_spark(paths,Path(work)/'warehouse'))as spark:
                with owned_context(open_registered_commerce_reader(spark,config.publication,
                        ordinary_session=ordinary,source_factory=source,destinations=destinations))as opened:
                    result=materialize_commerce_fabric_delta(spark,opened,node_path=nodes,edge_path=edges)
    except BaseException as error:primary=error
    finish(primary,[lambda:captured.renew(262144)])
    raw=(json.dumps(result,ensure_ascii=False,allow_nan=False,indent=2)+'\n').encode('utf-8')
    if len(raw)>1048576:raise ValueError('Bounded complete materialization report required')
    # The successful private report is released only after every owner closes.
    with os.fdopen(os.open(output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Incomplete materialization report write')
    return {'report':str(output),'node_path':str(nodes),'edge_path':str(edges),
        'release_sha256':result['release_sha256'],
        'scope':'Local original-publication Delta projection; no source mutation, ACK submission or Fabric deployment authority.'}
