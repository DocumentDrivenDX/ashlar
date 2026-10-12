"""Write finite original STRING graph carriers to fresh Spark Delta destinations."""
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit
import re
from ashlar.graph_release import GraphRelease,release_columns
from .puppy_release_document import load_graph_release
from .lifecycle import finish,owned_context
from .private_graph_custody import load_private_graph_release,PROFILE as PRIVATE_CUSTODY_PROFILE


def _destination(value):
    if type(value)is type(Path('/')):value=str(value)
    if type(value)is not str or not value or len(value)>4096 or any(c in value for c in ('`','\x00','\n','\r')):
        raise ValueError('Explicit Delta destination required')
    if value.startswith('/'):
        path=Path(value)
        if path.resolve()!=path:raise ValueError('Canonical absolute local Delta destination required')
        return str(path)
    parsed=urlsplit(value)
    if (parsed.scheme!='abfss' or not parsed.netloc.endswith('@onelake.dfs.fabric.microsoft.com')
            or not parsed.path.startswith('/')or parsed.query or parsed.fragment
            or any(part in ('','.','..')for part in parsed.path.split('/')[1:])):
        raise ValueError('Explicit OneLake or absolute local Delta destination required')
    return value


def _write_delta_graph(spark,document,nodes,edges):
    observations={}
    for kind,role,path in (('node','nodes',nodes),('edge','edges',edges)):
        names=release_columns(kind);rows=document[role]
        entries=[tuple(row[name]for name in names)for row in rows]
        schema=','.join(name+' STRING'for name in names)
        frame=spark.createDataFrame(entries,schema=schema)
        frame.write.format('delta').mode('errorifexists').save(path)
        quoted='delta.`'+path+'`'
        detail=spark.sql('DESCRIBE DETAIL '+quoted).select('id','location','format').first()
        version=spark.sql('DESCRIBE HISTORY '+quoted+' LIMIT 1').select('version').first()['version']
        uuid=detail['id'];location=detail['location']
        if (type(uuid)is not str or not re.fullmatch('[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}',uuid)
                or type(version)is not int or version!=0 or detail['format']!='delta'
                or type(location)is not str):raise ValueError('Fresh Delta UUID/version observation required')
        normalized=location.removeprefix('file:')if path.startswith('/')else location
        if normalized.rstrip('/')!=path.rstrip('/'):raise ValueError('Written Delta location differs')
        written=spark.read.format('delta').option('versionAsOf',version).load(path)
        fields=written.schema.fields
        if len(fields)!=len(names)or [(field.name,field.dataType.typeName())for field in fields]!=[(name,'string')for name in names]:
            raise ValueError('Complete written STRING schema differs')
        actual=[tuple(row)for row in written.select(*names).limit(len(entries)+1).collect()]
        if (any(len(row)!=len(names)or any(value is not None and type(value)is not str for value in row)for row in actual)
                or Counter(actual)!=Counter(entries)):
            raise ValueError('Complete written graph carrier bag differs')
        observations[role]={'path':path,'location':location,'uuid':uuid,'version':version,'rows':len(entries)}
    if observations['nodes']['uuid']==observations['edges']['uuid']:raise ValueError('Distinct native Delta targets required')
    return observations


def materialize_graph_release_delta(spark,payload,trusted_sha256,*,node_path,edge_path,policy,context):
    """Caller owns Spark and current release policy; partial writes remain on error.

    The strict public release loader limits input to 16 MiB and each bag to
    10,000 rows. Written readback is bounded by expected rows plus one.
    This helper observes materialization; it does not load or authorize a graph.
    """
    nodes=_destination(node_path);edges=_destination(edge_path)
    if nodes==edges or nodes.startswith(edges.rstrip('/')+'/')or edges.startswith(nodes.rstrip('/')+'/'):
        raise ValueError('Separate nonoverlapping node and edge destinations required')
    if not callable(getattr(policy,'admit_original_release',None)):raise ValueError('Actual original release policy required')
    document=load_graph_release(payload,trusted_sha256);release=GraphRelease(payload,trusted_sha256)
    original_admit=policy.admit_original_release
    def admit():
        result=original_admit(release,context)
        if policy.admit_original_release!=original_admit or result is not None:
            raise PermissionError('Current original release admission refused')
    primary=None;observations={}
    try:
        admit()
        observations=_write_delta_graph(spark,document,nodes,edges)
    except BaseException as error:primary=error
    finish(primary,[admit])
    return {'profile':'ashlar-graph-release-delta-materialization/0.1','release_sha256':trusted_sha256,
        'targets':observations,'scope':'Observed complete fresh STRING Delta carrier bags only; no immutability, deployment, loaded graph or query authority.'}


def materialize_private_graph_release_delta(spark,payload,trusted_sha256,*,
        custody_payload,trusted_custody_sha256,source,node_path,edge_path,policy,context):
    """Explicit private-local export branch; no public-loader fallback or rewrites.

    The independently supplied policy owns current original export/source
    admission and its writer interval. Receipt correspondence grants no authority.
    """
    nodes=_destination(node_path);edges=_destination(edge_path)
    if nodes==edges or nodes.startswith(edges.rstrip('/')+'/')or edges.startswith(nodes.rstrip('/')+'/'):
        raise ValueError('Separate nonoverlapping node and edge destinations required')
    if source is None or any(not callable(getattr(policy,name,None))for name in ('writer','admit_original_export')):
        raise ValueError('Actual source and original export policy required')
    document=load_private_graph_release(payload,trusted_sha256,custody_payload,trusted_custody_sha256)
    release=GraphRelease(payload,trusted_sha256);original_writer=policy.writer;original_admit=policy.admit_original_export
    def renew():
        load_private_graph_release(payload,trusted_sha256,custody_payload,trusted_custody_sha256)
        result=original_admit(release,custody_payload,source,context)
        if result is not None or policy.writer!=original_writer or policy.admit_original_export!=original_admit:
            raise PermissionError('Current original export admission refused')
    with owned_context(original_writer(context)):
        primary=None;observations=None
        try:
            renew();observations=_write_delta_graph(spark,document,nodes,edges)
        except BaseException as error:primary=error
        finish(primary,[renew])
    return {'profile':'ashlar-graph-release-delta-materialization/0.1','release_sha256':trusted_sha256,
        'custody_profile':PRIVATE_CUSTODY_PROFILE,'custody_sha256':trusted_custody_sha256,
        'targets':observations,'scope':'Observed fresh STRING carrier bags for explicitly admitted private-local export bytes; no public release fallback, future native availability, loaded graph or query authority.'}
