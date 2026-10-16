"""Fresh typed original17 Delta projection; caller owns Spark/storage access."""
from collections import Counter
import json,re
from pathlib import Path
from urllib.parse import urlsplit
from ashlar.graph_release import release_columns
from .fabric_pack_preparation import scenario_properties
from .graphframes_pack_scenarios import prepare_scenarios,INTEGER_FIELDS
from .finite_pack import FinitePackDefinition
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export
from .evolution_cli import InvocationFile
from .lifecycle import owned_context,finish


def destination(value):
    if type(value)is type(Path('/')):value=str(value)
    if type(value)is not str or not value or len(value)>4096 or any(c in value for c in ('`','\0','\n','\r')):
        raise ValueError('Explicit canonical Delta destination required')
    if value.startswith('/'):
        if str(Path(value).resolve())!=value:raise ValueError('Canonical local destination required')
    else:
        uri=urlsplit(value)
        if (uri.scheme!='abfss'or not uri.netloc.endswith('@onelake.dfs.fabric.microsoft.com')
                or not uri.path.startswith('/')or uri.query or uri.fragment
                or any(p in ('','.','..','latest','current')for p in uri.path.split('/')[1:])):
            raise ValueError('Explicit release-specific OneLake destination required')
    return value


def metadata(spark,path):
    table='delta.`'+path+'`'
    detail=spark.sql('DESCRIBE DETAIL '+table).select('id','location','format').first()
    version=spark.sql('DESCRIBE HISTORY '+table+' LIMIT 1').select('version').first()['version']
    location=detail['location'];normalized=location.removeprefix('file:')if type(location)is str and path.startswith('/')else location
    if (type(detail['id'])is not str or re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',detail['id'])is None
            or type(version)is not int or version!=0 or detail['format']!='delta'
            or type(normalized)is not str or normalized.rstrip('/')!=path.rstrip('/')):
        raise ValueError('Exact fresh native Delta identity/location/version required')
    return {'uuid':detail['id'],'version':version,'location':location,'path':path}


def readback(spark,path,properties,expected):
    names=[p['name']for p in properties]
    frame=spark.read.format('delta').option('versionAsOf',0).load(path)
    wanted=[(p['name'],'long'if p['type']=='INT64'else'string')for p in properties]
    if [(f.name,f.dataType.typeName())for f in frame.schema.fields]!=wanted:
        raise ValueError('Exact ordered typed Delta schema required')
    actual=[tuple(row)for row in frame.select(*names).limit(len(expected)+1).collect()]
    for row in actual:
        if len(row)!=len(properties):raise ValueError('Complete typed native cells required')
        for value,prop in zip(row,properties):
            if value is None:continue
            if prop['type']=='INT64':valid=type(value)is int and -(2**63)<=value<2**63
            else:valid=type(value)is str
            if not valid:raise ValueError('Exact native String/null/signed64 cells required')
    if Counter(actual)!=Counter(expected):raise ValueError('Complete typed Delta projection bag differs')


def materialize_fabric_pack_delta(spark,*,pack,model,graph,pack_file,selection,node_path,edge_path):
    """Preserve partial writes, refuse overwrite, and close file-use custody.

    This qualifies no Fabric graph load, immutable storage policy, live canonical
    publication, protected ACK or graph query. Native storage ACLs belong to Spark.
    """
    if pack not in INTEGER_FIELDS or type(selection)is not LocalGraphExportSelection:
        raise ValueError('Closed original17 pack and private export selection required')
    nodes,edges=destination(node_path),destination(edge_path)
    if nodes==edges or nodes.startswith(edges.rstrip('/')+'/')or edges.startswith(nodes.rstrip('/')+'/'):
        raise ValueError('Separate nonoverlapping destinations required')
    inputs=[InvocationFile.read(Path(p),262144)for p in (model,graph,pack_file)]
    protected=[str(p.path)for p in inputs]+[str(selection.release_path),str(selection.custody_path)]
    if any(path==p or path.startswith(p.rstrip('/')+'/')or p.startswith(path.rstrip('/')+'/')for path in (nodes,edges)for p in protected):
        raise ValueError('Delta destinations must be separate from original files')
    observations={}
    primary=None
    try:
        with owned_context(open_local_graph_export(selection))as source:
            with source.writer(source.context):
                def renew():
                    source.admit_original_export(source.release,source.custody_bytes,source.source,source.context)
                    for p in inputs:p.renew(262144)
                renew()
                profile=prepare_scenarios(FinitePackDefinition(pack),*(p.raw for p in inputs),json.loads(source.release.payload))
                node_properties=scenario_properties(profile)
                edge_properties=[{'name':n,'type':'STRING'}for n in release_columns('edge')]
                for role,path,properties in [('nodes',nodes,node_properties),('edges',edges,edge_properties)]:
                    renew()
                    names=[p['name']for p in properties];rows=[tuple(row[n]for n in names)for row in profile[role]]
                    schema=','.join(p['name']+(' BIGINT'if p['type']=='INT64'else' STRING')for p in properties)
                    spark.createDataFrame(rows,schema=schema).write.format('delta').mode('errorifexists').save(path)
                    observed=metadata(spark,path);readback(spark,path,properties,rows)
                    observations[role]={**observed,'rows':len(rows),'columns':len(properties)}
                    renew()
                if observations['nodes']['uuid']==observations['edges']['uuid']:raise ValueError('Distinct native Delta targets required')
                for role,path in [('nodes',nodes),('edges',edges)]:
                    if metadata(spark,path)!={k:observations[role][k]for k in ('uuid','version','location','path')}:
                        raise ValueError('Written Delta identity changed before closing')
                renew()
    except BaseException as error:primary=error
    finish(primary,[lambda p=p:p.renew(262144)for p in inputs])
    return {'profile':'ashlar-original17-delta-materialization/0.1','pack':pack,
        'release_sha256':selection.release_sha256,'custody_sha256':selection.custody_sha256,
        'targets':observations,'scope':'Fresh typed private-export Delta projection/readback only; no Fabric load, query or live publication authority.'}
