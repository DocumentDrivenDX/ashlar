"""Bounded, exact graph releases read under complete publication/pin custody."""
import base64
from dataclasses import dataclass
import hashlib
import json
import re
import os
import tempfile
from pathlib import Path
from types import MappingProxyType
from .native import _quoted
from .publication import ResolutionError, resolve_publication

NODE_INTS=('type_id','id','entity_version','root_id','source_position')
EDGE_INTS=('rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','source_position')
NODE_TEXT=('source_system','logical_key_json','schema_revision','props_json','retained_json','source_feed','source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id')
EDGE_TEXT=('source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id')
NULLABLE={'root_id','source_position','order_key','apply_batch_id','source_cursor_json','source_delivery_id'}


def release_columns(kind):
    """Ordered original canonical graph-release carrier columns."""
    if type(kind) is not str or kind not in ('node', 'edge'):
        raise ResolutionError('Exact canonical graph role required')
    return (NODE_INTS+NODE_TEXT if kind=='node' else EDGE_INTS+EDGE_TEXT)+('published_at','graph_id')+(('src','dst') if kind=='edge' else ())


def _text(value):
    if type(value) is not str or '\x00' in value:
        raise ResolutionError('Exact Unicode text carrier required')
    try:value.encode('utf-8')
    except UnicodeError as error:raise ResolutionError('Unicode scalar text required') from error
    return value


def _integer(value):
    _text(value)
    if len(value)>20 or not re.fullmatch(r'0|-?[1-9][0-9]*',value) or not -(2**63)<=int(value)<2**63:
        raise ResolutionError('Canonical signed64 text carrier required')
    return value


def graph_key(kind,source,type_id,entity_id):
    """Reversible exact tuple key. IDs remain canonical strings, never floats."""
    if kind not in ('node','edge') or not _text(source):
        raise ResolutionError('Explicit graph kind/source required')
    value=[kind,source,_integer(type_id),_integer(entity_id)]
    raw=json.dumps(value,ensure_ascii=False,separators=(',',':')).encode('utf-8')
    return 'ashlar-key/1:'+base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')


def decode_graph_key(value):
    _text(value)
    prefix='ashlar-key/1:'
    if not value.startswith(prefix):raise ResolutionError('Unknown graph key profile')
    try:
        payload=value[len(prefix):]
        if not re.fullmatch(r'[A-Za-z0-9_-]+',payload):raise ValueError('Noncanonical base64')
        items=json.loads(base64.b64decode(payload+'='*((-len(payload))%4),altchars=b'-_',validate=True).decode('utf-8'))
        if type(items) is not list or len(items)!=4 or graph_key(*items)!=value:raise ValueError('Noncanonical graph tuple')
    except (ValueError,UnicodeError,TypeError) as error:
        raise ResolutionError('Invalid reversible graph key') from error
    return tuple(items)


@dataclass(frozen=True)
class GraphRelease:
    """Immutable exact release bytes; file/engine activation has separate custody."""
    payload: bytes
    sha256: str


def _carrier(row,kind):
    ints=NODE_INTS if kind=='node' else EDGE_INTS
    texts=NODE_TEXT if kind=='node' else EDGE_TEXT
    if type(row) is not dict or set(row)!=set(ints+texts+('published_at',)):
        raise ResolutionError('Complete canonical graph carrier inventory required')
    for name,value in row.items():
        if value is None:
            if name not in NULLABLE:raise ResolutionError('Unexpected null graph carrier')
        elif name in ints or name=='published_at':_integer(value)
        else:_text(value)
    if int(row['entity_version'])<0 or not all(row[k] for k in ('source_system','schema_revision','source_feed','source_epoch')):
        raise ResolutionError('Explicit canonical graph revision/provenance required')
    typed='type_id' if kind=='node' else 'rel_type_id'
    identity={'source_system':row['source_system'],typed:int(row[typed]),'id':int(row['id'])}
    expected=hashlib.sha256(json.dumps(identity,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
    if row['lookup_hash']!=expected:raise ResolutionError('Canonical lookup identity hash mismatch')
    result=dict(row)
    result['graph_id']=graph_key(kind,row['source_system'],row[typed],row['id'])
    if kind=='edge':
        result['src']=graph_key('node',row['source_system'],row['source_type'],row['source_id'])
        result['dst']=graph_key('node',row['source_system'],row['target_type'],row['target_id'])
    return result


def validate_release_carrier(original, kind):
    """Validate one original canonical carrier and derive reversible identities.

    This pure correspondence check does not establish publication/read authority.
    """
    if type(kind) is not str or kind not in ('node', 'edge') or type(original) is not dict:
        raise ResolutionError('Exact original graph carrier and role required')
    return _carrier(original, kind)


def read_graph_release(executor,backend,pins,vector,policy,*,publication_id,node_table,edge_table,
                       context,supported_profiles,supported_revisions,max_nodes,max_edges):
    """Read a complete bounded publication graph, or refuse without releasing data.

    Host policy must bind the original manifest and validate/admit complete source
    semantics for every carrier. Exact text bags are residuals, not inferred scalar
    properties. Schema/type validation and external engine support are not implied.
    """
    if node_table==edge_table or any(t not in vector.targets for t in (node_table,edge_table)):
        raise ResolutionError('Distinct graph roles in complete original pin vector required')
    if any(type(n) is not int or not 0<=n<=1000000 for n in (max_nodes,max_edges)):
        raise ResolutionError('Explicit bounded release row budgets required')
    with pins.hold(vector,context=context):
        resolved=resolve_publication(backend,publication_id,{t:p[0] for t,p in vector.targets.items()},
            context=context,supported_profiles=supported_profiles,supported_revisions=supported_revisions)
        if any(resolved.snapshots[t].version!=p[1] for t,p in vector.targets.items()):
            raise ResolutionError('Publication differs from complete held version vector')
        def bind():
            if policy.bind_descriptor(resolved.descriptor,vector,context) is not None:
                raise ResolutionError('Original graph descriptor/pin custody incomplete')
        bind()
        def identity(table):
            rows=executor.query('DESCRIBE DETAIL '+_quoted(table),{}).rows
            if len(rows)!=1 or rows[0].get('id')!=resolved.snapshots[table].uuid:
                raise ResolutionError('Graph role replaced during release read')
        graphs=[]
        for table,kind,budget in [(node_table,'node',max_nodes),(edge_table,'edge',max_edges)]:
            identity(table)
            ints=NODE_INTS if kind=='node' else EDGE_INTS
            texts=NODE_TEXT if kind=='node' else EDGE_TEXT
            names=ints+texts+('published_at',)
            projection=','.join(('cast('+n+' AS STRING) AS '+n) if n in ints else n for n in ints+texts)
            projection+=',cast(unix_micros(published_at) AS STRING) AS published_at'
            result=executor.query('SELECT '+projection+' FROM '+_quoted(table)+' VERSION AS OF '+str(resolved.snapshots[table].version)+' LIMIT '+str(budget+1),{})
            if tuple(result.columns)!=tuple((n,'STRING') for n in names) or len(result.rows)>budget:
                raise ResolutionError('Exact complete graph carriers exceed profile or row budget')
            rows=[]
            for original in result.rows:
                row=_carrier(dict(original),kind)
                if policy.authorize_graph_row(resolved.descriptor,table,MappingProxyType(dict(original)),context) is not None:
                    raise ResolutionError('Graph row semantic/read admission incomplete')
                rows.append(row)
            if len({r['graph_id'] for r in rows})!=len(rows):raise ResolutionError('Duplicate independent graph identity')
            graphs.append(sorted(rows,key=lambda r:r['graph_id']))
            identity(table)
        nodes,edges=graphs;keys={r['graph_id'] for r in nodes}
        if any(r['src'] not in keys or r['dst'] not in keys for r in edges):
            raise ResolutionError('Typed graph endpoint absent from complete release')
        identity(node_table)
        identity(edge_table)
        if backend.validate_descriptor(resolved.descriptor,context) is not None or backend.authorize(context,publication_id,tuple(vector.targets)) is not None:
            raise ResolutionError('Graph release authority or retention expired')
        bind()
        if policy.authorize_graph_result(resolved.descriptor,tuple(MappingProxyType(r) for r in nodes),tuple(MappingProxyType(r) for r in edges),context) is not None:
            raise ResolutionError('Graph release closing policy incomplete')
        value={'format':'ashlar-graph-release/0.1','publication':dict(resolved.descriptor.raw),
            'snapshots':{t:{'uuid':s.uuid,'version':s.version} for t,s in resolved.snapshots.items()},
            'roles':{'nodes':node_table,'edges':edge_table},'nodes':nodes,'edges':edges,
            'mapping':{'identity':'ashlar-key/1','properties':'Exact original canonical text only; scalar promotion requires separate admitted mapping.',
                       'residuals':['Canonical props_json/retained_json preserved verbatim','History/tombstones/raw source remain in original pinned publication'],
                       'capabilities':{'reversibleIdentity':True,'independentEdges':True,'isolatedNodes':True,'exactCanonicalText':True,'selectedScalarPromotion':False,'nativeReleaseMaterialization':False,'engineExecution':False},
                       'losses':[],'engineSupport':[]}}
        try:payload=(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode('utf-8')
        except (TypeError,ValueError,UnicodeError) as error:raise ResolutionError('Exact release serialization unavailable') from error
        release=GraphRelease(payload,hashlib.sha256(payload).hexdigest())
    return release


def persist_graph_release(directory,release):
    """Atomically expose complete hash-addressed bytes; never overwrite a release.

    This local-file operation does not activate an engine or extend source read
    authority. The host must provide a private admitted release directory.
    """
    if not isinstance(release,GraphRelease) or type(release.payload) is not bytes or hashlib.sha256(release.payload).hexdigest()!=release.sha256:
        raise ResolutionError('Exact original release payload/digest required')
    directory=Path(directory)
    if directory.is_symlink() or not directory.is_dir():raise ResolutionError('Existing private release directory required')
    target=directory/(release.sha256+'.graph.json')
    handle,temporary=tempfile.mkstemp(prefix='.unpublished-graph-',dir=directory)
    try:
        with os.fdopen(handle,'wb') as file:
            file.write(release.payload);file.flush();os.fsync(file.fileno())
        try:os.link(temporary,target)
        except FileExistsError:
            if target.is_symlink() or not target.is_file() or target.read_bytes()!=release.payload:
                raise ResolutionError('Immutable release target conflict')
        descriptor=os.open(directory,os.O_RDONLY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
    finally:os.unlink(temporary)
    return target
