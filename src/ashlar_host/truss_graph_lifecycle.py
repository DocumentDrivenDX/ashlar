"""Public cohort target update and edge-first deletion with native identity retained."""
import json,os
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish


def parse_truss_target_configuration(configuration,output):
    config=decode_custody_bytes(snapshot(Path(configuration),65536))
    keys={'connection_uri','bun','document_path','document_revision','origin_path','object_id','edge_id','replacement_label','update_request_key','delete_request_key'}
    if type(config)is not dict or set(config)!=keys:raise ValueError('Exact selected cohort target configuration required')
    for key in ('connection_uri','document_revision','replacement_label','update_request_key','delete_request_key'):
        if type(config[key])is not str or not config[key]or len(config[key])>4096 or '\0'in config[key]:raise ValueError('Bounded explicit cohort target input required')
    if config['update_request_key']==config['delete_request_key']:raise ValueError('Distinct original request keys required')
    for key in ('object_id','edge_id'):
        if type(config[key])is not int or not 0<config[key]<9223372036854775808:raise ValueError('Positive signed64 native target identities required')
    def path(value):
        if type(value)is not str:raise ValueError('Canonical existing selected input required')
        selected=Path(value)
        if not selected.is_absolute()or selected.resolve()!=selected or not selected.is_file():raise ValueError('Canonical existing selected input required')
        return selected
    document=path(config['document_path']);bun=path(config['bun']);origin_path=path(config['origin_path'])
    raw=snapshot(document,1048576)
    if decode_custody_bytes(raw).get('id')!='cohort':raise ValueError('Original accepted cohort document required')
    origin=decode_custody_bytes(snapshot(origin_path,65536))
    if type(origin)is not dict or any(type(key)is not str or type(value)is not str for key,value in origin.items()):raise ValueError('Original string origin map required')
    destination=Path(output)
    if not destination.is_absolute()or not destination.parent.is_dir()or destination.parent.resolve()!=destination.parent or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical lifecycle report required')
    if any(destination==selected or destination in selected.parents or selected in destination.parents for selected in (document,bun,origin_path,Path(configuration).resolve())):
        raise ValueError('Lifecycle report must be separate from selected inputs')
    return config,raw,origin,destination


def target_delete_operations(edge,target):
    """Pure operation correspondence; the owning SDK must authorize the group."""
    from truss.groups import Delete,StoredObjectReference
    if str(target.id)!=edge.target.id or str(target.type_id)!=edge.target.type_id:raise ValueError('Selected edge target and object identity differ')
    return (Delete(operation='delete_edge',target=StoredObjectReference(identity=edge.identity),expected_version=str(edge.version)),
        Delete(operation='delete_object',target=StoredObjectReference(identity=edge.target),expected_version=str(target.version)))


def run_update_delete_truss_target_command(configuration,output):
    config,raw,origin,destination=parse_truss_target_configuration(configuration,output)
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,ObjectFound,ObjectNotFound,EdgeFound,EdgeNotFound
    from truss.contracts import MapValue,MapEntry,TextValue
    from truss.groups import UpdateObject,StoredObjectReference,AuthoredValue,GroupSuccess,CommittedReceiptReplay
    documents=(CatalogDocument('cohort',config['document_revision'],raw),)
    asserted=MapValue(entries=tuple(MapEntry(key=key,value=TextValue(kind='string',text=value))for key,value in origin.items()))
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss operation refused')
        return result.value
    @contextmanager
    def interval(connection):
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction());assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host));yield assembly,tx
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('Host detach refused; original recovery required')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    def current(assembly,tx):
        refs={record:value(assembly.get_type_reference_in_transaction(tx,document_id='cohort',module_id='m',record_id=record))for record in ('A','B')}
        relation=value(assembly.get_relationship_reference_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B'))
        found=value(assembly.get_edge_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B',edge_id=config['edge_id']))
        target=value(assembly.get_object_in_transaction(tx,document_id='cohort',module_id='m',record_id='B',object_id=config['object_id']))
        if relation is None or any(ref is None for ref in refs.values())or not isinstance(found,EdgeFound)or not isinstance(target,ObjectFound):raise RuntimeError('Current authorized selected cohort edge/target required')
        edge=found.edge;obj=target.object
        if (edge.identity.definition_pin!=relation.definition_pin or edge.source.definition_pin!=refs['A'].definition_pin or edge.target.definition_pin!=refs['B'].definition_pin
                or edge.target.id!=str(obj.id)or edge.target.type_id!=str(obj.type_id)or obj.id!=config['object_id']):raise RuntimeError('Current selected native identity and definition pins differ')
        return edge,obj
    def apply(assembly,tx,operations,key):
        group=value(assembly.group_context_in_transaction(tx));original_input=group.input(tuple(operations),asserted);request=group.request(key)
        result=value(assembly.apply_in_transaction(tx,original_input,request,documents=documents,bun_executable=config['bun']))
        if not isinstance(result,GroupSuccess):raise RuntimeError('Selected cohort group refused')
        return result,original_input,request
    def replay(assembly,tx,original):
        result=value(assembly.apply_in_transaction(tx,original[1],original[2],documents=documents,bun_executable=config['bun']))
        if not isinstance(result,GroupSuccess)or not isinstance(result.response,CommittedReceiptReplay)or result.response.semantic!=original[0].response.semantic or type(result.response.receipt_token)is not str:raise RuntimeError('Original committed receipt replay differs')
        return asdict(result)
    with psycopg.connect(config['connection_uri'],connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            edge,before=current(assembly,tx)
            update=apply(assembly,tx,(UpdateObject(target=StoredObjectReference(identity=edge.target),
                set=(AuthoredValue(name='B-label',value=TextValue(kind='string',text=config['replacement_label'])),),unset=(),expected_version=str(before.version)),),config['update_request_key'])
        connection.commit()
        with interval(connection)as (assembly,tx):
            current_edge,after=current(assembly,tx)
            if (current_edge!=edge or after.id!=before.id or after.type_id!=before.type_id or after.version!=before.version+1
                    or dict(after.values)!={**dict(before.values),'B-label':config['replacement_label']}or after.retained!=before.retained):raise RuntimeError('Complete stable-identity target update differs')
            update_receipt=replay(assembly,tx,update)
            deleted=apply(assembly,tx,target_delete_operations(current_edge,after),config['delete_request_key'])
        connection.commit()
        with interval(connection)as (assembly,tx):
            absent_edge=value(assembly.get_edge_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B',edge_id=config['edge_id']))
            absent_target=value(assembly.get_object_in_transaction(tx,document_id='cohort',module_id='m',record_id='B',object_id=config['object_id']))
            if not isinstance(absent_edge,EdgeNotFound)or not isinstance(absent_target,ObjectNotFound):raise RuntimeError('Selected edge/target deletion readback differs')
            delete_receipt=replay(assembly,tx,deleted)
            source=value(assembly.get_object_in_transaction(tx,document_id='cohort',module_id='m',record_id='A',object_id=int(edge.source.id)))
            if not isinstance(source,ObjectFound):raise RuntimeError('Independent source object was not preserved')
            result={'edge_before':asdict(edge),'target_before':asdict(before),'target_updated':asdict(after),
                'update_original':asdict(update[0]),'update_committed_receipt':update_receipt,'delete_original':asdict(deleted[0]),'delete_committed_receipt':delete_receipt,
                'edge_absent':asdict(absent_edge),'target_absent':asdict(absent_target),'source_after':asdict(source.object),
                'scope':'Selected cohort B logical-key label update retains native EntityKey, then edge-first target-only deletion; actual public Truss groups/readback/receipt. No feed/publication/ACK qualification.'}
        connection.commit()
    payload=(json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode();descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(payload)!=len(payload):raise OSError('Complete original lifecycle report write required')
        stream.flush();os.fsync(stream.fileno())
    return result
