"""Original selected Truss singleton rows under whole publication custody."""
import base64,json
from ashlar.manifest import manifest_pin_vector
from ashlar.singleton import read_singleton
from .delta_custody import encoded
from .driver import NativeDriver
from .lifecycle import owned_context,finish
from .truss_feed_sessions import TrussFeedSessions
from .truss_graph_admission import TrussGraphAdmission


class _SingletonPolicy:
    def __init__(self,driver,vector,context,table,source,kind,type_id,entity_id):
        self.driver=driver;self.vector=vector;self.context=context;self.table=table
        self.manifest=encoded(driver.policy.active['manifest'])
        self.expected=encoded(driver.policy.active['expected'])
        self.admission=driver.source_admission;self.sessions=driver.source_sessions
        self.facts=encoded(driver.admission_facts())
        role='object_current'if kind=='object'else'edge_current';typed='type_id'if kind=='object'else'rel_type_id'
        rows=json.loads(self.expected)[role]
        if any(row['source_system']!=source for row in rows):raise PermissionError('Original Truss installation source differs')
        selected=[row for row in rows if(row[typed],row['id'])==(str(type_id),str(entity_id))]
        if len(selected)>1:raise PermissionError('Independent singleton identity is ambiguous')
        self.row=selected[0]if selected else None
    def current(self):
        self.driver.require(self.context)
        if (self.driver.context is not self.context or self.driver.source_admission is not self.admission
                or self.driver.source_sessions is not self.sessions or self.driver.ack_sessions is not self.sessions
                or encoded(self.driver.admission_facts())!=self.facts
                or encoded(self.driver.policy.active['manifest'])!=self.manifest
                or encoded(self.driver.policy.active['expected'])!=self.expected):
            raise PermissionError('Original Truss singleton owner changed')
    def bind_descriptor(self,descriptor,vector,context):
        self.current()
        if context is not self.context or vector!=self.vector or encoded(dict(descriptor.raw))!=self.manifest:
            raise PermissionError('Exact original singleton manifest/vector required')
        self.driver.authorize(context,descriptor.publication_id,tuple(vector.targets))
    def authorize_row(self,descriptor,table,row,context):
        self.bind_descriptor(descriptor,self.vector,context)
        if table!=self.table or encoded(None if row is None else dict(row))!=encoded(self.row):
            raise PermissionError('Complete independently expected singleton row or absence differs')


def _identity(kind,type_id,entity_id):
    if type(kind)is not str or kind not in ('object','edge')or any(type(value)is not int or not 0<value<2**63 for value in (type_id,entity_id)):
        raise ValueError('Explicit positive signed64 Truss singleton identity required')


def read_original_truss_singleton(driver,*,kind,type_id,entity_id):
    """Read one positive signed64 native identity; preserve original full carriers."""
    if (type(driver)is not NativeDriver or type(driver.source_admission)is not TrussGraphAdmission
            or type(driver.source_sessions)is not TrussFeedSessions or driver.ack_sessions is not driver.source_sessions):
        raise PermissionError('Actual original Truss publication/source owner required')
    _identity(kind,type_id,entity_id)
    context=driver.context;manifest=driver.policy.active['manifest'];versions=json.loads(manifest['table_versions_json'])
    roles=('object_current','edge_current','tombstone','whole_source_history')
    if set(versions)!={driver.tables[role]for role in roles}:raise PermissionError('Complete original four-role singleton vector required')
    vector=manifest_pin_vector(manifest,{table:driver.transport.targets[table].uuid for table in versions},authority='private-local-process')
    source=json.loads(base64.b64decode(driver.source_admission.batch.cursor_before,validate=True))['installationId']
    table=driver.tables['object_current'if kind=='object'else'edge_current']
    with owned_context(driver.writer(driver.source_sessions.feed,context)):
        policy=_SingletonPolicy(driver,vector,context,table,source,kind,type_id,entity_id)
        primary=None;result=None
        try:
            result=read_singleton(driver.transport,driver,driver,vector,policy,publication_id=manifest['publication_id'],
                table=table,kind=kind,source=source,type_id=type_id,entity_id=entity_id,context=context,
                supported_profiles=['ashlar-delta/0.3'],supported_revisions={key:[value]for key,value in json.loads(manifest['schema_revisions_json']).items()})
        except BaseException as error:primary=error
        finish(primary,[policy.current])
    return result


def run_read_truss_singleton_command(configuration,output,*,kind,type_id,entity_id):
    _identity(kind,type_id,entity_id)
    from .truss_resume_cli import run_selected_truss_command
    from .truss_recovery import restore_applied_truss_publication
    def read(driver,config):
        with owned_context(driver.writer(driver.source_sessions.feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        row=read_original_truss_singleton(driver,kind=kind,type_id=type_id,entity_id=entity_id)
        return {'publication_id':driver.policy.active['manifest']['publication_id'],'kind':kind,'type_id':type_id,
            'entity_id':entity_id,'row':None if row is None else dict(row),
            'qualification':'Original selected Truss singleton under complete current publication/source/native custody; no ingestion, ACK, latest fallback or external-storage grant.'}
    return run_selected_truss_command(configuration,output,read)
