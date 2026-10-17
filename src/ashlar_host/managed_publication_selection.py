"""Read-only original configured-JSONL selection under its cooperating writer lock.

Retained artifacts and independent source state are correspondence, never native
admission. The caller must retain this owner across native use and all closings.
No journal initializer, producer, SQL service or credential discovery is used.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl,hashlib,json,os,re,sqlite3,stat
from pathlib import Path
from urllib.parse import quote
from uuid import UUID
from ashlar.schema import decode_retained_json
from ashlar.source_config import load_jsonl_configuration,JsonlConfiguration
from ashlar.source import jsonl_batches
from ashlar.staging import batch_row
from ashlar.source_checkpoint import jsonl_checkpoint,bind_source_descriptor
from ashlar.attempt_store import verify_request_digest
from ashlar.stored_publisher import validate_applied_artifact
from ashlar.apply import empty_state,plan_apply
from ashlar.whole_entity import changes_from_batch
from .evolution_cli import InvocationFile
from .lifecycle import finish
from .managed_source_oracle import retained_materialization_clock

_MINT=object()
ROLES=('object_current','edge_current','tombstone','whole_source_history')
def _encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)
def _json(raw):return decode_retained_json(raw if type(raw)is bytes else raw.encode())
def _require(ok):
    if not ok:raise PermissionError('Original managed publication selection correspondence refused')

@dataclass(frozen=True)
class ManagedSelectionFile:
    path:Path
    sha256:str
    def __post_init__(self):
        _require(type(self.path)is type(Path('/'))and self.path.is_absolute()and self.path.parent.resolve()==self.path.parent)
        _require(type(self.sha256)is str and re.fullmatch('[0-9a-f]{64}',self.sha256)is not None)
    def read(self):
        self.__post_init__()
        _require(type(self.sha256)is str and re.fullmatch('[0-9a-f]{64}',self.sha256)is not None)
        file=InvocationFile.read(self.path,262144)
        _require(hashlib.sha256(file.raw).hexdigest()==self.sha256)
        return file

@dataclass(frozen=True)
class ManagedPublicationSelectionSpec:
    installation:ManagedSelectionFile
    intake_proof:ManagedSelectionFile
    source_configuration:ManagedSelectionFile
    journal:Path
    publication_id:str
    completed_ordinal:int
    maximum_journal_bytes:int=134217728
    maximum_rows:int=10000
    def __post_init__(self):
        _require(all(type(p)is ManagedSelectionFile for p in (self.installation,self.intake_proof,self.source_configuration)))
        _require(type(self.journal)is type(Path('/'))and self.journal.is_absolute()and self.journal.parent.resolve()==self.journal.parent)
        _require(type(self.publication_id)is str and 0<len(self.publication_id.encode())<=1024 and '\0'not in self.publication_id)
        _require(type(self.completed_ordinal)is int and 1<=self.completed_ordinal<=4)
        _require(type(self.maximum_journal_bytes)is int and 1<=self.maximum_journal_bytes<=134217728)
        _require(type(self.maximum_rows)is int and 1<=self.maximum_rows<=10000)


def _identity(path,maximum):
    s=path.lstat();_require(stat.S_ISREG(s.st_mode)and s.st_uid==os.getuid()and not s.st_mode&0o022 and s.st_size<=maximum)
    return s.st_dev,s.st_ino


def _journal(path,maximum,rows):
    identity=_identity(path,maximum)
    total=path.stat().st_size
    wal=Path(str(path)+'-wal')
    if wal.exists():
        _identity(wal,maximum);total+=wal.stat().st_size
    _require(total<=maximum)
    db=sqlite3.connect('file:'+quote(str(path),safe='/')+'?mode=ro',uri=True,timeout=1);primary=None;result=None
    try:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        tables={'stream_scope':'id,workload','jsonl_consumer_scope':'stream,feed,epoch,source_sha256,config_json',
            'jsonl_consumer_progress':'stream,feed,epoch,position,original_json,digest',
            'snapshot_artifact':'operation,request_json,effects_json,targets_json,artifact_text,artifact_digest,capture_digest',
            'publisher_effect_artifact':'operation,request_json,steps_json,artifact_text,artifact_digest',
            'submission':'operation,request,digest,handle,response'}
        result={};total=0
        kinds=dict(db.execute("SELECT name,type FROM sqlite_master WHERE name IN ("+','.join('?'for _ in tables)+")",tuple(tables)).fetchall());_require(kinds=={t:'table'for t in tables})
        for table,columns in tables.items():
            order='stream,feed,epoch,position'if table=='jsonl_consumer_progress'else 'stream,feed,epoch'if table=='jsonl_consumer_scope'else 'id'if table=='stream_scope'else 'operation'
            cursor=db.execute('SELECT '+columns+' FROM '+table+' ORDER BY '+order+' LIMIT ?', (rows+1,));values=cursor.fetchall()
            _require(len(values)<=rows)
            for row in values:
                for value in row:
                    _require(value is None or type(value)in (str,int))
                    if type(value)is str:total+=len(value.encode());_require(total<=maximum)
            result[table]=values
        _require(_identity(path,maximum)==identity)
    except BaseException as e:primary=e
    finish(primary,[db.rollback,db.close])
    return identity,result


class ManagedPublicationSelection:
    def __setattr__(self,name,value):
        if getattr(self,'_sealed',False):raise AttributeError('Original managed selection is immutable')
        object.__setattr__(self,name,value)
    """Minted held local selection; native/producer admission remains separate."""
    def __init__(self,spec,lock_fd,*,_mint=None):
        _require(_mint is _MINT and type(spec)is ManagedPublicationSelectionSpec);spec.__post_init__()
        self.spec=spec;self._lock=lock_fd;self._lock_identity=(os.fstat(lock_fd).st_dev,os.fstat(lock_fd).st_ino);self._active=True
        self.files=tuple(p.read()for p in (spec.installation,spec.intake_proof,spec.source_configuration))
        self.installation_json,self.intake_proof_json=tuple(f.raw.decode()for f in self.files[:2])
        configured=_json(self.files[2].raw);_require(type(configured)is dict)
        # Bound every original reference before the existing public loader reads it.
        self.references=tuple(ManagedSelectionFile((self.files[2].path.parent/configured[name]['path']).resolve(),configured[name]['sha256']).read()for name in ('source','schema','intake','interpretation'))
        self.configuration=load_jsonl_configuration(self.files[2].path)
        _require(type(self.configuration)is JsonlConfiguration)
        for f in (*self.files,*self.references):f.renew(262144)
        self.batches=tuple(jsonl_batches(self.references[0].raw.splitlines(keepends=True),feed=self.configuration.feed,epoch=self.configuration.epoch))
        _require(1<=len(self.batches)<=4 and spec.completed_ordinal<=len(self.batches))
        self._journal_identity,self._baseline=_journal(spec.journal,spec.maximum_journal_bytes,spec.maximum_rows)
        self._select(self._baseline)
        self._baseline_json=_encoded(self._baseline);self._sealed=True
    def _select(self,journal):
        c=self.configuration;installation=_json(self.installation_json);proof=_json(self.intake_proof_json);namespace=installation.get('namespace')
        _require(type(namespace)is str and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',namespace)is not None and installation.get('state')=='installed')
        tables=installation.get('tables');_require(type(tables)is dict and set(tables)=={namespace+'.'+r for r in (*ROLES,'publication_manifest','publication_attempt_phase','source_record','property_journal')})
        uuids={t:v['uuid']for t,v in tables.items()};_require(len(set(uuids.values()))==len(uuids)and all(str(UUID(v))==v for v in uuids.values()))
        _require((proof['source_sha256'],proof['artifact_sha256'],proof['revision'],proof['validator_revision'])==(c.intake.source_sha256,c.intake.artifact_sha256,c.intake.document_revision,c.intake.validator_revision))
        _require(proof['table']==namespace+'.schema_intake'and type(proof['table_uuid'])is str and str(UUID(proof['table_uuid']))==proof['table_uuid']and proof['table_uuid']not in uuids.values())
        stream='native-configured-jsonl-stream:'+namespace;feed=c.feed;epoch=c.epoch;sha=hashlib.sha256(self.references[0].raw).hexdigest()
        _require(journal['jsonl_consumer_scope']==[(stream,feed,epoch,sha,_encoded({'feed':feed,'epoch':epoch}))])
        workload={'namespace':namespace,'source_sha256':sha,'uuids':uuids,'intake_source_sha256':c.intake.source_sha256,'intake_table_uuid':proof['table_uuid'],'source_configuration_sha256':hashlib.sha256(c.original).hexdigest()}
        _require(journal['stream_scope']==[(1,hashlib.sha256(_encoded(workload).encode()).hexdigest())])
        clock=retained_materialization_clock(journal['submission'],journal['stream_scope'][0][1])
        self.original_materialization_clock_us=clock.microseconds;self.original_actor_id=clock.actor_id;self.original_warehouse_id=clock.warehouse_id
        progress=journal['jsonl_consumer_progress'];_require(len(progress)==self.spec.completed_ordinal)
        expected_artifacts=set();expected_snapshots=set()
        state=empty_state();predecessor='empty-native-configured-jsonl-stream';selected=None
        for ordinal,(batch,row)in enumerate(zip(self.batches,progress),1):
            s,f,e,position,text,digest=row;_require((s,f,e,position)==(stream,feed,epoch,int(batch.cursor_after))and hashlib.sha256(text.encode()).hexdigest()==digest)
            value=_json(text);_require(type(value)is dict and set(value)=={'request','descriptor'});request=value['request'];verify_request_digest(request);staged=batch_row(batch)
            _require(request['stream']==stream and request['batch_id']==batch.batch_id and request['predecessor']==predecessor and request['source_checkpoint_json']==jsonl_checkpoint(batch)and request['source_batch_json']==staged['batch_json']and request['source_batch_digest']==staged['batch_digest']and request['schema_revisions_json']==_encoded({c.schema_alias:c.intake.document_revision}))
            expected_artifacts.add('publisher-effects:'+namespace+':'+request['request_digest']);expected_snapshots.add('stream-artifact:'+request['request_digest'])
            publication='configured-jsonl-stream-'+request['request_digest'];_require(value['descriptor']['publication_id']==publication)
            artifacts=[v for v in journal['publisher_effect_artifact']if v[0]=='publisher-effects:'+namespace+':'+request['request_digest']]
            _require(len(artifacts)==1);artifact=artifacts[0]
            _require(artifact[1]==_encoded(request)and type(artifact[3])is str and hashlib.sha256(artifact[3].encode()).hexdigest()==artifact[4])
            applied,descriptor=validate_applied_artifact(artifact[3],request);_require(dict(descriptor.raw)==value['descriptor'])
            bind_source_descriptor(request,descriptor,expected_publication_id=publication)
            _require(set(descriptor.versions)=={namespace+'.'+r for r in ROLES})
            report=descriptor.validation_report;_require(report.get('source_sha256')==sha and report.get('source_groups')==ordinal and report.get('intake_source_sha256')==c.intake.source_sha256 and report.get('intake_artifact_sha256')==c.intake.artifact_sha256)
            snapshots=[v for v in journal['snapshot_artifact']if v[0]=='stream-artifact:'+request['request_digest']];_require(len(snapshots)==1);snapshot=snapshots[0]
            _require(snapshot[1]==_encoded(request)and snapshot[4]==artifact[3]and snapshot[5]==artifact[4]and hashlib.sha256(_encoded(list(snapshot[1:4])).encode()).hexdigest()==snapshot[6])
            _require(_json(snapshot[2])==applied['effects']);targets=_json(snapshot[3]);_require(set(targets)==set(descriptor.versions))
            for table,target in targets.items():_require(target['uuid']==uuids[table]and target['version']==descriptor.versions[table])
            state=plan_apply(state,changes_from_batch(batch),schema_policy=c.policy);predecessor=publication;selected=request,descriptor,targets
        _require(selected is not None and selected[1].publication_id==self.spec.publication_id)
        _require(len(journal['publisher_effect_artifact'])==len(expected_artifacts)and {r[0]for r in journal['publisher_effect_artifact']}==expected_artifacts
            and len(journal['snapshot_artifact'])==len(expected_snapshots)and {r[0]for r in journal['snapshot_artifact']}==expected_snapshots)
        # Any interrupted native submission is not a completed read selection.
        for operation,request,digest,handle,response in journal['submission']:_require(_json(request).get('authority')==clock.actor_id and _json(request).get('body',{}).get('warehouse_id')==clock.warehouse_id and type(handle)is str and bool(handle)and type(response)is str and hashlib.sha256(request.encode()).hexdigest()==digest and _json(response).get('statement_id')==handle and _json(response).get('status',{}).get('state')=='SUCCEEDED')
        _require(max(len(state.current),len(state.history),len(state.tombstones))<=self.spec.maximum_rows)
        self.namespace=namespace;self.request_json=_encoded(selected[0]);self.descriptor=selected[1];self.targets_json=_encoded(selected[2]);self.expected_state=state
        self.table_uuids=tuple(sorted((t,uuids[t])for t in selected[1].versions))
    def renew(self):
        _require(self._active and type(self.spec)is ManagedPublicationSelectionSpec)
        lock=self.spec.journal.with_name(self.spec.journal.name+'.native-csv-stream-lock')
        _require(_identity(lock,1048576)==self._lock_identity and (os.fstat(self._lock).st_dev,os.fstat(self._lock).st_ino)==self._lock_identity)
        for f in (*self.files,*self.references):f.renew(262144)
        self.configuration.verify()
        fresh=load_jsonl_configuration(self.files[2].path)
        _require(type(self.configuration)is JsonlConfiguration and type(self.configuration.policy)is type(fresh.policy)and vars(self.configuration.policy)==vars(fresh.policy))
        for name in ('original','path','intake','source','schema','feed','epoch','schema_alias','references'):
            _require(getattr(self.configuration,name)==getattr(fresh,name))
        batches=tuple(jsonl_batches(self.references[0].raw.splitlines(keepends=True),feed=fresh.feed,epoch=fresh.epoch))
        _require(batches==self.batches)
        expected=empty_state()
        for batch in batches[:self.spec.completed_ordinal]:expected=plan_apply(expected,changes_from_batch(batch),schema_policy=fresh.policy)
        _require(expected==self.expected_state)
        identity,current=_journal(self.spec.journal,self.spec.maximum_journal_bytes,self.spec.maximum_rows)
        _require(identity==self._journal_identity and _encoded(current)==self._baseline_json)


@contextmanager
def open_managed_publication_selection(spec):
    _require(type(spec)is ManagedPublicationSelectionSpec);spec.__post_init__()
    path=spec.journal.with_name(spec.journal.name+'.native-csv-stream-lock');identity=_identity(path,1048576)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);owner=None;primary=None
    try:
        _require((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==identity)
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        owner=ManagedPublicationSelection(spec,fd,_mint=_MINT);owner.renew();yield owner
    except BaseException as e:primary=e
    def close_owner():
        if owner is not None:
            try:owner.renew()
            finally:object.__setattr__(owner,'_active',False)
    finish(primary,[close_owner,lambda:fcntl.flock(fd,fcntl.LOCK_UN),lambda:os.close(fd)])
