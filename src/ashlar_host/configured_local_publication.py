# SPDX-License-Identifier: MIT OR Apache-2.0
"""Sequential local Delta publication of an admitted configured STRING source.

Independent current source/writer/retention admission is mandatory. Complete bag
parity is correspondence, never that authority; local progress is not source ACK.
"""
from contextlib import contextmanager
import datetime,fcntl,json,os,stat
from pathlib import Path
from ashlar.native import NativeBackend,quote_table_identifier
from ashlar.publication import resolve_publication
from ashlar.publisher import publish_batch
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.stored_publisher import StoredPublisherBackend,validate_applied_artifact
from ashlar.effect_validation import validate_effect_snapshot
from ashlar.source_checkpoint import bind_source_descriptor
from .configured_source_local import ConfiguredLocalSource
from .managed_carriers import load_managed_carriers
from .managed_source_oracle import configured_prefix_carrier_rows
from .delta_custody import LocalDeltaTransport,LocalDeltaEffects,encoded,sha
from .driver import AttemptExecutor,CarrierPolicy,ManifestPort
from .publication_planning import local_effect_plan
from .graph_sql import graph_sql_plan
from .file_progress import JournaledJsonlProgress
from .lifecycle import finish,owned_context

ROLES=('object_current','edge_current','tombstone','whole_source_history')
def _require(value):
    if not value:raise ValueError('Original configured local publication refused')

class ConfiguredLocalPublicationDriver:
    def __init__(self,transport,policy,source,*,tables,context,materialization_clock_us,current_admission):
        _require(type(transport)is LocalDeltaTransport and type(source)is ConfiguredLocalSource and source.context is context and transport.policy is policy and callable(current_admission))
        _require(type(tables)is dict and set(tables)==set(ROLES)|{'attempts','manifest'}and len(set(tables.values()))==6)
        for t in tables.values():quote_table_identifier(t);_require(t in transport.targets)
        _require(type(materialization_clock_us)is str and materialization_clock_us.isascii()and materialization_clock_us.isdecimal()and len(materialization_clock_us)<=19 and str(int(materialization_clock_us))==materialization_clock_us and int(materialization_clock_us)<2**63)
        instant=datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)+datetime.timedelta(microseconds=int(materialization_clock_us))
        self.transport=transport;self.policy=policy;self.source=source;self.context=context;self.tables=dict(tables);self.clock=materialization_clock_us;self.instant=instant.isoformat();self.current_admission=current_admission
        self.carriers=load_managed_carriers();self.columns={r:self.carriers.ordered_columns(r)for r in ROLES};self._column_witness=encoded(self.columns)
        self.held=False;self.request=None;self.manifest=None;self.ordinal=None;self.expected=None;self.steps=None
        self._witness=(transport,policy,source,context,current_admission,encoded(tables),materialization_clock_us)
        self.progress=None;self._selected=None
        self._target_witness=tuple((r,transport.targets[tables[r]].table,transport.targets[tables[r]].uuid,transport.targets[tables[r]].path)for r in tables)
    def _correspondence(self):
        _require((self.transport,self.policy,self.source,self.context,self.current_admission,encoded(self.tables),self.clock)==self._witness)
        _require(tuple((r,self.transport.targets[self.tables[r]].table,self.transport.targets[self.tables[r]].uuid,self.transport.targets[self.tables[r]].path)for r in self.tables)==self._target_witness)
        if self._selected is not None:_require(encoded([self.ordinal,self.publication_id,self.request,self.expected,self.steps,self.elisions])==self._selected)
        _require(encoded(self.columns)==self._column_witness and self.columns=={r:self.carriers.ordered_columns(r)for r in ROLES})
        self.source.renew(self.context)
    def admit_current(self,manifest=None):
        self._correspondence()
        _require(self.current_admission(self.source,self.request,manifest,self.context)is None)
        self._correspondence()
    def require(self,context):
        _require(context is self.context and self.held);self._correspondence()
    @contextmanager
    def writer(self,stream,context):
        _require(context is self.context and not self.held and self.request is not None and stream==self.source.stream)
        path=self.transport.journal_path.parent/'.configured-source-writer.lock'
        parent=path.parent.lstat();_require(stat.S_ISDIR(parent.st_mode)and parent.st_uid==os.getuid()and not parent.st_mode&0o022)
        fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600);primary=None;held=False
        opened=None
        def lock_current():
            named=path.lstat();current_parent=path.parent.lstat();held_file=os.fstat(fd)
            _require(stat.S_ISREG(named.st_mode)and named.st_uid==os.getuid()and not named.st_mode&0o022
                     and stat.S_ISDIR(current_parent.st_mode)and current_parent.st_uid==os.getuid()and not current_parent.st_mode&0o022
                     and (named.st_dev,named.st_ino)==(held_file.st_dev,held_file.st_ino)==(opened.st_dev,opened.st_ino)
                     and (current_parent.st_dev,current_parent.st_ino)==(parent.st_dev,parent.st_ino))
        try:
            opened=os.fstat(fd);_require(stat.S_ISREG(opened.st_mode)and opened.st_uid==os.getuid()and not opened.st_mode&0o022)
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);held=True;self.held=True
            lock_current();self.admit_current(self.manifest);yield
        except BaseException as exc:primary=exc
        def release():
            self.held=False
            if held:fcntl.flock(fd,fcntl.LOCK_UN)
        finish(primary,[*([self._correspondence,lambda:self.admit_current(self.manifest),lock_current]if held else []),release,lambda:os.close(fd)])
    def _oracle(self,count):
        if count==0:return {r:[]for r in ROLES}
        return configured_prefix_carrier_rows(self.source.configuration,self.source.batches[:count],materialization_clock_us=self.clock,carriers=self.carriers,maximum_rows=1000).rows()
    def _pins(self):
        versions={}
        for role in ROLES:
            target=self.transport.targets[self.tables[role]]
            detail=self.transport.query('DESCRIBE DETAIL '+quote_table_identifier(target.table),{}).rows
            _require(len(detail)==1 and detail[0].get('id')==target.uuid and detail[0].get('location','').removeprefix('file:').rstrip('/')==str(target.path))
            history=self.transport.original_history(target);_require(bool(history)and type(history[0]['version'])is int and history[0]['version']>=0);versions[target.table]=history[0]['version']
        return versions
    def _check(self,versions,expected):
        _require(set(versions)=={self.tables[r]for r in ROLES})
        finish(None,[lambda r=r:validate_effect_snapshot(self.transport,self.tables[r],self.transport.targets[self.tables[r]].uuid,versions[self.tables[r]],self.columns[r],expected[r])for r in ROLES])
    def _scope(self):
        db=self.transport.db;facts=encoded({'source':self.source.metadata(self.context),'clock_us':self.clock,'tables':self.tables})
        exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='configured_local_source_scope'").fetchone()
        if not exists:
            _require(self.ordinal==1 and not db.execute('SELECT 1 FROM local_plan LIMIT 1').fetchone())
            with db:
                db.execute('CREATE TABLE configured_local_source_scope (stream TEXT PRIMARY KEY,facts TEXT NOT NULL)')
                db.execute('INSERT INTO configured_local_source_scope VALUES (?,?)',(self.source.stream,facts))
        _require(db.execute('SELECT stream,facts FROM configured_local_source_scope').fetchall()==[(self.source.stream,facts)])
    def select_original(self,ordinal):
        """Select exactly next or last completed batch; never infer native commit."""
        _require(self.request is None);spec=self.source.spec(ordinal,self.context)
        self.ordinal=ordinal;self.request=spec.request();self.publication_id=spec.publication_id
        self.expected=self._oracle(ordinal);before=self._oracle(ordinal-1)
        _,generated=graph_sql_plan(self.source.state_at(ordinal-1,self.context),self.source.batches[ordinal-1],{r:self.tables[r]for r in ROLES},materialized_at=self.instant,schema_policy=self.source.configuration.policy)
        self.steps,self.elisions=local_effect_plan(generated,before,{r:self.tables[r]for r in ROLES})
        self._selected=encoded([self.ordinal,self.publication_id,self.request,self.expected,self.steps,self.elisions])
        self.policy.active={'request':self.request,'steps':self.steps,'expected':self.expected,'manifest':None}
        with self.writer(self.source.stream,self.context):
            self._scope()
            if self.progress is None:
                self.progress=JournaledJsonlProgress(self.transport,self.source.configuration.source,self.source.metadata(self.context)['source_sha256'],self,self._resolved,stream=self.source.stream,feed=self.source.configuration.feed,epoch=self.source.configuration.epoch)
            entries=self.progress.retained_entries();_require(ordinal in (len(entries),len(entries)+1))
            for i,entry in enumerate(entries,1):
                _require(entry.request()==self.source.spec(i,self.context).request()and entry.descriptor.publication_id==self.source.spec(i,self.context).publication_id)
            self.replay=ordinal==len(entries)
            target=self.transport.targets[self.tables['attempts']]
            store=DeltaAttemptStore(AttemptExecutor(self),CarrierPolicy(self,'attempts'),target.table,target.uuid)
            with owned_context(store.session(self.context))as session:records=session.read(self.source.stream,self.request['batch_id'])
            for record in records:_require(json.loads(record.payload_json)['request']==self.request)
            interrupted=bool(records and records[-1].phase in ('applying','applied','committing','committed'))
            effects=LocalDeltaEffects(self.transport)
            if interrupted:
                # The actual retained whole-plan/ordinal/handle owner must admit
                # original recovery. Observed after-state alone grants nothing.
                effects.observe('effects:'+self.request['request_digest'],self.request['request_digest'],self.steps,context=self.context)
                if records[-1].phase in ('applied','committing','committed'):
                    artifact,descriptor=validate_applied_artifact(json.loads(records[-1].payload_json)['result_json'],self.request)
                    _require(descriptor.publication_id==self.publication_id)
                    self.manifest=artifact['manifest'];self.policy.active['manifest']=self.manifest
            if entries:
                previous=entries[-1].descriptor
                self._admit_retained(previous,self._oracle(len(entries)))
                if not interrupted:_require(self._pins()==dict(previous.versions))
            elif not interrupted:self._check(self._pins(),before)
            if not interrupted:
                # Freeze exact original operation identities before the portable
                # coordinator can durably advance to applying.
                effects.prepare('effects:'+self.request['request_digest'],self.request['request_digest'],self.steps,context=self.context)
        return spec
    def _admit_retained(self,descriptor,expected):
        self.admit_current(dict(descriptor.raw));self._check(dict(descriptor.versions),expected)
        table=self.tables['manifest'];target=self.transport.targets[table]
        class Policy:
            def authorize(_,context,publication_id,tables):_require(context is self.context);self.admit_current(dict(descriptor.raw))
            def validate_descriptor(_,value,context):_require(value==descriptor and context is self.context);self.admit_current(dict(value.raw))
            def validate_snapshot(_,table,uuid,version,columns):_require(uuid==self.transport.targets[table].uuid and version==descriptor.versions[table])
        backend=NativeBackend(self.transport,Policy(),table,target.uuid,{self.tables[r]:self.columns[r]for r in ROLES})
        actual=resolve_publication(backend,descriptor.publication_id,{self.tables[r]:self.transport.targets[self.tables[r]].uuid for r in ROLES},context=self.context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={self.source.configuration.schema_alias:[self.source.configuration.document_revision]})
        _require(actual.descriptor==descriptor)
    @contextmanager
    def _resolved(self,descriptor,context):
        self.require(context);primary=None
        try:self._admit_retained(descriptor,self.expected);yield descriptor
        except BaseException as exc:primary=exc
        finish(primary,[lambda:self._admit_retained(descriptor,self.expected)])
    def _apply(self,request,context,recovery):
        self.require(context);self.source.admit_request(request,context);_require(request==self.request and not self.replay)
        self.admit_current()
        if not recovery:self._check(self._pins(),self._oracle(self.ordinal-1))
        effects=LocalDeltaEffects(self.transport)
        proof=(effects.recover if recovery else effects.run)('effects:'+request['request_digest'],request['request_digest'],self.steps,context=context)
        versions=self._pins();self._check(versions,self.expected)
        manifest={'publication_id':self.publication_id,'profile_version':'ashlar-delta/0.3','table_versions_json':encoded(versions),'schema_revisions_json':request['schema_revisions_json'],
                  'source_progress_json':encoded({self.source.configuration.feed:json.loads(request['source_checkpoint_json'])}),
                  'validation_report_json':encoded({'complete':True,'request_digest':request['request_digest'],'predecessor':request['predecessor'],'source_admission':self.source.metadata(context),'source_oracle_sha256':sha(encoded(self.expected)),'zero_match_elisions':self.elisions,'ack_profile':'immutable-file-replay-only'}),'recorded_at':self.clock}
        _require(self.manifest is None or self.manifest==manifest);self.manifest=manifest;self.policy.active['manifest']=manifest;self.admit_current(manifest)
        return encoded({'effects':proof,'manifest':manifest})
    def apply(self,request,context):return self._apply(request,context,False)
    def recover_apply(self,request,context):return self._apply(request,context,True)
    def validate_manifest(self,row,context):
        self.require(context);_require(self.manifest is not None and row==self.manifest);self.admit_current(row);self._check(json.loads(row['table_versions_json']),self.expected)
    def validate(self,request,artifact,descriptor,context):
        self.require(context);self.source.admit_request(request,context);value,original=validate_applied_artifact(artifact,request)
        _require(request==self.request and original==descriptor and value['manifest']==self.manifest);self.validate_manifest(value['manifest'],context)
    def admit(self,request,descriptor,context):
        """Current admission for descriptor-bound local progress, not remote ACK."""
        self.require(context);_require(request==self.request and descriptor.publication_id==self.publication_id)
        bind_source_descriptor(request,descriptor,expected_publication_id=self.publication_id);self.validate_manifest(dict(descriptor.raw),context)
    def acknowledge(self,request,descriptor,context):
        self.require(context);self.admit(request,descriptor,context);self.progress.acknowledge(request,descriptor,context)
    def backend(self):
        _require(self.request is not None and self.progress is not None)
        target=self.transport.targets[self.tables['attempts']]
        return StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(self),CarrierPolicy(self,'attempts'),target.table,target.uuid),self,lambda request,context:ManifestPort(self,request))
    def publish_selected(self):
        _require(self.request is not None and self.progress is not None)
        if self.replay:
            self.manifest=dict(self.progress.retained_entries()[-1].descriptor.raw);self.policy.active['manifest']=self.manifest
        descriptor=publish_batch(self.backend(),self.source.stream,self.source.batches[self.ordinal-1],predecessor=self.request['predecessor'],schema_revisions_json=self.request['schema_revisions_json'],source_checkpoint_json=self.request['source_checkpoint_json'],context=self.context)
        _require(self.progress.completed_batches()==self.ordinal)
        return descriptor
