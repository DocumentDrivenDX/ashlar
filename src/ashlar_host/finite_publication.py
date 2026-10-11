"""Finite immutable-file publication over shared durable native lifecycle ports.

This source profile has local replay receipts, never a PostgreSQL checkpoint or
protected ACK. Current native writer/retention admission remains a mandatory,
independently supplied owner; correspondence and native row equality cannot grant it.
"""
from contextlib import contextmanager
from collections import Counter
import fcntl,json,re
from ashlar.apply import empty_state
from ashlar.native import quote_table_identifier
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.stored_publisher import StoredPublisherBackend,validate_applied_artifact
from .delta_custody import LocalDeltaEffects,encoded,sha
from .driver import AttemptExecutor,CarrierPolicy,ManifestPort,local_effect_plan
from .graph_sql import graph_sql_plan
from .lifecycle import finish,owned_context
from .supply_chain_finite_source import FiniteSupplyChainSource
from .finite_dataset import FinitePackSource
from .finite_projection import original_finite_pack_oracle
from .supply_chain_projection import original_supply_chain_oracle,CLOCK
from .supply_chain_request import source_snapshot

GRAPH_ROLES={'object_current','edge_current','tombstone','whole_source_history'}

class FiniteFileDriver:
    """Fresh original finite source and exact replay; no substitute recovery plans.

    The transport retains effect handles and generic metadata mutation recovery.
    The independently supplied current_admission must admit the exact source,
    request, complete manifest/pins and current writer/retention interval.
    """
    def __init__(self,transport,policy,context,tables,columns,source,publication_id,*,current_admission):
        if type(source)not in (FiniteSupplyChainSource,FinitePackSource) or not callable(current_admission):
            raise ValueError('Owned finite source and independent current admission required')
        if type(tables)is not dict or set(tables)!=GRAPH_ROLES|{'attempts','manifest'}or len(set(tables.values()))!=6:
            raise ValueError('Complete distinct native role inventory required')
        if type(publication_id)is not str or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:-]{0,127}',publication_id):
            raise ValueError('Explicit bounded publication identity required')
        self.transport,self.policy,self.context=transport,policy,context
        if type(columns)is not dict or set(columns)!=GRAPH_ROLES or any(type(schema)is not tuple or any(type(pair)is not tuple or len(pair)!=2 or any(type(v)is not str for v in pair)for pair in schema)for schema in columns.values()):
            raise ValueError('Owned exact baseline projection columns required')
        self.tables=source_snapshot(tables);self.columns={r:tuple(tuple(pair)for pair in schema)for r,schema in columns.items()};self.source=source
        self.publication_id=publication_id;self.current_admission=current_admission;self.held=False
        self.request=None;self.manifest=None
        self.source_system=source.batch.feed
        self.expected=(original_supply_chain_oracle(source.model,source.graph,source.bindings,source.batch,columns)if type(source)is FiniteSupplyChainSource else original_finite_pack_oracle(source.definition,source.model,source.graph,source.bindings,source.batch,columns))
    def require(self,context):
        if context is not self.context or not self.held:raise PermissionError('Original finite writer interval required')
        self.source.metadata()
    def admit_current(self,manifest=None):
        self.source.metadata()
        if self.current_admission(self.source,self.request,manifest,self.context)is not None:
            raise PermissionError('Independent current finite source/native admission incomplete')
        self.source.metadata()
    @contextmanager
    def writer(self,stream,context):
        if context is not self.context or self.held or self.request is None or stream!=self.request['stream']:
            raise PermissionError('Exact original finite writer required')
        primary=None
        with owned_context((self.transport.journal_path.parent/'.finite-source-writer.lock').open('a'))as lock:
            fcntl.flock(lock,fcntl.LOCK_EX);self.held=True
            try:
                self.source.renew();self.admit_current();yield
            except BaseException as error:primary=error
            def close():
                self.source.renew();self.admit_current(self.manifest)
            def release():
                self.held=False;fcntl.flock(lock,fcntl.LOCK_UN)
            finish(primary,[close,release])
    def _rows(self,role,version):
        columns=self.columns[role]
        if any(type(name)is not str or not re.fullmatch('[a-z_][a-z0-9_]*',name)or family not in ('STRING','BIGINT','TIMESTAMP')for name,family in columns):
            raise ValueError('Exact baseline native column inventory required')
        sql=','.join(('CAST(unix_micros('+name+') AS STRING)'if family=='TIMESTAMP'else 'CAST('+name+' AS STRING)')+' AS '+name for name,family in columns)
        table=quote_table_identifier(self.tables[role]);bound=len(self.expected[role])+1
        result=self.transport.query('SELECT '+sql+' FROM '+table+' VERSION AS OF '+str(version)+' LIMIT '+str(bound),{}).rows
        if len(result)>=bound:raise ValueError('Complete original finite native bag capacity exceeded')
        return [dict(row)for row in result]
    def _pins(self):
        versions={}
        for role in sorted(GRAPH_ROLES):
            target=self.transport.targets[self.tables[role]]
            detail=self.transport.query('DESCRIBE DETAIL '+quote_table_identifier(target.table),{}).rows
            if len(detail)!=1 or detail[0]['id']!=target.uuid:raise ValueError('Original native UUID differs')
            history=self.transport.original_history(target);version=history[0]['version']
            if type(version)is not int or version<0:raise ValueError('Observed exact native version required')
            versions[target.table]=version
        return versions
    def _check(self,versions,expected):
        for role in sorted(GRAPH_ROLES):
            rows=self._rows(role,versions[self.tables[role]])
            if Counter(map(encoded,rows))!=Counter(map(encoded,expected[role])):
                raise ValueError('Independent complete original native projection differs: '+role)
    def prepare_original(self,request):
        if self.request is not None:raise ValueError('Original finite driver already selected')
        request=source_snapshot(request);self.source.admit_request(request);self.request=request
        with self.writer(request['stream'],self.context):
            prior=self._pins();empty={role:[]for role in GRAPH_ROLES};self._check(prior,empty)
        _,generated=graph_sql_plan(empty_state(),self.source.batch,{r:self.tables[r]for r in GRAPH_ROLES},materialized_at=CLOCK,schema_policy=self.source.admit)
        steps,elisions=local_effect_plan(generated,empty,{r:self.tables[r]for r in GRAPH_ROLES})
        self.policy.active={'request':request,'steps':steps,'expected':self.expected,'manifest':None}
        self.prior=prior;self.steps=steps;self.elisions=elisions
    def restore_committed(self,request):
        """Read the original complete committed native phases; never apply effects."""
        if self.request is not None:raise ValueError('Original finite driver already selected')
        request=source_snapshot(request);self.source.admit_request(request);self.request=request
        with self.writer(request['stream'],self.context):
            target=self.transport.targets[self.tables['attempts']]
            store=DeltaAttemptStore(AttemptExecutor(self),CarrierPolicy(self,'attempts'),target.table,target.uuid)
            with store.session(self.context)as session:records=session.read(request['stream'],request['batch_id'])
            if len(records)!=5 or records[-1].phase!='committed':
                raise ValueError('Complete original committed phases required; no substitute effects')
            retained=json.loads(records[-1].payload_json)
            if retained['request']!=request:raise ValueError('Original committed request differs')
            artifact,descriptor=validate_applied_artifact(retained['result_json'],request)
            if json.loads(retained['descriptor_json'])!=dict(descriptor.raw):raise ValueError('Original retained committed descriptor differs')
            self.manifest=source_snapshot(artifact['manifest'])
            if self.manifest['publication_id']!=self.publication_id or json.loads(self.manifest['validation_report_json']).get('source_admission')!=self.source.metadata():
                raise ValueError('Original committed finite source/identity differs')
            self.policy.active={'request':self.request,'manifest':self.manifest}
            self.validate_manifest(self.manifest,self.context)
        return descriptor
    def _apply(self,request,context,recovery):
        self.require(context);self.source.admit_request(request)
        if request!=self.request:raise ValueError('Original finite request differs')
        self.admit_current()
        if not recovery:self._check(self.prior,{role:[]for role in GRAPH_ROLES})
        effects=LocalDeltaEffects(self.transport)
        proof=(effects.recover if recovery else effects.run)('effects:'+request['request_digest'],request['request_digest'],self.steps,context=context)
        versions=self._pins();self._check(versions,self.expected)
        progress={self.source_system:{'profile':'ashlar-immutable-file-replay/0.1','source_transaction_sha256':self.source.metadata()['source_transaction_sha256'],'request_digest':request['request_digest']}}
        manifest={'publication_id':self.publication_id,'profile_version':'ashlar-delta/0.3','table_versions_json':encoded(versions),'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':encoded(progress),
            'validation_report_json':encoded({'complete':True,'request_digest':request['request_digest'],'predecessor':request['predecessor'],'source_admission':self.source.metadata(),'source_oracle_sha256':sha(encoded(self.expected)),'zero_match_elisions':self.elisions,'ack_profile':'immutable-file-replay-only'}),'recorded_at':'1791547200000000'}
        if self.manifest is not None and self.manifest!=manifest:raise ValueError('Original finite proposal changed')
        self.manifest=manifest;self.policy.active['manifest']=manifest;self.admit_current(manifest)
        return encoded({'effects':proof,'manifest':manifest})
    def apply(self,request,context):return self._apply(request,context,False)
    def recover_apply(self,request,context):return self._apply(request,context,True)
    def validate_manifest(self,row,context):
        self.require(context)
        if self.manifest is None or source_snapshot(row)!=self.manifest:raise ValueError('Original finite manifest differs')
        self.source.admit_request(self.request);self.admit_current(row)
        self._check(json.loads(row['table_versions_json']),self.expected)
    def validate(self,request,artifact,descriptor,context):
        if request!=self.request or json.loads(artifact)['manifest']!=dict(descriptor.raw):raise ValueError('Original finite applied artifact differs')
        self.validate_manifest(dict(descriptor.raw),context)
    def acknowledge(self,request,descriptor,context):
        """Renew original finite-file receipt; never emits a protected ACK."""
        if request!=self.request:raise ValueError('Original finite replay receipt request differs')
        self.validate_manifest(dict(descriptor.raw),context);self.source.renew();self.admit_current(dict(descriptor.raw))
    def backend(self):
        target=self.transport.targets[self.tables['attempts']]
        return StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(self),CarrierPolicy(self,'attempts'),target.table,target.uuid),self,lambda request,context:ManifestPort(self,request))
