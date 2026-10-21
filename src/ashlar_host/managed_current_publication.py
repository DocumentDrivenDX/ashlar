# SPDX-License-Identifier: MIT OR Apache-2.0
"""Injected-Spark Unity Catalog transport and bounded current publication owner.

This module never starts Spark, connects to a warehouse, or ACKs a Truss page.
The task owner supplies its one serverless SparkSession and a separately
reviewed native protocol profile. Source ACK remains with the external holder.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import json
import re
from ashlar.native import SQLResult,quote_table_identifier
from ashlar.protocol import ReaderProtocolProfile,validate_protocol_detail
from .managed_current_core import ROLES
from .managed_current_carriers import load_current_managed_carriers

_IDENT=re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,127}\Z')

def _require(value,reason='Managed current publication transport refused'):
    if not value:raise ValueError(reason)

@dataclass(frozen=True)
class ManagedTableObservation:
    role:str
    name:str
    uuid:str
    version:int
    schema:tuple
    protocol:dict
    detail_digest:str

class ManagedCurrentSparkTransport:
    """Parameter-bound SQL transport over the caller-owned SparkSession."""
    def __init__(self,spark,*,maximum_rows=100000):
        _require(spark is not None and type(maximum_rows)is int and 1<=maximum_rows<=1000000)
        self.spark=spark;self.maximum_rows=maximum_rows
    def query(self,sql,parameters):
        _require(type(sql)is str and sql and type(parameters)is dict
            and all(type(k)is str and type(v)is str for k,v in parameters.items()),
            'Exact parameterized Spark SQL and string bindings required')
        frame=self.spark.sql(sql,args=dict(parameters))
        rows=frame.collect()
        _require(len(rows)<=self.maximum_rows,'Managed Spark result exceeds explicit row bound')
        # Read schema after collect too; this matches the complete adapter path
        # used by DeltaAttemptStore and preserves declared result columns.
        fields=tuple((field.name,field.dataType.simpleString()) for field in frame.schema.fields)
        result=[]
        for row in rows:
            value=row.asDict(recursive=True)
            _require(type(value)is dict and set(value)=={name for name,_ in fields})
            result.append(value)
        return SQLResult(tuple(result),fields)
    def prepare(self,sql,parameters):
        self.query(sql,parameters)
        return None

class ManagedCurrentTargetOwner:
    """Create or observe exactly six managed Delta tables and renew their pins."""
    def __init__(self,spark,*,catalog,schema,protocol_profile):
        _require(type(catalog)is str and _IDENT.fullmatch(catalog) and type(schema)is str and _IDENT.fullmatch(schema))
        _require(type(protocol_profile)is ReaderProtocolProfile)
        self.spark=spark;self.catalog=catalog;self.schema=schema;self.profile=protocol_profile
        self.carriers=load_current_managed_carriers()
        self.names={role:(catalog,schema,role) for role in ROLES}
        self.tables={role:'.'.join(self.names[role]) for role in ROLES}
        self.transport=ManagedCurrentSparkTransport(spark)
        self.opening=None
    def _existing(self):
        namespace='`'+self.catalog+'`.`'+self.schema+'`'
        frame=self.spark.sql('SHOW TABLES IN '+namespace)
        names={row['tableName'] for row in frame.collect()}
        return names.intersection(ROLES)
    def initialize(self):
        existing=self._existing()
        _require(not existing,'Managed current schema must contain none of the six target roles')
        for role in ROLES:
            self.spark.sql(self.carriers.create_sql(role,self.names[role]))
        self.opening=self.observe()
        return self.opening
    def open_existing(self):
        _require(self._existing()==set(ROLES),'All six exact current managed roles required')
        self.opening=self.observe()
        return self.opening
    def observe(self):
        observations={}
        for role in ROLES:
            table=self.tables[role];quoted=quote_table_identifier(table)
            detail_rows=self.transport.query('DESCRIBE DETAIL '+quoted,{}).rows
            _require(len(detail_rows)==1,'Unique native UC table detail required')
            detail=dict(detail_rows[0]);table_uuid=detail.get('id')
            _require(type(table_uuid)is str and table_uuid,'Native UC table UUID required')
            _require(detail.get('format')=='delta','Native Delta format required')
            type_rows=self.transport.query('SELECT table_type FROM system.information_schema.tables '
                'WHERE table_catalog=:catalog AND table_schema=:schema AND table_name=:name',
                {'catalog':self.catalog,'schema':self.schema,'name':role}).rows
            _require(len(type_rows)==1 and type_rows[0].get('table_type')=='MANAGED',
                'Unity Catalog information schema must observe a managed table')
            actual=tuple((field.name,field.dataType.simpleString(),field.nullable)
                         for field in self.spark.table(table).schema.fields)
            native={'STRING':'string','BIGINT':'long','BOOLEAN':'boolean','TIMESTAMP':'timestamp'}
            expected=tuple((name,native[kind],nullable) for name,kind,nullable in self.carriers.ordered_fields(role))
            _require(actual==expected,'Current UMF-generated native table schema differs')
            protocol_detail=dict(detail)
            _require(type(protocol_detail.get('minReaderVersion'))is int
                     and type(protocol_detail.get('minWriterVersion'))is int
                     and type(protocol_detail.get('tableFeatures'))in (list,tuple),
                     'Native DESCRIBE DETAIL protocol fields have their documented types')
            protocol_detail['minReaderVersion']=str(protocol_detail['minReaderVersion'])
            protocol_detail['minWriterVersion']=str(protocol_detail['minWriterVersion'])
            protocol_detail['tableFeatures']=json.dumps(list(protocol_detail['tableFeatures']),separators=(',',':'))
            protocol=validate_protocol_detail(protocol_detail,uuid=table_uuid,profile=self.profile)
            history=self.transport.query('DESCRIBE HISTORY '+quoted+' LIMIT 1',{}).rows
            _require(len(history)==1 and type(history[0].get('version'))is int and history[0]['version']>=0,
                'Exact current Delta table head required')
            raw=json.dumps(detail,sort_keys=True,separators=(',',':'),default=str).encode()
            import hashlib
            observations[role]=ManagedTableObservation(role,table,table_uuid,history[0]['version'],actual,
                protocol,hashlib.sha256(raw).hexdigest())
        _require(len({item.uuid for item in observations.values()})==len(ROLES),
            'Distinct six-role Unity Catalog UUIDs required')
        return observations
    def closing(self):
        _require(self.opening is not None,'Opening managed UC inventory required')
        current=self.observe()
        for role in ROLES:
            before=self.opening[role];after=current[role]
            _require((before.uuid,before.schema,before.protocol)==(after.uuid,after.schema,after.protocol),
                'Managed target identity/schema/protocol changed during task')
        return current
    @staticmethod
    def publication_data_vector(inventory):
        """Four graph-data heads only; manifest/attempt are install custody."""
        data_roles=('object_current','edge_current','tombstone','whole_source_history')
        _require(type(inventory)is dict and set(inventory)==set(ROLES))
        return {inventory[role].name:inventory[role].version for role in data_roles}

class ManagedCurrentPublicationError(ValueError):
    pass


def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)


def _sha(text):
    import hashlib
    raw=text if type(text)is bytes else text.encode()
    return hashlib.sha256(raw).hexdigest()


def _normalize_native(value):
    from datetime import datetime,timezone
    if isinstance(value,datetime):
        if value.tzinfo is None:value=value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec='microseconds')
    return value


class ManagedCurrentGraphPublicationDriver:
    """One-page publisher over the exact six-role managed UC owner.

    It reconstructs the complete bounded source state from retained history,
    validates the current four-role vector against the latest manifest, and
    applies a canonical plan from that observed baseline. Applying/uncertain
    outcomes always refuse resubmission; only exact committed replay is
    supported in this bounded slice.
    """
    def __init__(self,target_owner,admission,*,stream,materialized_at,context):
        from contextlib import contextmanager
        _require(type(target_owner)is ManagedCurrentTargetOwner and admission.current
            and type(stream)is str and stream and type(materialized_at)is str
            and re.fullmatch(r'0|[1-9][0-9]{0,18}',materialized_at)
            and int(materialized_at)<2**63,'Bounded managed current Truss publication inputs required')
        self.targets=target_owner;self.transport=target_owner.transport;self.admission=admission
        self.stream=stream;self.clock=materialized_at;self.context=context;self.request=None
        self.plan=None;self.plan_json=None;self.manifest=None;self.held=False;self.publication_id=None
        self.table_roles=('object_current','edge_current','tombstone','whole_source_history')
        self._contextmanager=contextmanager
        cursor=json.loads(__import__('base64').b64decode(admission.batch.cursor_before,validate=True))
        _require(cursor.get('profile')=='truss-caller-asserted-feed/0.1',
            'Original managed Truss page cursor required')
        self.revisions={revision:__import__('hashlib').sha256(selected['report']).hexdigest()
            for revision,selected in admission.revisions.items()}
        self.schema_revisions_json=_canonical(self.revisions)
        self.names={role:'.'.join(target_owner.names[role]) for role in target_owner.names}
        self.data_tables={role:self.names[role] for role in self.table_roles}
        self.columns={role:target_owner.carriers.ordered_fields(role) for role in ROLES}
        self.policy=_ManagedCurrentPolicy(self)
    def _require_context(self,context):
        _require(context is self.context,'Original injected task context required')
    def _source_checkpoint(self):
        from ashlar.source_checkpoint import truss_checkpoint
        return truss_checkpoint(self.admission.batch)
    def _request(self):
        from .publication_planning import truss_request_for
        from types import MappingProxyType
        return truss_request_for(self.stream,self.admission.batch,'managed-current-genesis/0.1',self.revisions)
    def _rows(self,role):
        return self.transport.query('SELECT * FROM '+quote_table_identifier(self.names[role]),{}).rows
    def _baseline_rows(self):
        return {role:[{key:_normalize_native(value) for key,value in dict(row).items()}
                      for row in self._rows(role)] for role in self.table_roles}
    def _head(self,role):
        rows=self.transport.query('DESCRIBE HISTORY '+quote_table_identifier(self.names[role])+' LIMIT 1',{}).rows
        _require(len(rows)==1 and type(rows[0].get('version'))is int)
        return rows[0]['version']
    def _expected_rows(self,steps,baseline_rows):
        expected={role:[dict(row) for row in baseline_rows[role]] for role in self.table_roles}
        for step in steps:
            statement=step['statement']
            role=next((r for r,t in self.data_tables.items()
                       if statement.startswith('INSERT INTO '+quote_table_identifier(t)+' ')
                       or statement.startswith('MERGE INTO '+quote_table_identifier(t)+' ')),None)
            if role is None:raise ManagedCurrentPublicationError('Plan contains an unowned data-table effect')
            if statement.startswith('MERGE INTO '):
                _require(role in ('object_current','edge_current') and set(step['parameters'])=={'keys'})
                keys=json.loads(step['parameters']['keys'])
                identity='type_id' if role=='object_current' else 'rel_type_id'
                selected={(item['source_system'],int(item['type_id']),int(item['id'])) for item in keys}
                expected[role]=[row for row in expected[role] if
                    (row['source_system'],int(row[identity]),int(row['id'])) not in selected]
            else:
                _require(set(step['parameters'])=={'rows'},'Exact generated insert row parameter required')
                expected[role].extend(json.loads(step['parameters']['rows']))
        return {role:self._cast_expected(role,rows) for role,rows in expected.items()}

    def _prior_state_from_history(self,excluded=(),*,compare_current=True):
        from ashlar.apply import Change,EntityKey,EntityState,empty_state,plan_apply
        excluded=set(excluded);rows=self._rows('whole_source_history')
        _require(len(rows)<=self.transport.maximum_rows,'Complete bounded current history required')
        changes=[];event_by_delivery={};all_markers=set()
        import base64
        import hashlib
        for row in rows:
            item=dict(row);raw=base64.b64decode(item['raw_base64'],validate=True)
            _require(_sha(raw)==item['digest'],'Native current history raw digest mismatch')
            value=json.loads(item['change_json']);key=value['state']['key'];state=value['state']
            change=Change(value['feed'],value['epoch'],value['delivery_id'],value['raw_digest'],
                value['operation'],EntityState(EntityKey(key['source'],key['kind'],key['type_id'],key['id']),
                state['version'],state['schema_revision'],state['props_json'],state['retained_json'],
                None if state.get('endpoints') is None else tuple(EntityKey(e['source'],e['kind'],e['type_id'],e['id']) for e in state['endpoints'])))
            _require((item['feed'],item['epoch'],item['delivery_id'],item['digest'])==
                (change.feed,change.epoch,change.delivery_id,change.raw_digest))
            marker=(change.feed,change.epoch,change.delivery_id)
            _require(marker not in all_markers,'Duplicate durable current history delivery')
            all_markers.add(marker)
            if marker in excluded:continue
            event_by_delivery[marker]=json.loads(raw)
            changes.append(change)
        changes.sort(key=lambda change:(change.state.key.source,change.state.key.kind,
            change.state.key.type_id,change.state.key.id,change.state.version))
        state=plan_apply(empty_state(),changes,schema_policy=lambda _change:None,
                         schema_transition_policy=lambda _old,_new:None)
        # Reconcile a newly observed current snapshot with complete retained
        # history. During exact committed replay the current rows include the
        # attempt itself, so only the retained pre-attempt plan is compared.
        expected={kind:{} for kind in ('object','edge')}
        latest={}
        for change in changes:
            latest[change.state.key]=change
        for key,value in state.current.items():expected[key.kind][key]=value
        for role,kind,idfield in (('object_current','object','type_id'),('edge_current','edge','rel_type_id')):
            if not compare_current:continue
            actual=self._rows(role);by_key={}
            for row in actual:
                item=dict(row);key=EntityKey(item['source_system'],kind,int(item[idfield]),int(item['id']))
                _require(key not in by_key,'Duplicate complete current role identity')
                by_key[key]=item
            _require(set(by_key)==set(expected[kind]),'Current rows differ from complete replayed source history')
            for key,entity in expected[kind].items():
                actual_row=by_key[key];change=latest[key]
                _require((int(actual_row['entity_version']),actual_row['schema_revision'],actual_row['props_json'],
                    actual_row['retained_json'],actual_row['source_feed'],actual_row['source_epoch'],
                    actual_row['source_delivery_id'])==(entity.version,entity.schema_revision,entity.props_json,
                    entity.retained_json,change.feed,change.epoch,change.delivery_id),
                    'Current role row differs from its exact retained entity afterimage')
                import hashlib
                identity={'source_system':key.source,
                    ('type_id' if kind=='object' else 'rel_type_id'):key.type_id,'id':key.id}
                _require(actual_row.get('lookup_hash')==hashlib.sha256(
                    json.dumps(identity,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
                    and (kind!='object' or actual_row.get('logical_key_json')=='[]')
                    and type(actual_row.get('apply_batch_id'))is str and bool(actual_row['apply_batch_id']),
                    'Current role identity metadata differs from retained graph identity')
                if kind=='edge':
                    endpoints=entity.endpoints
                    _require((int(actual_row['source_type']),int(actual_row['source_id']),
                        int(actual_row['target_type']),int(actual_row['target_id']))==
                        (endpoints[0].type_id,endpoints[0].id,endpoints[1].type_id,endpoints[1].id),
                        'Current edge endpoints differ from retained afterimage')
                    event=event_by_delivery[(change.feed,change.epoch,change.delivery_id)]
                    original_order=event['after'].get('orderKey')
                    expected_order=None if original_order=={'state':'null'} else original_order.get('text')
                    _require(actual_row.get('order_key')==expected_order,
                        'Current edge order key differs from exact retained afterimage')
        if compare_current:
            tombstone_rows=self._rows('tombstone')
            tombstones=state.tombstones
            _require(len(tombstone_rows)==len(tombstones),'Tombstone inventory differs from complete replayed history')
            seen=set()
            for row in tombstone_rows:
                item=dict(row);key=EntityKey(item['source_system'],item['entity_kind'],int(item['type_id']),int(item['id']))
                change=tombstones.get(key)
                _require(change is not None and key not in seen and
                    (int(item['entity_version']),item['source_feed'],item['source_epoch'],item['source_delivery_id'])==
                    (change.state.version,change.feed,change.epoch,change.delivery_id),
                    'Tombstone row differs from exact retained delete')
                seen.add(key)
        return state

    def _previous_progress(self,request):
        rows=self.transport.query('SELECT publication_id,source_progress_json,table_versions_json,recorded_at FROM '
            +quote_table_identifier(self.names['publication_manifest'])+' ORDER BY recorded_at DESC LIMIT 2',{}).rows
        checkpoint=json.loads(request['source_checkpoint_json'])
        if not rows:
            cursor=json.loads(__import__('base64').b64decode(self.admission.batch.cursor_before,validate=True))
            _require(cursor.get('nextXid')=='0' and request['predecessor']=='managed-current-genesis/0.1',
                'First managed publication must begin at the observed genesis cursor')
            _require(all(not self._rows(role) for role in self.table_roles),
                'Genesis cursor cannot adopt preexisting graph rows without a predecessor manifest')
            return {}
        _require(len(rows)>=1 and (len(rows)==1 or rows[0]['recorded_at']!=rows[1]['recorded_at']),
            'Unique latest durable managed predecessor required')
        latest=dict(rows[0]);_require(latest['publication_id']==request['predecessor'],
            'Current request predecessor must be the latest durable managed publication')
        progress=json.loads(latest['source_progress_json'])
        previous=progress.get(checkpoint['feed'])
        if previous is None:
            cursor=json.loads(__import__('base64').b64decode(checkpoint['previous'],validate=True))
            _require(cursor.get('nextXid')=='0','Prior durable progress for this feed is required')
        else:
            _require(previous['epoch']==checkpoint['epoch'] and previous['position']==checkpoint['previous'],
                'Exact contiguous source checkpoint from prior managed manifest required')
        actual_vector=json.loads(latest['table_versions_json']);current=self.targets.observe()
        _require(actual_vector=={self.names[role]:current[role].version for role in self.table_roles},
            'Latest predecessor manifest vector must equal current four-role heads')
        return progress
    def _make_plan(self,request,recorded_at=None,*,baseline_installation=None,
                   baseline_data_rows=None,previous_progress=None,prior_state=None):
        from ashlar.apply import empty_state
        from .graph_sql import graph_sql_plan
        from .publication_planning import local_effect_plan
        from datetime import datetime,timezone,timedelta
        clock=self.clock if recorded_at is None else recorded_at
        instant=(datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(microseconds=int(clock))).isoformat()
        baseline_data_rows=self._baseline_rows() if baseline_data_rows is None else baseline_data_rows
        prior_state=empty_state() if prior_state is None else prior_state
        after,generated=graph_sql_plan(prior_state,self.admission.batch,self.data_tables,
            materialized_at=instant,schema_policy=self.admission.admit,
            schema_transition_policy=self.admission.admit_transition,native_scalar_profile='typed')
        prior_keys={'object_current':[], 'edge_current':[], 'tombstone':[], 'whole_source_history':[]}
        prior_keys['object_current']=[{'source_system':row['source_system'],'type_id':str(row['type_id']),'id':str(row['id'])}
            for row in baseline_data_rows['object_current']]
        prior_keys['edge_current']=[{'source_system':row['source_system'],'rel_type_id':str(row['rel_type_id']),'id':str(row['id'])}
            for row in baseline_data_rows['edge_current']]
        steps,elisions=local_effect_plan(generated,prior_keys,self.data_tables)
        # Any apply interruption stays uncertain and refuses retry; the plan
        # nevertheless retains exact per-table expected commit count and rows.
        counts={role:0 for role in self.table_roles}
        for step in steps:
            role=next((r for r,t in self.data_tables.items()
                       if step['statement'].startswith(('INSERT INTO '+quote_table_identifier(t)+' ',
                                                        'MERGE INTO '+quote_table_identifier(t)+' '))),None)
            if role is None:raise ManagedCurrentPublicationError('Unowned genesis data statement refused')
            counts[role]+=1
        expected=self._expected_rows(steps,baseline_data_rows)
        self.publication_id='ashlar-current:'+_sha(request['request_digest'])
        baseline=baseline_installation or {role:{'name':self.names[role],'uuid':self.targets.opening[role].uuid,'version':self.targets.opening[role].version}
                  for role in ROLES}
        body={'profile':'ashlar-managed-current-exact-plan/0.1','request':dict(request),
            'sourceAdmission':self.admission.metadata(),'sourcePageSha256':_sha(self.admission.batch.begin),
            'publicationId':self.publication_id,'recordedAt':clock,'baselineInstallation':baseline,
            'baselineDataRows':baseline_data_rows,
            'previousProgress':previous_progress or {},
            'steps':steps,'zeroMatchElisions':elisions,'expectedRows':expected}
        text=_canonical(body)
        _require(len(text.encode())<=4*1024*1024,'Managed current original plan exceeds phase bound')
        return body,text
    def prepare_truss_observation(self,request,context,attempt):
        self._require_context(context);self.request=dict(request)
        if attempt is not None and attempt.phase=='committed':
            return None
        self.prepare_truss_apply(request,context)
    def prepare_truss_apply(self,request,context):
        self._require_context(context);self.request=dict(request)
        _require(self.request['source_checkpoint_json']==self._source_checkpoint()
            and self.request['schema_revisions_json']==self.schema_revisions_json,
            'Original current Truss page request required')
        previous=self._previous_progress(self.request)
        self.prior_state=self._prior_state_from_history()
        self.plan,self.plan_json=self._make_plan(self.request,previous_progress=previous,
                                                prior_state=self.prior_state)
    def prepared_plan(self,request,context):
        self._require_context(context)
        _require(self.plan_json is not None and dict(request)==self.request)
        return self.plan_json
    def load_prepared_plan(self,request,plan_json,context):
        self._require_context(context)
        _require(type(plan_json)is str and len(plan_json.encode())<=4*1024*1024,
            'Original durable prepared plan required')
        body=json.loads(plan_json)
        _require(_canonical(body)==plan_json and body.get('profile')=='ashlar-managed-current-exact-plan/0.1'
                 and body.get('request')==dict(request)
                 and body.get('sourceAdmission')==self.admission.metadata()
                 and body.get('sourcePageSha256')==_sha(self.admission.batch.begin),
                 'Durable original plan differs from current accepted Truss input')
        excluded={(change.feed,change.epoch,change.delivery_id) for change in self.admission.changes}
        prior_state=self._prior_state_from_history(excluded,compare_current=False)
        rebuilt,text=self._make_plan(dict(request),body['recordedAt'],
            baseline_installation=body['baselineInstallation'],
            baseline_data_rows=body['baselineDataRows'],
            previous_progress=body['previousProgress'],prior_state=prior_state)
        _require(text==plan_json,'Reconstructed plan differs from original prepared bytes')
        self.plan=body;self.plan_json=plan_json;self.request=dict(request);self.publication_id=body['publicationId']
        return None
    def validate_prepared_plan(self,request,plan_json,context):
        self._require_context(context)
        _require(self.plan_json is not None and plan_json==self.plan_json and dict(request)==self.request,
            'Exact original prepared plan required')
        inventory=self.targets.observe()
        for role,item in inventory.items():
            baseline=self.plan['baselineInstallation'][role]
            _require(item.uuid==baseline['uuid'] and item.name==baseline['name']
                and item.schema==self.targets.opening[role].schema and item.protocol==self.targets.opening[role].protocol,
                'Original six-role UC installation identity changed')
        return None
    def _cast_expected(self,role,rows):
        types={name:kind for name,kind,_ in self.columns[role]};result=[]
        for row in rows:
            actual={}
            for name,value in row.items():
                if value is None:actual[name]=None
                elif types[name]=='BIGINT':actual[name]=int(value)
                elif types[name]=='BOOLEAN':actual[name]=value if type(value)is bool else str(value).lower()=='true'
                elif types[name]=='TIMESTAMP':actual[name]=_normalize_native(__import__('datetime').datetime.fromisoformat(value.replace('Z','+00:00')))
                else:actual[name]=value
            result.append(actual)
        return result
    def _assert_role_state(self,role,expected,*,allow_empty_before):
        actual=[{k:_normalize_native(v) for k,v in dict(row).items()} for row in self._rows(role)]
        normalized=expected
        key=lambda row:_canonical(row)
        actual_sorted=sorted(actual,key=key);expected_sorted=sorted(normalized,key=key)
        if actual_sorted==expected_sorted:return 'post'
        raise ManagedCurrentPublicationError('Current native data role differs from exact genesis pre/post snapshot')
    def writer(self,stream,context):
        self._require_context(context);driver=self
        @self._contextmanager
        def hold():
            _require(not driver.held and stream==driver.stream,'Single original managed writer context required')
            driver.held=True;primary=None
            try:
                driver.targets.closing()
                yield None
            except BaseException as error:primary=error
            try:driver.targets.closing()
            except BaseException as error:
                if primary is None:primary=error
            driver.held=False
            if primary is not None:raise primary
        return hold()
    def _require_writer(self,context):
        self._require_context(context);_require(self.held,'Held original managed writer required')
        self.targets.closing()
    def apply(self,request,context):
        self._require_writer(context)
        _require(dict(request)==self.request and self.plan is not None)
        inventory=self.targets.observe()
        for role in self.table_roles:
            _require(inventory[role].version==self.plan['baselineInstallation'][role]['version']
                     and [{k:_normalize_native(v) for k,v in dict(row).items()} for row in self._rows(role)]==
                     self.plan['baselineDataRows'][role],
                     'Exact retained current role baseline required before first apply')
        for step in self.plan['steps']:
            self.transport.query(step['statement'],step['parameters'])
        self._validate_effect_rows()
        proof={'profile':'ashlar-managed-current-effect-proof/0.1','planSha256':_sha(self.plan_json),
            'dataRowsSha256':_sha(_canonical({r:self._cast_expected(r,self.plan['expectedRows'][r]) for r in self.table_roles}))}
        self.manifest=self._manifest(request)
        return _canonical({'effects':proof,'manifest':self.manifest})
    def recover_apply(self,request,context):
        self._require_writer(context)
        raise ManagedCurrentPublicationError(
            'Original apply outcome is uncertain; this owner has no terminal noncommit proof and will not resubmit')
    def _validate_effect_rows(self):
        inventory=self.targets.observe()
        for role in self.table_roles:
            _require(self._assert_role_state(role,self.plan['expectedRows'][role],allow_empty_before=False)=='post')
            target=self.names[role]
            effects=sum(step['statement'].startswith(('INSERT INTO '+quote_table_identifier(target)+' ',
                'MERGE INTO '+quote_table_identifier(target)+' ')) for step in self.plan['steps'])
            expected_head=self.plan['baselineInstallation'][role]['version']+effects
            _require(inventory[role].version==expected_head,'Current managed Delta effect head differs from original per-role plan')
    def _manifest(self,request):
        from .driver import progress_union
        from .managed_current_core import ROLES as all_roles
        inventory=self.targets.observe()
        data_roles=('object_current','edge_current','tombstone','whole_source_history')
        versions={self.names[role]:inventory[role].version for role in data_roles}
        checkpoint=json.loads(request['source_checkpoint_json'])
        manifest={'publication_id':self.publication_id,'profile_version':'ashlar-delta/0.3',
            'table_versions_json':_canonical(versions),'schema_revisions_json':request['schema_revisions_json'],
            'source_progress_json':_canonical(progress_union(self.plan.get('previousProgress',{}),checkpoint)),
            'validation_report_json':_canonical({'complete':True,'request_digest':request['request_digest'],
                'predecessor':request['predecessor'],'source_admission':self.admission.metadata(),
                'original_plan_sha256':_sha(self.plan_json),'effect_rows_sha256':_sha(_canonical(self.plan['expectedRows']))}),
            'recorded_at':self.plan['recordedAt']}
        from ashlar.manifest import validate_manifest_row
        validate_manifest_row(manifest);self.manifest=manifest
        return manifest
    def validate(self,request,artifact,descriptor,context):
        self._require_writer(context)
        from ashlar.stored_publisher import validate_applied_artifact
        value,actual=validate_applied_artifact(artifact,dict(request))
        _require(dict(request)==self.request and actual==descriptor and value['manifest']==self.manifest)
        self._validate_effect_rows()
        return None
    def validate_manifest(self,row,context):
        self._require_writer(context)
        _require(self.manifest is not None and dict(row)==self.manifest)
        data_roles=('object_current','edge_current','tombstone','whole_source_history')
        inventory=self.targets.observe()
        actual={self.names[role]:inventory[role].version for role in data_roles}
        _require(json.loads(row['table_versions_json'])==actual)
        return None
    def acknowledge(self,request,descriptor,context):
        # This is destination receipt validation only. The external source holder
        # independently reopens the receipt and owns TrussFeedSessions ACK.
        return self.validate_destination_receipt(request,descriptor,context)
    def validate_destination_receipt(self,request,descriptor,context):
        self._require_writer(context)
        from ashlar.source_checkpoint import bind_source_descriptor
        _require(dict(request)==self.request and descriptor.publication_id==self.publication_id)
        bind_source_descriptor(dict(request),descriptor,expected_publication_id=self.publication_id)
        rows=self.transport.query('SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json,cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM '+quote_table_identifier(self.names['publication_manifest'])+' WHERE publication_id=:id',{'id':descriptor.publication_id}).rows
        _require(len(rows)==1 and dict(rows[0])==dict(descriptor.raw),'Independently reopened exact durable UC manifest required')
        self.validate_manifest(dict(descriptor.raw),context)
        return None
    def receipt(self,descriptor):
        from ashlar.publication import Descriptor
        _require(isinstance(descriptor,Descriptor))
        self.validate_destination_receipt(self.request,descriptor,self.context)
        inventory=self.targets.closing()
        data_roles=('object_current','edge_current','tombstone','whole_source_history')
        row=dict(descriptor.raw)
        return {'profile':'ashlar-managed-current-destination-receipt/0.1',
            'publicationId':descriptor.publication_id,'requestDigest':self.request['request_digest'],
            'manifest':row,'manifestSha256':_sha(_canonical(row)),
            'dataRoleVector':{self.names[role]:{'uuid':inventory[role].uuid,'version':inventory[role].version} for role in data_roles},
            'weftBindingInputs':self._weft_binding_inputs(row,inventory),
            'installationInventory':{role:{'name':inventory[role].name,'uuid':inventory[role].uuid,
                'version':inventory[role].version,'schema':list(inventory[role].schema),
                'protocol':inventory[role].protocol} for role in ROLES},
            'sourceAuthority':'development Truss 0.0.44 page on managed Unity Catalog; external source ACK required'}

    def reopen_receipt(self,descriptor):
        """Build a destination receipt from a fresh UC read after coordination.

        This deliberately does not ACK the Truss source. The external holder
        must reopen this receipt independently before invoking its ACK owner.
        """
        from ashlar.publication import Descriptor
        from ashlar.source_checkpoint import bind_source_descriptor
        _require(isinstance(descriptor,Descriptor) and self.request is not None
                 and descriptor.publication_id==self.publication_id,
                 'Original committed managed destination descriptor required')
        bind_source_descriptor(dict(self.request),descriptor,
                              expected_publication_id=self.publication_id)
        inventory=self.targets.closing()
        row=descriptor.raw
        self._require_durable_manifest(row,inventory)
        data_roles=('object_current','edge_current','tombstone','whole_source_history')
        return {'profile':'ashlar-managed-current-destination-receipt/0.1',
            'publicationId':descriptor.publication_id,'requestDigest':self.request['request_digest'],
            'manifest':dict(row),'manifestSha256':_sha(_canonical(dict(row))),
            'dataRoleVector':{self.names[role]:{'uuid':inventory[role].uuid,'version':inventory[role].version} for role in data_roles},
            'weftBindingInputs':self._weft_binding_inputs(dict(row),inventory),
            'installationInventory':{role:{'name':inventory[role].name,'uuid':inventory[role].uuid,
                'version':inventory[role].version,'schema':list(inventory[role].schema),
                'protocol':inventory[role].protocol} for role in ROLES},
            'sourceAuthority':'development Truss 0.0.44 page on managed Unity Catalog; external source ACK required'}

    def _weft_binding_inputs(self,row,inventory):
        """Exact independently observed compiler binding inputs; four data roles only."""
        roles=('object_current','edge_current','tombstone','whole_source_history')
        return {'manifest':dict(row),
            'manifestUuid':inventory['publication_manifest'].uuid,
            'dataRoles':{role:{'name':inventory[role].name,'uuid':inventory[role].uuid,
                'version':inventory[role].version} for role in roles}}

    def _require_durable_manifest(self,row,inventory):
        data_roles=('object_current','edge_current','tombstone','whole_source_history')
        expected={self.names[role]:inventory[role].version for role in data_roles}
        _require(json.loads(row['table_versions_json'])==expected,
                 'Reopened manifest must retain exact four-role publication vector')
        fields=('publication_id','profile_version','table_versions_json','source_progress_json',
                'schema_revisions_json','validation_report_json')
        projection=','.join(fields)+",cast(unix_micros(recorded_at) AS STRING) AS recorded_at"
        actual=self.transport.query('SELECT '+projection+' FROM '+quote_table_identifier(
            self.names['publication_manifest'])+' WHERE publication_id=:id',
            {'id':row['publication_id']}).rows
        _require(len(actual)==1 and dict(actual[0])==dict(row),
                 'Exact durable managed UC manifest must be independently reopened')


class _ManagedCurrentPolicy:
    def __init__(self,driver):self.driver=driver
    @property
    def context(self):return self.driver.context
    @contextmanager
    def writer(self,table,uuid,context):
        from contextlib import contextmanager as _contextmanager
        driver=self.driver;driver._require_context(context)
        role=next((r for r,name in driver.names.items() if name==table),None)
        _require(role is not None and driver.targets.opening[role].uuid==uuid)
        @_contextmanager
        def held():
            driver._require_writer(context)
            yield None
            driver._require_writer(context)
        with held():yield None
    def admit(self,row,context):
        self.driver._require_writer(context)
        _require(self.driver.manifest is not None and dict(row)==self.driver.manifest,
            'Exact retained managed current manifest intent required')
        return None


class ManagedCurrentManifestPort:
    """Manifest coordinator port: exact commit plus read-only uncertain recovery."""
    def __init__(self,driver,request,context):
        self.driver=driver;self.request=dict(request);self.context=context
        role='publication_manifest';item=driver.targets.opening[role]
        from ashlar.manifest import DeltaManifestStore
        self.store=DeltaManifestStore(driver.transport,driver.policy,driver.names[role],item.uuid)
    def prepare(self,row,*,context):
        self.driver._require_writer(context)
        self.driver.validate_manifest(dict(row),context)
        return None
    def commit(self,row,*,context):
        return self.store.commit(dict(row),context=context)
    def _observe(self,row,context):
        self.driver._require_writer(context)
        actual=self.store.executor.query('SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json,cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM '+self.store.sql_table+' WHERE publication_id=:id',{'id':row['publication_id']}).rows
        if len(actual)!=1 or dict(actual[0])!=dict(row):
            raise ManagedCurrentPublicationError('Original manifest commit is uncertain; no replacement submitted')
        return dict(row)
    def recover(self,row,*,context):return self._observe(row,context)
    def reconcile(self,row,*,context):return self._observe(row,context)


def run_managed_current_publication_task(spark,*,binding,core_config,protocol_profile,
        stream,predecessor,schema_revisions_json,materialized_at,expected_environment):
    """Run one current Truss page on the caller's injected UC Spark task.

    The caller must provide a digest-bound accepted development-source bundle
    and an exact selected-environment contract. This function never starts a
    Spark session, creates the schema, calls Databricks Jobs, or ACKs Truss.
    """
    from .managed_current_core import ManagedCurrentCoreConfig,restore_managed_current_admission
    from ashlar.attempt_store import DeltaAttemptStore
    from ashlar.publisher import publish_batch
    from ashlar.source_checkpoint import truss_checkpoint
    from .publication_planning import truss_request_for
    from .truss_backend import TrussPublisherBackend

    keys={'runtimeName','sparkVersion','sparkBuildText','runtimeTag','principal','catalog','schema','protocol'}
    _require(type(expected_environment)is dict and set(expected_environment)==keys
             and all(value is None or (type(value)is str and value) for value in expected_environment.values()),
             'Exact selected managed task environment contract required')
    required={'runtimeName','sparkVersion','principal','catalog','schema','protocol'}
    _require(all(type(expected_environment[key])is str and expected_environment[key] for key in required),
             'Selected runtime, Spark version, actor, destination, and protocol must be pinned')
    _require(type(stream)is str and stream and type(predecessor)is str and predecessor
             and type(schema_revisions_json)is str and type(materialized_at)is str,
             'Exact original managed current request inputs required')
    _require(type(core_config)is ManagedCurrentCoreConfig
             and core_config.catalog==expected_environment['catalog']
             and core_config.schema==expected_environment['schema'],
             'Selected Unity Catalog destination differs from task environment')
    revisions=json.loads(schema_revisions_json)
    _require(type(revisions)is dict and bool(revisions) and _canonical(revisions)==schema_revisions_json,
             'Canonical complete current source schema revisions required')
    opening_environment=_observe_task_environment(spark,protocol_profile)
    _require(opening_environment==expected_environment,
             'Observed managed task environment differs from the selected public observations')
    _require(expected_environment['runtimeName']=='databricks-serverless'
             and expected_environment['protocol']==protocol_profile.identity,
             'Selected serverless runtime/protocol profile differs')

    admission=restore_managed_current_admission(binding,core_config)
    _require(admission.batch.feed and admission.batch.batch_id)
    target=ManagedCurrentTargetOwner(spark,catalog=core_config.catalog,schema=core_config.schema,
                                     protocol_profile=protocol_profile)
    existing=target._existing()
    if not existing:target.initialize()
    elif existing==set(ROLES):target.open_existing()
    else:raise ManagedCurrentPublicationError('Partial six-role managed target refuses publication')
    context=object()
    driver=ManagedCurrentGraphPublicationDriver(target,admission,stream=stream,
                                                 materialized_at=materialized_at,context=context)
    request=truss_request_for(stream,admission.batch,predecessor,revisions)
    _require(request['source_checkpoint_json']==truss_checkpoint(admission.batch),
             'Exact accepted current Truss checkpoint required')
    attempts_role='publication_attempt_phase';attempt_table=target.names[attempts_role]
    attempts_uuid=target.opening[attempts_role].uuid
    attempts=DeltaAttemptStore(driver.transport,driver.policy,attempt_table,attempts_uuid,
                               original_request=request)
    backend=TrussPublisherBackend(attempts,driver,
        lambda original,held:ManagedCurrentManifestPort(driver,original,held),context=context)
    descriptor=publish_batch(backend,stream,admission.batch,predecessor=predecessor,
        schema_revisions_json=schema_revisions_json,context=context,
        source_checkpoint_json=request['source_checkpoint_json'])
    receipt=driver.reopen_receipt(descriptor)
    closing_environment=_observe_task_environment(spark,protocol_profile)
    _require(closing_environment==opening_environment,
             'Managed task runtime, principal, catalog, schema, or protocol changed before close')
    return {'profile':'ashlar-managed-current-task-result/0.1',
        'runtimeIdentity':{'opening':opening_environment,'closing':closing_environment,
            'privateDatabricksBuildIdentity':'unknown'},
        'receipt':receipt,
        'sourceAck':'external-holder-independent-reopen-required'}


def _observe_task_environment(spark,protocol_profile):
    """Capture public Spark/Databricks task identity without inventing a build ID."""
    transport=ManagedCurrentSparkTransport(spark)
    identity=transport.query('SELECT current_user() AS principal,current_catalog() AS catalog,'
                            'current_schema() AS schema',{}).rows
    _require(len(identity)==1,'Unique public managed task identity required')
    try:
        build_rows=transport.query('SELECT version() AS sparkBuildText',{}).rows
        spark_build_text=build_rows[0].get('sparkBuildText') if len(build_rows)==1 else None
    except Exception:
        # Some selected serverless runtimes may not expose the SQL function;
        # an unknown public build string stays unknown, never inferred.
        spark_build_text=None
    conf=spark.conf
    try:runtime_tag=conf.get('spark.databricks.clusterUsageTags.sparkVersion')
    except Exception:runtime_tag=None
    observed={'runtimeName':'databricks-serverless','sparkVersion':spark.version,
        'sparkBuildText':spark_build_text,'runtimeTag':runtime_tag,
        'principal':identity[0].get('principal'),'catalog':identity[0].get('catalog'),
        'schema':identity[0].get('schema'),'protocol':protocol_profile.identity}
    required=('runtimeName','sparkVersion','principal','catalog','schema','protocol')
    _require(all(type(observed[key])is str and observed[key] for key in required),
             'Required public runtime, Spark version, actor, destination, or protocol observation is missing')
    _require(all(observed[key] is None or (type(observed[key])is str and observed[key])
                 for key in ('sparkBuildText','runtimeTag')),
             'Optional public runtime observations must be text or explicitly unknown')
    return observed
