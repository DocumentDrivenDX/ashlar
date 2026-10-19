# SPDX-License-Identifier: MIT OR Apache-2.0
"""Installed existing-publication read composition; no registration or mutation."""
from dataclasses import dataclass
from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
from ashlar.schema import decode_retained_json
from ashlar.native import NativeBackend,quote_table_identifier
from ashlar.singleton import read_singleton
from ashlar.manifest import manifest_pin_vector,bind_manifest_pins
from ashlar.pins import PostgresPins
from ashlar.effect_validation import validate_effect_snapshot
from ashlar.protocol import ReaderProtocolProfile,inspect_protocol
from ashlar.retention_policy import SQLRetentionProvider,RetentionGate
from .managed_carriers import load_managed_carriers,validate_managed_installation
from .managed_publication_selection import ManagedPublicationSelection,ROLES
from .managed_source_oracle import configured_prefix_carrier_rows
from .managed_pin_reader import ManagedPinReader
from .managed_sql_read import ManagedSQLReader
from .managed_record_admission import admit_managed_records
from .native_owner_lane import NativeOwnerLane
from .lifecycle import finish

PROFILE=ReaderProtocolProfile('private-databricks-sql-fixture/2026-10-08',(3,),(7,),
    {'appendOnly','clustering','deletionVectors','domainMetadata','invariants','rowTracking','v2Checkpoint'})

def _require(value):
    if not value:raise PermissionError('Managed singleton admission refused')

@dataclass(frozen=True)
class ManagedSingletonSelector:
    source:str
    type_id:int
    entity_id:int
    def __post_init__(self):
        _require(type(self.source)is str and 0<len(self.source.encode())<=1024 and '\0'not in self.source)
        _require(all(type(n)is int and -(2**63)<=n<2**63 for n in (self.type_id,self.entity_id)))

class _Capture:
    def __init__(self,path):
        _require(type(path)is type(Path())and path.is_absolute()and path.resolve()==path)
        s=path.lstat();_require(stat.S_ISDIR(s.st_mode)and s.st_uid==os.getuid()and stat.S_IMODE(s.st_mode)==0o700 and not any(path.iterdir()))
        self.path=path;self.identity=(s.st_dev,s.st_ino);self.count=0;self.total=0
    def renew(self):
        s=self.path.lstat();_require((s.st_dev,s.st_ino)==self.identity and s.st_uid==os.getuid()and stat.S_IMODE(s.st_mode)==0o700)
    def write(self,raw):
        self.renew();_require(type(raw)is bytes and self.count<512 and self.total+len(raw)<=8388608)
        path=self.path/(str(self.count)+'.json');self.count+=1;self.total+=len(raw)
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);primary=None
        try:
            _require(type(n:=os.write(fd,raw))is int and n==len(raw))
        except BaseException as exc:primary=exc
        finish(primary,[lambda:os.close(fd)])


def read_managed_singleton(selection,sql_reader,pin_reader,*,producer,selector,context,capture_directory):
    """Read one object with original full source/native/pin admission and closing.

    Callers retain actual selection and SQL owner contexts until this returns;
    their outer closing checks must succeed before publishing any report.
    """
    _require(type(selection)is ManagedPublicationSelection and type(sql_reader)is ManagedSQLReader and type(pin_reader)is ManagedPinReader and type(selector)is ManagedSingletonSelector)
    selector.__post_init__();_require(pin_reader.context is context)
    _require(selector.source==selection.configuration.policy.source_system and selector.type_id in selection.configuration.policy.types)
    selection.renew();sql_reader.renew();capture=_Capture(capture_directory)
    receipts=admit_managed_records(producer,selection.configuration.intake,selection.configuration.policy,selection.batches[:selection.spec.completed_ordinal])
    for receipt in receipts:capture.write(receipt)
    carriers=load_managed_carriers();installation=decode_retained_json(selection.installation_json.encode());namespace=validate_managed_installation(installation,carriers)
    _require(namespace==selection.namespace and sql_reader.config.actor_name==installation['authenticated_owner']and sql_reader.config.actor_id==selection.original_actor_id)
    expected=configured_prefix_carrier_rows(selection.configuration,selection.batches[:selection.spec.completed_ordinal],materialization_clock_us=selection.original_materialization_clock_us,carriers=carriers,maximum_rows=1000).rows()
    uuids=dict(selection.table_uuids);tables={r:namespace+'.'+r for r in ROLES};columns={t:carriers.ordered_columns(r)for r,t in tables.items()}
    targets=decode_retained_json(selection.targets_json.encode())
    _require(set(targets)==set(uuids)and all(targets[t]['columns']==[list(c)for c in columns[t]]and targets[t]['rows']==expected[r]for r,t in tables.items()))
    descriptor=selection.descriptor;actor=installation['authenticated_owner'];vector=manifest_pin_vector(dict(descriptor.raw),uuids,authority=actor)
    def page(kind,name,value,ordinal):capture.write(json.dumps({'kind':kind,'name':name,'ordinal':ordinal,'page':value},ensure_ascii=False,separators=(',',':'),allow_nan=False).encode())
    lane=NativeOwnerLane(sql_reader.workspace,actor,page)
    proof=decode_retained_json(selection.intake_proof_json.encode());intake=selection.configuration.intake
    def custody():
        finish(None,[selection.renew,sql_reader.renew,capture.renew])
    def authority():
        custody();lane.admit('catalog',namespace.split('.')[0]);lane.admit('schema',namespace)
        for table in (*installation['tables'],proof['table']):lane.admit('table',table,managed=True)
        table=quote_table_identifier(proof['table']);detail=sql_reader.query('DESCRIBE DETAIL '+table,{}).rows
        _require(len(detail)==1 and detail[0].get('id')==proof['table_uuid'])
        original=intake.row();projection=','.join('cast(complete_interpretation AS STRING) AS complete_interpretation'if k=='complete_interpretation'else k for k in original)
        rows=sql_reader.query('SELECT '+projection+' FROM '+table+' WHERE document_id=:id AND document_revision=:revision',{'id':intake.document_id,'revision':intake.document_revision}).rows
        _require(rows==[dict(original,complete_interpretation=str(original['complete_interpretation']).lower())])
        detail=sql_reader.query('DESCRIBE DETAIL '+table,{}).rows;_require(len(detail)==1 and detail[0].get('id')==proof['table_uuid'])
    gate=RetentionGate(SQLRetentionProvider(sql_reader,uuids,defaults={'data_retention':'interval 7 days','log_retention':'interval 30 days'},default_profile='azure-databricks-delta-documented-defaults/2026-09-11',max_observation_span_us=180000000),minimum_margin_us=600000000)
    interval={'held':False,'parity':False,'poisoned':False}
    class Policy:
        def authorize(self,supplied,publication_id,requested):
            _require(supplied is context and publication_id==descriptor.publication_id and set(requested)==set(uuids));authority()
        def validate_descriptor(self,value,supplied):
            try:
                _require(supplied is context and value==descriptor and interval['held']and not interval['poisoned']);authority()
            except BaseException:
                interval['poisoned']=True
                raise
            callbacks=[]
            for role,table in tables.items():
                def check(role=role,table=table):
                    inspect_protocol(sql_reader,table,uuids[table],profile=PROFILE)
                    if not interval['parity']:
                        validate_effect_snapshot(sql_reader,table,uuids[table],descriptor.versions[table],columns[table],expected[role])
                callbacks.append(check)
            try:finish(None,[*callbacks,lambda:gate.check(value,supplied)])
            except BaseException:
                interval['poisoned']=True
                raise
            interval['parity']=True
        def validate_snapshot(self,table,uuid,version,actual_columns):
            _require(table in uuids and (uuid,version,tuple(actual_columns))==(uuids[table],descriptor.versions[table],columns[table]));inspect_protocol(sql_reader,table,uuid,profile=PROFILE)
        def bind_descriptor(self,value,supplied_vector,supplied):
            _require(supplied is context and supplied_vector==vector);bind_manifest_pins(value,vector,uuids,authority=actor);custody()
        def authorize_row(self,value,table,row,supplied):
            self.bind_descriptor(value,vector,supplied);_require(table==tables['object_current'])
            matches=[r for r in expected['object_current']if(r['source_system'],r['type_id'],r['id'])==(selector.source,str(selector.type_id),str(selector.entity_id))]
            _require(len(matches)<=1 and ((row is None and not matches)or(row is not None and len(matches)==1 and dict(row)==matches[0])))
        def authorize_read(self,value,supplied):
            _require(value==vector and supplied is context);authority()
    policy=Policy();operation={'primary':None}
    class Transactions:
        @contextmanager
        def transaction(self,supplied):
            with pin_reader.transaction(supplied)as session:
                yield session
                # PostgresPins has completed its closing checks. A failed read
                # must reach the concrete owner before it decides commit/rollback.
                finish(operation['primary'],[])
    registered_pins=PostgresPins(Transactions(),policy)
    class ReadPins:
        @contextmanager
        def hold(self,value,*,context):
            primary=None
            try:
                with registered_pins.hold(value,context=context):
                    interval['held']=True
                    try:yield value
                    except BaseException as exc:primary=exc;operation['primary']=exc
                    # Preserve the body failure but still run independent native
                    # closing while locks are held, then the original pin closing.
                    try:policy.validate_descriptor(descriptor,context)
                    except BaseException as exc:
                        if primary is None or isinstance(primary,Exception)and not isinstance(exc,Exception):primary=exc
                        else:
                            try:primary.cleanup_failed=True
                            except BaseException:pass
                    operation['primary']=primary
            except BaseException as exc:
                finish(primary,[lambda:(_ for _ in ()).throw(exc)])
            finally:
                interval.update(held=False,parity=False,poisoned=True)
            finish(primary,[])
    pins=ReadPins()
    manifest=namespace+'.publication_manifest';backend=NativeBackend(sql_reader,policy,manifest,installation['tables'][manifest]['uuid'],columns)
    primary=None;result=None
    try:
        result=read_singleton(sql_reader,backend,pins,vector,policy,publication_id=descriptor.publication_id,table=tables['object_current'],kind='object',source=selector.source,type_id=selector.type_id,entity_id=selector.entity_id,context=context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={selection.configuration.schema_alias:[intake.document_revision]})
    except BaseException as exc:primary=exc
    finish(primary,[selection.renew,sql_reader.renew,capture.renew])
    return dict(result)if result is not None else None
