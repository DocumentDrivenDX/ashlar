# SPDX-License-Identifier: MIT OR Apache-2.0
"""Typed full medical read capture and existing ACK SELECT correspondence owner."""
import json
from pathlib import Path
from urllib.parse import urlsplit,unquote
from .ack import receipt_bytes
from .delta_custody import encoded
from .evolution_verification_types import VerificationBounds
from .existing_outbox_authority import ExistingOutboxAuthority
from .lifecycle import finish
from .local_physical_carriers import original_local_physical_carriers
from .medical_publication_plan import MedicalPublicationPlan
from .medical_read_sources import MedicalReadSourceFactory
from .postgres import Session
from .read_only_transport import ReadOnlyDeltaTransport

class MedicalReadObservation:
    """Created by driver binding, inside current read-only source/native custody."""
    def __init__(self,plan,transport,bounds,factory,source_policy,renew,native_detail):
        if type(plan)is not MedicalPublicationPlan or type(transport)is not ReadOnlyDeltaTransport or type(bounds)is not VerificationBounds or type(factory)is not MedicalReadSourceFactory or type(source_policy)is not ExistingOutboxAuthority:
            raise ValueError('Exact medical read capture owners required')
        if not callable(renew)or not callable(native_detail):raise ValueError('Bound read driver callbacks required')
        self.plan=plan;self.transport=transport;self.bounds=bounds;self.factory=factory;self.source_policy=source_policy;self.renew=renew;self.native_detail=native_detail
        self._selected=(plan,transport,bounds,factory,source_policy,renew,native_detail)
        self.snapshot_used=0;self.snapshot_seen=set()
    def require_selection(self):
        if any(a is not b for a,b in zip(self._selected,(self.plan,self.transport,self.bounds,self.factory,self.source_policy,self.renew,self.native_detail))):
            raise PermissionError('Original medical read observation owners changed')
    def snapshot(self,table,version):
        """One complete resolver/closing vector shares a cumulative byte budget."""
        self.require_selection();self.renew();view=self.plan.snapshot();roles={v:k for k,v in view['tables'].items()if k in view['expected']}
        if table not in roles:raise PermissionError('Medical read snapshot outside data vector')
        if len(self.snapshot_seen)==4:self.snapshot_seen=set();self.snapshot_used=0
        if table in self.snapshot_seen:raise PermissionError('Repeated incomplete medical read snapshot vector')
        self.snapshot_seen.add(table)
        target=self.transport.targets[table]
        snapshot,schema=self.transport.bounded_snapshot(target,version,remaining_bytes=self.bounds.total_bytes-self.snapshot_used)
        self.snapshot_used+=len(encoded({'schema':schema,'snapshot':snapshot}).encode())
        if self.snapshot_used>self.bounds.total_bytes:raise PermissionError('Cumulative medical resolver capture exceeded')
        expected=json.loads(view['schemas'][roles[table]])
        if encoded(schema)!=encoded(expected)or encoded(snapshot['rows'])!=encoded(sorted(view['expected'][roles[table]],key=encoded)):
            raise PermissionError('Complete medical resolver schema/bag differs')
        self.renew();return snapshot
    def capture(self,row,context):
        self.require_selection();self.renew();view=self.plan.snapshot();versions=json.loads(row['table_versions_json']);transport=self.transport;bounds=self.bounds
        if transport.observation_bounds is not bounds:raise PermissionError('Actual medical read capture interval required')
        if set(versions)!={view['tables'][r]for r in view['expected']}:raise PermissionError('Complete medical read vector required')
        schemas=original_local_physical_carriers()['carriers'];used=[0];observations={};primary=None
        def native(role):
            target=transport.targets[view['tables'][role]];detail=self.native_detail(target);location=urlsplit(detail['location'])
            if location.scheme!='file'or location.netloc or location.query or location.fragment or Path(unquote(location.path.rstrip('/')))!=target.path:
                raise PermissionError('Original medical read native location differs')
            history=transport.original_history(target)
            if not history or type(history[0].get('version'))is not int or target.table in versions and not any(type(x.get('version'))is int and x['version']==versions[target.table]for x in history):raise PermissionError('Original medical read retained history differs')
            version=versions[target.table]if target.table in versions else history[0]['version']
            snapshot,schema=transport.bounded_snapshot(target,version,remaining_bytes=bounds.total_bytes-used[0]);used[0]+=len(encoded({'schema':schema,'snapshot':snapshot}).encode())
            if used[0]>bounds.total_bytes:raise PermissionError('Cumulative complete medical read capture exceeded')
            if encoded(schema)!=encoded(json.loads(schemas[role]['schemaJson'])):raise PermissionError('Complete generated medical read schema differs')
            if role in view['expected']and encoded(snapshot['rows'])!=encoded(sorted(view['expected'][role],key=encoded)):raise PermissionError('Independent original medical read bag differs')
            observations[role]={'uuid':target.uuid,'version':version,'row_sha256':snapshot['row_sha256'],'rows':len(snapshot['rows'])}
        try:
            for role in view['tables']:native(role)
        except BaseException as error:primary=error
        used[0]=0
        finish(primary,[lambda role=role:native(role)for role in view['tables']]+[self.renew])
        return observations
    def ack(self,request,manifest,context):
        self.require_selection();self.renew();registration=self.factory.registration
        _,_,checkpoint,receipt=receipt_bytes(registration.source.scope,request,manifest)
        connection=registration.metadata_factory(context);primary=None;observed=None;session=Session(connection)
        def admit():self.source_policy.admit_scope(registration.source.scope,session,context)
        def observe():
            admit()
            rows=session.query('SELECT * FROM "'+registration.source.scope.service_schema+'".observe(CAST(:scope AS uuid),CAST(:position AS bigint))',{'scope':registration.source.scope.scope_id,'position':checkpoint['position']}).rows
            expected={'position':checkpoint['position'],'request_hex':request.hex(),'manifest_hex':manifest.hex(),'receipt_hex':receipt.hex()}
            if rows!=[expected]:raise PermissionError('Exact existing medical protected ACK bytes differ')
            return rows[0]
        try:observed=observe()
        except BaseException as error:primary=error
        finish(primary,[admit,observe,connection.rollback,connection.close,self.renew]);return observed
