"""Read-only retained medical commit observations; never submission or ACK authority."""
import hashlib,json,re
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.stored_publisher import validate_applied_artifact
from .delta_custody import encoded
from .driver import AttemptExecutor,CarrierPolicy
from .lifecycle import finish,owned_context
from .local_physical_carriers import original_local_physical_carriers
from .medical_commit_inspection_configuration import OriginalMedicalCommitInspectionConfig,read_selected
from .medical_process_inspection import MedicalProcessInspection
from .medical_publication_plan import STREAM
from .postgres import Session


def inspect_medical_commit(driver,plan,source,*,context,bounds,installation):
    """Inspect exact committing custody, current source/ACK and native manifest bags.

    Caller holds the real installation lease and full medical recovery preflight.
    No publisher backend, manifest mutation or acknowledgement submitter is used.
    """
    view=plan.snapshot();request=view['request'];transport=driver.transport
    original=None;phases=None;manifest_observation=None;proposal=None;primary=None;manifest_operations=None
    def artifact():
        nonlocal original,proposal
        driver.require(context);plan.require_request(request)
        maximum=min(bounds.input_bytes,3*1024*1024)
        rows=transport.db.execute('SELECT request,length(CAST(artifact AS BLOB)),substr(CAST(artifact AS BLOB),1,?) FROM local_publication_artifact WHERE request_digest=?',
            (maximum+1,request['request_digest'])).fetchall()
        if (len(rows)!=1 or rows[0][0]!=encoded(request)or type(rows[0][1])is not int
                or not 0<rows[0][1]<=maximum or type(rows[0][2])is not bytes or len(rows[0][2])!=rows[0][1]):
            raise PermissionError('Complete retained original applied artifact required')
        observed=rows[0][2].decode('utf-8','strict')
        if original is not None and original!=observed:raise PermissionError('Original applied artifact changed during inspection')
        value,descriptor=validate_applied_artifact(observed,request)
        driver.policy.active['manifest']=value['manifest']
        driver.validate(request,observed,descriptor,context)
        original=observed;proposal=(encoded(value['manifest'])+'\n').encode()
        return value['manifest']
    def phase():
        nonlocal phases
        target=transport.targets[driver.tables['attempts']]
        store=DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid)
        with owned_context(store.session(context))as session:observed=session.read(STREAM,request['batch_id'])
        if [row.phase for row in observed]!=['prepared','applying','applied','committing']:
            raise PermissionError('Exact retained original committing phase prefix required')
        if phases is not None and phases!=observed:raise PermissionError('Original phase custody changed during inspection')
        payload=json.loads(observed[-1].payload_json)
        if payload['request']!=request or payload['result_json']!=original or payload['descriptor_json']is not None:
            raise PermissionError('Exact committing original artifact correspondence required')
        phases=observed
    def ack():
        registration=source.ports.registration;connection=None;session=None;error=None;answer=None
        def read():
            nonlocal answer
            if session is None:return
            source.authority.admit_scope(registration.source.scope,session,context)
            rows=session.query('SELECT * FROM "'+registration.source.scope.service_schema+'".observe(CAST(:scope AS uuid),CAST(:position AS bigint))',
                {'scope':registration.source.scope.scope_id,'position':'1'}).rows
            if rows!=[{'position':'0','request_hex':None,'manifest_hex':None,'receipt_hex':None}]:
                raise PermissionError('Protected original ACK zero with no receipt required')
            answer={'position':'0','receipt':None}
        try:
            connection=registration.metadata_factory(context);session=Session(connection);read()
        except BaseException as caught:error=caught
        finish(error,[read]+([]if connection is None else[connection.rollback,connection.close]))
        return answer
    def operations():
        nonlocal manifest_operations
        rows=transport.db.execute('SELECT operation,state,intent_sha FROM local_operation WHERE operation LIKE ? LIMIT 65',
            ('manifest:'+request['request_digest']+':%',)).fetchall()
        if len(rows)>64 or any(type(name)is not str or len(name)>1024 or state not in ('prepared','submitted','committed')or type(digest)is not str or re.fullmatch('[0-9a-f]{64}',digest)is None for name,state,digest in rows):
            raise PermissionError('Bounded exact original manifest operation inventory required')
        observed=sorted(rows)
        if manifest_operations is not None and manifest_operations!=observed:raise PermissionError('Original manifest operation custody changed during inspection')
        manifest_operations=observed
    def manifest():
        nonlocal manifest_observation
        target=transport.targets[driver.tables['manifest']]
        history=transport.original_history(target)
        anchor=installation['native']['manifest']['registration']
        if (target.uuid!=anchor['uuid']or not history or type(history[0]['version'])is not int
                or [row['version']for row in history]not in ([1,0],[2,1,0])):
            raise PermissionError('Complete original manifest history and initial anchor required')
        for current,expected in zip(history[-2:],anchor['history']):
            if any(current.get(key)!=value for key,value in expected.items()):raise PermissionError('Original initial manifest history changed')
        snapshot,schema=transport.bounded_snapshot(target,history[0]['version'],remaining_bytes=bounds.total_bytes)
        if encoded(schema)!=encoded(json.loads(original_local_physical_carriers()['carriers']['manifest']['schemaJson'])):
            raise PermissionError('Exact original manifest schema required')
        row=json.loads(original)['manifest']
        if history[0]['version']==1 and snapshot['rows']==[]:
            if any(state!='prepared'for _,state,_ in manifest_operations):raise PermissionError('Unresolved original manifest submission cannot establish absence')
            classification='observed-absent-at-anchor'
        elif history[0]['version']==2 and snapshot['rows']==[row]:
            metadata=json.loads(history[0].get('userMetadata')or '{}')
            if (set(metadata)!={'profile','installation_id','request_digest','operation','operation_sha256'}
                    or re.fullmatch('[0-9a-f]{64}',metadata.get('operation_sha256',''))is None
                    or re.fullmatch('manifest:'+request['request_digest']+':[0-9a-f]{64}',metadata.get('operation',''))is None
                    or metadata.get('profile')!='ashlar-local-delta-commit/0.1'
                    or metadata.get('installation_id')!=installation['installation_id']
                    or metadata.get('request_digest')!=request['request_digest']
                    or not metadata.get('operation','').startswith('manifest:'+request['request_digest']+':')):
                raise PermissionError('Exact original manifest commit metadata required')
            matched=[record for record in manifest_operations if record[0]==metadata['operation']]
            if not matched:
                classification='observed-exact-manifest-row-no-original-intent'
            else:
                if len(matched)!=1 or matched[0][1]!='committed' or matched[0][2]!=metadata['operation_sha256']:
                    raise PermissionError('Original manifest intent/receipt remains unresolved')
                maximum=transport.operation_capacity['max_intent_bytes']if transport.operation_capacity else 4194304
                retained=transport.db.execute('SELECT length(CAST(intent AS BLOB)),substr(CAST(intent AS BLOB),1,?) FROM local_operation WHERE operation=?',
                    (maximum+1,metadata['operation'])).fetchall()
                if (len(retained)!=1 or type(retained[0][0])is not int or not 0<retained[0][0]<=maximum
                        or type(retained[0][1])is not bytes or len(retained[0][1])!=retained[0][0]):
                    raise PermissionError('Exact bounded original manifest intent required')
                intent=json.loads(retained[0][1].decode('utf-8','strict'))
                proof=transport.inspect_committed(metadata['operation'],intent['statement'],intent['parameters'],
                    intent_digest=request['request_digest'],context=context)
                if (proof['operation_sha256']!=metadata['operation_sha256']or proof['uuid']!=target.uuid
                        or proof['version']!=2 or proof['snapshot']!=snapshot):
                    raise PermissionError('Actual original manifest receipt/native capture differs')
                classification='exact-existing-commit'

        else:raise PermissionError('Unresolved or foreign native manifest outcome')
        observed={'classification':classification,'uuid':target.uuid,'history':history,'snapshot':snapshot,'schema':schema}
        if manifest_observation is not None and encoded(observed)!=encoded(manifest_observation):raise PermissionError('Native manifest observation changed during inspection')
        manifest_observation=observed
    try:artifact();phase();ack();operations();manifest()
    except BaseException as error:primary=error
    finish(primary,[artifact,phase,ack,operations,manifest,lambda:driver.source_admit(request)])
    return {'profile':'ashlar-original-medical-commit-outcome-observation/0.1',
        'classification':manifest_observation['classification'],'installation_id':installation['installation_id'],
        'request_digest':request['request_digest'],'manifest':manifest_observation,
        'protected_ack':{'position':'0','receipt':None},'manifest_operations':manifest_operations,'proposal_sha256':hashlib.sha256(proposal).hexdigest(),
        'scope':'Private cooperative development writer observation only. Exclusive original storage access/future executor fencing remain external assumptions. No recovery, submission, ACK or prospective authorization grant.'},proposal


def inspect_original_medical_commit_local(config,*,supervision=None):
    from .medical_inspection_selection import create_medical_inspection
    observer=create_medical_inspection(config)
    from .original_medical_local_publication import operate_original_medical_commit_inspection
    return operate_original_medical_commit_inspection(config.recovery,observer,supervision=supervision)
