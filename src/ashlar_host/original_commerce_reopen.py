"""Publisher-capable reopening of a retained original applied commerce fixture."""
import json
from contextlib import contextmanager
from dataclasses import dataclass
from ashlar.manifest import manifest_pin_vector
from ashlar.stored_publisher import validate_applied_artifact
from .commerce_admission import CommerceAdmission,original_commerce_oracle
from .commerce_publication_sources import OriginalCommerceSourceOwner
from .delta_custody import LocalDeltaTransport,encoded,sha
from .driver import NativeDriver,ROOT
from .lifecycle import owned_context
from .original_commerce_publication import OriginalCommercePublicationOperator,OriginalCommerceInstallationLease,OriginalCommerceNativePolicy,close_original_publication
from .private_graph_custody import decode_custody_bytes,require_custody_digest
from .schema_rows import fixture_columns


@dataclass(frozen=True)
class OpenedOriginalCommercePublisher:
    driver:NativeDriver
    context:object
    request:dict
    manifest:dict
    vector:object
    descriptor:object


def restore_original_commerce_applied(driver,request_digest):
    """Restore retained original plans with an independent original graph oracle."""
    require_custody_digest(request_digest)
    if type(driver)is not NativeDriver or type(driver.source_admission)is not CommerceAdmission:
        raise PermissionError('Actual original commerce publisher required')
    driver.require(driver.context);db=driver.transport.db
    rows=db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?',(request_digest,)).fetchall()
    plans=db.execute('SELECT original FROM local_source_plan WHERE request_digest=?',(request_digest,)).fetchall()
    if len(rows)!=1 or len(plans)!=1 or any(type(raw)is not str or len(raw.encode())>4194304 for raw in (*rows[0],plans[0][0])):
        raise ValueError('Complete bounded original applied commerce custody required')
    request=decode_custody_bytes(rows[0][0].encode());artifact,descriptor=validate_applied_artifact(rows[0][1],request)
    plan=decode_custody_bytes(plans[0][0].encode());required={'request','generated_steps','selected_steps','zero_match_elisions','complete_prior_oracle','observed_native_prior'}
    if set(plan)!=required or plan['request']!=request or request['request_digest']!=request_digest:
        raise ValueError('Exact original commerce source plan differs')
    admission=driver.source_admission;model,graph,bindings,_=[raw for path,raw in admission.originals]
    expected=original_commerce_oracle(model,graph,json.loads(bindings),admission.batch,driver.columns)
    manifest=artifact['manifest'];report=json.loads(manifest['validation_report_json'])
    if (report['source_oracle_sha256']!=sha(encoded(expected))or report['original_source_plan_sha256']!=sha(plans[0][0])
            or report['source_admission']!=admission.metadata()or request['predecessor']!='explicit-original-commerce-origin'
            or any(plan['complete_prior_oracle'][role]!=[]for role in driver.columns)):
        raise ValueError('Original commerce source/prior oracle differs')
    active={'request':request,'steps':plan['selected_steps'],'expected':expected,'manifest':manifest,'publication_id':manifest['publication_id'],
        'previous_progress':{},'progress':json.loads(manifest['source_progress_json']),'previous_expected':plan['complete_prior_oracle'],
        'generated_steps':plan['generated_steps'],'elisions':plan['zero_match_elisions'],'recorded_at':manifest['recorded_at']}
    previous=driver.policy.active;driver.policy.active=active
    try:driver.observe_original_commerce_applied(request,rows[0][1],driver.context)
    except BaseException:
        driver.policy.active=previous;raise
    vector=manifest_pin_vector(manifest,{table:driver.transport.targets[table].uuid for table in descriptor.versions},authority='private-local-process')
    return OpenedOriginalCommercePublisher(driver,driver.context,request,manifest,vector,descriptor)


@contextmanager
def open_original_commerce_publisher(operator,admission,request_digest):
    """Own real ordinary source/ACK, installation lease and native transport.

    Opening observes only. Caller must separately hold driver.writer for ACK;
    this owner never appends source data, replays effects or creates a phase.
    """
    if type(operator)is not OriginalCommercePublicationOperator or type(admission)is not CommerceAdmission:
        raise ValueError('Actual original commerce operator and public admission required')
    operator.__post_init__();admission.metadata();require_custody_digest(request_digest)
    LocalDeltaTransport.require_existing_journal(operator.journal,operator.installation_id,operator.targets,capacity=operator.capacity)
    context=object()
    with owned_context(OriginalCommerceInstallationLease.open(operator,context))as lease:
        with owned_context(operator.native_session())as spark:
            if spark.version!='3.5.3':raise ValueError('Selected existing Spark3.5.3 required')
            policy=OriginalCommerceNativePolicy(context,operator.targets,lease)
            transport=LocalDeltaTransport(spark,operator.journal,operator.installation_id,operator.targets,policy,capacity=operator.capacity)
            driver=None;owner=None;primary=None
            try:
                lease.verify(context,transport);columns=fixture_columns(ROOT);tables={target.table.split('.')[-1]:target.table for target in operator.targets}
                owner=OriginalCommerceSourceOwner(operator.ports,lease)
                driver=NativeDriver.with_registered_evolution(transport,policy,context,tables,admission.changes,columns,
                    source_admission=admission,source_sessions=owner.sources(),ack_factory=owner.bind)
                with owned_context(driver.writer('private-original-commerce',context)):
                    opened=restore_original_commerce_applied(driver,request_digest)
                yield opened
                admission.metadata()
            except BaseException as error:primary=error
            close_original_publication(primary,owner,driver,context,lease,transport)
