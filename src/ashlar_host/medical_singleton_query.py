"""One original medical singleton in the actual held read-only publication.

This is a development source workflow, not UC, Truss or clinical authority.
Compiler SQL is executed unchanged against fresh session-local external aliases.
The outer medical reader owns source/pin/Spark closing and result publication.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib,json,uuid
from pathlib import Path
from types import SimpleNamespace
from ashlar.medical_source import SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA
from ashlar.native import quote_table_identifier
from ashlar.publication import resolve_publication
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths,compile_implicit_arithmetic_distribution,installed_implicit_arithmetic_schema_bundle
from ashlar.weft_path_decode import PathDecodeConfig
from .count_star_admission import admit_implicit_arithmetic_artifact
from .count_star_execution import execute_commerce_implicit_arithmetic
from .count_star_schema import make_offline_implicit_arithmetic_schema_validation
from .delta_custody import encoded
from .driver import NativeDriver
from .exact_report import report_value
from .implicit_arithmetic_profile import PROFILE,ImplicitArithmeticAdmissionConfig,ImplicitArithmeticExecutionConfig
from .lifecycle import finish
from .medical_publication_plan import FEED
from .medical_read_sources import MedicalReadSourceFactory
from .medical_singleton_request import medical_singleton_request,medical_singleton_oracle
from .original_medical_publication import MedicalAdmission
from .path_capture import PathCaptureConfig
from .read_only_transport import ReadOnlyDeltaTransport


@dataclass(frozen=True)
class MedicalSingletonQuery:
    paths:ImplicitArithmeticDistributionPaths
    maximum_artifact_bytes:int=16777216
    def __post_init__(self):
        if (type(self.paths)is not ImplicitArithmeticDistributionPaths or self.paths.package is not None
                or any(not isinstance(p,Path)or not p.is_absolute()or p.resolve()!=p for p in (self.paths.index,self.paths.output))):
            raise ValueError('Explicit canonical installed f36/0.4.2 compiler paths required')
        if type(self.maximum_artifact_bytes)is not int or not 1<=self.maximum_artifact_bytes<=16777216:
            raise ValueError('Exact bounded original compiler artifact required')



class _MedicalQueryProvider:
    def __init__(self,driver,context,resolved,request,aliases):
        if (type(driver)is not NativeDriver or type(driver.transport)is not ReadOnlyDeltaTransport
                or type(driver.source_admission)is not MedicalAdmission
                or type(getattr(driver,'query_source_factory',None))is not MedicalReadSourceFactory):
            raise PermissionError('Actual original medical read owner required')
        driver.require(context)
        if not driver.pin_held:raise PermissionError('Actual complete medical publication hold required')
        self.driver,self.context,self.resolved,self.aliases=driver,context,resolved,dict(aliases)
        self.expected=encoded(json.loads(request['target']['bindingJson']));self.active=False;self.closed=None
    def runtime(self,context):
        if context is not self.context or not self.active:raise PermissionError('Original active medical query context required')
        self.driver.require(context)
        if not self.driver.pin_held:raise PermissionError('Original medical native pins released')
        self.driver.policy.renew()
        if self.driver.transport.spark.version!='4.0.1':raise PermissionError('Selected local Spark4.0.1 required')
    def resolve(self,context):
        self.runtime(context)
        manifest=dict(self.resolved.descriptor.raw);required={t:self.driver.transport.targets[t].uuid for t in json.loads(manifest['table_versions_json'])}
        return resolve_publication(self.driver,manifest['publication_id'],required,context=context,
            supported_profiles=['ashlar-delta/0.3'],supported_revisions={FEED:[SOURCE_SHA]})
    @contextmanager
    def interval(self,context):
        if context is not self.context or self.active:raise PermissionError('Nonnested original medical query interval required')
        self.active=True;primary=None;opening=None;closing=None
        try:
            opening=self.resolve(context)
            self.driver.observe_original_medical_read(opening.descriptor,context=context)
            yield
        except BaseException as error:primary=error
        def close():
            nonlocal closing
            closing=self.resolve(context)
            observation=self.driver.observe_original_medical_read(closing.descriptor,context=context)
            if opening is not None and opening!=closing:raise PermissionError('Original medical query publication changed')
            self.closed={'profile':'ashlar-original-medical-query-interval/0.1','publication':dict(closing.descriptor.raw),'closing_observation':observation}
        try:finish(primary,[close])
        finally:self.active=False
    def closed_interval_custody(self,context):
        if context is not self.context or self.active or self.closed is None:raise PermissionError('Complete original medical query closing required')
        return json.loads(encoded(self.closed))
    def admit_binding(self,binding,resolved,context):
        self.runtime(context)
        if encoded(binding)!=self.expected or resolved!=self.resolved:raise PermissionError('Original complete medical binding/publication differs')
        for table,alias in self.aliases.items():
            target=self.driver.transport.targets[table]
            detail=self.driver.transport.spark.sql('DESCRIBE DETAIL '+quote_table_identifier(alias)).first().asDict()
            if detail['id']!=target.uuid or detail['location'].removeprefix('file:').rstrip('/')!=str(target.path):raise PermissionError('Original medical external alias identity differs')
    def native_table_schema(self,table,resolved,context):
        self.runtime(context)
        matches=[native for native,alias in self.aliases.items()if alias.split('.')==table['name']]
        if len(matches)!=1 or resolved!=self.resolved:raise PermissionError('Original medical schema table differs')
        native=matches[0];target=self.driver.transport.targets[native];snapshot=resolved.snapshots[native]
        if table!={'name':self.aliases[native].split('.'),'uuid':target.uuid,'version':snapshot.version}:raise PermissionError('Original medical schema pin differs')
        schema=self.driver.transport.spark.read.format('delta').option('versionAsOf',snapshot.version).load(str(target.path)).schema
        return {'table':table,'schema':json.loads(schema.json()),'nativeTypes':[[f.name,f.dataType.simpleString().upper()]for f in schema.fields]}


def execute_medical_singleton(selection,driver,context,resolved,model,graph,baseline,bindings,*,output,bounds):
    """Nominal internal read seam; no source interval or Spark acquisition here."""
    if type(selection)is not MedicalSingletonQuery:raise ValueError('Exact medical singleton selection required')
    selection.__post_init__()
    # Check the actual nominal source/pin owners before compiler or alias effects.
    if (type(driver)is not NativeDriver or type(driver.transport)is not ReadOnlyDeltaTransport
            or type(driver.source_admission)is not MedicalAdmission
            or type(getattr(driver,'query_source_factory',None))is not MedicalReadSourceFactory):
        raise PermissionError('Actual original medical read owner required')
    driver.require(context)
    if not driver.pin_held:raise PermissionError('Actual complete medical publication hold required')
    for raw,digest in ((model,SOURCE_SHA),(graph,GRAPH_SHA),(baseline,PUBLIC_SHA)):
        if type(raw)is not bytes or hashlib.sha256(raw).hexdigest()!=digest:raise PermissionError('Exact original medical source bytes required')
    namespace='medical_query_'+uuid.uuid4().hex
    versions=json.loads(resolved.descriptor.raw['table_versions_json'])
    aliases={table:'spark_catalog.'+namespace+'.'+table.rsplit('.',1)[-1]for table in versions}
    registry=[{'table':t.table,'uuid':t.uuid}for t in driver.transport.targets.values()]
    request=medical_singleton_request(model,graph,baseline,bindings,dict(resolved.descriptor.raw),registry,aliases)
    raw=(encoded(request)+'\n').encode();maximum=selection.maximum_artifact_bytes
    if len(raw)>maximum:raise ValueError('Original request bound exceeded')
    schemas=dict(installed_implicit_arithmetic_schema_bundle(selection.paths))
    admission=ImplicitArithmeticAdmissionConfig(maximum,make_offline_implicit_arithmetic_schema_validation({k:schemas[k]for k,_ in PROFILE.schemas}))
    responses=[]
    for _ in range(2):
        response=compile_implicit_arithmetic_distribution(selection.paths,raw)
        if type(response)is not bytes or len(response)>maximum:raise ValueError('Original response bound exceeded')
        responses.append(response)
    if responses[0]!=responses[1]:raise PermissionError('Original medical fresh recompile differs')
    artifact=json.loads(responses[0]);admit_implicit_arithmetic_artifact(request,artifact,json.loads(responses[1]),config=admission)
    if {o['id']for o in artifact['obligations']}!={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity'}:
        raise PermissionError('Unsupported original medical singleton obligations')
    # Isolated session catalog metadata only. Never render or rewrite compiler SQL.
    location=output/'query-catalog'
    if location.exists()or location.is_symlink():raise PermissionError('Fresh medical alias namespace required')
    location.mkdir(mode=0o700)
    spark=driver.transport.spark
    if any(c in str(location)for c in ("'",'\\','\0')):raise ValueError('Exact local alias location required')
    spark.sql('CREATE DATABASE '+('`spark_catalog`.`'+namespace+'`')+" LOCATION '"+str(location)+"'").collect()
    for table,alias in aliases.items():
        path=driver.transport.targets[table].path
        if any(c in str(path)for c in ("'",'\\','\0')):raise ValueError('Exact native path required')
        spark.sql('CREATE TABLE '+quote_table_identifier(alias)+" USING DELTA LOCATION '"+str(path)+"'").collect()
    provider=_MedicalQueryProvider(driver,context,resolved,request,aliases)
    def native_files():
        driver.policy.renew()
        return {'model':hashlib.sha256(model).hexdigest(),'graph':hashlib.sha256(graph).hexdigest(),'baseline':hashlib.sha256(baseline).hexdigest()}
    opened=SimpleNamespace(provider=provider,context=context,model=model,graph=graph,native_files=native_files,original_native_files=native_files())
    execution=ImplicitArithmeticExecutionConfig(admission,PathCaptureConfig(bounds.rows,bounds.cell_bytes,bounds.total_bytes),PathDecodeConfig(bounds.cell_bytes),None,None)
    result=execute_commerce_implicit_arithmetic(opened,request,artifact,json.loads(responses[1]),config=execution,original_oracle=medical_singleton_oracle)
    return {'profile':'ashlar-original-medical-singleton/0.1','request':request,'response':artifact,'original_request_utf8':raw.decode(),
        'original_response_utf8':responses[0].decode(),'original_recompiled_response_utf8':responses[1].decode(),'evidence':report_value(result,bounds.receipt_bytes),
        'qualification':'Provisional exact original medical singleton; outer medical read/source/Spark closing required.'}
