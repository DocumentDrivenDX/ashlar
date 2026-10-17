"""Finite original medical cases in one actual source/publication/native hold."""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib,json,uuid
from types import SimpleNamespace
from ashlar.manifest import manifest_pin_vector
from ashlar.pins import PinVector
from ashlar.native import quote_table_identifier
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths,compile_implicit_arithmetic_distribution,installed_implicit_arithmetic_schema_bundle
from ashlar.weft_path_decode import PathDecodeConfig
from .count_star_admission import admit_implicit_arithmetic_artifact
from .count_star_execution import execute_commerce_implicit_arithmetic
from .count_star_schema import make_offline_implicit_arithmetic_schema_validation
from .delta_custody import encoded
from .exact_report import report_value
from .implicit_arithmetic_profile import PROFILE,ImplicitArithmeticAdmissionConfig,ImplicitArithmeticExecutionConfig
from .lifecycle import finish,owned_context
from .medical_query_requests import prepare_medical_queries,require_medical_case_bag
from .medical_singleton_query import MedicalSingletonQuery,MedicalSingletonBuffer,MedicalQueryProvider,medical_query_closing_report
from .path_capture import PathCaptureConfig


@dataclass(frozen=True)
class MedicalQueryBatch:
    paths:ImplicitArithmeticDistributionPaths
    cases:tuple=('singleton','two-hop')
    maximum_artifact_bytes:int=16777216
    def __post_init__(self):
        MedicalSingletonQuery(self.paths,self.maximum_artifact_bytes)
        if (type(self.cases)is not tuple or not self.cases or len(set(self.cases))!=len(self.cases)
                or any(type(c)is not str or c not in ('singleton','two-hop')for c in self.cases)):
            raise ValueError('Closed unique full-row singleton/two-hop selection required')


@dataclass(frozen=True)
class MedicalQueryBatchBuffer:
    cases:tuple
    closing_json:str
    def __post_init__(self):
        if type(self.cases)is not tuple or not 1<=len(self.cases)<=6:raise ValueError('Complete bounded medical batch required')
        names=[];total=0
        for row in self.cases:
            if type(row)is not tuple or len(row)!=3:raise ValueError('Immutable medical case required')
            name,buffer,semantic=row
            if type(name)is not str or name not in ('singleton',*(f'two-hop-{i}'for i in range(1,6))) or type(buffer)is not MedicalSingletonBuffer:
                raise ValueError('Exact original medical case buffer required')
            buffer.__post_init__();names.append(name)
            if type(semantic)is not str or len(semantic.encode())>1048576:raise ValueError('Bounded exact medical semantic evidence required')
            total+=len(buffer.request)+len(buffer.response)+len(buffer.evidence_json.encode())+len(semantic.encode())
        if len(set(names))!=len(names)or total>4194304 or type(self.closing_json)is not str or len(self.closing_json.encode())>1048576:
            raise ValueError('Complete medical batch capacity exceeded')
        paths={name for name in names if name.startswith('two-hop-')}
        if paths and paths!={f'two-hop-{i}'for i in range(1,6)}:raise ValueError('Complete five-pair medical two-hop inventory required')
    def protocol_files(self):
        self.__post_init__();files=[]
        for name,buffer,semantic in self.cases:
            prefix='weft-medical-'+name
            files.extend(((prefix+'-request.json',buffer.request),(prefix+'-response.json',buffer.response),
                (prefix+'-evidence.json',(encoded({'case':name,'evidence':json.loads(buffer.evidence_json),'semantic_rows':json.loads(semantic)})+'\n').encode())))
        if any(len(raw)>1048576 for name,raw in files if name.endswith('-evidence.json'))or sum(len(raw)for _,raw in files)>4194304:
            raise ValueError('Complete original medical retained evidence capacity exceeded')
        return tuple(files)
    def report(self):
        self.__post_init__();files={name:{'file':name,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}for name,raw in self.protocol_files()}
        cases=[]
        for name,buffer,semantic in self.cases:
            prefix='weft-medical-'+name
            cases.append({'case':name,'original_protocol':{'request':files[prefix+'-request.json'],'response':files[prefix+'-response.json'],'recompiled_response':files[prefix+'-response.json']},
                'evidence':files[prefix+'-evidence.json'],'recompile_equal':True,'semantic_rows':json.loads(semantic)})
        return {'profile':'ashlar-original-medical-query-batch/0.1','cases':cases,'batch_closing':json.loads(self.closing_json),
            'qualification':'Completed finite original medical full-row singleton and/or complete two-hop cases through original source/publication/native and outer closing; other five corpus cases, UC, Truss and clinical authority are not qualified.'}


class MedicalBatchQueryProvider(MedicalQueryProvider):
    """Full native boundaries once; fresh original graph custody for every query."""
    def __init__(self,*args):
        super().__init__(*args)
        manifest=dict(self.resolved.descriptor.raw)
        self.vector=manifest_pin_vector(manifest,{t:self.driver.transport.targets[t].uuid for t in self.resolved.snapshots},authority='private-local-process')
        self.batch_context=None;self.batch_closed=None
    def renew(self,context):
        self.driver.require(context)
        held=getattr(self.driver,'_held_original_vector',None)
        if context is not self.context or type(held)is not PinVector or held!=self.vector:
            raise PermissionError('Original complete acquired medical batch vector required')
        self.driver.renew_original_graph_read_source(self.vector,context=context)
        if encoded(self.driver.policy.active['manifest'])!=encoded(dict(self.resolved.descriptor.raw)):
            raise PermissionError('Original immutable medical batch manifest changed')
    @contextmanager
    def batch_interval(self,context):
        if context is not self.context or self.batch_context is not None:raise PermissionError('Nonnested actual medical batch required')
        self.batch_context=context;primary=None
        try:
            self.renew(context)
            self.driver.observe_original_medical_read(self.resolved.descriptor,context=context)
            yield
        except BaseException as error:primary=error
        def observe():
            observation=self.driver.observe_original_medical_read(self.resolved.descriptor,context=context)
            self.batch_closed={'publication':dict(self.resolved.descriptor.raw),'closing_observation':medical_query_closing_report(observation)}
        try:finish(primary,[lambda:self.renew(context),observe])
        finally:self.batch_context=None
    def resolve(self,context):
        self.runtime(context)
        if self.batch_context is not context:raise PermissionError('Actual still-held medical batch required')
        self.renew(context)
        return self.resolved
    @contextmanager
    def interval(self,context):
        if context is not self.context or self.batch_context is not context or self.active:raise PermissionError('Actual nonnested medical statement interval required')
        self.active=True;self.closed=None;primary=None
        try:self.resolve(context);yield
        except BaseException as error:primary=error
        def close():
            self.resolve(context)
            self.closed={'profile':'ashlar-original-medical-batch-statement-renewal/0.1','publication_id':self.resolved.descriptor.publication_id,
                'qualification':'Fresh original graph/native-byte/source renewal inside one complete medical hold; batch and outer closing still required.'}
        try:finish(primary,[close])
        finally:self.active=False


def execute_medical_query_batch(selection,driver,context,resolved,model,graph,baseline,bindings,*,output,bounds):
    if type(selection)is not MedicalQueryBatch:raise ValueError('Exact finite medical batch selection required')
    selection.__post_init__()
    from .driver import NativeDriver
    from .read_only_transport import ReadOnlyDeltaTransport
    from .original_medical_publication import MedicalAdmission
    from .medical_read_sources import MedicalReadSourceFactory
    if (type(driver)is not NativeDriver or type(driver.transport)is not ReadOnlyDeltaTransport
            or type(driver.source_admission)is not MedicalAdmission or type(getattr(driver,'query_source_factory',None))is not MedicalReadSourceFactory):
        raise PermissionError('Actual original medical batch source owner required')
    driver.require(context)
    if not driver.pin_held:raise PermissionError('Actual complete medical batch hold required')
    namespace='medical_batch_'+uuid.uuid4().hex
    aliases={t:'spark_catalog.'+namespace+'.'+t.rsplit('.',1)[-1]for t in resolved.snapshots}
    registry=[{'table':t.table,'uuid':t.uuid}for t in driver.transport.targets.values()]
    prepared=prepare_medical_queries(model,graph,baseline,bindings,dict(resolved.descriptor.raw),registry,aliases,selection.cases)
    # Exact same nominal owner checks as the already qualified singleton, before effects.
    provider=MedicalBatchQueryProvider(driver,context,resolved,prepared[0]['request'],aliases)
    maximum=min(selection.maximum_artifact_bytes,bounds.input_bytes)
    schemas=dict(installed_implicit_arithmetic_schema_bundle(selection.paths))
    admission=ImplicitArithmeticAdmissionConfig(maximum,make_offline_implicit_arithmetic_schema_validation({k:schemas[k]for k,_ in PROFILE.schemas}))
    compiled=[]
    for case in prepared:
        raw=(encoded(case['request'])+'\n').encode()
        if len(raw)>maximum:raise ValueError('Complete original medical request bound exceeded')
        responses=tuple(compile_implicit_arithmetic_distribution(selection.paths,raw)for _ in range(2))
        if any(type(r)is not bytes or len(r)>maximum for r in responses)or responses[0]!=responses[1]:raise PermissionError('Complete original medical fresh recompile differs')
        artifact=json.loads(responses[0]);admit_implicit_arithmetic_artifact(case['request'],artifact,json.loads(responses[1]),config=admission)
        compiled.append((case,raw,responses,artifact))
    location=output/'query-catalog'
    if location.exists()or location.is_symlink():raise PermissionError('Fresh medical batch alias namespace required')
    location.mkdir(mode=0o700);spark=driver.transport.spark
    paths=[location,*[driver.transport.targets[t].path for t in aliases]]
    if any(any(c in str(p)for c in ("'",'\\','\0'))for p in paths):raise ValueError('Exact local medical alias paths required')
    spark.sql('CREATE DATABASE `spark_catalog`.`'+namespace+"` LOCATION '"+str(location)+"'").collect()
    for table,alias in aliases.items():spark.sql('CREATE TABLE '+quote_table_identifier(alias)+" USING DELTA LOCATION '"+str(driver.transport.targets[table].path)+"'").collect()
    def native_files():
        driver.policy.renew()
        return {'model':hashlib.sha256(model).hexdigest(),'graph':hashlib.sha256(graph).hexdigest(),'baseline':hashlib.sha256(baseline).hexdigest()}
    opened=SimpleNamespace(provider=provider,context=context,model=model,graph=graph,native_files=native_files,original_native_files=native_files())
    execution=ImplicitArithmeticExecutionConfig(admission,PathCaptureConfig(bounds.rows,bounds.cell_bytes,bounds.total_bytes),PathDecodeConfig(bounds.cell_bytes),None,None)
    buffers=[]
    with owned_context(provider.batch_interval(context)):
        for case,raw,responses,artifact in compiled:
            result=execute_commerce_implicit_arithmetic(opened,case['request'],artifact,json.loads(responses[1]),config=execution,
                original_oracle=lambda m,g,r,case=case:case['oracle'])
            semantic=require_medical_case_bag(model,graph,bindings,case,artifact,result)
            buffers.append((case['id'],MedicalSingletonBuffer(raw,responses[0],responses[1],encoded(report_value(result,bounds.receipt_bytes))),encoded(semantic)))
    return MedicalQueryBatchBuffer(tuple(buffers),encoded(provider.batch_closed))
