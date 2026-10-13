"""Exact authored payments currency query through installed 0.4.2."""
from dataclasses import dataclass,replace
import hashlib,json
from pathlib import Path
from types import SimpleNamespace
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths,compile_implicit_arithmetic_distribution,installed_implicit_arithmetic_schema_bundle
from .implicit_arithmetic_profile import PROFILE,ImplicitArithmeticAdmissionConfig,ImplicitArithmeticExecutionConfig
from .count_star_schema import make_offline_implicit_arithmetic_schema_validation
from .count_star_admission import admit_implicit_arithmetic_artifact
from .count_star_execution import execute_commerce_implicit_arithmetic
from .finite_pack import FinitePackDefinition,UMF_REVISION
from .finite_dataset import FinitePackSource
from .finite_publication import FiniteFileDriver
from .pack_count_star_reader import FinitePackProvider
from .pack_count_star_request import original_finite_pack_binding_request
from .pack_count_star_query import finite_pack_public_source
from .payments_oracle import original_oracle
from .supply_chain_request import source_snapshot
from .delta_custody import encoded

@dataclass(frozen=True)
class PaymentsScenarioCompileConfig:
    paths:ImplicitArithmeticDistributionPaths
    maximum_artifact_bytes:int
    def __post_init__(self):
        if type(self.paths)is not ImplicitArithmeticDistributionPaths or self.paths.package is not None or any(type(p)is not type(Path('/'))or not p.is_absolute()for p in (self.paths.index,self.paths.output)):
            raise ValueError('Explicit original installed0.4.2 distribution required')
        if type(self.maximum_artifact_bytes)is not int or not 1<=self.maximum_artifact_bytes<=16777216:
            raise ValueError('Exact positive bounded artifact limit required')

def payments_scenario_request(case,pack,model,graph,bindings,manifest,registry,aliases):
    definition=FinitePackDefinition('payments');definition.inputs(model,graph,pack)
    if type(case)is not str or case!='currency':raise ValueError('Exact admitted original currency case required')
    sql=dict(definition.cases(model,graph,pack))[case]
    metadata=source_snapshot([bindings,manifest,registry,aliases])
    if any(type(v)is not dict for v in (metadata[0],metadata[1],metadata[3]))or type(metadata[2])is not list:
        raise ValueError('Exact original publication metadata required')
    if any(type(v)is not str or len(v.split('.'))!=3 or any(not p or '\0'in p for p in v.split('.'))for v in metadata[3].values()):
        raise ValueError('Complete original three-component aliases required')
    request=original_finite_pack_binding_request(definition,sql,model,graph,*metadata)
    request.update(interfaceVersion=PROFILE.compile_version,dialect=PROFILE.dialect)
    request['target'].update({k:dict(PROFILE.backend)[k]for k in ('backendId','backendVersion','targetProfile')})
    return request

def payments_scenario_oracle(model,graph):
    original=original_oracle(model,graph);document=json.loads(model)
    def rows(name):return [o['values']for o in original['objects']if o['type']=={'document':document['id'],'module':'domain','element':name}]
    values={row['ledger_entries.currency']for row in rows('ledger_entries')}
    if any(type(value)is not str for value in values):raise ValueError('Exact original required currency strings required')
    return {'rows':[[value]for value in sorted(values)],
        'witnesses':{'original_objects':len(original['objects']),'original_edges':len(original['edges'])},
        'scope':'Complete independent original ledger-entry currency set; exact authored DISTINCT only, no decimal or arithmetic execution qualification.'}


def query_payments_scenario(driver,case,pack,aliases,compile_config,execution_config,*,observe):
    if type(driver)is not FiniteFileDriver or type(driver.source)is not FinitePackSource or driver.manifest is None:
        raise ValueError('Actual original committed finite driver required')
    if type(compile_config)is not PaymentsScenarioCompileConfig or type(execution_config)is not ImplicitArithmeticExecutionConfig or not callable(observe):
        raise ValueError('Explicit original installed0.4.2 execution configuration required')
    compile_config.__post_init__();execution_config.__post_init__()
    source=driver.source
    if type(source.definition)is not FinitePackDefinition or source.definition.name!='payments' or source.context is not driver.context or execution_config.public_source_revision!=UMF_REVISION:
        raise ValueError('Original payments/publicsource owner required')
    source.renew();aliases=source_snapshot(aliases)
    request=payments_scenario_request(case,pack,source.model,source.graph,source.bindings,driver.manifest,
        [{'table':t.table,'uuid':t.uuid}for t in driver.transport.targets.values()],aliases)
    raw=(encoded(request)+'\n').encode();maximum=compile_config.maximum_artifact_bytes
    if len(raw)>maximum:raise ValueError('Original complete request resource bound')
    schemas=dict(installed_implicit_arithmetic_schema_bundle(compile_config.paths))
    admission=ImplicitArithmeticAdmissionConfig(maximum,make_offline_implicit_arithmetic_schema_validation({k:schemas[k]for k,_ in PROFILE.schemas}))
    responses=[]
    for iteration in range(2):
        response=compile_implicit_arithmetic_distribution(compile_config.paths,raw);observe(case,iteration,raw,response)
        if type(response)is not bytes or len(response)>maximum:raise ValueError('Original complete response resource bound')
        responses.append(response)
    if responses[0]!=responses[1]:raise ValueError('Original fresh compiler response differs')
    artifact=json.loads(responses[0]);admit_implicit_arithmetic_artifact(request,artifact,json.loads(responses[1]),config=admission)
    execution_config=replace(execution_config,admission=admission,public_source=finite_pack_public_source(source,execution_config.maximum_public_source_bytes or maximum))
    provider=FinitePackProvider(driver,aliases,json.loads(request['target']['bindingJson']))
    def native_files():
        metadata=source.metadata()
        return {'model':hashlib.sha256(source.model).hexdigest(),'graph':hashlib.sha256(source.graph).hexdigest(),
            'finite_source_receipt':hashlib.sha256(source.raw).hexdigest(),'source_transaction':metadata['source_transaction_sha256']}
    opened=SimpleNamespace(provider=provider,context=driver.context,model=source.model,graph=source.graph,bindings=source.bindings,native_files=native_files,original_native_files=native_files())
    result=execute_commerce_implicit_arithmetic(opened,request,artifact,json.loads(responses[1]),config=execution_config,
        original_oracle=lambda model,graph,request:payments_scenario_oracle(model,graph))
    source.renew()
    return {'case':case,'evidence':result,'qualification':'Provisional unchanged original payments currency query only; outer source/transport/Spark closure required. No financial arithmetic or protected ACK qualification.'}
