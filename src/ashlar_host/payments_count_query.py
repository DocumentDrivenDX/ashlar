"""Explicit original payments base-record COUNT, under existing finite custody."""
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from ashlar.weft_count_star_distribution import compile_count_star_distribution,installed_count_star_schema_bundle
from .finite_pack import FinitePackDefinition,UMF_REVISION
from .finite_dataset import FinitePackSource
from .finite_publication import FiniteFileDriver
from .pack_count_star_compile import PackCountStarCompileConfig
from .pack_count_star_reader import FinitePackProvider
from .pack_count_star_query import finite_pack_public_source
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .count_star_admission import BACKEND,CountStarAdmissionConfig,admit_count_star_artifact
from .count_star_schema import make_offline_count_star_schema_validation
from .delta_custody import encoded
from .payments_oracle import original_oracle
from .supply_chain_request import source_snapshot

def payments_count_request(record,model,graph,bindings,manifest,registry,aliases):
    """Build one explicit base COUNT; no original financial scenario is changed."""
    definition=FinitePackDefinition('payments');definition.inputs(model,graph)
    if type(record)is not str:raise ValueError('Exact original record name required')
    document=json.loads(model)
    records=[e['id']for m in document['modules']if m['id']=='domain'for e in m['elements']if e['kind']=='record']
    if record not in records:raise ValueError('Declared original payments record required')
    values=source_snapshot([bindings,manifest,registry,aliases])
    if any(type(v)is not dict for v in (values[0],values[1],values[3]))or type(values[2])is not list:
        raise ValueError('Exact original binding metadata required')
    if any(type(v)is not str or len(v.split('.'))!=3 or any(not p or '\0'in p for p in v.split('.'))for v in values[3].values()):
        raise ValueError('Complete three-component publication aliases required')
    from .pack_count_star_request import original_finite_pack_binding_request
    request=original_finite_pack_binding_request(definition,'SELECT COUNT(*) AS n FROM '+record+' a',model,graph,*values)
    binding=json.loads(request['target']['bindingJson'])
    fields={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
    for r in binding['records']:
        for p in r['properties']:
            logical=p['logical']
            if fields[(logical['module'],logical['element'])]['nullability']=='absent-allowed':
                p['home']['encoding']='ashlar-weft-json-native-null/0.1-candidate'
    raw=encoded(binding)
    request.update(interfaceVersion='weft-compile/0.4.1',dialect='weft-sql/0.4.1')
    request['target']={k:BACKEND[k]for k in ('backendId','backendVersion','targetProfile')}
    request['target'].update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    return request

def payments_count_oracle(record,model,graph):
    original=original_oracle(model,graph);document=json.loads(model)
    identity={'document':document['id'],'module':'domain','element':record}
    if not any(e['kind']=='record'and e['id']==record for m in document['modules']if m['id']=='domain'for e in m['elements']):
        raise ValueError('Original payments record required')
    count=sum(o['type']==identity for o in original['objects'])
    return {'rows':[[str(count)]],'witnesses':{'record':record,'objects':len(original['objects']),'edges':len(original['edges'])},
        'scope':'Independent complete original object population; exact base record count only, no financial scenario execution.'}

def query_payments_count(driver,record,aliases,compile_config,execution_config,*,observe):
    """Provisional result; caller closes original source/transport/Spark first."""
    if type(driver)is not FiniteFileDriver or type(driver.source)is not FinitePackSource or driver.manifest is None:
        raise ValueError('Actual original committed finite driver required')
    if type(compile_config)is not PackCountStarCompileConfig or type(execution_config)is not CountStarExecutionConfig or not callable(observe):
        raise ValueError('Explicit original compiler/execution configuration required')
    compile_config.__post_init__();execution_config.__post_init__()
    source=driver.source
    if type(source.definition)is not FinitePackDefinition or source.definition.name!='payments' or source.context is not driver.context:
        raise ValueError('Original payments source owner required')
    if execution_config.public_source_revision!=UMF_REVISION:
        raise ValueError('Actual owning public UMF revision required')
    source.renew();aliases=source_snapshot(aliases)
    request=payments_count_request(record,source.model,source.graph,source.bindings,driver.manifest,
        [{'table':t.table,'uuid':t.uuid}for t in driver.transport.targets.values()],aliases)
    raw=(encoded(request)+'\n').encode()
    if len(raw)>compile_config.maximum_artifact_bytes:raise ValueError('Original request bound exceeded')
    schemas=dict(installed_count_star_schema_bundle(compile_config.paths))
    admission=CountStarAdmissionConfig(compile_config.maximum_artifact_bytes,make_offline_count_star_schema_validation(
        {k:schemas[k]for k in ('compile-request-v0.4.1.schema.json','compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')}))
    responses=[]
    for iteration in range(2):
        response=compile_count_star_distribution(compile_config.paths,raw);observe(record,iteration,raw,response)
        if type(response)is not bytes or len(response)>compile_config.maximum_artifact_bytes:raise ValueError('Original response bound exceeded')
        responses.append(response)
    if responses[0]!=responses[1]:raise ValueError('Original fresh compiler response differs')
    artifact=json.loads(responses[0]);admit_count_star_artifact(request,artifact,json.loads(responses[1]),config=admission)
    source_bound=execution_config.maximum_public_source_bytes
    if source_bound is None:source_bound=admission.maximum_artifact_bytes
    execution_config=replace(execution_config,admission=admission,
        public_source=finite_pack_public_source(source,source_bound))
    provider=FinitePackProvider(driver,aliases,json.loads(request['target']['bindingJson']))
    def native_files():
        metadata=source.metadata()
        return {'model':hashlib.sha256(source.model).hexdigest(),'graph':hashlib.sha256(source.graph).hexdigest(),
            'finite_source_receipt':hashlib.sha256(source.raw).hexdigest(),'source_transaction':metadata['source_transaction_sha256']}
    opened=SimpleNamespace(provider=provider,context=driver.context,model=source.model,graph=source.graph,bindings=source.bindings,
        native_files=native_files,original_native_files=native_files())
    result=execute_commerce_count_star(opened,request,artifact,json.loads(responses[1]),config=execution_config,
        original_oracle=lambda model,graph,request:payments_count_oracle(record,model,graph))
    source.renew()
    return {'record':record,'evidence':result,'qualification':'Provisional publication-bound original payments base count only; outer source/transport/Spark closure required. No financial scenario or protected ACK qualification.'}
