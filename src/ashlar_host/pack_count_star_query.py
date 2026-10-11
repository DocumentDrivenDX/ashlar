"""All original archaeology and ecology queries through shared held041 execution.

The caller owns the actual reader/transport and Spark cleanup. This port releases
provisional evidence only; final persistence must follow all outer cleanup gates.
"""
from dataclasses import replace
from collections import Counter
import hashlib,json
from types import SimpleNamespace
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .pack_count_star_compile import PackCountStarCompileConfig,compile_pack_count_star_cases
from .supply_chain_request import source_snapshot
from .finite_dataset import FinitePackSource
from .finite_publication import FiniteFileDriver
from .finite_pack import UMF_REVISION,FinitePackDefinition
from .pack_count_star_reader import FinitePackProvider
from .pack_count_star_oracle import pack_count_star_result_oracle
from .delta_custody import encoded


def finite_pack_public_source(source,maximum):
    """Actual validated whole dataset plus independent original Field projections."""
    if type(source)is not FinitePackSource or type(maximum)is not int or not 0<maximum<=16777216:raise ValueError('Owned bounded original source required')
    if type(source.definition)is not FinitePackDefinition or any(type(v)is not bytes for v in (source.model,source.graph,source.raw)):raise ValueError('Original typed source carriers required')
    facts=source.definition.inputs(source.model,source.graph);source.metadata()
    SOURCE_SHA=facts['model_sha256'];SOURCE_SYSTEM=facts['source_system']
    originals=(source.model,source.graph,source.raw);bindings=source_snapshot(source.bindings)
    _,expected_bindings=source.definition.build(*originals[:2])
    if encoded(bindings)!=encoded(expected_bindings):raise ValueError('Original complete carrier binding differs')
    model=json.loads(originals[0]);graph=json.loads(originals[1])
    fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    properties={tuple(p['identity']):p['property_id']for p in bindings['properties']}
    entities={(e['kind'],e['originalKey']):e for e in bindings['entities']}
    def renew():
        source.metadata()
        if any(type(v)is not bytes for v in (source.model,source.graph,source.raw)) or (source.model,source.graph,source.raw)!=originals or encoded(source_snapshot(source.bindings))!=encoded(bindings):raise ValueError('Original public source metadata changed')
        if type(source.definition)is not FinitePackDefinition:raise ValueError('Original typed definition changed')
        source.definition.inputs(*originals[:2])
    def validate(request,artifact,projections):
        renew();request=source_snapshot(request);artifact=source_snapshot(artifact);projections=source_snapshot(projections)
        if request['modules'][0]['documentJson'].encode()!=source.model:raise ValueError('Original finite public source differs')
        for projection in projections:
            check=projection['check'];record=check['record'];field=check['field']
            if any(identity['documentId']!=model['id']or identity['revision']!=SOURCE_SHA for identity in (record,field))or check.get('publicSourceOnly')is not True:
                raise ValueError('Original finite public Field scope differs')
            definition=fields[(record['module'],record['element'])];scalar=fields[(field['module'],field['element'])]
            if definition['kind']!='record' or scalar['kind']!='field' or {'module':field['module'],'element':field['element']}not in definition['members'] or properties[(model['id'],field['module'],field['element'])]!=check['propertyId']:
                raise ValueError('Original finite Field/carrier home differs')
            logical={'family':scalar['scalarType'],'facets':scalar.get('facets',{}),'nullable':scalar['nullability']!='required'}
            if check['logicalType']!=logical:raise ValueError('Original finite logical Field differs')
            expected=[]
            for obj in graph['objects']:
                if obj['type']!={'document':model['id'],'module':record['module'],'element':record['element']}:continue
                carrier=entities[('object',obj['key'])];props=[]
                for member in definition['members']:
                    authored=fields[(member['module'],member['element'])];key=properties[(model['id'],member['module'],member['element'])];token=obj['values'][member['element']]
                    value='null'if token is None else json.dumps(token,ensure_ascii=False,separators=(',',':'))if authored['scalarType']=='string'else token
                    props.append(json.dumps(key)+':'+value)
                expected.append({'source_system':SOURCE_SYSTEM,'type_id':carrier['type_id'],'id':carrier['id'],'schema_revision':SOURCE_SHA,'props_json':'{'+','.join(props)+'}','extracted_token':obj['values'][field['element']]})
            if Counter(map(encoded,projection['rows']))!=Counter(map(encoded,expected)):raise ValueError('Complete original finite public Field projection differs')
        original={'sourceText':request['modules'][0]['documentJson'],'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':projections}
        text=json.dumps(original,sort_keys=True,separators=(',',':'),ensure_ascii=True)
        receipt=json.dumps({'originalRequestText':text,'umfRevision':UMF_REVISION,'admitted':True,'originalDatasetReceiptText':source.raw.decode(),
            'scope':'Actual selected public whole finite dataset and complete original native Field correspondence; no authority grant'},sort_keys=True,separators=(',',':'),ensure_ascii=True)
        if len(text.encode())>maximum or len(receipt.encode())>maximum:raise ValueError('Complete finite public source receipt bound')
        renew()
        return {'originalRequestText':text,'originalReceiptText':receipt,'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
    return validate


def query_pack_count_star_cases(driver,pack,aliases,compile_config,execution_config,*,observe,observe_native=None):
    """Execute original SQL without repairs/fallback under actual held publication."""
    if type(compile_config)is not PackCountStarCompileConfig or type(execution_config)is not CountStarExecutionConfig:
        raise ValueError('Typed original finite query configuration required')
    if execution_config.public_source_revision!=UMF_REVISION or execution_config.public_source is None:
        raise ValueError('Actual owning original public finite source port required')
    if observe_native is not None and not callable(observe_native):raise ValueError('Callable original native observation port required')
    compile_config.__post_init__();execution_config.__post_init__()
    if type(driver)is not FiniteFileDriver or type(driver.source)is not FinitePackSource or driver.source.context is not driver.context or driver.manifest is None:raise ValueError('Actual restored original pack driver required')
    source=driver.source
    if type(source.definition)is not FinitePackDefinition:raise ValueError('Original typed definition required')
    source.definition.inputs(source.model,source.graph,pack);source.renew();aliases=source_snapshot(aliases)
    registry=[{'table':t.table,'uuid':t.uuid}for t in driver.transport.targets.values()]
    compiled=compile_pack_count_star_cases(compile_config,source.definition,pack,source.model,source.graph,source.bindings,driver.manifest,registry,aliases,observe=observe)
    provisional=[]
    for case in compiled['cases']:
        request=case['request'];provider=FinitePackProvider(driver,aliases,json.loads(request['target']['bindingJson']))
        def native_files():
            metadata=source.metadata();return {'model':hashlib.sha256(source.model).hexdigest(),'graph':hashlib.sha256(source.graph).hexdigest(),
                'finite_source_receipt':hashlib.sha256(source.raw).hexdigest(),'source_transaction':metadata['source_transaction_sha256']}
        opened=SimpleNamespace(provider=provider,context=driver.context,model=source.model,graph=source.graph,bindings=source.bindings,native_files=native_files,original_native_files=native_files())
        case_execution=execution_config
        if observe_native is not None:
            def native_observation(captured,name=case['id']):
                observe_native(name,captured)
                if execution_config.native_observer is not None:execution_config.native_observer(captured)
            case_execution=replace(execution_config,native_observer=native_observation)
        result=execute_commerce_count_star(opened,request,case['response'],json.loads(case['responseBytes']),config=case_execution,
            original_oracle=lambda model,graph,request,name=case['id']:pack_count_star_result_oracle(source.definition,name,model,graph))
        provisional.append({'id':case['id'],'evidence':result})
    source.renew()
    return {'cases':provisional,'qualification':'Provisional original finite pack native query evidence; outer reader/transport/Spark and closing original producer/lease cleanup required before persistence. No PostgreSQL/protected ACK.'}
