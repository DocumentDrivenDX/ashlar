"""All five original finite supply-chain queries through shared held041 execution.

The caller owns the actual reader/transport and Spark cleanup. This port releases
provisional evidence only; final persistence must follow all outer cleanup gates.
"""
from collections import Counter
import hashlib,json
from types import SimpleNamespace
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .supply_chain_compile import SupplyChainCompileConfig,compile_supply_chain_cases
from .supply_chain_request import supply_chain_count_star_request,source_snapshot,SOURCE_SHA,SOURCE_SYSTEM
from .supply_chain_finite_source import UMF_REVISION
from .supply_chain_reader import FiniteSupplyChainProvider
from .supply_chain_oracle import supply_chain_result_oracle
from .delta_custody import encoded


def finite_public_source(source,maximum):
    """Actual validated whole dataset plus independent original Field projections."""
    model=json.loads(source.model);graph=json.loads(source.graph)
    fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    properties={tuple(p['identity']):p['property_id']for p in source.bindings['properties']}
    entities={(e['kind'],e['originalKey']):e for e in source.bindings['entities']}
    def validate(request,artifact,projections):
        source.metadata()
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
        receipt=json.dumps({'originalRequestText':text,'umfRevision':UMF_REVISION,'admitted':True,'originalDatasetReceiptText':source.receipt.decode(),
            'scope':'Actual selected public whole finite dataset and complete original native Field correspondence; no authority grant'},sort_keys=True,separators=(',',':'),ensure_ascii=True)
        if len(text.encode())>maximum or len(receipt.encode())>maximum:raise ValueError('Complete finite public source receipt bound')
        source.metadata()
        return {'originalRequestText':text,'originalReceiptText':receipt,'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
    return validate


def query_supply_chain_cases(driver,pack,aliases,compile_config,execution_config,*,observe):
    """Execute original SQL without repairs/fallback under actual held publication."""
    if type(compile_config)is not SupplyChainCompileConfig or type(execution_config)is not CountStarExecutionConfig:
        raise ValueError('Typed original finite query configuration required')
    if execution_config.public_source_revision!=UMF_REVISION or execution_config.public_source is None:
        raise ValueError('Actual owning original public finite source port required')
    source=driver.source;source.renew();aliases=source_snapshot(aliases)
    registry=[{'table':t.table,'uuid':t.uuid}for t in driver.transport.targets.values()]
    compiled=compile_supply_chain_cases(compile_config,pack,source.model,source.graph,source.bindings,driver.manifest,registry,aliases,observe=observe)
    provisional=[]
    for case in compiled['cases']:
        request=case['request'];provider=FiniteSupplyChainProvider(driver,aliases,json.loads(request['target']['bindingJson']))
        def native_files():
            source.metadata();return {'model':hashlib.sha256(source.model).hexdigest(),'graph':hashlib.sha256(source.graph).hexdigest(),
                'finite_source_receipt':hashlib.sha256(source.receipt).hexdigest(),'source_transaction':hashlib.sha256(source.raw).hexdigest()}
        opened=SimpleNamespace(provider=provider,context=driver.context,model=source.model,graph=source.graph,bindings=source.bindings,native_files=native_files,original_native_files=native_files())
        result=execute_commerce_count_star(opened,request,case['response'],json.loads(case['responseBytes']),config=execution_config,
            original_oracle=lambda model,graph,request,name=case['id']:supply_chain_result_oracle(name,model,graph))
        provisional.append({'id':case['id'],'evidence':result})
    source.renew()
    return {'cases':provisional,'qualification':'Provisional original finite supply-chain native query evidence; outer reader/transport/Spark and closing original producer/lease cleanup required before persistence. No PostgreSQL/protected ACK.'}
