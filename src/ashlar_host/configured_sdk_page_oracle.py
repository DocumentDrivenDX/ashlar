# SPDX-License-Identifier: MIT OR Apache-2.0
"""Independent required-STRING source-prefix page expectations, never authority."""
import hashlib
from ashlar.schema import decode_retained_json
from ashlar.typed_graph_source_config import TypedGraphJsonlConfiguration
from ashlar.typed_graph_source import TypedGraphSourcePolicy
from ashlar.weft_binding import LAYOUT_SHA256
from .configured_local_read import ConfiguredLocalRead
from .configured_typed_graph_sdk_binding import configured_typed_graph_sdk_request
from .delta_custody import encoded

STRING={'family':'string','facets':{},'nullable':False}
_IR={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
_BACKEND={'backendId':'ashlar.databricks','backendVersion':'0.2.0-local-spark4.2-pages-candidate','interfaceVersion':'weft-backend/0.2.7','targetProfile':'spark4.2.0-delta4.4.1-candidate'}

def _require(value):
    if not value:raise ValueError('Configured SDK original page oracle refused')


def configured_sdk_page_oracle(scope,request,program):
    """Admit exact original first-page metadata and return a renewable pure oracle."""
    _require(type(scope)is ConfiguredLocalRead and type(request)is dict and type(program)is dict)
    scope.renew();scope.parity();config=scope.source.configuration
    _require(type(config)is TypedGraphJsonlConfiguration and type(config.policy)is TypedGraphSourcePolicy);config.verify()
    model=config.model_source;source=scope.source.original_source
    _require(set(program)=={'after','artifact','compiler','outputs','page','pageSize','request','status','structuralDigest','version'})
    _require(program.get('version')=='weft.engine-page/0.1'and program.get('status')=='compiled'and program.get('compiler')=='ashlar-spark4-pages'and program.get('request')==request and program.get('after')is None)
    size=program.get('pageSize');_require(type(size)is int and 1<=size<=999)
    binding=decode_retained_json(request['target']['bindingJson'].encode());names=sorted(scope.descriptor.versions);tables=binding['publication']['tables']
    _require(type(tables)is list and len(tables)==len(names)==4)
    aliases={name:'.'.join(table['name'])for name,table in zip(names,tables)}
    def rebuilt():return configured_typed_graph_sdk_request(config,dict(scope.descriptor.raw),scope.vector,scope.manifest_uuid,aliases,request['sql'],request['parameters'],layout_sha256=LAYOUT_SHA256)
    _require(rebuilt()==request)
    declared=request['parameters'];_require(type(declared)is dict and all(type(n)is str and bool(n)and type(v)is dict and set(v)=={'family','value'}and v['family']=='string'and type(v['value'])is str for n,v in declared.items()))
    artifact=program['artifact'];pins=[m['pin']for m in request['modules']]
    _require(type(artifact)is dict and artifact.get('status')=='compiled'and artifact.get('backend')==_BACKEND and artifact.get('interfaceVersion')=='weft-compile/0.2.7'and artifact.get('dialect')=='weft-sql/0.2.0'and artifact.get('modelPins')==pins and artifact.get('bindingSha256')==request['target']['bindingSha256'])
    ir=artifact.get('logicalPlan');_require(type(ir)is dict and set(ir)==_IR and ir['irVersion']=='weft-ir/0.2.7'and ir['modulePins']==pins and ir['aggregate']is False and ir['joins']==ir['groups']==[]and ir['limit']==size+1 and ir['readProfile']=={'subset':'entity-page','version':'weft-application-read/0.2.0'})
    scan=ir['source'];_require(type(scan)is dict and set(scan)=={'occurrence','pin','record'}and type(scan['occurrence'])is str and bool(scan['occurrence'])and scan['pin']in pins)
    records=[r for r in binding['records']if r['logical']==scan['record']];_require(len(records)==1);record=records[0];tid=record['typeId'];_require(tid in config.bindings)
    document=decode_retained_json(model);elements={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
    def logical(module,element):return {'documentId':document['id'],'revision':config.document_revision,'module':module,'element':element}
    used={}
    def field(identity):
        homes=[p['home']for p in record['properties']if p['logical']==identity];_require(len(homes)==1)
        home=homes[0];_require(type(home)is dict and set(home)=={'kind','propertyId'}and home['kind']=='props')
        selected=config.bindings[tid]['fields'].get(home['propertyId']);_require(selected is not None)
        module,element,family=selected;definition=elements[module,element]
        _require(identity==logical(module,element)and family=='string'and definition.get('scalarType')=='string'and definition.get('nullability')=='required'and definition.get('cardinality')=='one'and definition.get('facets',{})=={})
        used[encoded(identity)]={'availability':'required','identity':identity,'kind':'scalar','type':STRING}
        return home['propertyId']
    declaration=elements[scan['record']['module'],scan['record']['element']];keys=[k for k in declaration.get('keys',[])if k.get('primary')is True]
    _require(len(keys)==1 and 1<=len(keys[0]['fields'])<=16)
    keyfields=[logical(f['module'],f['element'])for f in keys[0]['fields']];keyhomes=tuple(field(f)for f in keyfields)
    key={'id':keys[0]['id'],'fields':keyfields,'types':[STRING for _ in keyfields]};_require(ir['pageKey']==key)
    page=program.get('page');_require(type(page)is dict and set(page)=={'generatedContinuationFilter','generatedOrderPositions','generatedOutputPositions','generatedSpans','keyOutputs','pageSize','visibleOutputs'})
    visible=page['visibleOutputs'];_require(type(visible)is int and 1<=visible<=128)
    hidden=list(range(visible,visible+len(keyfields)))
    _require(page=={'generatedContinuationFilter':None,'generatedOrderPositions':list(range(len(keyfields))),'generatedOutputPositions':hidden,'generatedSpans':'diagnostic-anchors','keyOutputs':hidden,'pageSize':size,'visibleOutputs':visible})
    def span(v):_require(type(v)is dict and set(v)=={'start','end'}and type(v['start'])is int and type(v['end'])is int and 0<=v['start']<=v['end']<=65536)
    _require(type(ir['order'])is list and len(ir['order'])==len(keyfields))
    for order,identity in zip(ir['order'],keyfields):
        _require(type(order)is dict and set(order)=={'identity','scan','span','type'}and order['identity']==identity and order['scan']==scan['occurrence']and order['type']==STRING);span(order['span'])
    outputs=ir['outputs'];columns=artifact.get('columns');_require(type(outputs)is list and type(columns)is list and len(outputs)==len(columns)==visible+len(keyfields))
    outputhomes=[];nameset=set();program_outputs=[]
    for i,(output,column)in enumerate(zip(outputs,columns)):
        _require(type(output)is dict and set(output)=={'expression','name'}and type(output['name'])is str and bool(output['name'])and output['name']not in nameset);nameset.add(output['name'])
        expr=output['expression'];_require(type(expr)is dict and set(expr)=={'op','identity','scan'}and expr['op']=='field'and expr['scan']==scan['occurrence'])
        identity=expr['identity'];outputhomes.append(field(identity))
        if i>=visible:_require(output['name']=='__weft_page_key_'+str(i-visible)and identity==keyfields[i-visible])
        _require(type(column)is dict and type(column.get('position'))is int and column=={'nullable':False,'outputName':output['name'],'position':i+1,'representation':{'kind':'scalar','logicalType':STRING,'carrier':'text','decoder':'text'},'sourceIdentities':[identity]})
        if i<visible:program_outputs.append({'carrier':'string','name':output['name'],'nullable':False})
    _require(program.get('outputs')==program_outputs)
    filters=[];parameters=set();_require(type(ir['filters'])is list and len(ir['filters'])<=128)
    for predicate in ir['filters']:
        _require(type(predicate)is dict and set(predicate)=={'left','op','right'}and predicate['op']=='equal')
        left,right=predicate['left'],predicate['right']
        _require(type(left)is dict and set(left)=={'identity','scan','span','type'}and left['scan']==scan['occurrence']and left['type']==STRING);span(left['span'])
        pid=field(left['identity'])
        _require(type(right)is dict and set(right)=={'kind','name','span','type','value'}and right['kind']=='parameter'and type(right['name'])is str and right['name']in declared and right['type']==STRING and right['value']==declared[right['name']]['value']);span(right['span'])
        parameters.add(right['name']);filters.append((pid,right['name']))
    _require(parameters==set(declared))
    caps={'key.uniqueStable','limit','order.asc','project','scan','type.string'}|({'equal','filter','parameter.named'}if filters else set())
    if len(filters)>1:caps.add('and')
    _require(type(ir['requiredCapabilities'])is list and sorted(ir['requiredCapabilities'])==sorted(caps)and type(ir['typeGraph'])is list and sorted(map(encoded,ir['typeGraph']))==sorted(map(encoded,used.values())))
    original_request=encoded(request);original_program=encoded(program);original_rows=encoded(scope.source_rows())
    def current():
        scope.renew();scope.parity();config.verify()
        _require(config.model_source==model and scope.source.original_source==source and encoded(request)==original_request and encoded(program)==original_program and rebuilt()==request and encoded(scope.source_rows())==original_rows)
    def expected(parameters_dict,after_tuple_or_None):
        current();_require(type(parameters_dict)is dict and set(parameters_dict)==set(declared)and all(type(v)is str and '\0'not in v and len(v.encode('utf-8'))<=65536 for v in parameters_dict.values())and len(encoded(parameters_dict).encode())<=1048576)
        after=after_tuple_or_None
        _require(after is None or type(after)is tuple and len(after)==len(keyfields)and all(type(v)is str and '\0'not in v and len(v.encode('utf-8'))<=65536 for v in after))
        selected=[];seen=set();records=0
        for row in scope.source_rows()['object_current']:
            if row['source_system']!=config.source_system or row['type_id']!=tid:continue
            records+=1;_require(records<=1000 and row['schema_revision']==config.document_revision)
            props=decode_retained_json(row['props_json'].encode());_require(type(props)is dict and all(type(props.get(pid))is str and '\0'not in props[pid]for pid in set(keyhomes+tuple(outputhomes)+tuple(p for p,_ in filters))))
            original_key=tuple(props[p]for p in keyhomes);_require(original_key not in seen);seen.add(original_key)
            if any(props[pid]!=parameters_dict[name]for pid,name in filters):continue
            if after is not None and tuple(v.encode('utf-8')for v in original_key)<=tuple(v.encode('utf-8')for v in after):continue
            selected.append((tuple(v.encode('utf-8')for v in original_key),[props[p]for p in outputhomes]))
        selected.sort(key=lambda item:item[0]);rows=[values for _,values in selected[:size+1]]
        current()
        return {'rows':rows,'witnesses':{'publication_id':scope.descriptor.publication_id,'source_sha256':hashlib.sha256(source).hexdigest(),'source_records':records,'matching_after_records':len(selected),'page_size':size,'lookahead':True},'scope':'Independent original source-prefix required STRING authored-key page with named equality filters; visible plus hidden keys and lookahead only, no native/source/continuation authority.'}
    current();return expected
