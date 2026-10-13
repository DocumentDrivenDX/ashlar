"""One forward literal-key HAS_RELATED filter, under accepted Truss semantics."""
import hashlib,json
from .truss_graph_admission import TrussGraphAdmission
from .delta_custody import encoded


def admit_truss_relationship_filter(admission,artifact,binding,expected,*,revision,document_id):
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:
        raise ValueError('Actual accepted relationship filter source required')
    selected=admission.revisions[revision];model,raw=selected['models'][document_id];digest=hashlib.sha256(raw).hexdigest()
    pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']};string={'family':'string','facets':{},'nullable':False}
    if (artifact.get('status')!='compiled'or artifact.get('interfaceVersion')!='weft-compile/0.2.0'or artifact.get('dialect')!='weft-sql/0.2.0'or artifact.get('compilerVersion')!='weft/0.1.0'
            or artifact.get('modelPins')!=[pin]or binding['modelPins']!=[pin]):raise ValueError('Exact public relationship filter compiler profile required')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if (type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.2.0'or plan['aggregate']is not False
            or any(plan[key]!=[]for key in ('groups','joins','order'))or any(plan[key]is not None for key in ('limit','pageKey','readProfile'))
            or plan['modulePins']!=[pin]or plan['requiredCapabilities']!=['filter','project','relationship.exists','scan','type.string']
            or type(plan['filters'])is not list or len(plan['filters'])!=1):raise ValueError('Only one unordered forward HAS_RELATED predicate supported')
    predicate=plan['filters'][0]
    if set(predicate)!={'key','op','relationship','scan'}or predicate['op']!='hasRelated'or predicate['scan']!='s0':raise ValueError('Exact HAS_RELATED filter required')
    relation=predicate['relationship'];source=plan['source'];installation=json.loads(admission.batch.begin)['prior']['installationId']
    if set(source)!={'occurrence','pin','record'}or source['occurrence']!='s0'or source['pin']!=pin:raise ValueError('Exact original filter scan required')
    mappings=[item for item in binding.get('relationships',[])if item['logical']==relation.get('identity')]
    if len(mappings)!=1:raise ValueError('Actual accepted filter relationship required')
    mapping=mappings[0];edge=selected['edges'].get(mapping['typeId'])
    if (edge is None or mapping['kind']!='edge'or mapping['sourceSystem']!=installation or mapping['schemaRevision']!=revision
            or mapping['acceptedDefinition']!=edge['declaration']or edge['owner']!=(document_id,mapping['logical']['module'],mapping['logical']['relationship'])
            or mapping['source']!=source['record']):raise ValueError('Original filter relationship allocation differs')
    index=mapping['table'];tables=binding['publication']['tables']
    if (mapping['logical']!={'documentId':document_id,'revision':digest,'module':edge['owner'][1],'relationship':edge['owner'][2]}
            or type(index)is not int or not 0<=index<len(tables) or tables[index]['name'][-1]!='edge_current'):
        raise ValueError('Original filter relationship identity/table differs')
    definitions={(module['id'],element['id']):element for module in model['modules']for element in module['elements']}
    logical=lambda ref:{'documentId':document_id,'revision':digest,'module':ref['module'],'element':ref['element']}
    def record(identity,type_id):
        matches=[item for item in binding['records']if item['logical']==identity];actual=selected['types'].get(type_id)
        if len(matches)!=1 or actual is None or actual['owner']!=(document_id,identity['module'],identity['element']):raise ValueError('Actual typed filter endpoint required')
        item=matches[0];index=item['table'];tables=binding['publication']['tables']
        if (item['typeId']!=type_id or item['kind']!='object'or item['sourceSystem']!=installation or item['schemaRevision']!=revision
                or type(index)is not int or not 0<=index<len(tables)or tables[index]['name'][-1]!='object_current'):raise ValueError('Exact physical filter endpoint required')
        return item
    source_map=record(mapping['source'],edge['endpoints'][0]);target_map=record(mapping['target'],edge['endpoints'][1])
    def home(item,identity):
        declaration=definitions.get((identity['module'],identity['element']))
        fields=[field for field in item['properties']if field['logical']==identity]
        allocations=[field for field in json.loads(selected['report'])['properties']if field['typeId']==item['typeId']and field['moduleId']==identity['module']and field['elementId']==identity['element']]
        if (identity.get('documentId')!=document_id or identity.get('revision')!=digest or declaration is None
                or declaration.get('scalarType')!='string'or declaration.get('nullability')!='required'or declaration.get('facets',{})!={}
                or len(fields)!=1 or len(allocations)!=1 or fields[0]['home']!={'kind':'props','propertyId':allocations[0]['propertyId']}):raise ValueError('Original required STRING property home differs')
        return fields[0]['home']['propertyId']
    def key(item,key_id):
        declaration=definitions[item['logical']['module'],item['logical']['element']]
        choices=[value for value in declaration.get('keys',[])if value['id']==key_id]if key_id is not None else [value for value in declaration.get('keys',[])if value.get('primary')is True]
        if len(choices)!=1:raise ValueError('Actual declared endpoint key required')
        identities=[logical(ref)for ref in choices[0]['fields']]
        for identity in identities:home(item,identity)
        return {'id':choices[0]['id'],'fields':identities,'types':[string for _ in identities]}
    declaration=edge['declaration'];source_key=key(source_map,declaration['source'][0].get('key'));target_key=key(target_map,declaration['target'][0].get('key'))
    wanted={'from':mapping['source'],'to':mapping['target'],'identity':mapping['logical'],'inverse':False,'sourceKey':source_key,'targetKey':target_key,
        'sourceMultiplicity':declaration['sourceMultiplicity'],'targetMultiplicity':declaration['targetMultiplicity'],'targetLifecycle':declaration['targetLifecycle']}
    if relation!=wanted:raise ValueError('Actual forward filter declaration/key order differs')
    literals=predicate['key'];values=[]
    if type(literals)is not list or len(literals)!=len(target_key['fields']):raise ValueError('Complete authored target key tuple required')
    for literal in literals:
        if (set(literal)!={'kind','span','type','value'}or literal['kind']!='literal'or literal['type']!=string or type(literal['value'])is not str
                or type(literal['span'])is not dict or set(literal['span'])!={'start','end'}
                or any(type(literal['span'][part])is not int or literal['span'][part]<0 for part in ('start','end'))or literal['span']['end']<literal['span']['start']):raise ValueError('Exact required STRING key literal required')
        values.append(literal['value'])
    fields=[];names=[];columns=artifact['columns'];outputs=plan['outputs']
    if not 1<=len(outputs)<=64 or len(columns)!=len(outputs):raise ValueError('Complete bounded filter projections required')
    for position,(column,output)in enumerate(zip(columns,outputs),1):
        expression=output['expression']
        if (set(output)!={'name','expression'}or set(expression)!={'op','scan','identity'}or expression['op']!='field'or expression['scan']!='s0'
                or set(column)!={'nullable','outputName','position','representation','sourceIdentities'}or column['position']!=position
                or type(output['name'])is not str or not output['name']or output['name']in names or column['outputName']!=output['name']or column['nullable']is not False
                or column['sourceIdentities']!=[expression['identity']]or column['representation']!={'kind':'scalar','logicalType':string,'carrier':'text','decoder':'text'}):raise ValueError('Exact source STRING filter output required')
        names.append(output['name']);fields.append((output['name'],home(source_map,expression['identity'])))
    identities=[output['expression']['identity']for output in outputs]
    if plan['typeGraph']!=[{'availability':'required','identity':identity,'kind':'scalar','type':string}for identity in dict((encoded(value),value)for value in identities).values()]:raise ValueError('Complete filter type graph differs')
    target_homes=[home(target_map,identity)for identity in target_key['fields']]
    objects={(row['source_system'],row['type_id'],row['id']):row for row in expected['object_current']};answer=[]
    if len(objects)!=len(expected['object_current']):raise ValueError('Duplicate original filter object identity')
    for (system,type_id,entity_id),row in objects.items():
        if (system,type_id,row['schema_revision'])!=(installation,source_map['typeId'],revision):continue
        matched=False
        for carrier in expected['edge_current']:
            if (carrier['source_system'],carrier['rel_type_id'],carrier['source_type'],carrier['source_id'],carrier['schema_revision'])!=(installation,mapping['typeId'],source_map['typeId'],entity_id,revision):continue
            if carrier['target_type']!=target_map['typeId']:raise ValueError('Original filter edge target type differs')
            target=objects[installation,target_map['typeId'],carrier['target_id']]
            if target['schema_revision']!=revision:raise ValueError('Original filter target revision differs')
            props=json.loads(target['props_json']);tuple_values=[props[field]for field in target_homes]
            if any(type(value)is not str for value in tuple_values):raise ValueError('Original filter target STRING values required')
            if tuple_values==values:matched=True
        if matched:
            props=json.loads(row['props_json']);result={name:props[field]for name,field in fields}
            if any(type(value)is not str for value in result.values()):raise ValueError('Original filter source STRING values required')
            answer.append(result)
    return answer
