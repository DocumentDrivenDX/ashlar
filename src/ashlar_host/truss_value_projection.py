"""Pure optional STRING projection admission; live publication guards stay owned."""
import hashlib,json
from collections import Counter
from dataclasses import dataclass
from types import SimpleNamespace
from ashlar.schema import decode_retained_json
from ashlar.weft_decode import decode_string_column
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission


@dataclass(frozen=True)
class TrussValueProjection:
    columns_json:str
    policy_json:str
    intake_json:str
    expected_json:str
    def decode(self,column,value):
        columns=json.loads(self.columns_json)
        if type(column)is not dict or encoded(column)not in [encoded(item)for item in columns]:raise ValueError('Exact admitted projection column required')
        policy=json.loads(self.policy_json)
        policy['property_fields']={kind:{pid:tuple(ref)for pid,ref in fields.items()}for kind,fields in policy['property_fields'].items()}
        decoded=decode_string_column(column,value,SimpleNamespace(**policy),SimpleNamespace(**json.loads(self.intake_json)))
        return decoded if type(decoded)is str else dict(decoded)
    def require_complete_bag(self,rows):
        if type(rows)is not list or Counter(map(encoded,rows))!=Counter(map(encoded,json.loads(self.expected_json))):
            raise ValueError('Independent complete original optional STRING bag differs')


def admit_truss_value_projection(admission,artifact,request,manifest,expected,*,revision,document_id):
    """Bind only direct, unordered, unfiltered single-scan STRING fields.

    The caller still runs every emitted compiler obligation, under genuine
    original publication/source custody, before any native result is decoded.
    """
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:raise ValueError('Actual accepted Truss projection source required')
    selected=admission.revisions[revision]
    model,original=selected['models'][document_id]
    if encoded(model)!=encoded(json.loads(original)):raise ValueError('Exact original declared model bytes required')
    digest=hashlib.sha256(original).hexdigest()
    pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    binding=decode_retained_json(request['target']['bindingJson'].encode())
    names=sorted(json.loads(manifest['table_versions_json']));tables=binding['publication']['tables']
    if len(tables)!=len(names):raise ValueError('Complete original projection binding required')
    # Reuse the public owner of the complete binding rather than another mapper.
    from .truss_weft_query import truss_string_compile_request
    rebuilt=truss_string_compile_request(admission,manifest,{name:table['uuid']for name,table in zip(names,tables)},
        binding['publication']['manifestUuid'],request['sql'],revision=revision,document_id=document_id,
        aliases={name:'.'.join(table['name'])for name,table in zip(names,tables)})
    if encoded(rebuilt)!=encoded(request)or artifact.get('bindingSha256')!=request['target']['bindingSha256']:
        raise ValueError('Complete original projection request/binding differs')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if (type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.2.0'or plan['aggregate']is not False
            or any(plan[key]!=[]for key in ('filters','groups','joins','order'))or any(plan[key]is not None for key in ('limit','pageKey','readProfile'))
            or plan['modulePins']!=[pin]or artifact.get('modelPins')!=[pin]or binding['modelPins']!=[pin]
            or plan['requiredCapabilities']!=['project','scan','type.string','value.presence']):
        raise ValueError('Only direct unfiltered optional STRING projection supported')
    source=plan['source']
    if type(source)is not dict or set(source)!={'occurrence','pin','record'}or source['occurrence']!='s0'or source['pin']!=pin:
        raise ValueError('Exact original projection scan required')
    matches=[record for record in binding['records']if record['logical']==source['record']]
    if len(matches)!=1:raise ValueError('Exact original projection record required')
    record=matches[0];actual=selected['types'][record['typeId']]
    if actual['owner']!=(document_id,source['record']['module'],source['record']['element'])or record['kind']!='object':raise ValueError('Original accepted object projection required')
    elements={(module['id'],element['id']):element for module in model['modules']for element in module['elements']}
    home={encoded(prop['logical']):prop['home']['propertyId']for prop in record['properties']}
    policy={'source_sha256':digest,'property_fields':{record['typeId']:{}},'types':{record['typeId']:{}}}
    columns=artifact.get('columns');outputs=plan['outputs'];descriptors={};selections=[];optional=0;output_names=set()
    if type(columns)is not list or not 1<=len(columns)<=64 or len(outputs)!=len(columns):raise ValueError('Bounded exact projection output inventory required')
    for position,(column,output)in enumerate(zip(columns,outputs),1):
        expression=output.get('expression');identity=expression.get('identity')if type(expression)is dict else None
        if (type(output)is not dict or set(output)!={'name','expression'}or type(expression)is not dict or set(expression)!={'op','scan','identity'}
                or expression['op']!='field'or expression['scan']!='s0'or type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}
                or identity['documentId']!=document_id or identity['revision']!=digest or encoded(identity)not in home):raise ValueError('Exact selected original Field projection required')
        field=elements[identity['module'],identity['element']];availability=field['nullability'];pid=home[encoded(identity)]
        if field.get('kind')!='field'or field.get('scalarType')!='string'or field.get('cardinality')!='one'or availability not in ('required','absent-allowed'):
            raise ValueError('Original STRING field availability required')
        string={'family':'string','facets':{},'nullable':False}
        representation={'kind':'value','descriptor':identity,'nativeNull':False}if availability=='absent-allowed'else{'kind':'scalar','logicalType':string,'carrier':'text','decoder':'text'}
        if (type(column)is not dict or set(column)!={'nullable','outputName','position','representation','sourceIdentities'}or type(column['position'])is not int or column['position']!=position
                or column['nullable']is not False or column['outputName']!=output['name']or type(output['name'])is not str or not output['name']or output['name']in output_names
                or column['sourceIdentities']!=[identity]or column['representation']!=representation):raise ValueError('Exact original optional result descriptor required')
        output_names.add(output['name']);optional+=availability=='absent-allowed'
        policy['property_fields'][record['typeId']][pid]=[identity['module'],identity['element']]
        policy['types'][record['typeId']][pid]=availability
        descriptors[encoded(identity)]={'availability':availability,'identity':identity,'kind':'scalar','type':string}
        selections.append((column,pid,availability))
    if not optional:raise ValueError('Optional value projection requires an optional field')
    graph=plan['typeGraph']
    if type(graph)is not list or len(graph)!=len(descriptors)or encoded({encoded(item.get('identity')):item for item in graph})!=encoded(descriptors):
        raise ValueError('Complete original projection type graph differs')
    # Public decoder expects logical field tuples, not mutable caller policies.
    intake={'source_sha256':digest,'document_id':document_id,'document_revision':digest}
    rows=expected['object_current']
    if type(rows)is not list or len(rows)>10000:raise ValueError('Bounded independent original row bag required')
    wanted=[]
    for row in rows:
        if (row['source_system'],row['type_id'])!=(record['sourceSystem'],record['typeId']):continue
        prior=admission.revisions.get(row['schema_revision'])
        prior_type=None if prior is None else prior['types'].get(record['typeId'])
        if (prior_type is None or prior_type['owner']!=actual['owner']or prior_type['keys']!=actual['keys']
                or any(actual['fields'].get(pid)!=pin for pid,pin in prior_type['fields'].items())):
            raise ValueError('Original compatible row schema/key correspondence required')
        props=decode_retained_json(row['props_json'].encode());result={}
        if type(props)is not dict:raise ValueError('Complete original property map required')
        for column,pid,availability in selections:
            if pid not in prior_type['fields']and (availability!='absent-allowed'or pid in props):raise ValueError('Required original field absent from prior schema')
            if pid in props:
                if type(props[pid])is not str:raise ValueError('Exact independent original STRING property required')
                result[column['outputName']]={'state':'value','value':props[pid]}if availability=='absent-allowed'else props[pid]
            elif availability=='absent-allowed':result[column['outputName']]={'state':'absent'}
            else:raise ValueError('Required independent source property missing')
        wanted.append(result)
    # JSON snapshots keep the pure decoder inputs detached from mutable admission.
    return TrussValueProjection(encoded(columns),encoded(policy),encoded(intake),encoded(wanted))
