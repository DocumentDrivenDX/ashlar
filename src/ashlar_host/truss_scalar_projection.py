"""Direct required scalar projections from original accepted faceted models."""
import hashlib,json
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from ashlar.schema import decode_retained_json
from ashlar.weft_decode import decode_exact_scalar
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission

@dataclass(frozen=True)
class TrussScalarProjection:
    columns_json:str
    expected_json:str
    def decode(self,column,value):
        if encoded(column)not in [encoded(c)for c in json.loads(self.columns_json)]:raise ValueError('Exact admitted scalar column required')
        scalar=decode_exact_scalar(column['representation'],value)
        family=column['representation']['logicalType']['family']
        return {family+'_token':scalar.original}if family in ('integer','decimal')else scalar.value
    def require_complete_bag(self,rows):
        columns=json.loads(self.columns_json)
        def normalized(row):
            if type(row)is not dict or set(row)!={c['outputName']for c in columns}:raise ValueError('Complete exact scalar result row required')
            out={}
            for c in columns:
                value=row[c['outputName']];family=c['representation']['logicalType']['family']
                if family in ('integer','decimal'):
                    if type(value)is not dict or set(value)!={family+'_token'}:raise ValueError('Exact numeric token result required')
                    scalar=decode_exact_scalar(c['representation'],value[family+'_token'])
                    # Decimal.normalize uses ambient precision; avoid it entirely.
                    number=Decimal(scalar.original)
                    numerator,denominator=number.as_integer_ratio()
                    out[c['outputName']]=[numerator,denominator]
                else:out[c['outputName']]=decode_exact_scalar(c['representation'],value if family=='string'else ('true'if value is True else 'false'if value is False else value)).value
            return encoded(out)
        if type(rows)is not list or Counter(map(normalized,rows))!=Counter(map(normalized,json.loads(self.expected_json))):raise ValueError('Independent complete original scalar bag differs')

def admit_truss_scalar_projection(admission,artifact,request,manifest,expected,*,revision,document_id,physical_revisions):
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:raise ValueError('Actual original scalar admission required')
    selected=admission.revisions[revision];model,original=selected['models'][document_id]
    if encoded(model)!=encoded(json.loads(original)):raise ValueError('Original scalar model bytes required')
    digest=hashlib.sha256(original).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    binding=decode_retained_json(request['target']['bindingJson'].encode());names=sorted(json.loads(manifest['table_versions_json']));tables=binding['publication']['tables']
    if len(names)!=len(tables):raise ValueError('Complete scalar publication binding required')
    from .truss_compatible_binding import truss_compatible_compile_request
    rebuilt=truss_compatible_compile_request(admission,manifest,{n:t['uuid']for n,t in zip(names,tables)},binding['publication']['manifestUuid'],request['sql'],revision=revision,document_id=document_id,physical_revisions=physical_revisions,aliases={n:'.'.join(t['name'])for n,t in zip(names,tables)})
    if encoded(rebuilt)!=encoded(request)or artifact.get('bindingSha256')!=request['target']['bindingSha256']:raise ValueError('Original scalar request correspondence required')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.2.0'or plan['aggregate']is not False or any(plan[k]!=[]for k in ('filters','groups','joins','order'))or any(plan[k]is not None for k in ('limit','pageKey','readProfile'))or plan['modulePins']!=[pin]or artifact.get('modelPins')!=[pin]or binding['modelPins']!=[pin]:raise ValueError('Only direct required scalar projection supported')
    source=plan['source']
    if type(source)is not dict or set(source)!={'occurrence','pin','record'}or source['occurrence']!='s0'or source['pin']!=pin:raise ValueError('Exact scalar scan required')
    records=[r for r in binding['records']if r['logical']==source['record']]
    if len(records)!=1:raise ValueError('One original scalar record required')
    record=records[0];actual=selected['types'][record['typeId']]
    if record['kind']!='object'or actual['owner']!=(document_id,source['record']['module'],source['record']['element']):raise ValueError('Original accepted scalar object required')
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};home={encoded(p['logical']):p['home']['propertyId']for p in record['properties']}
    columns=artifact.get('columns');outputs=plan['outputs'];graph={};selections=[];families=set();output_names=set()
    if type(columns)is not list or not 1<=len(columns)<=64 or len(columns)!=len(outputs):raise ValueError('Bounded scalar column inventory required')
    for position,(column,output)in enumerate(zip(columns,outputs),1):
        expression=output.get('expression');identity=expression.get('identity')if type(expression)is dict else None
        if type(output)is not dict or set(output)!={'name','expression'}or type(expression)is not dict or set(expression)!={'op','scan','identity'}or expression['op']!='field'or expression['scan']!='s0'or type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}or identity['documentId']!=document_id or identity['revision']!=digest or encoded(identity)not in home:raise ValueError('Exact original scalar Field required')
        field=elements[identity['module'],identity['element']];family=field.get('scalarType');facets=field.get('facets',{})
        admitted={'string':{},'boolean':{},'integer':{'integerWidth':{'bits':64,'signed':True}},'decimal':{'precision':18,'scale':4}}
        if field.get('kind')!='field'or field.get('cardinality')!='one'or field.get('nullability')!='required'or family not in admitted or facets!=admitted[family]:raise ValueError('Required original exact scalar domain required')
        logical={'family':family,'facets':facets,'nullable':False};representation={'kind':'scalar','logicalType':logical,'carrier':'text','decoder':{'string':'text','boolean':'boolean','integer':'exact-integer','decimal':'exact-decimal'}[family]}
        if type(column)is not dict or set(column)!={'nullable','outputName','position','representation','sourceIdentities'}or type(column['position'])is not int or column['position']!=position or column['nullable']is not False or column['representation']!=representation or column['sourceIdentities']!=[identity]or column['outputName']!=output['name']or type(output['name'])is not str or not output['name']or output['name']in output_names:raise ValueError('Exact authored scalar result descriptor required')
        output_names.add(output['name']);families.add(family);graph[encoded(identity)]={'availability':'required','identity':identity,'kind':'scalar','type':logical};selections.append((column,home[encoded(identity)]))
    if plan['requiredCapabilities']!=sorted(['project','scan']+['type.'+f for f in families]):raise ValueError('Exact scalar capabilities required')
    if type(plan['typeGraph'])is not list or len(plan['typeGraph'])!=len(graph)or encoded({encoded(g.get('identity')):g for g in plan['typeGraph']})!=encoded(graph):raise ValueError('Complete exact scalar type graph required')
    rows=expected['object_current'];wanted=[]
    if type(rows)is not list or len(rows)>10000:raise ValueError('Bounded independent scalar source bag required')
    for row in rows:
        if (row['source_system'],row['type_id'])!=(record['sourceSystem'],record['typeId']):continue
        if row['schema_revision']not in physical_revisions:raise ValueError('Source outside admitted physical revisions')
        prior=admission.revisions[row['schema_revision']]['types'][record['typeId']]
        if prior['owner']!=actual['owner']or prior['keys']!=actual['keys']or any(actual['fields'].get(pid)!=v for pid,v in prior['fields'].items()):raise ValueError('Original scalar row correspondence required')
        props=decode_retained_json(row['props_json'].encode());result={}
        if type(props)is not dict:raise ValueError('Complete scalar property map required')
        for column,pid in selections:
            if pid not in prior['fields']or pid not in props:raise ValueError('Required scalar source field missing')
            family=column['representation']['logicalType']['family'];value=props[pid]
            if family=='integer'and type(value)is not int or family=='decimal'and type(value)not in (int,Decimal)or family=='string'and type(value)is not str or family=='boolean'and type(value)is not bool:raise ValueError('Original scalar source family differs')
            # Comparison-only carrier: source bytes stay retained; compiler tokens stay exact.
            token=('0'if value==0 else format(value,'f'))if family=='decimal'else str(value)
            result[column['outputName']]={family+'_token':token}if family in ('integer','decimal')else value
        wanted.append(result)
    return TrussScalarProjection(encoded(columns),encoded(wanted))
