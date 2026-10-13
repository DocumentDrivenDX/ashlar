"""One public required-STRING equality INNER JOIN COUNT, not an edge join."""
from collections import Counter
import hashlib,json
from .truss_graph_admission import TrussGraphAdmission


def admit_truss_join_count(admission,artifact,binding,expected,*,revision,document_id):
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:
        raise ValueError('Actual accepted join count source required')
    selected=admission.revisions[revision];model,raw=selected['models'][document_id]
    digest=hashlib.sha256(raw).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    string={'family':'string','facets':{},'nullable':False};integer={'family':'integer','facets':{},'nullable':False}
    if (artifact.get('status')!='compiled'or artifact.get('interfaceVersion')!='weft-compile/0.2.0'or artifact.get('dialect')!='weft-sql/0.2.0'
            or artifact.get('compilerVersion')!='weft/0.1.0'or artifact.get('modelPins')!=[pin]or binding['modelPins']!=[pin]):
        raise ValueError('Exact public join count compiler profile required')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if (type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.2.0'or plan['aggregate']is not True
            or any(plan[key]!=[]for key in ('filters','groups','order','typeGraph'))or any(plan[key]is not None for key in ('limit','pageKey','readProfile'))
            or plan['modulePins']!=[pin]or plan['requiredCapabilities']!=['aggregate','aggregate.count','equal','innerJoin','project','scan','type.string']
            or type(plan['joins'])is not list or len(plan['joins'])!=1):raise ValueError('Only one unfiltered STRING inner join count supported')
    join=plan['joins'][0]
    if set(join)!={'on','right'}or type(join['on'])is not list or len(join['on'])!=1:raise ValueError('One original field equality required')
    predicate=join['on'][0]
    if set(predicate)!={'op','left','right'}or predicate['op']!='equal'or set(predicate['right'])!={'kind','field'}or predicate['right']['kind']!='field':raise ValueError('Exact typed field equality required')
    installation=json.loads(admission.batch.begin)['prior']['installationId']
    definitions={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    reports=json.loads(selected['report'])['properties'];tables=binding['publication']['tables']
    def endpoint(scan,field,occurrence):
        if set(scan)!={'occurrence','pin','record'}or scan['occurrence']!=occurrence or scan['pin']!=pin:raise ValueError('Exact join scan required')
        identity=scan['record'];maps=[r for r in binding['records']if r['logical']==identity]
        if len(maps)!=1:raise ValueError('Actual join record allocation required')
        record=maps[0];actual=selected['types'].get(record['typeId']);index=record['table']
        if (actual is None or actual['owner']!=(document_id,identity['module'],identity['element'])or identity.get('revision')!=digest or identity.get('documentId')!=document_id
                or record['kind']!='object'or record['sourceSystem']!=installation or record['schemaRevision']!=revision
                or type(index)is not int or not 0<=index<len(tables)or tables[index]['name'][-1]!='object_current'):
            raise ValueError('Exact accepted join physical record required')
        if set(field)!={'identity','scan','span','type'}or field['scan']!=occurrence or field['type']!=string:raise ValueError('Exact required STRING join field required')
        span=field['span']
        if type(span)is not dict or set(span)!={'start','end'}or any(type(span[k])is not int or span[k]<0 for k in span)or span['end']<span['start']:raise ValueError('Original field span required')
        logical=field['identity'];homes=[p for p in record['properties']if p['logical']==logical]
        allocated=[p for p in reports if p['typeId']==record['typeId']and p['moduleId']==logical['module']and p['elementId']==logical['element']]
        declaration=definitions.get((logical['module'],logical['element']))
        if (logical.get('documentId')!=document_id or logical.get('revision')!=digest or declaration is None
                or declaration.get('scalarType')!='string'or declaration.get('nullability')!='required'or declaration.get('facets',{})!={}
                or len(homes)!=1 or len(allocated)!=1 or allocated[0]['propertyId']not in actual['required']
                or homes[0]['home']!={'kind':'props','propertyId':allocated[0]['propertyId']}):raise ValueError('Actual accepted join property home required')
        counts=Counter()
        for row in expected['object_current']:
            if (row['source_system'],row['type_id'],row['schema_revision'])!=(installation,record['typeId'],revision):continue
            value=json.loads(row['props_json'])[allocated[0]['propertyId']]
            if type(value)is not str:raise ValueError('Original required join STRING value required')
            counts[value]+=1
        return counts
    left=endpoint(plan['source'],predicate['left'],'s0');right=endpoint(join['right'],predicate['right']['field'],'s1')
    outputs=plan['outputs'];columns=artifact['columns']
    if type(outputs)is not list or len(outputs)!=1 or type(columns)is not list or len(columns)!=1:raise ValueError('One global join count output required')
    output=outputs[0];column=columns[0];name=output.get('name')
    if (type(name)is not str or not name or output!={'name':name,'expression':{'op':'count','type':integer}}
            or column!={'nullable':False,'outputName':name,'position':1,'representation':{'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'},
                'sourceIdentities':[plan['source']['record'],join['right']['record']]}):raise ValueError('Exact original count descriptor required')
    count=sum(n*right[value]for value,n in left.items())
    if count>9223372036854775807:raise ValueError('Exact signed64 join count bound required')
    return [{name:count}]
