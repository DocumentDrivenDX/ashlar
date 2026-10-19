# SPDX-License-Identifier: MIT OR Apache-2.0
"""One exact required numeric group plus COUNT over the complete source bag."""
import hashlib,json
from dataclasses import dataclass
from decimal import Decimal
from collections import Counter
from ashlar.schema import decode_retained_json
from ashlar.weft_decode import decode_exact_scalar
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_compatible_binding import truss_compatible_compile_request,admit_truss_compatible_revisions

@dataclass(frozen=True)
class TrussNumericGroupedCount:
    columns_json:str
    expected_json:str
    count_name:str
    def decode(self,column,value):
        if encoded(column)not in [encoded(c)for c in json.loads(self.columns_json)]:raise ValueError('Exact numeric grouped count column required')
        is_count=column['outputName']==self.count_name
        decoded=decode_exact_scalar(column['representation'],value,count=is_count)
        return decoded.value if is_count else {column['representation']['logicalType']['family']+'_token':decoded.original}
    def require_complete_bag(self,rows):
        columns=json.loads(self.columns_json)
        def normalize(row):
            if type(row)is not dict or set(row)!={c['outputName']for c in columns}:raise ValueError('Complete numeric grouped count row required')
            result={}
            for column in columns:
                name=column['outputName'];value=row[name]
                if name==self.count_name:
                    if type(value)is not int:raise ValueError('Exact nonnegative grouped count required')
                    result[name]=decode_exact_scalar(column['representation'],str(value),count=True).value
                else:
                    family=column['representation']['logicalType']['family'];key=family+'_token'
                    if type(value)is not dict or set(value)!={key}:raise ValueError('Exact numeric grouping token required')
                    decoded=decode_exact_scalar(column['representation'],value[key]);number=Decimal(decoded.original);a,b=number.as_integer_ratio();result[name]=[a,b]
            return encoded(result)
        if type(rows)is not list or Counter(map(normalize,rows))!=Counter(map(normalize,json.loads(self.expected_json))):raise ValueError('Independent complete original numeric grouped bag differs')


def admit_truss_numeric_grouped_count(admission,artifact,request,manifest,expected,*,revision,document_id,physical_revisions):
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:raise ValueError('Actual original numeric grouping admission required')
    selected=admission.revisions[revision];model,original=selected['models'][document_id]
    if encoded(model)!=encoded(json.loads(original)):raise ValueError('Original numeric grouping model required')
    digest=hashlib.sha256(original).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    binding=decode_retained_json(request['target']['bindingJson'].encode());names=sorted(json.loads(manifest['table_versions_json']));tables=binding['publication']['tables']
    if len(names)!=len(tables):raise ValueError('Complete numeric grouping publication required')
    rebuilt=truss_compatible_compile_request(admission,manifest,{n:t['uuid']for n,t in zip(names,tables)},binding['publication']['manifestUuid'],request['sql'],revision=revision,document_id=document_id,physical_revisions=physical_revisions,aliases={n:'.'.join(t['name'])for n,t in zip(names,tables)})
    if encoded(rebuilt)!=encoded(request):raise ValueError('Original numeric grouping request differs')
    proof=admit_truss_compatible_revisions(admission,artifact,binding,revision=revision,document_id=document_id)
    if proof['physical_revisions']!=list(physical_revisions):raise ValueError('Exact numeric grouping revision set required')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.2.0'or plan['aggregate']is not True or any(plan[k]!=[]for k in ('filters','joins','order'))or any(plan[k]is not None for k in ('limit','pageKey','readProfile'))or plan['modulePins']!=[pin]:raise ValueError('Only one unfiltered unordered numeric grouping supported')
    source=plan['source'];groups=plan['groups']
    if type(source)is not dict or set(source)!={'occurrence','pin','record'}or source['occurrence']!='s0'or source['pin']!=pin or type(groups)is not list or len(groups)!=1:raise ValueError('Exact single numeric grouping scan required')
    records=[r for r in binding['records']if r['logical']==source['record']]
    if len(records)!=1:raise ValueError('One original numeric grouping record required')
    record=records[0];actual=selected['types'][record['typeId']];group=groups[0]
    if type(group)is not dict or set(group)!={'identity','scan','span','type'}or group['scan']!='s0':raise ValueError('Exact original numeric grouping field required')
    identity=group['identity'];span=group['span']
    if type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}or identity['documentId']!=document_id or identity['revision']!=digest or type(span)is not dict or set(span)!={'start','end'}or any(type(span[k])is not int or span[k]<0 for k in span)or span['end']<span['start']:raise ValueError('Exact original numeric group identity/span required')
    declarations={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};declaration=declarations.get((identity['module'],identity['element']));homes=[p['home']for p in record['properties']if p['logical']==identity]
    domains={'integer':{'integerWidth':{'bits':64,'signed':True}},'decimal':{'precision':18,'scale':4}}
    family=declaration.get('scalarType')if type(declaration)is dict else None
    if declaration is None or declaration.get('kind')!='field'or declaration.get('cardinality')!='one'or declaration.get('nullability')!='required'or family not in domains or declaration.get('facets',{})!=domains[family]or len(homes)!=1 or homes[0].get('kind')!='props'or homes[0].get('propertyId')not in actual['required']:raise ValueError('Actual exact required numeric grouping domain/home required')
    logical={'family':family,'facets':domains[family],'nullable':False};representation={'kind':'scalar','logicalType':logical,'carrier':'text','decoder':'exact-'+family};pid=homes[0]['propertyId']
    if group['type']!=logical or plan['typeGraph']!=[{'availability':'required','identity':identity,'kind':'scalar','type':logical}]or plan['requiredCapabilities']!=['aggregate','aggregate.count','group','project','scan','type.'+family]:raise ValueError('Exact numeric grouping type graph/capabilities required')
    columns=artifact.get('columns');outputs=plan['outputs'];group_name=None;count_name=None;names=set();integer={'family':'integer','facets':{},'nullable':False}
    if type(columns)is not list or len(columns)!=2 or type(outputs)is not list or len(outputs)!=2:raise ValueError('Exactly group and count outputs required')
    for position,(column,output)in enumerate(zip(columns,outputs),1):
        if type(output)is not dict or set(output)!={'name','expression'}or type(output['name'])is not str or not output['name']or output['name']in names:raise ValueError('Distinct exact numeric grouped output names required')
        name=output['name'];names.add(name)
        if output['expression']=={'op':'field','scan':'s0','identity':identity}:
            if group_name is not None:raise ValueError('One numeric group output required')
            group_name=name;rep=representation;identities=[identity]
        elif output['expression']=={'op':'count','type':integer}:
            if count_name is not None:raise ValueError('One grouped count output required')
            count_name=name;rep={'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'};identities=[source['record']]
        else:raise ValueError('Only original numeric group and COUNT supported')
        if type(column)is not dict or type(column.get('position'))is not int or column!={'nullable':False,'outputName':name,'position':position,'representation':rep,'sourceIdentities':identities}:raise ValueError('Exact numeric grouped output descriptor required')
    if group_name is None or count_name is None:raise ValueError('Complete numeric grouping/count outputs required')
    rows=expected['object_current'];populations={}
    if type(rows)is not list or len(rows)>10000:raise ValueError('Bounded independent numeric grouping source bag required')
    for row in rows:
        if (row['source_system'],row['type_id'])!=(record['sourceSystem'],record['typeId']):continue
        if row['schema_revision']not in physical_revisions:raise ValueError('Grouping row outside admitted physical revisions')
        prior=admission.revisions[row['schema_revision']]['types'].get(record['typeId'])
        if prior is None or prior['owner']!=actual['owner']or prior['keys']!=actual['keys']or any(actual['fields'].get(p)!=v for p,v in prior['fields'].items())or pid not in prior['fields']:raise ValueError('Original numeric grouping row correspondence required')
        props=decode_retained_json(row['props_json'].encode())
        if type(props)is not dict or pid not in props:raise ValueError('Required independent numeric grouping value missing')
        value=props[pid]
        if family=='integer'and type(value)is not int or family=='decimal'and type(value)not in (int,Decimal):raise ValueError('Original numeric group source family differs')
        token=('0'if value==0 else format(value,'f'))if family=='decimal'else str(value);decode_exact_scalar(representation,token);key=Decimal(token).as_integer_ratio()
        if key not in populations:populations[key]=[token,0]
        populations[key][1]+=1
        if populations[key][1]>2**63-1:raise ValueError('Exact nonnegative signed64 grouped count required')
    wanted=[{group_name:{family+'_token':token},count_name:count}for token,count in populations.values()]
    return TrussNumericGroupedCount(encoded(columns),encoded(wanted),count_name)
