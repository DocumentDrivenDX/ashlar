# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact required scalar equality joins; complete independent pair bags."""
import hashlib,json
from decimal import Decimal
from collections import defaultdict
from ashlar.schema import decode_retained_json
from ashlar.weft_decode import decode_exact_scalar
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_scalar_projection import TrussScalarProjection, decode_truss_lexical_properties
from .truss_compatible_binding import truss_compatible_compile_request,admit_truss_compatible_join_revisions


def _admit_truss_scalar_join(admission,artifact,request,manifest,expected,*,revision,document_id,physical_revisions,count_mode=False):
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:raise ValueError('Actual original numeric join admission required')
    selected=admission.revisions[revision];model,original=selected['models'][document_id]
    if encoded(model)!=encoded(json.loads(original)):raise ValueError('Original numeric join model bytes required')
    digest=hashlib.sha256(original).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    binding=decode_retained_json(request['target']['bindingJson'].encode());names=sorted(json.loads(manifest['table_versions_json']));tables=binding['publication']['tables']
    if len(names)!=len(tables):raise ValueError('Complete numeric join publication required')
    rebuilt=truss_compatible_compile_request(admission,manifest,{n:t['uuid']for n,t in zip(names,tables)},binding['publication']['manifestUuid'],request['sql'],revision=revision,document_id=document_id,physical_revisions=physical_revisions,aliases={n:'.'.join(t['name'])for n,t in zip(names,tables)})
    if encoded(rebuilt)!=encoded(request):raise ValueError('Original numeric join request correspondence required')
    proof=admit_truss_compatible_join_revisions(admission,artifact,binding,revision=revision,document_id=document_id)
    if proof['physical_revisions']!=list(physical_revisions):raise ValueError('Exact explicit numeric join revisions required')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.2.0'or plan['aggregate']is not count_mode or any(plan[k]!=[]for k in ('filters','groups','order'))or any(plan[k]is not None for k in ('limit','pageKey','readProfile'))or plan['modulePins']!=[pin]:raise ValueError('Only one direct numeric equality INNER JOIN supported')
    joins=plan['joins']
    if type(joins)is not list or len(joins)!=1 or type(joins[0])is not dict or set(joins[0])!={'on','right'}or type(joins[0]['on'])is not list or len(joins[0]['on'])!=1:raise ValueError('One original numeric equality required')
    predicate=joins[0]['on'][0]
    if type(predicate)is not dict or set(predicate)!={'op','left','right'}or predicate['op']!='equal'or type(predicate['right'])is not dict or set(predicate['right'])!={'kind','field'}or predicate['right']['kind']!='field':raise ValueError('Original numeric field equality required')
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};records={};graph={};families=set()
    domains={'string':{},'boolean':{},'integer':{'integerWidth':{'bits':64,'signed':True}},'decimal':{'precision':18,'scale':4}}
    def field(scan,identity):
        if type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}or identity['documentId']!=document_id or identity['revision']!=digest:raise ValueError('Original numeric join field identity required')
        record=records[scan];actual=selected['types'][record['typeId']];declaration=elements.get((identity['module'],identity['element']))
        homes=[p['home']for p in record['properties']if p['logical']==identity]
        if declaration is None or declaration.get('kind')!='field'or declaration.get('cardinality')!='one'or declaration.get('nullability')!='required'or declaration.get('scalarType')not in domains or declaration.get('facets',{})!=domains[declaration['scalarType']]or len(homes)!=1 or homes[0].get('kind')!='props'or homes[0].get('propertyId')not in actual['required']:raise ValueError('Actual required exact numeric join field allocation required')
        family=declaration['scalarType'];logical={'family':family,'facets':declaration.get('facets',{}),'nullable':False};families.add(family)
        graph[encoded(identity)]={'availability':'required','identity':identity,'kind':'scalar','type':logical}
        representation={'kind':'scalar','logicalType':logical,'carrier':'text','decoder':{'string':'text','boolean':'boolean','integer':'exact-integer','decimal':'exact-decimal'}[family]}
        return homes[0]['propertyId'],representation
    for scan,source in [('s0',plan['source']),('s1',joins[0]['right'])]:
        if type(source)is not dict or set(source)!={'occurrence','pin','record'}or source['occurrence']!=scan or source['pin']!=pin:raise ValueError('Original numeric join scan required')
        matches=[r for r in binding['records']if r['logical']==source['record']]
        if len(matches)!=1:raise ValueError('One exact numeric join record required')
        records[scan]=matches[0]
    join_fields={}
    for scan,value in [('s0',predicate['left']),('s1',predicate['right']['field'])]:
        if type(value)is not dict or set(value)!={'identity','scan','span','type'}or value['scan']!=scan:raise ValueError('Original typed numeric join field required')
        span=value['span']
        if type(span)is not dict or set(span)!={'start','end'}or any(type(span[k])is not int or span[k]<0 for k in span)or span['end']<span['start']:raise ValueError('Exact original numeric join span required')
        pid,representation=field(scan,value['identity'])
        if representation['logicalType']!=value['type']or value['type']['family']not in (('integer','decimal')if count_mode else('integer','decimal','string')):raise ValueError('Required signed64 or decimal18,4 join domain required')
        join_fields[scan]=(pid,representation)
    if join_fields['s0'][1]!=join_fields['s1'][1]:raise ValueError('Identical original numeric equality domains required')
    string_join=join_fields['s0'][1]['logicalType']['family']=='string'
    graph={} # Original IR type graph describes outputs; join types remain in the predicate.
    columns=artifact.get('columns');outputs=plan['outputs'];selections=[];output_names=set()
    if type(columns)is not list or not 1<=len(columns)<=64 or type(outputs)is not list or len(columns)!=len(outputs):raise ValueError('Bounded numeric join projections required')
    if count_mode:
        integer={'family':'integer','facets':{},'nullable':False}
        if len(columns)!=1 or type(outputs[0])is not dict or set(outputs[0])!={'name','expression'}or type(outputs[0]['name'])is not str or not outputs[0]['name']:raise ValueError('One original numeric join count output required')
        name=outputs[0]['name']
        wanted={'nullable':False,'outputName':name,'position':1,'representation':{'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'},'sourceIdentities':[plan['source']['record'],joins[0]['right']['record']]}
        if outputs[0]['expression']!={'op':'count','type':integer}or type(columns[0])is not dict or type(columns[0].get('position'))is not int or columns[0]!=wanted:raise ValueError('Exact original numeric join count descriptor required')
    else:
        for position,(column,output)in enumerate(zip(columns,outputs),1):
            if type(output)is not dict or set(output)!={'name','expression'}:raise ValueError('Original numeric join projection required')
            expression=output['expression'];name=output['name']
            if type(expression)is not dict or set(expression)!={'op','scan','identity'}or expression['op']!='field'or expression['scan']not in records or type(name)is not str or not name or name in output_names:raise ValueError('Exact required numeric join output required')
            pid,representation=field(expression['scan'],expression['identity'])
            if string_join and representation['logicalType']['family']!='string':raise ValueError('STRING equality join admits only consumed required STRING projections')
            wanted={'nullable':False,'outputName':name,'position':position,'representation':representation,'sourceIdentities':[expression['identity']]}
            if type(column)is not dict or type(column.get('position'))is not int or column!=wanted:raise ValueError('Exact original numeric join result descriptor required')
            output_names.add(name);selections.append((name,expression['scan'],pid,representation))
    if plan['requiredCapabilities']!=sorted(['equal','innerJoin','project','scan']+(['aggregate','aggregate.count']if count_mode else[])+['type.'+f for f in families]):raise ValueError('Exact numeric join capability inventory required')
    if type(plan['typeGraph'])is not list or len(plan['typeGraph'])!=len(graph)or encoded({encoded(g.get('identity')):g for g in plan['typeGraph']})!=encoded(graph):raise ValueError('Complete original numeric join type graph required')
    rows=expected['object_current']
    if type(rows)is not list or len(rows)>10000:raise ValueError('Bounded independent numeric join source bag required')
    def source_value(props,pid,representation):
        if pid not in props:raise ValueError('Required original numeric join value missing')
        value=props[pid];family=representation['logicalType']['family']
        if family=='integer'and type(value)is not int or family=='decimal'and type(value)not in (int,Decimal)or family=='string'and type(value)is not str or family=='boolean'and type(value)is not bool:raise ValueError('Original numeric join source family differs')
        token=('0'if value==0 else format(value,'f'))if family=='decimal'else str(value)
        carrier=token if family in ('integer','decimal','string')else ('true'if value else 'false')
        scalar=decode_exact_scalar(representation,carrier)
        return {family+'_token':token}if family in ('integer','decimal')else scalar.value
    populations={}
    for scan,record in records.items():
        actual=selected['types'][record['typeId']];groups=defaultdict(list);join_pid,join_rep=join_fields[scan]
        for row in rows:
            if (row['source_system'],row['type_id'])!=(record['sourceSystem'],record['typeId']):continue
            if row['schema_revision']not in physical_revisions:raise ValueError('Numeric join row outside exact physical revisions')
            prior=admission.revisions[row['schema_revision']]['types'].get(record['typeId'])
            if prior is None or prior['owner']!=actual['owner']or prior['keys']!=actual['keys']or any(actual['fields'].get(pid)!=value for pid,value in prior['fields'].items()):raise ValueError('Original numeric join row correspondence required')
            props=decode_truss_lexical_properties(row['props_json'])if string_join else decode_retained_json(row['props_json'].encode())
            if type(props)is not dict:raise ValueError('Complete numeric join source property map required')
            relevant=[(pid,rep)for _,occ,pid,rep in selections if occ==scan]+[(join_pid,join_rep)]
            if any(pid not in prior['fields']for pid,_ in relevant):raise ValueError('Required field outside original row schema')
            for pid,rep in relevant:source_value(props,pid,rep)
            value=source_value(props,join_pid,join_rep)
            key=value if string_join else Decimal(value[join_rep['logicalType']['family']+'_token']).as_integer_ratio()
            groups[key].append(props)
        populations[scan]=groups
    count=sum(len(left)*len(populations['s1'].get(key,()))for key,left in populations['s0'].items())
    if count_mode:
        if count>2**63-1:raise ValueError('Exact nonnegative signed64 join count required')
        return [{name:count}]
    if count>10000:raise ValueError('Complete numeric join exceeds matched pair bound')
    wanted=[]
    for key,left in populations['s0'].items():
        for a in left:
            for b in populations['s1'].get(key,()):wanted.append({name:source_value(a if scan=='s0'else b,pid,rep)for name,scan,pid,rep in selections})
    return TrussScalarProjection(encoded(columns),encoded(wanted))


def admit_truss_scalar_join_projection(admission,artifact,request,manifest,expected,*,revision,document_id,physical_revisions):
    """Complete original required scalar join projection bag."""
    return _admit_truss_scalar_join(admission,artifact,request,manifest,expected,revision=revision,document_id=document_id,physical_revisions=physical_revisions)


def admit_truss_scalar_join_count(admission,artifact,request,manifest,expected,*,revision,document_id,physical_revisions):
    """One original global COUNT over exact numeric equality matched pairs."""
    return _admit_truss_scalar_join(admission,artifact,request,manifest,expected,revision=revision,document_id=document_id,physical_revisions=physical_revisions,count_mode=True)
