# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original authored two-hop expansion COUNT; exact independent edge occurrences."""
import hashlib,json,re
from collections import defaultdict
from dataclasses import dataclass
from ashlar.schema import decode_retained_json
from ashlar.weft_decode import decode_exact_scalar
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_implicit_arithmetic_request import truss_implicit_arithmetic_request

@dataclass(frozen=True)
class TrussTwoHopCount:
    columns_json:str
    expected_count:int
    distinct_targets:bool=False
    def decode(self,column,value):
        if encoded(column)not in [encoded(c)for c in json.loads(self.columns_json)]:raise ValueError('Original admitted two-hop count column required')
        representation=column['representation']
        if self.distinct_targets:representation={k:v for k,v in representation.items()if k!='pathTarget'}
        return decode_exact_scalar(representation,value,count=True).value
    def require_complete_bag(self,rows):
        name=json.loads(self.columns_json)[0]['outputName']
        if type(rows)is not list or len(rows)!=1 or type(rows[0])is not dict or set(rows[0])!={name}or type(rows[0][name])is not int or rows[0][name]!=self.expected_count:raise ValueError('Independent complete original two-hop count differs')

def _admit_truss_two_hop_count(admission,artifact,request,manifest,expected,*,revision,document_id,distinct_targets=False,root_projection=False,maximum_rows=10000):
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:raise ValueError('Actual original two-hop source admission required')
    selected=admission.revisions[revision];model,original=selected['models'][document_id]
    if encoded(model)!=encoded(json.loads(original)):raise ValueError('Original declared two-hop model bytes required')
    digest=hashlib.sha256(original).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    binding=decode_retained_json(request['target']['bindingJson'].encode());names=sorted(json.loads(manifest['table_versions_json']));tables=binding['publication']['tables']
    if len(names)!=len(tables):raise ValueError('Complete original two-hop publication binding required')
    rebuilt=truss_implicit_arithmetic_request(admission,manifest,{n:t['uuid']for n,t in zip(names,tables)},binding['publication']['manifestUuid'],request['sql'],revision=revision,document_id=document_id,aliases={n:'.'.join(t['name'])for n,t in zip(names,tables)})
    if encoded(rebuilt)!=encoded(request)or artifact.get('bindingSha256')!=request['target']['bindingSha256']:raise ValueError('Original two-hop request/binding correspondence required')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','pathExpansion','readProfile','requiredCapabilities','source','typeGraph'}
    if type(plan)is not dict or set(plan)!=keys or plan['aggregate']is not (not root_projection) or plan['irVersion']!='weft-ir/0.4.2'or any(plan[k]!=[]for k in ('filters','groups','joins','order'))or not root_projection and plan['typeGraph']!=[]or any(plan[k]is not None for k in ('limit','pageKey','readProfile'))or plan['modulePins']!=[pin]or artifact.get('modelPins')!=[pin]or binding['modelPins']!=[pin]:raise ValueError('Only unfiltered unordered authored two-hop profile supported')
    source=plan['source'];expansion=plan['pathExpansion']
    if type(source)is not dict or set(source)!={'occurrence','pin','record'}or source['occurrence']!='s0'or source['pin']!=pin or type(expansion)is not dict or set(expansion)!={'occurrence','path'}or expansion['occurrence']!='p0':raise ValueError('Exact original two-hop source/expansion required')
    path=expansion['path']
    if type(path)is not dict or set(path)!={'hopSpans','hops','span','startScan'}or path['startScan']!='s0'or type(path['hops'])is not list or len(path['hops'])!=2 or type(path['hopSpans'])is not list or len(path['hopSpans'])!=2:raise ValueError('Exactly two authored relationship hops required')
    def span(value):
        if type(value)is not dict or set(value)!={'start','end'}or any(type(value[k])is not int or not 0<=value[k]<=len(request['sql'].encode())for k in value)or value['end']<value['start']:raise ValueError('Exact original path span required')
    span(path['span'])
    for value in path['hopSpans']:span(value)
    definitions={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};record_maps={encoded(r['logical']):r for r in binding['records']};record_actual={};key_homes={}
    def record(identity):
        if type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}or identity['documentId']!=document_id or identity['revision']!=digest or encoded(identity)not in record_maps:raise ValueError('Original accepted path record identity required')
        value=record_maps[encoded(identity)];actual=selected['types'].get(value['typeId']);index=value.get('table')
        if actual is None or actual['owner']!=(document_id,identity['module'],identity['element'])or value['kind']!='object'or value['schemaRevision']!=revision or type(index)is not int or not 0<=index<len(tables)or tables[index]['name'][-1]!='object_current':raise ValueError('Original physical path record mapping required')
        record_actual[value['typeId']]=value
        return value,actual
    def key(identity,key_id):
        bound,actual=record(identity);declared=definitions[identity['module'],identity['element']];choices=[k for k in declared.get('keys',[])if k['id']==key_id]if key_id is not None else[k for k in declared.get('keys',[])if k.get('primary')is True]
        if len(choices)!=1:raise ValueError('One original authored endpoint key required')
        chosen=choices[0];fields=[];homes=[]
        for member in chosen['fields']:
            declaration=definitions[member['module'],member['element']];field={'documentId':document_id,'revision':digest,'module':member['module'],'element':member['element']};matches=[p['home']for p in bound['properties']if p['logical']==field]
            if declaration.get('kind')!='field'or declaration.get('scalarType')!='string'or declaration.get('nullability')!='required'or declaration.get('cardinality')!='one'or declaration.get('facets',{})!={}or len(matches)!=1 or matches[0].get('kind')!='props'or matches[0].get('propertyId')not in actual['required']:raise ValueError('Exact required authored STRING endpoint key home required')
            fields.append(field);homes.append(matches[0]['propertyId'])
        if not fields or tuple(homes)!=actual['keys'].get(chosen['id']):raise ValueError('Original actual accepted endpoint key allocation required')
        key_homes[bound['typeId'],chosen['id']]=tuple(homes)
        return {'id':chosen['id'],'fields':fields,'types':[{'family':'string','facets':{},'nullable':False}for _ in fields]}
    current=source['record'];record(current);directions=[];inverse_used=False
    for hop in path['hops']:
        hop_keys={'from','to','identity','inverse','sourceKey','targetKey','sourceMultiplicity','targetMultiplicity','targetLifecycle'}
        if type(hop)is not dict or set(hop)!=hop_keys or type(hop['inverse'])is not bool or hop['from']!=current:raise ValueError('Original contiguous authored path required')
        identity=hop['identity'];matches=[r for r in binding.get('relationships',[])if r['logical']==identity]
        if len(matches)!=1 or type(identity)is not dict or set(identity)!={'documentId','revision','module','relationship'}or identity['documentId']!=document_id or identity['revision']!=digest:raise ValueError('Original accepted relationship mapping required')
        relation=matches[0];actual=selected['edges'].get(relation['typeId']);declaration=relation['acceptedDefinition'];index=relation.get('table');inverse=hop['inverse'];inverse_used|=inverse
        if actual is None or actual['owner']!=(document_id,identity['module'],identity['relationship'])or actual['declaration']!=declaration or relation['kind']!='edge'or relation['schemaRevision']!=revision or type(index)is not int or not 0<=index<len(tables)or tables[index]['name'][-1]!='edge_current'or declaration.get('directed')is not True or declaration.get('targetLifecycle')!='independent'or inverse and not declaration.get('inverse'):raise ValueError('Original canonical directed independent edge home required')
        canonical_source,canonical_target=record(relation['source'])[0],record(relation['target'])[0]
        if actual['endpoints']!=(canonical_source['typeId'],canonical_target['typeId'])or relation['sourceSystem']!=canonical_source['sourceSystem']or relation['sourceSystem']!=canonical_target['sourceSystem']:raise ValueError('Original accepted typed endpoint restrictions required')
        from_record,to_record=(relation['target'],relation['source'])if inverse else(relation['source'],relation['target'])
        source_key=key(from_record,declaration['target'if inverse else'source'][0].get('key'));target_key=key(to_record,declaration['source'if inverse else'target'][0].get('key'))
        wanted={'from':from_record,'to':to_record,'identity':identity,'inverse':inverse,'sourceKey':source_key,'targetKey':target_key,'sourceMultiplicity':declaration['sourceMultiplicity'],'targetMultiplicity':declaration['targetMultiplicity'],'targetLifecycle':declaration['targetLifecycle']}
        if hop!=wanted:raise ValueError('Original authored hop/key/multiplicity/lifecycle differs')
        directions.append((relation,inverse,record(from_record)[0],record(to_record)[0]));current=hop['to']
    capabilities=(['project','relationship.pathExpansion','relationship.twoHopPaths','scan','type.string']if root_projection else['aggregate','aggregate.pathTargetDistinctCount'if distinct_targets else'aggregate.count','project','relationship.pathExpansion','relationship.twoHopPaths','scan'])+(['relationship.inverse']if inverse_used else[])
    if plan['requiredCapabilities']!=sorted(capabilities):raise ValueError('Exact selected path count capabilities required')
    integer={'family':'integer','facets':{},'nullable':False};outputs=plan['outputs'];columns=artifact.get('columns')
    if type(outputs)is not list or len(outputs)!=1 or type(columns)is not list or len(columns)!=1 or type(outputs[0])is not dict or set(outputs[0])!={'name','expression'}or type(outputs[0]['name'])is not str or not outputs[0]['name']:raise ValueError('One original path count output required')
    name=outputs[0]['name']
    expression={'op':'countDistinctPathTargets','pathOccurrence':'p0','type':integer}if distinct_targets else{'op':'count','type':integer}
    representation={'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'}
    identities=[source['record']]
    if root_projection:
        if type(maximum_rows)is not int or not 1<=maximum_rows<=10000:raise ValueError('Bounded exact root path rows required')
        root_key=path['hops'][0]['sourceKey']
        if len(root_key['fields'])!=1:raise ValueError('One original required STRING root key field required')
        field=root_key['fields'][0];logical_type={'family':'string','facets':{},'nullable':False}
        expression={'op':'field','scan':'s0','identity':field}
        representation={'kind':'scalar','logicalType':logical_type,'carrier':'text','decoder':'text'}
        identities=[field]
        if plan['typeGraph']!=[{'availability':'required','identity':field,'kind':'scalar','type':logical_type}]:raise ValueError('Exact original root field type graph required')
    if distinct_targets:
        terminal=path['hops'][-1]
        representation['pathTarget']={'record':terminal['to'],'key':terminal['targetKey'],'pathOccurrence':'p0'}
        identities=[terminal['to'],*terminal['targetKey']['fields']]
    if outputs[0]['expression']!=expression or type(columns[0])is not dict or type(columns[0].get('position'))is not int or columns[0]!={'nullable':False,'outputName':name,'position':1,'representation':representation,'sourceIdentities':identities}:raise ValueError('Exact original path count/terminal-key descriptor required')
    nodes=expected['object_current'];edges=expected['edge_current']
    if type(nodes)is not list or type(edges)is not list or len(nodes)>10000 or len(edges)>10000:raise ValueError('Bounded complete independent node/edge populations required')
    def identity(value):
        if type(value)is not str or len(value)>20 or re.fullmatch(r'0|-?[1-9][0-9]*',value)is None or not -(2**63)<=int(value)<=2**63-1:raise ValueError('Exact signed64 physical path identity required')
        return value
    objects={};object_props={}
    for row in nodes:
        if row['type_id']not in record_actual:continue
        bound=record_actual[row['type_id']]
        if row['source_system']!=bound['sourceSystem']:continue
        if row['schema_revision']!=revision:raise ValueError('Path node outside original physical revision')
        node_id=identity(row['id']);node_key=(row['source_system'],row['type_id'],node_id)
        if node_key in objects:raise ValueError('Duplicate physical path object identity')
        props=decode_retained_json(row['props_json'].encode())
        if type(props)is not dict:raise ValueError('Complete exact original path property map required')
        for (tid,_),homes in key_homes.items():
            if tid==row['type_id']and any(type(props.get(pid))is not str for pid in homes):raise ValueError('Required original path STRING key value missing')
        objects[node_key]=row;object_props[node_key]=props
    populations=[]
    for relation,inverse,from_record,to_record in directions:
        bag=[];seen=set()
        for row in edges:
            if (row['source_system'],row['rel_type_id'])!=(relation['sourceSystem'],relation['typeId']):continue
            if row['schema_revision']!=revision:raise ValueError('Path edge outside original physical revision')
            edge_id=identity(row['id'])
            if edge_id in seen:raise ValueError('Duplicate physical path edge identity')
            seen.add(edge_id);src=identity(row['source_id']);dst=identity(row['target_id']);canonical=selected['edges'][relation['typeId']]['endpoints']
            if (row['source_type'],row['target_type'])!=canonical:raise ValueError('Exact original path typed endpoints required')
            start,end=(dst,src)if inverse else(src,dst)
            from_key=(relation['sourceSystem'],from_record['typeId'],start);to_key=(relation['sourceSystem'],to_record['typeId'],end)
            if from_key not in objects or to_key not in objects:raise ValueError('Original complete path endpoints unavailable')
            bag.append((edge_id,from_key,to_key))
        populations.append(bag)
    start_record=record(source['record'])[0];starts={k for k in objects if k[:2]==(start_record['sourceSystem'],start_record['typeId'])};second=defaultdict(list)
    for edge_id,start,end in populations[1]:second[start].append((edge_id,end))
    if root_projection:
        from .truss_two_hop_projection import TrussTwoHopProjection
        homes=key_homes[start_record['typeId'],path['hops'][0]['sourceKey']['id']]
        rows=[]
        for edge_id,start,end in populations[0]:
            if start not in starts:continue
            for second_edge,terminal in second[end]:
                if len(rows)>=maximum_rows:raise ValueError('Complete root path bag exceeds selected row bound')
                rows.append({name:object_props[start][homes[0]]})
        return TrussTwoHopProjection(encoded(columns),encoded(rows))
    if distinct_targets:
        terminal=path['hops'][-1];terminal_record=record(terminal['to'])[0]
        homes=key_homes[terminal_record['typeId'],terminal['targetKey']['id']]
        eligible_middle={end for edge_id,start,end in populations[0]if start in starts};targets=set()
        for edge_id,start,end in populations[1]:
            if start in eligible_middle:targets.add(tuple(object_props[end][pid]for pid in homes))
        count=len(targets)
    else:
        count=sum(len(second[end])for edge_id,start,end in populations[0]if start in starts)
    if count>2**63-1:raise ValueError('Nonnegative signed64 independent path count required')
    return TrussTwoHopCount(encoded(columns),count,distinct_targets)


def admit_truss_two_hop_count(admission,artifact,request,manifest,expected,*,revision,document_id):
    return _admit_truss_two_hop_count(admission,artifact,request,manifest,expected,revision=revision,document_id=document_id)


def admit_truss_two_hop_target_count(admission,artifact,request,manifest,expected,*,revision,document_id):
    """Global exact authored terminal key tuple count, preserving original descriptors."""
    return _admit_truss_two_hop_count(admission,artifact,request,manifest,expected,revision=revision,document_id=document_id,distinct_targets=True)

def admit_truss_two_hop_projection(admission,artifact,request,manifest,expected,*,revision,document_id,maximum_rows=10000):
    return _admit_truss_two_hop_count(admission,artifact,request,manifest,expected,revision=revision,document_id=document_id,root_projection=True,maximum_rows=maximum_rows)
