"""Exact direct required numeric arithmetic source oracle; no SQL interpretation."""
import hashlib,json,re
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from ashlar.schema import decode_retained_json
from ashlar.weft_decode import decode_exact_scalar
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_implicit_arithmetic_request import truss_implicit_arithmetic_request


def _number(domain,token):
    if type(domain)is not dict or domain.get('family')not in ('integer','decimal')or set(domain)!=({'family'}if domain['family']=='integer'else{'family','scale'}):raise ValueError('Exact arithmetic domain required')
    scale=domain.get('scale',0)
    if type(scale)is not int or not 0<=scale<=38 or type(token)is not str or len(token)>80 or re.fullmatch('-?[0-9]+(?:\\.[0-9]+)?',token)is None:raise ValueError('Finite exact arithmetic carrier required')
    whole,_,fraction=token.lstrip('-').partition('.')
    if domain['family']=='integer'and fraction or len(fraction)>scale:raise ValueError('Exact arithmetic coefficient/scale bound required')
    coefficient=whole+fraction+'0'*(scale-len(fraction))
    if len(coefficient.lstrip('0'))>38:raise ValueError('Exact arithmetic coefficient/scale bound required')
    return Fraction(Decimal(token))

@dataclass(frozen=True)
class TrussArithmeticProjection:
    columns_json:str
    expected_json:str
    def decode(self,column,value):
        if encoded(column)not in [encoded(c)for c in json.loads(self.columns_json)]:raise ValueError('Exact admitted arithmetic column required')
        logical=column['representation']['logicalType'];domain={'family':logical['family'],**logical['facets']};_number(domain,value)
        return {logical['family']+'_token':value}
    def require_complete_bag(self,rows):
        columns=json.loads(self.columns_json)
        def normalized(row):
            if type(row)is not dict or set(row)!={c['outputName']for c in columns}:raise ValueError('Complete arithmetic result row required')
            result={}
            for c in columns:
                logical=c['representation']['logicalType'];value=row[c['outputName']];key=logical['family']+'_token'
                if type(value)is not dict or set(value)!={key}:raise ValueError('Exact arithmetic output token required')
                n=_number({'family':logical['family'],**logical['facets']},value[key]);result[c['outputName']]=[n.numerator,n.denominator]
            return encoded(result)
        if type(rows)is not list or Counter(map(normalized,rows))!=Counter(map(normalized,json.loads(self.expected_json))):raise ValueError('Independent complete original arithmetic bag differs')


def admit_truss_arithmetic_projection(admission,artifact,request,manifest,expected,*,revision,document_id):
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:raise ValueError('Actual original arithmetic source admission required')
    selected=admission.revisions[revision];model,original=selected['models'][document_id]
    if encoded(model)!=encoded(json.loads(original)):raise ValueError('Original arithmetic model bytes required')
    digest=hashlib.sha256(original).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    binding=decode_retained_json(request['target']['bindingJson'].encode());names=sorted(json.loads(manifest['table_versions_json']));tables=binding['publication']['tables']
    if len(names)!=len(tables):raise ValueError('Complete arithmetic publication binding required')
    rebuilt=truss_implicit_arithmetic_request(admission,manifest,{n:t['uuid']for n,t in zip(names,tables)},binding['publication']['manifestUuid'],request['sql'],revision=revision,document_id=document_id,aliases={n:'.'.join(t['name'])for n,t in zip(names,tables)})
    if encoded(rebuilt)!=encoded(request)or artifact.get('bindingSha256')!=request['target']['bindingSha256']:raise ValueError('Exact original arithmetic request correspondence required')
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.4.2'or plan['aggregate']is not False or any(plan[k]!=[]for k in ('filters','groups','joins','order','typeGraph'))or any(plan[k]is not None for k in ('limit','pageKey','readProfile'))or plan['modulePins']!=[pin]or artifact.get('modelPins')!=[pin]or binding['modelPins']!=[pin]:raise ValueError('Only direct unfiltered required arithmetic projections supported')
    source=plan['source']
    if type(source)is not dict or set(source)!={'occurrence','pin','record'}or source['occurrence']!='s0'or source['pin']!=pin:raise ValueError('Exact arithmetic source scan required')
    records=[r for r in binding['records']if r['logical']==source['record']]
    if len(records)!=1:raise ValueError('One original arithmetic source record required')
    record=records[0];actual=selected['types'][record['typeId']]
    if record['kind']!='object'or actual['owner']!=(document_id,source['record']['module'],source['record']['element'])or record['schemaRevision']!=revision:raise ValueError('Original accepted arithmetic object required')
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};home={encoded(p['logical']):p['home']['propertyId']for p in record['properties']};capabilities={'project','scan'};nodes=0
    def expression(expr,depth=0):
        nonlocal nodes
        nodes+=1
        if nodes>256 or depth>32 or type(expr)is not dict or set(expr)!={'domain','kind','span'}:raise ValueError('Bounded original arithmetic expression required')
        span=expr['span']
        if type(span)is not dict or set(span)!={'start','end'}or any(type(span[k])is not int or not 0<=span[k]<=len(request['sql'].encode())for k in span)or span['end']<span['start']:raise ValueError('Exact original arithmetic span required')
        domain=expr['domain'];_number(domain,'0');kind=expr['kind'];op=kind.get('op')if type(kind)is dict else None
        if op=='field':
            if set(kind)!={'op','field'}:raise ValueError('Closed original arithmetic Field required')
            field=kind['field'];identity=field.get('identity')if type(field)is dict else None
            if type(field)is not dict or set(field)!={'identity','scan','span','type'}or field['scan']!='s0'or field['span']!=span or type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}or identity['documentId']!=document_id or identity['revision']!=digest or encoded(identity)not in home:raise ValueError('Exact original arithmetic Field identity required')
            declaration=elements[identity['module'],identity['element']];family=declaration.get('scalarType');facets=declaration.get('facets',{})
            admitted={'integer':{'integerWidth':{'bits':64,'signed':True}},'decimal':{'precision':18,'scale':4}}
            wanted={'family':family,**({'scale':4}if family=='decimal'else{})}
            if declaration.get('kind')!='field'or declaration.get('cardinality')!='one'or declaration.get('nullability')!='required'or family not in admitted or facets!=admitted[family]or field['type']!={'family':family,'facets':facets,'nullable':False}or domain!=wanted or home[encoded(identity)]not in actual['required']:raise ValueError('Original required exact arithmetic Field domain required')
            capabilities.add('type.'+family);representation={'kind':'scalar','logicalType':field['type'],'carrier':'text','decoder':'exact-'+family}
            pid=home[encoded(identity)]
            def evaluate(props):
                if pid not in props:raise ValueError('Required original arithmetic source property missing')
                value=props[pid]
                if family=='integer'and type(value)is not int or family=='decimal'and type(value)not in (int,Decimal):raise ValueError('Original arithmetic source family differs')
                token=('0'if value==0 else format(value,'f'))if family=='decimal'else str(value)
                decode_exact_scalar(representation,token)
                return Fraction(Decimal(token))
            return domain,[identity],evaluate
        if op=='literal':
            if set(kind)!={'op','value'}:raise ValueError('Closed exact arithmetic literal required')
            number=_number(domain,kind['value'])
            literal_domain={'family':'decimal','scale':len(kind['value'].partition('.')[2])}if '.'in kind['value']else{'family':'integer'}
            if domain!=literal_domain:raise ValueError('Exact original literal domain differs')
            capabilities.add('type.'+domain['family']);return domain,[],lambda props:number
        if op!='binary'or set(kind)!={'op','operator','left','right'}or kind['operator']not in ('+','-','*'):raise ValueError('Only original plus/minus/multiply arithmetic supported')
        left,ids_a,eval_a=expression(kind['left'],depth+1);right,ids_b,eval_b=expression(kind['right'],depth+1)
        family='decimal'if 'decimal'in (left['family'],right['family'])else'integer';scale=left.get('scale',0)+right.get('scale',0)if kind['operator']=='*'else max(left.get('scale',0),right.get('scale',0));wanted={'family':family,**({'scale':scale}if family=='decimal'else{})}
        if domain!=wanted or scale>38:raise ValueError('Exact derived arithmetic domain differs')
        capabilities.update(('arithmetic.'+kind['operator'],'arithmetic.exact.'+family));identities=[]
        for identity in ids_a+ids_b:
            if identity not in identities:identities.append(identity)
        def evaluate(props):
            a,b=eval_a(props),eval_b(props)
            result=a+b if kind['operator']=='+'else a-b if kind['operator']=='-'else a*b
            coefficient=result*10**scale
            if coefficient.denominator!=1 or len(str(abs(coefficient.numerator)))>38:raise ValueError('Independent arithmetic exceeds finite exact output domain')
            return result
        return domain,identities,evaluate
    columns=artifact.get('columns');outputs=plan['outputs'];evaluations=[];output_names=set()
    if type(columns)is not list or not 1<=len(columns)<=64 or type(outputs)is not list or len(outputs)!=len(columns):raise ValueError('Bounded complete arithmetic outputs required')
    for position,(column,output)in enumerate(zip(columns,outputs),1):
        if type(output)is not dict or set(output)!={'name','expression'}or type(output['name'])is not str or not output['name']or output['name']in output_names:raise ValueError('Exact distinct arithmetic output names required')
        expr=output['expression']
        if type(expr)is not dict or set(expr)!={'op','expression'}or expr['op']!='arithmetic':raise ValueError('Only original arithmetic output supported')
        domain,identities,evaluate=expression(expr['expression']);logical={'family':domain['family'],'facets':{k:v for k,v in domain.items()if k!='family'},'nullable':False}
        wanted={'nullable':False,'outputName':output['name'],'position':position,'representation':{'kind':'scalar','logicalType':logical,'carrier':'text','decoder':'exact-'+domain['family']},'sourceIdentities':identities}
        if type(column)is not dict or type(column.get('position'))is not int or column!=wanted:raise ValueError('Exact original arithmetic result descriptor required')
        output_names.add(output['name']);evaluations.append((output['name'],domain,evaluate))
    if plan['requiredCapabilities']!=sorted(capabilities):raise ValueError('Exact arithmetic capability inventory required')
    rows=expected['object_current'];wanted=[]
    if type(rows)is not list or len(rows)>10000:raise ValueError('Bounded independent arithmetic source bag required')
    for row in rows:
        if (row['source_system'],row['type_id'])!=(record['sourceSystem'],record['typeId']):continue
        if row['schema_revision']!=revision:raise ValueError('Arithmetic row outside selected original revision')
        props=decode_retained_json(row['props_json'].encode())
        if type(props)is not dict:raise ValueError('Complete arithmetic source property map required')
        result={}
        for name,domain,evaluate in evaluations:
            coefficient=evaluate(props)*10**domain.get('scale',0)
            if coefficient.denominator!=1 or len(str(abs(coefficient.numerator)))>38:raise ValueError('Independent arithmetic output overflow')
            number=coefficient.numerator;scale=domain.get('scale',0);digits=str(abs(number)).zfill(scale+1);token=('-'if number<0 else'')+(digits[:-scale]+'.'+digits[-scale:]if scale else digits)
            result[name]={domain['family']+'_token':token}
        wanted.append(result)
    return TrussArithmeticProjection(encoded(columns),encoded(wanted))
