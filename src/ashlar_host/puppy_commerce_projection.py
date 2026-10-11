"""Exact finite commerce projection from the qualified PuppyGraph profile.

Original Integer/Decimal domains remain unchanged. Signed64 quantities and
scale2 coefficients are an explicit backend representation; all unfiltered
intermediates must qualify before native filters or computation.
"""
import re, json
from decimal import Decimal, localcontext

PROFILE_EXACT = 'ashlar-commerce-exact-graph-arithmetic/0.1'

def signed64(value):
    if type(value)is not int or not -(2**63)<=value<2**63:raise ValueError('Backend capability: exact signed64 representation/operation refused')
    return value

def quantity(token):
    if type(token)is not str or len(token)>21 or not re.fullmatch('-?(0|[1-9][0-9]*)',token):raise ValueError('Backend capability: exact integer token form refused')
    return signed64(int(token))

def coefficient(token,facets):
    if type(facets)is not dict or any(type(k)is not str or type(v)is not int for k,v in facets.items())or facets!={'precision':18,'scale':2}or type(token)is not str or not 0<len(token)<=64 or not re.fullmatch('-?(0|[1-9][0-9]*)(\.[0-9]+)?',token):raise ValueError('Backend capability: declared fixed decimal profile refused')
    with localcontext()as context:
        context.prec=max(50,len(token)*2);value=Decimal(token);scaled=value*100
        if not value.is_finite()or scaled!=scaled.to_integral_value():raise ValueError('Backend capability: exact scale2 coefficient refused')
        return signed64(int(scaled))

def projection(value,graph,nodes,edges,model):
    fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};by_key={o['key']:o for o in graph['objects']};rows=[];guards=[]
    for r in value['nodes']:
        original=by_key[nodes[r['graph_id']]];record=original['type']['element'];values=original['values'];row=dict(r)
        row.update(original_record=record,original_key=original['key'],template_id=None,quantity_long=None,quantity_token=None,amount_coefficient=None,amount_token=None,unit_price_coefficient=None,unit_price_token=None,currency=None,decimal_scale='2')
        if record+'.id'in values:
            identity=json.loads(values[record+'.id'])
            if type(identity)is not list or len(identity)!=3 or identity[:2]!=[42,0]or type(identity[2])is not str:raise ValueError('Explicit original scenario replay identity required')
            row['template_id']=identity[2]
        for field_id,token in values.items():
            field=fields['domain',field_id];name=field_id.split('.')[-1]
            if field.get('scalarType')=='integer':
                if name!='quantity':raise ValueError('Unimplemented selected original integer field')
                row['quantity_long']=quantity(token);row['quantity_token']=token
                guards.append({'original_key':original['key'],'field':field_id,'logical_domain':'original mathematicalInteger (no width assigned)','original_token':token,'backend':'signed64 exact-or-capability-error','exact_integer':str(row['quantity_long'])})
            elif field.get('scalarType')=='decimal':
                if name not in ('amount','unit_price'):raise ValueError('Unimplemented selected decimal field')
                row[name+'_coefficient']=coefficient(token,field.get('facets'));row[name+'_token']=token
                guards.append({'original_key':original['key'],'field':field_id,'original_token':token,'authored_facets':field['facets'],'backend':'signed64 coefficient at scale2 exact-or-capability-error','exact_coefficient':str(row[name+'_coefficient'])})
            elif name=='currency':row['currency']=token
        rows.append(row)
    assigned={r['original_key']:r for r in rows};original_edges=graph['edges'];intermediate=[]
    relation=lambda e:e['relationship']['id']
    for f in original_edges:
        if relation(f)!='fulfillments.line_id':continue
        a,b=assigned[f['source']],assigned[f['target']]
        for returned in original_edges:
            if relation(returned)=='returns.line_id'and returned['target']==f['target']:
                c=assigned[returned['source']];subtract=signed64(b['quantity_long']-a['quantity_long']);add=signed64(subtract+c['quantity_long'])
                intermediate.append({'scenario':'partial-return','source_keys':[a['original_key'],b['original_key'],c['original_key']],'subtract':str(subtract),'add':str(add)})
    for refund in original_edges:
        if relation(refund)!='refunds.return_id':continue
        returned=assigned[refund['target']]
        for line in original_edges:
            if relation(line)!='returns.line_id'or line['source']!=refund['target']:continue
            for product in original_edges:
                if relation(product)=='order_lines.product_id'and product['source']==line['target']:
                    price=assigned[product['target']];multiply=signed64(returned['quantity_long']*price['unit_price_coefficient'])
                    intermediate.append({'scenario':'refund','source_keys':[refund['source'],refund['target'],line['target'],product['target']],'multiply_coefficient':str(multiply),'scale':2})
    projected_edges=[dict(r,original_relationship=next(e['relationship']['id']for e in original_edges if e['key']==edges[r['graph_id']]))for r in value['edges']]
    return rows,projected_edges,guards,intermediate


def original_projection(release, custody, release_sha256, custody_sha256):
    """Reconstruct complete original correspondence; this grants no authority."""
    import base64, hashlib
    from collections import Counter
    from ashlar.commerce_source import build_transaction
    from ashlar.graph_release import validate_release_carrier
    from .private_graph_custody import load_private_graph_release
    from .commerce_admission import original_commerce_oracle
    from .schema_rows import fixture_columns
    from .resources import RESOURCE_ROOT
    value = load_private_graph_release(release, release_sha256, custody, custody_sha256)
    admission = json.loads(value['publication']['validation_report_json'])['source_admission']
    model_bytes = base64.b64decode(admission['original_model_base64'], validate=True)
    graph_bytes = base64.b64decode(admission['original_graph_base64'], validate=True)
    binding_bytes = base64.b64decode(admission['original_bindings_base64'], validate=True)
    if hashlib.sha256(binding_bytes).hexdigest() != admission['development_bindings_sha256']:
        raise ValueError('Original development binding bytes differ')
    bindings = json.loads(binding_bytes)
    batch, rebuilt = build_transaction(model_bytes, graph_bytes,
        source_system=value['nodes'][0]['source_system'], binding_profile=bindings['profile'])
    if rebuilt != bindings:
        raise ValueError('Complete original development binding inventory differs')
    expected = original_commerce_oracle(model_bytes, graph_bytes, bindings, batch, fixture_columns(RESOURCE_ROOT))
    def bag(rows):
        return Counter(json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(',', ':')) for r in rows)
    for kind, role, source in [('node','nodes','object_current'),('edge','edges','edge_current')]:
        if bag(value[role]) != bag(validate_release_carrier(r, kind) for r in expected[source]):
            raise ValueError('Complete independently reconstructed original carrier bag differs')
    graph = json.loads(graph_bytes)
    assigned = {(b['kind'],b['originalKey']):b for b in bindings['entities']}
    nodes = {r['graph_id']:next(o['key'] for o in graph['objects']
        if assigned['object',o['key']]['id']==r['id'] and assigned['object',o['key']]['type_id']==r['type_id']) for r in value['nodes']}
    edges = {r['graph_id']:next(e['key'] for e in graph['edges']
        if assigned['edge',e['key']]['id']==r['id'] and assigned['edge',e['key']]['type_id']==r['rel_type_id']) for r in value['edges']}
    rows, edge_rows, guards, intermediates = projection(value,graph,nodes,edges,json.loads(model_bytes))
    return {'value':value,'model':model_bytes,'graph':graph_bytes,'bindings':binding_bytes,
        'nodes':rows,'edges':edge_rows,'numeric_input_guards':guards,
        'all_unfiltered_intermediate_guards':intermediates}


def exact_scenario_expected(model,graph,pack):
    """Independent original oracle mapped to this explicit native result profile."""
    from .commerce_original_scenarios import commerce_original_cases, original_commerce_scenario_oracle
    result={}
    for name,sql in commerce_original_cases(pack):
        rows=original_commerce_scenario_oracle(model,graph,{'sql':sql})['rows']
        if name=='partial-return':
            result[name]=[[quantity(cell)for cell in row]for row in rows]
        else:
            values=[]
            for row in rows:
                identity=json.loads(row[0])
                if type(identity)is not list or len(identity)!=3 or identity[:2]!=[42,0]or type(identity[2])is not str:
                    raise ValueError('Exact selected original template identity required')
                values.append([identity[2]])
            result[name]=values
    return result


def commerce_snapshot(value,maximum_bytes=16*1024*1024):
    """Bound passive observations before copying or encoding; no executable types."""
    if type(maximum_bytes)is not int or not 1<=maximum_bytes<=16*1024*1024:
        raise ValueError('Explicit bounded commerce snapshot required')
    remaining=maximum_bytes;nodes=0;active=set()
    def charge(amount):
        nonlocal remaining
        remaining-=amount
        if remaining<0:raise ValueError('Bounded passive commerce encoded bytes required')
    def visit(item,depth):
        nonlocal nodes
        nodes+=1
        if depth>64 or nodes>250000:raise ValueError('Bounded passive commerce structure required')
        if type(item)is str:
            charge(2)
            for char in item:
                code=ord(char)
                if 0xD800<=code<=0xDFFF:raise ValueError('Exact UTF8 commerce text required')
                charge(2 if char in '\"\\\b\f\n\r\t' else 6 if code<32 else
                       1 if code<128 else 2 if code<2048 else 3 if code<65536 else 4)
            return
        if item is None:charge(4);return
        if type(item)is bool:charge(4 if item else 5);return
        if type(item)is int:
            if not -(2**63)<=item<2**63:raise ValueError('Exact native signed64 passive integer required')
            charge(len(str(item)));return
        if type(item)not in (list,dict):raise ValueError('Exact passive commerce carrier required')
        if id(item)in active:raise ValueError('Cyclic commerce carrier refused')
        active.add(id(item))
        try:
            charge(2+max(0,len(item)-1))
            if type(item)is list:
                for entry in item:visit(entry,depth+1)
            else:
                for key,entry in item.items():
                    if type(key)is not str:raise ValueError('Exact passive member name required')
                    visit(key,depth+1);charge(1);visit(entry,depth+1)
        finally:active.remove(id(item))
    visit(value,0)
    def copy(item):
        if type(item)is list:return [copy(entry)for entry in item]
        if type(item)is dict:return {key:copy(entry)for key,entry in item.items()}
        return item
    return copy(value)



def renew_original_commerce_admission(admission,original):
    """Renew the existing public genuine producer admission, not artifact claims."""
    import base64,hashlib
    from .commerce_admission import CommerceAdmission
    from ashlar.commerce_source import build_transaction
    if type(admission)is not CommerceAdmission:
        raise PermissionError('Actual public original commerce admission required')
    facts=commerce_snapshot(admission.metadata())
    for field,key in [('original_model_base64','model'),('original_graph_base64','graph'),('original_bindings_base64','bindings')]:
        if type(facts.get(field))is not str or base64.b64decode(facts[field],validate=True)!=original[key]:
            raise PermissionError('Current genuine original source correspondence changed')
    bindings=json.loads(original['bindings'])
    batch,_=build_transaction(original['model'],original['graph'],source_system=original['value']['nodes'][0]['source_system'],binding_profile=bindings['profile'])
    if facts.get('source_transaction_sha256')!=hashlib.sha256(batch.begin+b''.join(r.raw for r in batch.records)+batch.commit).hexdigest():
        raise PermissionError('Genuine original transaction differs')
    return facts
