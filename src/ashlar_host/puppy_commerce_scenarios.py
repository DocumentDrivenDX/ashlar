# SPDX-License-Identifier: MIT OR Apache-2.0
"""Qualified four original commerce queries through supplied current held ports.

No SDK, credential, activation, provisioning or authority is inferred here.
The selected finite LONG/coefficient profile never widens logical UMF domains.
"""
import json
from collections import Counter
from dataclasses import dataclass
from .lifecycle import owned_context, finish
from .puppy_commerce_projection import commerce_snapshot as source_snapshot
from .puppy_commerce_projection import signed64
from .puppy_gremlin import native_string

def bag(rows): return Counter(json.dumps(r,ensure_ascii=False,sort_keys=True,separators=(',',':')) for r in rows)
row_bag=bag
native_text=lambda value:native_string(value,nullable=False)
scalar_integer=signed64

def verify_intermediates(raw,prepared,scenario):
    actual=[]
    for row in raw:
        if scenario=='partial-return':actual.append({'scenario':scenario,'source_keys':row['source_keys'],'subtract':str(scalar_integer(row['subtract'])),'add':str(scalar_integer(row['add']))})
        else:actual.append({'scenario':'refund','source_keys':row['source_keys'],'multiply_coefficient':str(scalar_integer(row['multiply_coefficient'])),'scale':2})
    expected=[r for r in prepared['all_unfiltered_intermediate_guards']if r['scenario']==scenario]
    if bag(actual)!=bag(expected):raise ValueError('Unfiltered native exact intermediate bag differs')

def cypher_queries(prepared,query,records):
    for role,kind in [('nodes','Node'),('edges','Edge')]:
        names=list(prepared[role][0]);field=lambda n:'carrier_id'if n=='id'else'carrier_key'if n=='graph_id'else n
        pattern='(n:ExactCommerceNode)'if kind=='Node'else'(s:ExactCommerceNode)-[n:ExactCommerceEdge]->(t:ExactCommerceNode)'
        rows=query('full-'+role,'MATCH '+pattern+' RETURN '+','.join('n.'+field(n)+' AS '+n for n in names)+',id(n) AS native_id'+(',id(s) AS native_src,id(t) AS native_dst'if kind=='Edge'else''));actual=[]
        for r in rows:
            row={n:r[n]for n in names}
            for n in ('quantity_long','amount_coefficient','unit_price_coefficient'):
                if n in row and row[n]is not None:scalar_integer(row[n])
            if r['native_id']!='ExactCommerce'+kind+'['+row['graph_id']+']':raise ValueError('Exact native identity differs')
            if kind=='Edge'and(r['native_src']!='ExactCommerceNode['+row['src']+']'or r['native_dst']!='ExactCommerceNode['+row['dst']+']'):raise ValueError('Actual native incidence differs')
            actual.append(row)
        if row_bag(actual)!=row_bag(prepared[role]):raise ValueError('All exact numeric/raw source carriers must qualify before arithmetic')
    partial="MATCH (f:ExactCommerceNode)-[fl:ExactCommerceEdge]->(l:ExactCommerceNode) MATCH (r:ExactCommerceNode)-[rl:ExactCommerceEdge]->(l) WHERE fl.original_relationship = 'fulfillments.line_id' AND rl.original_relationship = 'returns.line_id'"
    refund="MATCH (r:ExactCommerceNode)-[rr:ExactCommerceEdge]->(t:ExactCommerceNode) MATCH (t)-[tl:ExactCommerceEdge]->(l:ExactCommerceNode) MATCH (l)-[lp:ExactCommerceEdge]->(p:ExactCommerceNode) WHERE rr.original_relationship = 'refunds.return_id' AND tl.original_relationship = 'returns.line_id' AND lp.original_relationship = 'order_lines.product_id'"
    rows=query('partial-intermediates',partial+' RETURN [f.original_key,l.original_key,r.original_key] AS source_keys,l.quantity_long-f.quantity_long AS subtract,l.quantity_long-f.quantity_long+r.quantity_long AS add');verify_intermediates(rows,prepared,'partial-return')
    rows=query('refund-intermediates',refund+' RETURN [r.original_key,t.original_key,l.original_key,p.original_key] AS source_keys,t.quantity_long*p.unit_price_coefficient AS multiply_coefficient');verify_intermediates(rows,prepared,'refund')
    raw=query('partial-return',partial+' AND f.quantity_long<l.quantity_long RETURN f.quantity_long AS fulfilled,r.quantity_long AS returned,l.quantity_long-f.quantity_long+r.quantity_long AS remaining')
    actual={'partial-return':[[scalar_integer(r[k])for k in ('fulfilled','returned','remaining')]for r in raw]}
    queries={
      'fulfillment':"MATCH (f:ExactCommerceNode)-[fl:ExactCommerceEdge]->(l:ExactCommerceNode) WHERE fl.original_relationship = 'fulfillments.line_id' AND f.quantity_long>l.quantity_long RETURN f.template_id AS id",
      'settlement':"MATCH (p:ExactCommerceNode)-[pi:ExactCommerceEdge]->(i:ExactCommerceNode) WHERE pi.original_relationship = 'payments.invoice_id' AND p.amount_coefficient=i.amount_coefficient AND p.currency=i.currency RETURN p.template_id AS id",
      'refund':refund+' AND r.amount_coefficient=t.quantity_long*p.unit_price_coefficient RETURN r.template_id AS id'}
    for name,q in queries.items():actual[name]=[[r['id']]for r in query(name,q)]
    for name,rows in prepared['expected'].items():
        if bag(actual[name])!=bag(rows):raise ValueError('Native exact financial scenario differs '+name)
    return {'language':'Cypher','records':records,'actual':actual,'scope':'Actual opt-in finite coefficient representation; original logical domains/tokens retained; noDOUBLE/generalunbounded arithmetic.'}

def gremlin_queries(prepared,query,records):
    for role,kind in [('nodes','Node'),('edges','Edge')]:
        names=list(prepared[role][0]);field=lambda n:'carrier_id'if n=='id'else'carrier_key'if n=='graph_id'else n
        script='g.'+('V'if kind=='Node'else'E')+"().hasLabel('ExactCommerce"+kind+"').project("+','.join(repr(n)for n in names+['native_id']+(['native_src','native_dst']if kind=='Edge'else[]))+')'+''.join('.by(__.coalesce(__.values('+repr(field(n))+'),__.constant(null)))'for n in names)+'.by(__.id())'+('.by(__.outV().id()).by(__.inV().id())'if kind=='Edge'else'')
        rows=query('full-'+role,script);actual=[]
        for r in rows:
            row={n:r[n]for n in names}
            for n in ('quantity_long','amount_coefficient','unit_price_coefficient'):
                if n in row and row[n]is not None:scalar_integer(row[n])
            if native_text(r['native_id'])!='ExactCommerce'+kind+'['+row['graph_id']+']':raise ValueError('Exact native identity differs')
            if kind=='Edge'and(native_text(r['native_src'])!='ExactCommerceNode['+row['src']+']'or native_text(r['native_dst'])!='ExactCommerceNode['+row['dst']+']'):raise ValueError('Actual native incidence differs')
            actual.append(row)
        if row_bag(actual)!=row_bag(prepared[role]):raise ValueError('All exact numeric/raw source carriers must qualify before arithmetic')
    partial="g.V().hasLabel('ExactCommerceNode').as('f').outE('ExactCommerceEdge').has('original_relationship','fulfillments.line_id').inV().as('l').inE('ExactCommerceEdge').has('original_relationship','returns.line_id').outV().as('r').select('f','l','r')"
    refund="g.V().hasLabel('ExactCommerceNode').as('r').outE('ExactCommerceEdge').has('original_relationship','refunds.return_id').inV().as('t').outE('ExactCommerceEdge').has('original_relationship','returns.line_id').inV().as('l').outE('ExactCommerceEdge').has('original_relationship','order_lines.product_id').inV().as('p').select('r','t','l','p')"
    defs="def m=it.get();org.apache.tinkerpop.gremlin.structure.Vertex f=(org.apache.tinkerpop.gremlin.structure.Vertex)m.get('f');org.apache.tinkerpop.gremlin.structure.Vertex l=(org.apache.tinkerpop.gremlin.structure.Vertex)m.get('l');org.apache.tinkerpop.gremlin.structure.Vertex r=(org.apache.tinkerpop.gremlin.structure.Vertex)m.get('r');"
    rows=query('partial-intermediates',partial+".map{ "+defs+" ['source_keys':[f.value('original_key'),l.value('original_key'),r.value('original_key')],'subtract':((Long)l.value('quantity_long'))-((Long)f.value('quantity_long')),'add':((Long)l.value('quantity_long'))-((Long)f.value('quantity_long'))+((Long)r.value('quantity_long'))] }");verify_intermediates(rows,prepared,'partial-return')
    rows=query('refund-intermediates',refund+".map{ def m=it.get(); ['source_keys':['r','t','l','p'].collect{k->((String)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get(k)).value('original_key'))},'multiply_coefficient':((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('t')).value('quantity_long'))*((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('p')).value('unit_price_coefficient'))] }");verify_intermediates(rows,prepared,'refund')
    raw=query('partial-return',partial+".filter{((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)it.get().get('f')).value('quantity_long'))<((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)it.get().get('l')).value('quantity_long'))}.map{ "+defs+" [((Long)f.value('quantity_long')),((Long)r.value('quantity_long')),((Long)l.value('quantity_long'))-((Long)f.value('quantity_long'))+((Long)r.value('quantity_long'))] }")
    actual={'partial-return':[[scalar_integer(v)for v in row]for row in raw]}
    queries={
      'fulfillment':"g.V().hasLabel('ExactCommerceNode').as('f').outE('ExactCommerceEdge').has('original_relationship','fulfillments.line_id').inV().as('l').select('f','l').filter{((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)it.get().get('f')).value('quantity_long'))>((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)it.get().get('l')).value('quantity_long'))}.map{((String)((org.apache.tinkerpop.gremlin.structure.Vertex)it.get().get('f')).value('template_id'))}",
      'settlement':"g.V().hasLabel('ExactCommerceNode').as('p').outE('ExactCommerceEdge').has('original_relationship','payments.invoice_id').inV().as('i').select('p','i').filter{def m=it.get();((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('p')).value('amount_coefficient'))==((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('i')).value('amount_coefficient'))&&((String)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('p')).value('currency'))==((String)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('i')).value('currency'))}.map{((String)((org.apache.tinkerpop.gremlin.structure.Vertex)it.get().get('p')).value('template_id'))}",
      'refund':refund+".filter{def m=it.get();((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('r')).value('amount_coefficient'))==((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('t')).value('quantity_long'))*((Long)((org.apache.tinkerpop.gremlin.structure.Vertex)m.get('p')).value('unit_price_coefficient'))}.map{((String)((org.apache.tinkerpop.gremlin.structure.Vertex)it.get().get('r')).value('template_id'))}"}
    for name,q in queries.items():actual[name]=[[r]for r in query(name,q)]
    for name,rows in prepared['expected'].items():
        if bag(actual[name])!=bag(rows):raise ValueError('Native exact financial scenario differs '+name)
    return {'language':'Gremlin','records':records,'actual':actual,'scope':'Native Gremlin Groovy integer operators on qualifiedLONG properties; no Gremlin.math (DOUBLE), no binaryfloat/generalunbounded arithmetic.'}

@dataclass(frozen=True)
class PuppyCommerceScenarioConfig:
    language: str
    release: bytes
    custody: bytes
    release_sha256: str
    custody_sha256: str
    pack: bytes
    maximum_rows: int = 1000
    maximum_capture_bytes: int = 16*1024*1024

    def __post_init__(self):
        if any(type(v)is not bytes or not 0<len(v)<=16*1024*1024 for v in (self.release,self.custody,self.pack)):
            raise ValueError('Exact bounded original scenario source required')
        if type(self.language)is not str or self.language not in ('Cypher','Gremlin'):
            raise ValueError('Explicit exact commerce query language required')
        if type(self.maximum_rows)is not int or not 1<=self.maximum_rows<=10000 or type(self.maximum_capture_bytes)is not int or not 1<=self.maximum_capture_bytes<=16*1024*1024:
            raise ValueError('Explicit finite native observation bounds required')


def execute_commerce_scenarios(prepared,config,transport,policy,context,*,observe,source_admission):
    """Provisional result only after mandatory native/source hold and renewal.

    policy.hold/admit_exact_commerce/renew_exact_commerce independently bind the
    actual native engine/schema/carrier and current original source permission.
    transport.query executes original selected spelling with bounded ordinary
    transport timeouts. Neither prepared digests nor successful rows grant access.
    """
    if type(config)is not PuppyCommerceScenarioConfig or not callable(observe):
        raise ValueError('Typed scenario configuration and raw sink required')
    config.__post_init__()
    original=source_snapshot(prepared)
    from .puppy_commerce_projection import PROFILE_EXACT
    if original.get('format')!=PROFILE_EXACT or original.get('release_sha256')!=config.release_sha256 or original.get('custody_sha256')!=config.custody_sha256:
        raise ValueError('Exact selected prepared profile and original digests required')
    from .puppy_commerce_projection import original_projection,exact_scenario_expected
    reconstructed=original_projection(config.release,config.custody,config.release_sha256,config.custody_sha256)
    for name in ('nodes','edges','numeric_input_guards','all_unfiltered_intermediate_guards'):
        if original[name]!=reconstructed[name]:raise ValueError('Complete original exact projection differs')
    if original['expected']!=exact_scenario_expected(reconstructed['model'],reconstructed['graph'],config.pack):
        raise ValueError('Independent complete original scenario oracle differs')
    records=[]
    from .puppy_commerce_projection import renew_original_commerce_admission
    with owned_context(policy.hold(context)):
        renew_original_commerce_admission(source_admission,reconstructed)
        policy.admit_exact_commerce(source_snapshot(original),context)
        primary=None;result=None
        try:
            def query(name,script):
                renew_original_commerce_admission(source_admission,reconstructed)
                policy.renew_exact_commerce(source_snapshot(original),context)
                rows=source_snapshot(transport.query(config.language,script,context),config.maximum_capture_bytes)
                raw=json.dumps(rows,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
                if type(rows)is not list or len(rows)>config.maximum_rows or len(raw)>config.maximum_capture_bytes:
                    raise ValueError('Bounded complete native observation required')
                record={'case':name,'query'if config.language=='Cypher'else'script':script,'native_rows':rows}
                observe(source_snapshot(record));records.append(record)
                renew_original_commerce_admission(source_admission,reconstructed)
                policy.renew_exact_commerce(source_snapshot(original),context)
                return rows
            result=(cypher_queries if config.language=='Cypher'else gremlin_queries)(original,query,records)
        except BaseException as error:primary=error
        finish(primary,(lambda:renew_original_commerce_admission(source_admission,reconstructed),lambda:policy.renew_exact_commerce(source_snapshot(original),context)))
    return result
