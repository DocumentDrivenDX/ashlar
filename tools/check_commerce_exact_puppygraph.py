"""Actual finite exact commerce scenario arithmetic; never Gremlin math/DOUBLE."""
import argparse,base64,hashlib,json,os,subprocess
from pathlib import Path
from prepare_commerce_exact_puppygraph import projection,PROFILE_EXACT
from run_commerce_release_graphframes import original,bag
from run_graph_release_graphframes import load_release,row_bag
from private_graph_custody import PROFILE
from check_puppygraph_releases import native_text

def admitted(release,custody,rs,cs,prepared,prepared_sha):
    if hashlib.sha256(prepared).hexdigest()!=prepared_sha:raise ValueError('Trusted exact adapter receipt bytes differ')
    p=json.loads(prepared)
    if p['format']!=PROFILE_EXACT or p['release_sha256']!=rs or p['custody_sha256']!=cs:raise ValueError('Original adapter custody differs')
    value=load_release(release,rs,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=cs);graph,nodes,edges,a=original(value);model=json.loads(base64.b64decode(a['original_model_base64']))
    projected=projection(value,graph,nodes,edges,model)
    if projected!=(p['nodes'],p['edges'],p['numeric_input_guards'],p['all_unfiltered_intermediate_guards']):raise ValueError('Every source/representability/intermediate guard must precede query acquisition')
    return p

def native_custody(prepared,language,endpoint):
    expected_endpoint='bolt://127.0.0.1:17887'if language=='Cypher'else'ws://127.0.0.1:18182/gremlin'
    if endpoint!=expected_endpoint:raise ValueError('Explicit dedicated local native route required')
    container='ashlar-release-puppy113'
    result=subprocess.run(['docker','inspect',container],check=True,capture_output=True,text=True)
    facts=json.loads(result.stdout)[0];ports=facts['HostConfig']['PortBindings']
    expected_ports={'7687/tcp':[{'HostIp':'127.0.0.1','HostPort':'17887'}],'8081/tcp':[{'HostIp':'127.0.0.1','HostPort':'18881'}],'8182/tcp':[{'HostIp':'127.0.0.1','HostPort':'18182'}]}
    image='sha256:6d016d5a7e3eab0a57bd4d23c4e218467d00c7661fcda215ea2a429941a68cee'
    if not isinstance(facts.get('Id'),str) or not facts['Id'] or facts['Name']!='/'+container or facts['Image']!=image or ports!=expected_ports or facts['State']['Running']is not True or facts['HostConfig']['Memory']!=3221225472 or facts['HostConfig']['NanoCpus']!=2000000000:raise ValueError('Dedicated native instance/image/loopback binding differs')
    hashes=subprocess.run(['docker','exec',container,'sha256sum','/tmp/releases.duckdb','/tmp/commerce.duckdb','/tmp/augmentations.duckdb','/tmp/commerce-exact.duckdb'],check=True,capture_output=True,text=True)
    observed={line.split()[1]:line.split()[0]for line in hashes.stdout.splitlines()}
    expected={'/tmp/releases.duckdb':'5d69ce5c13549053e3691e689e3f135f09baf7427b9d1d93ee7a42d8aa9ed989','/tmp/commerce.duckdb':'f9c4e8d4ec7edf3c6943eb6187db6d1a54f0ba85e480988ee4e8cdfd229278e4','/tmp/augmentations.duckdb':'926e9de45d0a0409a6a9be88e933b63332d81fa2316e6a19f0303700eafdfb71','/tmp/commerce-exact.duckdb':prepared['database_sha256']}
    if observed!=expected:raise ValueError('Complete fixture native sealed database inventory differs')
    return {'container_id':facts['Id'],'container':container,'image':image,'ports':ports,'sealed_databases':observed,'memory_bytes':facts['HostConfig']['Memory'],'nano_cpus':facts['HostConfig']['NanoCpus']}

def scalar_integer(value):
    if not isinstance(value,int)or isinstance(value,bool)or not -(2**63)<=value<2**63:raise ValueError('Exact native signed64 integer result required; float/bool refused')
    return int(value)

def verify_intermediates(raw,prepared,scenario):
    actual=[]
    for row in raw:
        if scenario=='partial-return':actual.append({'scenario':scenario,'source_keys':row['source_keys'],'subtract':str(scalar_integer(row['subtract'])),'add':str(scalar_integer(row['add']))})
        else:actual.append({'scenario':'refund','source_keys':row['source_keys'],'multiply_coefficient':str(scalar_integer(row['multiply_coefficient'])),'scale':2})
    expected=[r for r in prepared['all_unfiltered_intermediate_guards']if r['scenario']==scenario]
    if bag(actual)!=bag(expected):raise ValueError('Unfiltered native exact intermediate bag differs')

def cypher(prepared,endpoint,user,password):
    from neo4j import GraphDatabase
    records=[]
    with GraphDatabase.driver(endpoint,auth=(user,password),connection_timeout=5)as driver:
        with driver.session()as session:
            def query(name,q):
                rows=session.run(q).data();records.append({'case':name,'query':q,'native_rows':rows});return rows
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

def gremlin(prepared,endpoint,user,password):
    from gremlin_python.driver import client,serializer
    remote=client.Client(endpoint,'g',username=user,password=password,message_serializer=serializer.GraphSONSerializersV3d0());records=[]
    def query(name,script):
        rows=remote.submit(script).all().result(timeout=20);records.append({'case':name,'script':script,'native_rows':rows});return rows
    try:
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
    finally:remote.close()
    return {'language':'Gremlin','records':records,'actual':actual,'scope':'Native Gremlin Groovy integer operators on qualifiedLONG properties; no Gremlin.math (DOUBLE), no binaryfloat/generalunbounded arithmetic.'}
def execute_checked(prepared,language,endpoint,user,password):
    opening=native_custody(prepared,language,endpoint)
    result=(cypher if language=='Cypher'else gremlin)(prepared,endpoint,user,password)
    closing=native_custody(prepared,language,endpoint)
    if opening!=closing:raise ValueError('Closing native instance/database custody changed; result withheld')
    result['opening_closing_native_custody']=opening
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','prepared','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('release-sha256','custody-sha256','prepared-sha256','endpoint'):p.add_argument('--'+n,required=True)
    p.add_argument('--language',choices=['Cypher','Gremlin'],required=True);a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh query receipt required')
    prepared=admitted(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,a.prepared.read_bytes(),a.prepared_sha256)
    result=execute_checked(prepared,a.language,a.endpoint,os.environ['ASHLAR_PUPPY_USER'],os.environ['ASHLAR_PUPPY_PASSWORD']);result.update(profile=PROFILE_EXACT,prepared_sha256=a.prepared_sha256,expected=prepared['expected'])
    a.output.write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
