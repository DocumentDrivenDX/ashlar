"""Exact original pack carrier and common-query checks for Cypher and Gremlin.

Read-only checker: activation belongs to the separately governed owner. These
common cases do not qualify the seventeen original authored SQL scenarios.
"""
import json
from collections import Counter
from run_graph_release_graphframes import columns, row_bag
from check_puppygraph_releases import native_text

PACKS = ('archaeology', 'ecology', 'medical')

def bag(rows):
    return Counter(json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(',', ':')) for r in rows)

def integer(value):
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 2**63:
        raise ValueError('Exact nonnegative signed64 count required')
    return int(value)

def field(name):
    return {'id':'carrier_id', 'graph_id':'carrier_key'}.get(name, name)

def projection(language, pack, kind, prefix=''):
    if pack not in PACKS or kind not in ('node', 'edge'):
        raise ValueError('Closed pack and carrier kind required')
    label = pack.title() + ('Node' if kind == 'node' else 'Edge')
    names = list(columns(kind)) + ['original_key', 'original_type', 'native_id']
    if kind == 'edge': names += ['native_src', 'native_dst']
    if language == 'Cypher':
        pattern = '(n:'+label+')' if kind == 'node' else '(s:'+pack.title()+'Node)-[n:'+label+']->(t:'+pack.title()+'Node)'
        return ('MATCH '+pattern+' '+prefix+' RETURN '+','.join('n.'+field(n)+' AS '+n for n in columns(kind))+
                ',n.original_key AS original_key,n.original_type AS original_type,id(n) AS native_id'+
                (',id(s) AS native_src,id(t) AS native_dst' if kind == 'edge' else ''))
    if language != 'Gremlin': raise ValueError('Closed native protocol required')
    start = prefix or ('g.'+('V' if kind == 'node' else 'E')+"().hasLabel('"+label+"')")
    return (start+'.project('+','.join(repr(n) for n in names)+')'+''.join(
        '.by(__.coalesce(__.values('+repr(field(n))+'),__.constant(null)))' for n in list(columns(kind))+['original_key','original_type'])+
        '.by(__.id())'+('.by(__.outV().id()).by(__.inV().id())' if kind == 'edge' else ''))

def carriers(raw, pack, kind, expected, keys, graph):
    names = set(columns(kind)) | {'original_key','original_type','native_id'}
    if kind == 'edge': names |= {'native_src','native_dst'}
    lookup = {r['key']:r for r in graph['objects' if kind == 'node' else 'edges']}
    result=[]; label=pack.title()
    for r in raw:
        if type(r) is not dict or set(r) != names: raise ValueError('Complete closed native projection required')
        row={n:r[n] for n in columns(kind)}
        if any(v is not None and type(v) is not str for v in row.values()): raise ValueError('String/null cells required')
        key=keys.get(row['graph_id'])
        if key is None or r['original_key'] != key: raise ValueError('Original key correspondence differs')
        typ=lookup[key]['type' if kind == 'node' else 'relationship']
        if r['original_type'] != json.dumps(typ,sort_keys=True,separators=(',',':')): raise ValueError('Original type differs')
        if native_text(r['native_id']) != label+('Node' if kind == 'node' else 'Edge')+'['+row['graph_id']+']': raise ValueError('Native identity differs')
        if kind == 'edge' and (native_text(r['native_src']) != label+'Node['+row['src']+']' or native_text(r['native_dst']) != label+'Node['+row['dst']+']'): raise ValueError('Native incidence differs')
        result.append(row)
    if row_bag(result) != row_bag(expected): raise ValueError('Complete original carrier bag differs')
    return result

def queries(language, pack):
    if pack not in PACKS: raise ValueError('Closed pack required')
    n=pack.title()+'Node';e=pack.title()+'Edge'
    if language == 'Cypher':
        return {
          'one-hop': ('MATCH (a:'+n+')-[e:'+e+']->(b:'+n+') RETURN a.original_key AS source,e.original_key AS edge,b.original_key AS target', ['source','edge','target']),
          'two-hop': ('MATCH (a:'+n+')-[e:'+e+']->(b:'+n+') MATCH (b)-[f:'+e+']->(c:'+n+') RETURN a.original_key AS source,e.original_key AS edge1,b.original_key AS middle,f.original_key AS edge2,c.original_key AS target',['source','edge1','middle','edge2','target']),
          'grouped-count': ('MATCH (a:'+n+')-[e:'+e+']->(b:'+n+') RETURN a.original_key AS source,e.original_type AS relationship,count(e) AS occurrences,count(DISTINCT b) AS destinations',['source','relationship','occurrences','destinations']),
          'filtered-total': ('MATCH (n:'+n+') WHERE n.source_system=$source AND n.type_id=$type RETURN count(n) AS total',['total']),
          'singleton': (projection(language,pack,'node','WHERE n.carrier_key=$key'),None),
          'filtered-limit': (projection(language,pack,'node','WHERE n.source_system=$source AND n.type_id=$type')+' ORDER BY n.original_key LIMIT 1',None),
          'isolates': (projection(language,pack,'node','WHERE NOT (n)-[:'+e+']-()'),None)}
    if language != 'Gremlin': raise ValueError('Closed protocol required')
    start="g.V().hasLabel('"+n+"')";walk=start+".as('a').outE('"+e+"').as('e').inV().as('b')"
    def projected(s,names,steps): return s+'.project('+','.join(repr(x)for x in names)+')'+''.join('.by('+x+')'for x in steps)
    return {
      'one-hop':(projected(walk,['source','edge','target'],["__.select('a').values('original_key')","__.select('e').values('original_key')","__.select('b').values('original_key')"]),['source','edge','target']),
      'two-hop':(projected(walk+".outE('"+e+"').as('f').inV().as('c')",['source','edge1','middle','edge2','target'],["__.select('"+x+"').values('original_key')"for x in ['a','e','b','f','c']]),['source','edge1','middle','edge2','target']),
      'grouped-count':("g.E().hasLabel('"+e+"').group().by(__.project('source','relationship').by(__.outV().values('original_key')).by('original_type')).by(__.fold()).unfold().project('source','relationship','occurrences','destinations').by(__.select(keys).select('source')).by(__.select(keys).select('relationship')).by(__.select(values).unfold().count()).by(__.select(values).unfold().inV().dedup().count())",['source','relationship','occurrences','destinations']),
      'filtered-total':(start+".has('source_system',source).has('type_id',type).count()",['total']),
      'singleton':(projection(language,pack,'node',start+".where(__.values('carrier_key').is(key))"),None),
      'filtered-limit':(projection(language,pack,'node',start+".has('source_system',source).has('type_id',type).order().by('original_key').limit(1)"),None),
      'isolates':(projection(language,pack,'node',start+".not(__.bothE('"+e+"'))"),None)}

def verify_query(name, raw, names, expected):
    if name == 'filtered-total':
        values=[integer(r['total']) for r in raw] if raw and isinstance(raw[0],dict) else [integer(v) for v in raw]
        if values != [expected['filtered-total']]: raise ValueError('Native filtered total differs')
        return values
    if any(type(r)is not dict or set(r)!=set(names) for r in raw): raise ValueError('Closed native result required')
    actual=[]
    for r in raw:
        cells=[]
        for n in names:
            v=r[n]
            if n in ('occurrences','destinations'):v=integer(v)
            elif type(v)is not str:raise ValueError('Exact original key/type String required')
            cells.append(v)
        actual.append(cells)
    if bag(actual)!=bag(expected[name]):raise ValueError('Complete native query bag differs '+name)
    return actual


def execute_pack(language, pack, value, graph, nodes, edges, admission, expected, query):
    """Execute under caller's mandatory complete opening/closing native custody.

    query(script,bindings) must return original native rows. No client activation
    or publication admission is supplied by this function.
    """
    import base64
    bindings=json.loads(base64.b64decode(admission['original_bindings_base64']))
    typ=json.loads(expected['filtered-type'])
    identity=['object',typ['module'],typ['element']]
    matches=[r['type_id']for r in bindings['types']if r['identity']==identity]
    if len(matches)!=1:raise ValueError('One exact original type binding required')
    source={r['source_system']for r in value['nodes']}
    if len(source)!=1:raise ValueError('One admitted original source scope required')
    params={'source':next(iter(source)),'type':matches[0]}
    reverse={v:k for k,v in nodes.items()}
    params['key']=reverse[expected['singleton'][0]]
    records=[]
    for kind,role,name in [('node','nodes','all-objects'),('edge','edges','all-edges')]:
        script=projection(language,pack,kind);raw=query(script,{})
        actual=carriers(raw,pack,kind,value[role],nodes if kind=='node'else edges,graph)
        records.append({'case':name,'query':script,'bindings':{},'native_rows':raw,'canonical_rows':actual})
    for name,(script,names)in queries(language,pack).items():
        raw=query(script,params if name in ('singleton','filtered-limit','filtered-total')else {})
        if names is not None:actual=verify_query(name,raw,names,expected)
        else:
            wanted_keys=expected[name]
            wanted=[r for key in wanted_keys for r in value['nodes']if nodes[r['graph_id']]==key]
            actual=carriers(raw,pack,'node',wanted,nodes,graph)
            if name in ('singleton','filtered-limit')and actual!=wanted:raise ValueError('Ordered full carrier selection differs')
        records.append({'case':name,'query':script,'bindings':params if name in ('singleton','filtered-limit','filtered-total')else {},'native_rows':raw,'canonical_rows':actual})
    return {'pack':pack,'language':language,'records':records,'common_cases_passed':7,
            'additional_controls':['filtered-total','isolates'],'original_authored_scenarios_qualified':False}

def run_held(prepared, language, query, observe):
    """Mandatory independently admitted native custody before/after all queries.

    The caller provides original-admitted tuples, never receipt-only booleans.
    An observation must contain the entire independently trusted engine/model/
    five-database inventory. No partial report is released on closing failure.
    """
    import copy
    frozen=copy.deepcopy(prepared)
    if [p['pack']for p in frozen]!=list(PACKS):raise ValueError('Complete three-pack inventory required')
    opening=observe()
    if type(opening)is not dict or set(opening)!={'container','requested_model_sha256','observed_model_sha256','catalog_observed','databases'}:
        raise ValueError('Complete native custody observation required')
    if type(opening['databases'])is not dict or set(opening['databases'])!={'/tmp/releases.duckdb','/tmp/commerce.duckdb','/tmp/augmentations.duckdb','/tmp/commerce-exact.duckdb','/tmp/packs.duckdb'}:
        raise ValueError('Complete five sealed databases required')
    reports=[execute_pack(language,p['pack'],p['value'],p['graph'],p['nodes'],p['edges'],p['admission'],p['expected'],query)for p in frozen]
    closing=observe()
    if closing!=opening:raise ValueError('Whole native custody changed before result release')
    return {'format':'ashlar-original-pack-puppygraph-common/0.1','language':language,
            'reports':reports,'opening':opening,'closing':closing,'common_cases_passed':21,
            'original_authored_scenarios_qualified':False,
            'loss':'Nullable carrier SQL NULL returns native null through explicit projection; opaque props_json retains original logical presence. No scalar promotion.'}

def admit_prepared(candidates, directory, trusted):
    """Recompute original semantics and every prepared cell before network access."""
    import hashlib
    from pathlib import Path
    from private_graph_custody import PROFILE
    from run_graph_release_graphframes import load_release
    from run_pack_release_graphframes import original, query_oracle
    required={'receipt.json','model.json','packs.duckdb'}
    if type(trusted)is not dict or set(trusted)!=required:raise ValueError('Three independently trusted prepared digests required')
    directory=Path(directory)
    for name in required:
        if hashlib.sha256((directory/name).read_bytes()).hexdigest()!=trusted[name]:raise ValueError('Prepared original bytes changed')
    receipt=json.loads((directory/'receipt.json').read_bytes())
    if receipt['format']!='ashlar-original-pack-puppygraph-preparation/0.1' or receipt['database_sha256']!=trusted['packs.duckdb'] or receipt['engine_executed']is not False:raise ValueError('Original preparation receipt differs')
    required_candidate={'pack','release','release_sha256','custody','custody_sha256','publication'}
    if type(candidates)is not list or [c.get('pack')for c in candidates]!=list(PACKS)or any(set(c)!=required_candidate for c in candidates):raise ValueError('Exact original candidates required')
    import duckdb
    connection=duckdb.connect(str(directory/'packs.duckdb'),read_only=True)
    prepared=[]
    try:
        for c,r in zip(candidates,receipt['reports']):
            value=load_release(Path(c['release']).read_bytes(),c['release_sha256'],custody_profile=PROFILE,custody_payload=Path(c['custody']).read_bytes(),trusted_custody_sha256=c['custody_sha256'])
            graph,nodes,edges,admission=original(c['pack'],value,c['publication']);expected=query_oracle(graph)
            if (r['pack']!=c['pack']or r['release_sha256']!=c['release_sha256']or r['custody_sha256']!=c['custody_sha256']or r['source_admission']!=admission or r['expected']!=expected or r['original_node_keys']!=nodes or r['original_edge_keys']!=edges or r['original_snapshots']!=value['snapshots']):raise ValueError('Original complete preparation correspondence differs')
            for kind,role,keys in [('node','nodes',nodes),('edge','edges',edges)]:
                names=list(columns(kind))+['carrier_key','carrier_id','original_key','original_type']
                table=c['pack']+'_'+role
                desc=connection.execute('DESCRIBE carrier.'+table).fetchall()
                if [x[0]for x in desc]!=names or any(x[1]!='VARCHAR'for x in desc):raise ValueError('Closed all-String prepared schema differs')
                lookup={x['key']:x for x in graph['objects'if kind=='node'else'edges']}
                wanted=[]
                for row in value[role]:
                    key=keys[row['graph_id']]
                    typ=lookup[key]['type'if kind=='node'else'relationship']
                    wanted.append([row[n]for n in columns(kind)]+[row['graph_id'],row['id'],key,json.dumps(typ,sort_keys=True,separators=(',',':'))])
                actual=[list(x)for x in connection.execute('SELECT * FROM carrier.'+table).fetchall()]
                if bag(actual)!=bag(wanted):raise ValueError('Every original prepared cell must match')
            prepared.append(dict(pack=c['pack'],value=value,graph=graph,nodes=nodes,edges=edges,admission=admission,expected=expected))
    finally:connection.close()
    if len(receipt['reports'])!=3:raise ValueError('Extra preparation reports refused')
    return prepared

def observe_native(language, endpoint, model, databases, user, password):
    import hashlib,subprocess,urllib.request,base64
    expected_endpoint={'Cypher':'bolt://127.0.0.1:17887','Gremlin':'ws://127.0.0.1:18182/gremlin'}
    if endpoint!=expected_endpoint.get(language):raise ValueError('Dedicated loopback route required')
    if set(databases)!={'/tmp/releases.duckdb','/tmp/commerce.duckdb','/tmp/augmentations.duckdb','/tmp/commerce-exact.duckdb','/tmp/packs.duckdb'}:raise ValueError('Complete trusted database inventory required')
    facts=json.loads(subprocess.run(['docker','inspect','ashlar-release-puppy113'],check=True,capture_output=True,text=True).stdout)[0]
    ports={'7687/tcp':[{'HostIp':'127.0.0.1','HostPort':'17887'}],'8081/tcp':[{'HostIp':'127.0.0.1','HostPort':'18881'}],'8182/tcp':[{'HostIp':'127.0.0.1','HostPort':'18182'}]}
    if (type(facts.get('Id'))is not str or not facts['Id'] or facts['Name']!='/ashlar-release-puppy113'or facts['Image']!='sha256:6d016d5a7e3eab0a57bd4d23c4e218467d00c7661fcda215ea2a429941a68cee'or facts['State']['Running']is not True or facts['HostConfig']['PortBindings']!=ports or facts['HostConfig']['Memory']!=3221225472 or facts['HostConfig']['NanoCpus']!=2000000000):raise ValueError('Original bounded owned engine differs')
    output=subprocess.run(['docker','exec','ashlar-release-puppy113','sha256sum',*sorted(databases)],check=True,capture_output=True,text=True).stdout
    observed={line.split()[1]:line.split()[0]for line in output.splitlines()}
    if observed!=databases:raise ValueError('Whole sealed database inventory differs')
    request=urllib.request.Request('http://127.0.0.1:18881/schemajson',headers={'Authorization':'Basic '+base64.b64encode((user+':'+password).encode()).decode()})
    with urllib.request.urlopen(request,timeout=10)as response:active=json.loads(response.read())
    if active.get('node')!=model['node']or active.get('edge')!=model['edge']or ('catalog'in active and active['catalog']!=model['catalog']):raise ValueError('Complete active model differs')
    return {'container':{'id':facts['Id'],'image':facts['Image'],'ports':ports,'memory':facts['HostConfig']['Memory'],'nano_cpus':facts['HostConfig']['NanoCpus']},'requested_model_sha256':hashlib.sha256(json.dumps(model,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'catalog_observed':'catalog'in active,'observed_model_sha256':hashlib.sha256(json.dumps(active,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'databases':observed}


def check(prepared,language,endpoint,model,databases,user,password):
    observe=lambda:observe_native(language,endpoint,model,databases,user,password)
    # Gate before even constructing a native query client.
    observe()
    if language=='Cypher':
        from neo4j import GraphDatabase
        with GraphDatabase.driver(endpoint,auth=(user,password),connection_timeout=5)as driver:
            with driver.session()as session:
                report=run_held(prepared,language,lambda script,bindings:session.run(script,**bindings).data(),observe)
    elif language=='Gremlin':
        from gremlin_python.driver import client,serializer
        remote=client.Client(endpoint,'g',username=user,password=password,message_serializer=serializer.GraphSONSerializersV3d0())
        try:report=run_held(prepared,language,lambda script,bindings:remote.submit(script,bindings=bindings).all().result(timeout=20),observe)
        finally:remote.close()
    else:raise ValueError('Closed protocol required')
    return report

if __name__=='__main__':
    import argparse,hashlib,os
    from pathlib import Path
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('candidates','prepared','trusted-prepared','model','databases','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('model-sha256','language','endpoint'):p.add_argument('--'+n,required=True)
    a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh complete report required')
    model_bytes=a.model.read_bytes()
    if hashlib.sha256(model_bytes).hexdigest()!=a.model_sha256:raise ValueError('Trusted complete activation model differs')
    model=json.loads(model_bytes);addition=json.loads((a.prepared/'model.json').read_bytes())
    if set(model)!={'catalog','node','edge'}:raise ValueError('Closed complete model required')
    for role in ('catalog','node','edge'):
        for entry in addition[role]:
            if model[role].count(entry)!=1:raise ValueError('Every exact original prepared mapping required')
    admitted=admit_prepared(json.loads(a.candidates.read_bytes()),a.prepared,json.loads(a.trusted_prepared.read_bytes()))
    databases=json.loads(a.databases.read_bytes())
    trusted=json.loads(a.trusted_prepared.read_bytes())
    if databases.get('/tmp/packs.duckdb')!=trusted['packs.duckdb']:raise ValueError('Original sealed pack database differs')
    report=check(admitted,a.language,a.endpoint,model,databases,os.environ['ASHLAR_PUPPY_USER'],os.environ['ASHLAR_PUPPY_PASSWORD'])
    a.output.write_text(json.dumps(report,sort_keys=True,ensure_ascii=False,indent=2)+'\n')
