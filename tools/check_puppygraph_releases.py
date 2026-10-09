"""Actual Cypher/Gremlin checks on two explicit immutable local release labels."""
import argparse,json
from pathlib import Path
from run_graph_release_graphframes import load_release,columns,oracle


def bag(rows):return sorted(json.dumps(r,ensure_ascii=False,sort_keys=True) for r in rows)


def native_text(value):
    if type(value) is dict and set(value)=={'@type','@value'} and value['@type']=='g:String':value=value['@value']
    if type(value) is not str:raise ValueError('Exact native string identity required')
    return value


def assert_endpoints(release,row,native_source,native_target):
    for key,native in [('src',native_source),('dst',native_target)]:
        if native_text(native)!=release+'Node['+row[key]+']':raise ValueError('Actual incident vertex differs from original endpoint')


def check(inputs,*,bolt,gremlin,user,password):
    from neo4j import GraphDatabase
    from gremlin_python.driver import client,serializer
    from gremlin_python.statics import long as GremlinLong
    releases={name:load_release(Path(path).read_bytes(),digest) for name,(path,digest) in inputs.items()}
    if set(releases)!={'R1','R2'}:raise ValueError('Two explicit immutable release handles required')
    records=[]
    with GraphDatabase.driver(bolt,auth=(user,password),connection_timeout=5) as driver:
        with driver.session() as session:
            for name,value in releases.items():
                for kind,role in [('node','nodes'),('edge','edges')]:
                    label=name+('Node' if kind=='node' else 'Edge');names=columns(kind)
                    field=lambda n:'carrier_id' if n=='id' else 'carrier_key' if n=='graph_id' else n
                    pattern='(x:'+label+')' if kind=='node' else '(s)-[x:'+label+']->(t)'
                    query='MATCH '+pattern+' RETURN '+','.join('x.'+field(n)+' AS '+n for n in names)+',id(x) AS native_id'
                    if kind=='edge':query+=',id(s) AS native_src,id(t) AS native_dst'
                    raw=session.run(query).data();actual=[];incidents=[]
                    for row in raw:
                        native=row.pop('native_id')
                        if native!=label+'['+row['graph_id']+']':raise ValueError('Native release-qualified identity differs')
                        if kind=='edge':
                            source=row.pop('native_src');target=row.pop('native_dst');assert_endpoints(name,row,source,target)
                            incidents.append({'graph_id':row['graph_id'],'native_source':source,'native_target':target})
                        actual.append(row)
                    if bag(actual)!=bag(value[role]):raise ValueError('Exact full carrier parity differs')
                    records.append({'release':name,'language':'Cypher','query':query,'rows':actual,'fullParity':True,'nativeIdentityParity':True,'nativeIncidentRows':incidents})
                expected=oracle(value)
                controls=[('nodes','MATCH (n:'+name+'Node) RETURN count(n) AS value'),('edges','MATCH ()-[e:'+name+'Edge]->() RETURN count(e) AS value'),('isolates','MATCH (n:'+name+'Node) WHERE NOT (n)--() RETURN count(n) AS value'),('two_hop','MATCH (a:'+name+'Node)-[e1:'+name+'Edge]->(b:'+name+'Node) MATCH (b)-[e2:'+name+'Edge]->(c:'+name+'Node) RETURN count(*) AS value')]
                for control,query in controls:
                    rows=session.run(query).data()
                    if rows!=[{'value':expected[control]}]:raise ValueError('Cypher control differs: '+control)
                    records.append({'release':name,'language':'Cypher','control':control,'query':query,'rows':rows,'passed':True})
                scripts=[('nodes',"g.V().hasLabel('"+name+"Node').count()"),('edges',"g.E().hasLabel('"+name+"Edge').count()"),('two_hop',"g.V().hasLabel('"+name+"Node').outE('"+name+"Edge').inV().outE('"+name+"Edge').inV().count()")]
                remote=client.Client(gremlin,'g',username=user,password=password,message_serializer=serializer.GraphSONSerializersV3d0())
                try:
                    for control,script in scripts:
                        data=remote.submit(script).all().result(timeout=20)
                        if type(data) is not list or len(data)!=1 or type(data[0]) not in (int,GremlinLong) or data!=[expected[control]]:raise ValueError('Gremlin control differs: '+control)
                        records.append({'release':name,'language':'Gremlin','control':control,'query':script,'rows':data,'passed':True})
                    for kind,role in [('node','nodes'),('edge','edges')]:
                        label=name+('Node' if kind=='node' else 'Edge')
                        script="g."+('V' if kind=='node' else 'E')+"().hasLabel('"+label+"').project('native_id','graph_id','props_json').by(__.id()).by('carrier_key').by('props_json')"
                        if kind=='edge':script="g.E().hasLabel('"+label+"').project('native_id','graph_id','props_json','src','dst','native_src','native_dst').by(__.id()).by('carrier_key').by('props_json').by('src').by('dst').by(__.outV().id()).by(__.inV().id())"
                        data=remote.submit(script).all().result(timeout=20)
                        native_ids=[];incidents=[]
                        for row in data:
                            native=row.pop('native_id');native_ids.append({'graph_id':row['graph_id'],'native_id':native})
                            if native_text(native)!=label+'['+row['graph_id']+']':raise ValueError('Gremlin native identity differs')
                            if kind=='edge':
                                source=row.pop('native_src');target=row.pop('native_dst');assert_endpoints(name,row,source,target)
                                incidents.append({'graph_id':row['graph_id'],'native_source':source,'native_target':target})
                        wanted=[{k:r[k] for k in (['graph_id','props_json']+(['src','dst'] if kind=='edge' else []))} for r in value[role]]
                        if bag(data)!=bag(wanted):raise ValueError('Gremlin exact property/key parity differs')
                        records.append({'release':name,'language':'Gremlin','query':script,'rows':data,'identityValueParity':True,'nativeIdentityRows':native_ids,'nativeIncidentRows':incidents})
                finally:remote.close()
    return {'profile':'ashlar-puppygraph-release-query/0.1','releases':{n:d for n,(_,d) in inputs.items()},'records':records,'allPassed':True,'qualification':'Actual local immutable release-label queries; source carrier export via DuckDB, no direct UC protocol, remote atomic activation or Truss authority claim.'}


if __name__=='__main__':
    import os
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('r1','r2'):p.add_argument('--'+n,type=Path,required=True);p.add_argument('--'+n+'-sha256',required=True)
    p.add_argument('--bolt',required=True);p.add_argument('--gremlin',required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh output required')
    result=check({'R1':(a.r1,a.r1_sha256),'R2':(a.r2,a.r2_sha256)},bolt=a.bolt,gremlin=a.gremlin,user=os.environ['ASHLAR_PUPPY_USER'],password=os.environ['ASHLAR_PUPPY_PASSWORD'])
    a.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print('Passed two releases in Cypher and Gremlin')
