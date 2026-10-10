"""Bounded actual named-commerce Cypher queries; explicit immutable custody pair."""
import argparse,hashlib,json,os
from pathlib import Path
from prepare_commerce_puppygraph import prepare
from run_commerce_release_graphframes import original,query_oracle,bag
from run_graph_release_graphframes import load_release,columns,row_bag
from check_commerce_graphframes_limit import selection
from private_graph_custody import PROFILE

def carrier(props,kind):
    if type(props)is not dict:raise ValueError('Native complete property map required')
    result={n:props.get('carrier_id' if n=='id' else 'carrier_key' if n=='graph_id' else n) for n in columns(kind)}
    return result

def check(release,custody,release_sha,custody_sha,*,bolt,user,password):
    from neo4j import GraphDatabase
    value=load_release(release,release_sha,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=custody_sha)
    graph,nodes,edges,_=original(value);expected=query_oracle(graph)
    _,typ,keys=selection(graph);expected['filtered-limit']=keys[:1];expected['filtered-total']=len(keys)
    records=[]
    with GraphDatabase.driver(bolt,auth=(user,password),connection_timeout=5) as driver:
        with driver.session() as session:
            for kind,role in [('node','nodes'),('edge','edges')]:
                query='MATCH (x:CommerceNode) RETURN properties(x) AS p,id(x) AS native' if kind=='node' else 'MATCH (s:CommerceNode)-[x:CommerceEdge]->(t:CommerceNode) RETURN properties(x) AS p,id(x) AS native,id(s) AS source,id(t) AS target'
                raw=session.run(query).data();rows=[]
                for r in raw:
                    row=carrier(r['p'],kind)
                    if r['native']!='Commerce'+('Node' if kind=='node' else 'Edge')+'['+row['graph_id']+']':raise ValueError('Native named identity differs')
                    if kind=='edge' and (r['source']!='CommerceNode['+row['src']+']' or r['target']!='CommerceNode['+row['dst']+']'):raise ValueError('Native endpoint incidence differs')
                    rows.append(row)
                if row_bag(rows)!=row_bag(value[role]):raise ValueError('Native full original carrier bag differs')
                records.append({'case':'all-objects' if kind=='node' else 'all-edges','query':query,'native_rows':raw,'canonical_rows':rows})
            queries={
              'one-hop':('MATCH (s:CommerceNode)-[e:CommerceEdge]->(t:CommerceNode) RETURN s.original_key AS source,e.original_key AS edge,t.original_key AS target',['source','edge','target']),
              'two-hop':('MATCH (a:CommerceNode)-[e:CommerceEdge]->(b:CommerceNode) MATCH (b)-[f:CommerceEdge]->(c:CommerceNode) RETURN a.original_key AS source,e.original_key AS edge1,b.original_key AS middle,f.original_key AS edge2,c.original_key AS target',['source','edge1','middle','edge2','target']),
              'grouped-count':('MATCH (s:CommerceNode)-[e:CommerceEdge]->(t:CommerceNode) RETURN s.original_key AS source,e.original_type AS relationship,count(e) AS occurrences,count(DISTINCT t) AS destinations',['source','relationship','occurrences','destinations'])}
            for name,(query,cols) in queries.items():
                raw=session.run(query).data();actual=[[r[n] for n in cols] for r in raw]
                if bag(actual)!=bag(expected[name]):raise ValueError('Native complete original bag differs '+name)
                records.append({'case':name,'query':query,'native_rows':raw,'expected':expected[name]})
            selected_rows=[r for r in value['nodes'] if nodes[r['graph_id']] in keys]
            selected_type=selected_rows[0]['type_id']
            if any(r['type_id']!=selected_type for r in selected_rows):raise ValueError('Original type binding inconsistent')
            totalq="MATCH (n:CommerceNode) WHERE n.type_id = '"+selected_type+"' RETURN count(n) AS total"
            total=session.run(totalq).data()
            if total!=[{'total':len(keys)}]:raise ValueError('Original filtered total differs')
            for name,query,params,selected in [
             ('singleton','MATCH (n:CommerceNode) RETURN properties(n) AS p ORDER BY n.original_key LIMIT 1',{},expected['singleton']),
             ('filtered-limit',"MATCH (n:CommerceNode) WHERE n.type_id = '"+selected_type+"' RETURN properties(n) AS p ORDER BY n.original_key LIMIT 1",{},keys[:1])]:
                raw=session.run(query,**params).data();actual=[carrier(r['p'],'node') for r in raw]
                wanted=[next(r for r in value['nodes'] if nodes[r['graph_id']]==key) for key in selected]
                if actual!=wanted:raise ValueError('Ordered complete selected carrier differs '+name)
                records.append({'case':name,'query':query,'parameters':params,'native_rows':raw,'expected_complete_rows':wanted,'selected_original_type':typ if name=='filtered-limit' else None,'binding_selected_type_id':selected_type if name=='filtered-limit' else None,'original_total':len(keys) if name=='filtered-limit' else 1,'native_total':total if name=='filtered-limit' else None})
    return {'format':'ashlar-commerce-puppygraph-cypher/0.1','release_sha256':release_sha,'custody_sha256':custody_sha,'records':records,'all_passed':True,'scope':'Actual named immutable local export Cypher; no scalar promotion, UC, continued native source retention, atomic activation, or Gremlin qualification.'}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('release-sha256','custody-sha256','bolt'):p.add_argument('--'+n,required=True)
    a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh output required')
    r=check(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,bolt=a.bolt,user=os.environ['ASHLAR_PUPPY_USER'],password=os.environ['ASHLAR_PUPPY_PASSWORD'])
    a.output.write_text(json.dumps(r,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
