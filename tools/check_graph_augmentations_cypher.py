"""Native Cypher shared fixture queries; exact declared text/Value/state carriers."""
import argparse,json,os
from pathlib import Path
from run_graph_augmentations_graphframes import admitted
from prepare_graph_augmentations_puppygraph import source_carriers
from run_graph_release_graphframes import row_bag

def check(model,source,receipt,umf,endpoint,user,password):
    from neo4j import GraphDatabase
    fixture,public=admitted(model,source,receipt,umf);nodes,edges,_=source_carriers(fixture,model);records=[]
    with GraphDatabase.driver(endpoint,auth=(user,password),connection_timeout=5)as driver:
        with driver.session()as session:
            def query(name,script,parameters=None):
                rows=session.run(script,**(parameters or {})).data();records.append({'case':name,'query':script,'parameters':parameters or {},'native_rows':rows});return rows
            for kind,rows in [('node',nodes),('edge',edges)]:
                names=list(rows[0]);pattern='(n:AuthoredNode)'if kind=='node'else'(s:AuthoredNode)-[n:AuthoredEdge]->(t:AuthoredNode)'
                script='MATCH '+pattern+' RETURN '+','.join('n.'+name+' AS '+name for name in names)+',id(n) AS native_id'+(',id(s) AS native_src,id(t) AS native_dst'if kind=='edge'else'')
                raw=query('full-'+kind,script);actual=[]
                for r in raw:
                    row={n:r[n]for n in names};label='Authored'+('Node'if kind=='node'else'Edge')
                    if r['native_id']!=label+'['+row['carrier_key']+']':raise ValueError('Native exact identity differs')
                    if kind=='edge'and(r['native_src']!='AuthoredNode['+row['src']+']'or r['native_dst']!='AuthoredNode['+row['dst']+']'):raise ValueError('Native exact incidence differs')
                    actual.append(row)
                if row_bag(actual)!=row_bag(rows):raise ValueError('Full original Value/state native carriers differ')
            topology=next(a for a in fixture['originalAugmentations']if a['id']=='A-TOPOLOGY');original=[[e['key'],e['source'],e['target']]for e in fixture['edges']]
            one=[[s,e,t]for e,s,t in original if s==topology['queryStart']];two=[[s,e,m,f,t]for e,s,m in original for f,m2,t in original if s==topology['queryStart']and m==m2]
            for name,script,fields,expected in [
              ('one-hop',"MATCH (a:AuthoredNode)-[e:AuthoredEdge]->(b:AuthoredNode) WHERE a.carrier_key = $start RETURN a.carrier_key AS source,e.carrier_key AS edge,b.carrier_key AS target",['source','edge','target'],one),
              ('two-hop',"MATCH (a:AuthoredNode)-[e:AuthoredEdge]->(b:AuthoredNode) MATCH (b)-[f:AuthoredEdge]->(c:AuthoredNode) WHERE a.carrier_key = $start RETURN a.carrier_key AS source,e.carrier_key AS edge1,b.carrier_key AS middle,f.carrier_key AS edge2,c.carrier_key AS target",['source','edge1','middle','edge2','target'],two)]:
                rows=query(name,script,{'start':topology['queryStart']});actual=[[r[n]for n in fields]for r in rows]
                bag=lambda rows:sorted(json.dumps(r,sort_keys=True)for r in rows)
                if bag(actual)!=bag(expected):raise ValueError('Complete original walk bags differ')
                if name=='two-hop'and sorted({r[-1]for r in actual})!=topology['twoHopDistinctDestinations']:raise ValueError('Distinct destinations differ')
            if query('isolate',"MATCH (n:AuthoredNode) WHERE n.carrier_key = $key AND NOT (n)-[:AuthoredEdge]-() RETURN count(n) AS count",{'key':'A0'})!=[{'count':1}]:raise ValueError('Original isolate differs')
            for n in fixture['nodes']:
                if n['case']not in ('A-SCALAR','A-PRESENCE'):continue
                field=next(m['field']['element']for m in n['values']if m['field']['element']!='id'and m['state']=='present')if n['case']=='A-SCALAR'else {'presence-absent':'text','presence-null':'text','presence-present-empty-string':'text','presence-present-zero':'integer','presence-present-false':'boolean'}[n['key']]
                script='MATCH (n:AuthoredNode) WHERE n.carrier_key = $key RETURN n.'+field+'_state AS state,n.'+field+'_value_json AS value_json,n.'+field+'_token AS token'
                rows=query(n['case']+':'+n['key'],script,{'key':n['key']});carrier=next(r for r in nodes if r['carrier_key']==n['key'])
                expected=[{'state':carrier[field+'_state'],'value_json':carrier[field+'_value_json'],'token':carrier[field+'_token']}]
                if rows!=expected:raise ValueError('Native exact scalar/state extraction differs')
    return {'format':'ashlar-authored-shared-puppygraph-cypher/0.1','original_public_receipt':public,'records':records,'qualification':'Actual authored source-profile native Cypher fullcarrier/scalar/state/walk queries; no scalar promotion, canonical publication, ACK, UC or production authority.'}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('model','source','receipt','umf','output'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--bolt',required=True);a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh receipt required')
    result=check(a.model.read_bytes(),a.source.read_bytes(),a.receipt.read_bytes(),a.umf,a.bolt,os.environ['ASHLAR_PUPPY_USER'],os.environ['ASHLAR_PUPPY_PASSWORD'])
    a.output.write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
