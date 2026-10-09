"""Native shared scalar/presence/multigraph queries on authored source carriers."""
import argparse,json,os
from pathlib import Path
from run_graph_augmentations_graphframes import admitted
from prepare_graph_augmentations_puppygraph import source_carriers
from check_puppygraph_releases import native_text
from run_graph_release_graphframes import row_bag

def check(model,source,receipt,umf,endpoint,user,password):
    from gremlin_python.driver import client,serializer
    fixture,public=admitted(model,source,receipt,umf);nodes,edges,names=source_carriers(fixture,model)
    remote=client.Client(endpoint,'g',username=user,password=password,message_serializer=serializer.GraphSONSerializersV3d0());records=[]
    def query(name,script,bindings=None):
        rows=remote.submit(script,bindings=bindings or {}).all().result(timeout=20)
        records.append({'case':name,'script':script,'bindings':bindings or {},'native_rows':rows});return rows
    try:
        for kind,rows in [('node',nodes),('edge',edges)]:
            columns=list(rows[0]);extras=['native_id']+(['native_src','native_dst']if kind=='edge'else[])
            script='g.'+('V'if kind=='node'else'E')+"().hasLabel('Authored"+('Node'if kind=='node'else'Edge')+"').project("+','.join(repr(n)for n in columns+extras)+')'+''.join('.by(__.coalesce(__.values('+repr(n)+'),__.constant(null)))'for n in columns)+'.by(__.id())'+('.by(__.outV().id()).by(__.inV().id())'if kind=='edge'else'')
            actual=query('full-'+kind,script);carriers=[]
            for r in actual:
                carrier={n:r[n]for n in columns};label='Authored'+('Node'if kind=='node'else'Edge')
                if native_text(r['native_id'])!=label+'['+carrier['carrier_key']+']':raise ValueError('Actual native identity differs')
                if kind=='edge'and(native_text(r['native_src'])!='AuthoredNode['+carrier['src']+']'or native_text(r['native_dst'])!='AuthoredNode['+carrier['dst']+']'):raise ValueError('Actual native incidence differs')
                carriers.append(carrier)
            if row_bag(carriers)!=row_bag(rows):raise ValueError('Exact full source Value/state carrier parity differs')
        topology=next(a for a in fixture['originalAugmentations']if a['id']=='A-TOPOLOGY')
        source_edges=[[e['key'],e['source'],e['target']]for e in fixture['edges']]
        expected_one=[[s,e,t]for e,s,t in source_edges if s==topology['queryStart']]
        expected_two=[[s,e,m,f,t]for e,s,m in source_edges for f,m2,t in source_edges if s==topology['queryStart']and m==m2]
        for name,script,columns,expected in [
          ('one-hop',"g.V().hasLabel('AuthoredNode').has('carrier_key',start).as('a').outE('AuthoredEdge').as('e').inV().as('b').select('a','e','b').by('carrier_key')",['a','e','b'],expected_one),
          ('two-hop',"g.V().hasLabel('AuthoredNode').has('carrier_key',start).as('a').outE('AuthoredEdge').as('e').inV().as('b').outE('AuthoredEdge').as('f').inV().as('c').select('a','e','b','f','c').by('carrier_key')",['a','e','b','f','c'],expected_two)]:
            rows=query(name,script,{'start':topology['queryStart']});actual=[[r[n]for n in columns]for r in rows]
            encode=lambda rows:sorted(json.dumps(r,sort_keys=True)for r in rows)
            if encode(actual)!=encode(expected):raise ValueError('Complete original walk occurrence bag differs')
            if name=='two-hop'and sorted({r[-1]for r in actual})!=topology['twoHopDistinctDestinations']:raise ValueError('Distinct destination set differs')
        if query('isolate',"g.V().hasLabel('AuthoredNode').has('carrier_key',isolated).not(__.bothE('AuthoredEdge')).count()",{'isolated':'A0'})!=[1]:raise ValueError('Original isolate lost')
        for n in fixture['nodes']:
            if n['case']not in ('A-SCALAR','A-PRESENCE'):continue
            member=next(m for m in n['values']if m['field']['element']!='id'and m['state']=='present')if n['case']=='A-SCALAR'else next(m for m in n['values']if m['field']['element']=={'presence-absent':'text','presence-null':'text','presence-present-empty-string':'text','presence-present-zero':'integer','presence-present-false':'boolean'}[n['key']])
            field=member['field']['element'];columns=[field+'_state',field+'_value_json',field+'_token']
            script="g.V().hasLabel('AuthoredNode').has('carrier_key',originalKey).project('state','value_json','token')"+''.join('.by(__.coalesce(__.values('+repr(c)+'),__.constant(null)))'for c in columns)
            rows=query(n['case']+':'+n['key'],script,{'originalKey':n['key']});carrier=next(r for r in nodes if r['carrier_key']==n['key'])
            expected=[dict(zip(['state','value_json','token'],[carrier[c]for c in columns]))]
            if rows!=expected:raise ValueError('Native exact scalar or five-state extraction differs')
    finally:remote.close()
    return {'format':'ashlar-authored-shared-puppygraph-gremlin/0.1','original_public_receipt':public,'records':records,'qualification':'Actual separate authored source-profile native Gremlin fullcarrier/scalar-token/state/walk checks. No scalar type promotion, canonical publication, ACK, UC, production authority or general predicate equivalence.'}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('model','source','receipt','umf','output'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--gremlin',required=True);a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh query receipt required')
    result=check(a.model.read_bytes(),a.source.read_bytes(),a.receipt.read_bytes(),a.umf,a.gremlin,os.environ['ASHLAR_PUPPY_USER'],os.environ['ASHLAR_PUPPY_PASSWORD'])
    a.output.write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
