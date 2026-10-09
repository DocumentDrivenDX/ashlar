"""Exact original commerce common Gremlin queries on an admitted immutable export."""
import argparse,json,os
from pathlib import Path
from run_commerce_release_graphframes import original,query_oracle,bag
from run_graph_release_graphframes import load_release,columns,row_bag
from check_commerce_graphframes_limit import selection
from private_graph_custody import PROFILE
from check_puppygraph_releases import native_text

def projection(kind):
    names=list(columns(kind));aliases=['carrier_id' if n=='id' else 'carrier_key' if n=='graph_id' else n for n in names]
    return ".project("+','.join(repr(n) for n in names+['native_id']+(['native_src','native_dst'] if kind=='edge' else []))+")"+''.join(".by(__.coalesce(__.values("+repr(n)+"),__.constant(null)))" for n in aliases)+'.by(__.id())'+('.by(__.outV().id()).by(__.inV().id())' if kind=='edge' else '')

def check(release,custody,rs,cs,*,endpoint,user,password):
    from gremlin_python.driver import client,serializer
    value=load_release(release,rs,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=cs)
    graph,nodes,edges,_=original(value);expected=query_oracle(graph);_,typ,keys=selection(graph)
    records=[];remote=client.Client(endpoint,'g',username=user,password=password,message_serializer=serializer.GraphSONSerializersV3d0())
    def query(name,script,bindings=None):
        rows=remote.submit(script,bindings=bindings or {}).all().result(timeout=20)
        if type(rows)is not list:raise ValueError('Exact native row list required')
        records.append({'case':name,'script':script,'bindings':bindings or {},'native_rows':rows});return rows
    def carriers(rows,kind):
        out=[]
        for r in rows:
            if type(r)is not dict:raise ValueError('Exact native full row required')
            row={n:r[n] for n in columns(kind)}
            label='Commerce'+('Node' if kind=='node' else 'Edge')
            if native_text(r['native_id'])!=label+'['+row['graph_id']+']':raise ValueError('Native identity differs')
            if kind=='edge' and (native_text(r['native_src'])!='CommerceNode['+row['src']+']' or native_text(r['native_dst'])!='CommerceNode['+row['dst']+']'):raise ValueError('Native incidence differs')
            out.append(row)
        return out
    try:
        for kind,role in [('node','nodes'),('edge','edges')]:
            script='g.'+('V' if kind=='node' else 'E')+"().hasLabel('Commerce"+('Node' if kind=='node' else 'Edge')+"')"+projection(kind)
            actual=carriers(query('all-objects' if kind=='node' else 'all-edges',script),kind)
            if row_bag(actual)!=row_bag(value[role]):raise ValueError('Complete original carrier bag differs')
        walks={
          'one-hop':("g.V().hasLabel('CommerceNode').as('a').outE('CommerceEdge').as('e').inV().as('b').select('a','e','b').by('original_key')",['a','e','b']),
          'two-hop':("g.V().hasLabel('CommerceNode').as('a').outE('CommerceEdge').as('e').inV().as('b').outE('CommerceEdge').as('f').inV().as('c').select('a','e','b','f','c').by('original_key')",['a','e','b','f','c'])}
        for name,(script,names) in walks.items():
            actual=[[r[n] for n in names] for r in query(name,script)]
            if bag(actual)!=bag(expected[name]):raise ValueError('Complete original walk bag differs '+name)
        grouped=[]
        for source,relationship,count,distinct in expected['grouped-count']:
            source_gid=next(g for g,k in nodes.items() if k==source)
            edge=next(e for e in graph['edges'] if e['source']==source and json.dumps(e['relationship'],sort_keys=True,separators=(',',':'))==relationship)
            rel_type=next(r['rel_type_id'] for r in value['edges'] if edges[r['graph_id']]==edge['key'])
            base="g.V().hasLabel('CommerceNode').has('carrier_key',sourceKey).outE('CommerceEdge').has('rel_type_id',relationshipType)"
            bindings={'sourceKey':source_gid,'relationshipType':rel_type}
            occurrence=query('grouped-count',base+'.count()',bindings);destinations=query('grouped-distinct',base+'.inV().dedup().count()',bindings)
            if len(occurrence)!=1 or len(destinations)!=1:raise ValueError('Native grouped scalar cardinality differs')
            grouped.append([source,relationship,occurrence[0],destinations[0]])
        if bag(grouped)!=bag(expected['grouped-count']):raise ValueError('Native grouping differs')
        # Full original key equality is mandatory here; never substitute ordering.
        singleton=query('singleton',"g.V().hasLabel('CommerceNode').filter(__.values('original_key').is(originalKey))"+projection('node'),{'originalKey':expected['singleton'][0]})
        wanted=[r for r in value['nodes'] if nodes[r['graph_id']]==expected['singleton'][0]]
        if carriers(singleton,'node')!=wanted:raise ValueError('Exact full-key singleton capability refused')
        selected_type=next(r['type_id'] for r in value['nodes'] if nodes[r['graph_id']]==keys[0])
        bindings={'recordType':selected_type};base="g.V().hasLabel('CommerceNode').has('type_id',recordType)"
        total=query('filtered-total',base+'.count()',bindings)
        limited=query('filtered-limit',base+".order().by('original_key').limit(1)"+projection('node'),bindings)
        wanted=[r for r in value['nodes'] if nodes[r['graph_id']]==keys[0]]
        if total!=[len(keys)] or carriers(limited,'node')!=wanted:raise ValueError('Native observable ordered limit differs')
    finally:remote.close()
    return {'format':'ashlar-commerce-puppygraph-gremlin/0.1','release_sha256':rs,'custody_sha256':cs,'records':records,'original_expected':expected,'filtered_original_type':typ,'filtered_original_total':len(keys),'filtered_limit':1,'all_passed':True,'scope':'Actual local named immutable commerce export Gremlin only; no scalar promotion, UC, continued native retention or production authority.'}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('release-sha256','custody-sha256','gremlin'):p.add_argument('--'+n,required=True)
    a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh output required')
    r=check(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,endpoint=a.gremlin,user=os.environ['ASHLAR_PUPPY_USER'],password=os.environ['ASHLAR_PUPPY_PASSWORD'])
    a.output.write_text(json.dumps(r,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
