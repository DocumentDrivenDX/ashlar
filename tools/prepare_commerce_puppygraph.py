"""Named immutable commerce export -> exact local DuckDB/PuppyGraph adapter.

Preparation only. No container activation or engine query qualification.
"""
import argparse,hashlib,json
from pathlib import Path
from run_commerce_release_graphframes import original,query_oracle
from check_commerce_graphframes_limit import selection
from run_graph_release_graphframes import load_release,columns,row_bag
from private_graph_custody import PROFILE

def prepare(release,custody,release_sha,custody_sha,output):
    import duckdb
    value=load_release(release,release_sha,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=custody_sha)
    graph,nodes,edges,admission=original(value)
    output=Path(output)
    if output.exists():raise ValueError('Fresh adapter output required')
    output.mkdir(parents=True);database=output/'commerce.duckdb'
    model={'catalog':[{'name':'ashlar_commerce','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:/tmp/commerce.duckdb'}}],'node':[],'edge':[]}
    c=duckdb.connect(str(database));c.execute('CREATE SCHEMA carrier')
    try:
        for kind,role in [('node','nodes'),('edge','edges')]:
            names=list(columns(kind));rows=value[role];mapping=nodes if kind=='node' else edges
            originals=graph['objects'] if kind=='node' else graph['edges'];lookup={r['key']:r for r in originals}
            extra=['carrier_key','carrier_id','original_key','original_type']
            c.execute('CREATE TABLE carrier.'+role+'('+','.join('"'+n+'" VARCHAR' for n in names+extra)+')')
            entries=[]
            for r in rows:
                key=mapping[r['graph_id']];source=lookup[key]
                typ=source['type' if kind=='node' else 'relationship']
                entries.append([r[n] for n in names]+[r['graph_id'],r['id'],key,json.dumps(typ,sort_keys=True,separators=(',',':'))])
            if entries:c.executemany('INSERT INTO carrier.'+role+' VALUES ('+','.join('?' for _ in names+extra)+')',entries)
            actual=c.execute('SELECT '+','.join('"'+n+'"' for n in names)+' FROM carrier.'+role).fetchall()
            if row_bag(dict(zip(names,r)) for r in actual)!=row_bag(rows):raise ValueError('Exact complete DuckDB carrier parity differs')
            fields=[n for n in names if n not in ('id','graph_id')]+extra
            key_name=kind+'_key'
            entry={'label':'Commerce'+('Node' if kind=='node' else 'Edge'),'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'ashlar_commerce','schema':'carrier','table':role,'mappedField':[{'sourceFieldName':n,'targetFieldName':n} for n in fields]+[{'sourceFieldName':'graph_id','targetFieldName':key_name}]}},'id':[{'name':key_name,'type':'STRING'}],'attribute':[{'name':n,'type':'STRING'} for n in fields]}
            if kind=='edge':entry.update(fromNodeLabel='CommerceNode',toNodeLabel='CommerceNode',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
            model[kind].append(entry)
    finally:c.close()
    raw=database.read_bytes();database.chmod(0o444)
    expected=query_oracle(graph)
    queries={
      'all-objects':'MATCH (n:CommerceNode) RETURN properties(n) AS carrier',
      'all-edges':'MATCH (s:CommerceNode)-[e:CommerceEdge]->(t:CommerceNode) RETURN properties(e) AS carrier,id(s) AS native_source,id(t) AS native_target',
      'one-hop':'MATCH (s:CommerceNode)-[e:CommerceEdge]->(t:CommerceNode) RETURN s.original_key AS source,e.original_key AS edge,t.original_key AS target',
      'two-hop':'MATCH (a:CommerceNode)-[e:CommerceEdge]->(b:CommerceNode) MATCH (b)-[f:CommerceEdge]->(c:CommerceNode) RETURN a.original_key AS source,e.original_key AS edge1,b.original_key AS middle,f.original_key AS edge2,c.original_key AS target',
      'grouped-count':'MATCH (s:CommerceNode)-[e:CommerceEdge]->(t:CommerceNode) RETURN s.original_key AS source,e.original_type AS relationship,count(e) AS occurrences,count(DISTINCT t) AS destinations',
      'singleton':'MATCH (n:CommerceNode) RETURN properties(n) AS carrier ORDER BY n.original_key LIMIT 1',
      'filtered-limit':'MATCH (n:CommerceNode) WHERE n.original_type = $type RETURN properties(n) AS carrier ORDER BY n.original_key LIMIT 1',
      'filtered-total':'MATCH (n:CommerceNode) WHERE n.original_type = $type RETURN count(n) AS total'}
    _,selected,ordered=selection(graph)
    selected_type=next(r['type_id'] for r in value['nodes'] if nodes[r['graph_id']]==ordered[0])
    queries['filtered-limit']="MATCH (n:CommerceNode) WHERE n.type_id = '"+selected_type+"' RETURN properties(n) AS carrier ORDER BY n.original_key LIMIT 1"
    queries['filtered-total']="MATCH (n:CommerceNode) WHERE n.type_id = '"+selected_type+"' RETURN count(n) AS total"
    expected['filtered-limit']=ordered[:1];expected['filtered-total']=len(ordered)
    corpus={'format':'ashlar-commerce-puppygraph-common-request/0.1','queries':queries,'parameters':{'key':expected['singleton'][0],'type':selected},'expected':expected,'original_node_keys':nodes,'original_edge_keys':edges,'expected_carriers':{'nodes':value['nodes'],'edges':value['edges']}}
    receipt={'format':'ashlar-commerce-puppygraph-preparation/0.1','release_sha256':release_sha,'custody_sha256':custody_sha,'duckdb_version':duckdb.__version__,'database_sha256':hashlib.sha256(raw).hexdigest(),'source_admission':admission,'original_snapshots':value['snapshots'],'full_carrier_parity':True,'qualification':'Named single immutable commerce export preparation only; no activated PuppyGraph, engine queries, scalar promotion, UC, or continued source retention.'}
    for name,obj in [('model.json',model),('queries.json',corpus),('receipt.json',receipt)]:
        (output/name).write_text(json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    return receipt

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','output'):p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--release-sha256',required=True);p.add_argument('--custody-sha256',required=True)
    a=p.parse_args();print(json.dumps(prepare(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,a.output)))
