"""Three admitted immutable pack exports -> exact local DuckDB/PuppyGraph tables.

Preparation only: no engine activation, scalar promotion, canonical-source lease,
Unity Catalog or Truss authority. Original model/source admission remains required.
"""
import argparse,hashlib,json
from pathlib import Path
from private_graph_custody import PROFILE
from run_graph_release_graphframes import load_release,columns,row_bag
from run_pack_release_graphframes import PACKS,original,query_oracle


def prepare(candidates,output):
    required={'pack','release','release_sha256','custody','custody_sha256','publication'}
    if (type(candidates)is not list or any(type(c)is not dict or set(c)!=required for c in candidates)
            or [c['pack']for c in candidates]!=list(PACKS)):
        raise ValueError('All three exact original release/custody/source candidates required')
    output=Path(output)
    if output.exists():raise ValueError('Fresh immutable adapter output required')
    prepared=[]
    for c in candidates:
        value=load_release(Path(c['release']).read_bytes(),c['release_sha256'],custody_profile=PROFILE,
            custody_payload=Path(c['custody']).read_bytes(),trusted_custody_sha256=c['custody_sha256'])
        graph,nodes,edges,admission=original(c['pack'],value,c['publication'])
        prepared.append((c,value,graph,nodes,edges,admission,query_oracle(graph)))
    import duckdb
    output.mkdir(mode=0o700);database=output/'packs.duckdb'
    model={'catalog':[{'name':'ashlar_packs','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:/tmp/packs.duckdb'}}],'node':[],'edge':[]}
    reports=[];connection=duckdb.connect(str(database))
    try:
        connection.execute('CREATE SCHEMA carrier')
        for c,value,graph,nodes,edges,admission,expected in prepared:
            label=c['pack'].title();observations=[]
            for kind,role in [('node','nodes'),('edge','edges')]:
                names=list(columns(kind));extra=['carrier_key','carrier_id','original_key','original_type']
                mapping=nodes if kind=='node'else edges
                lookup={r['key']:r for r in graph['objects'if kind=='node'else'edges']}
                table=c['pack']+'_'+role
                entries=[]
                for row in value[role]:
                    if set(row)!=set(names)or any(v is not None and type(v)is not str for v in row.values()):
                        raise ValueError('Exact closed original String/null carrier required')
                    key=mapping[row['graph_id']];source=lookup[key]
                    original_type=json.dumps(source['type'if kind=='node'else'relationship'],sort_keys=True,separators=(',',':'))
                    entries.append([row[n]for n in names]+[row['graph_id'],row['id'],key,original_type])
                connection.execute('CREATE TABLE carrier.'+table+'('+','.join('"'+n+'" VARCHAR'for n in names+extra)+')')
                if entries:connection.executemany('INSERT INTO carrier.'+table+' VALUES ('+','.join('?'for _ in names+extra)+')',entries)
                actual=connection.execute('SELECT '+','.join('"'+n+'"'for n in names)+' FROM carrier.'+table).fetchall()
                if row_bag(dict(zip(names,r))for r in actual)!=row_bag(value[role]):raise ValueError('Full DuckDB carrier bag differs')
                attributes=[n for n in names if n not in ('id','graph_id')]+extra
                key_name=kind+'_key'
                entry={'label':label+('Node'if kind=='node'else'Edge'),
                    'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'ashlar_packs','schema':'carrier','table':table,
                        'mappedField':[{'sourceFieldName':n,'targetFieldName':n}for n in attributes]+[{'sourceFieldName':'graph_id','targetFieldName':key_name}]}},
                    'id':[{'name':key_name,'type':'STRING'}],'attribute':[{'name':n,'type':'STRING'}for n in attributes]}
                if kind=='edge':entry.update(fromNodeLabel=label+'Node',toNodeLabel=label+'Node',
                    fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
                model[kind].append(entry);observations.append({'table':table,'rows':len(entries),'full_carrier_parity':True})
            reports.append({'pack':c['pack'],'release_sha256':c['release_sha256'],'custody_sha256':c['custody_sha256'],
                'source_admission':admission,'original_snapshots':value['snapshots'],'observations':observations,
                'expected':expected,'original_node_keys':nodes,'original_edge_keys':edges})
        connection.execute('CHECKPOINT')
    finally:connection.close()
    database.chmod(0o444)
    receipt={'format':'ashlar-original-pack-puppygraph-preparation/0.1','duckdb_version':duckdb.__version__,
        'database_sha256':hashlib.sha256(database.read_bytes()).hexdigest(),'database_bytes':database.stat().st_size,
        'reports':reports,'qualification':__doc__,'engine_executed':False}
    for name,value in [('model.json',model),('receipt.json',receipt)]:
        (output/name).write_text(json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    return receipt


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--candidates',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();r=prepare(json.loads(a.candidates.read_bytes()),a.output)
    print('Prepared exact immutable pack tables:',sum(len(x['observations'])for x in r['reports']))
