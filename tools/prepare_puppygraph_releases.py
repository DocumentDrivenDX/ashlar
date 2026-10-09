"""Hash-bound local graph releases -> exact immutable DuckDB/PuppyGraph carrier.

Local release export only; no direct Unity Catalog access or engine execution.
"""
import argparse,hashlib,json
from pathlib import Path
from run_graph_release_graphframes import load_release,columns


def prepare(inputs,output):
    import duckdb
    if set(inputs)!={'R1','R2'}:raise ValueError('Two explicit release handles required')
    values={}
    for name,(path,digest) in inputs.items():
        values[name]=load_release(Path(path).read_bytes(),digest)
    output=Path(output);output.mkdir(exist_ok=False)
    database=output/'releases.duckdb'
    model={'catalog':[{'name':'ashlar_releases','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:/tmp/releases.duckdb'}}],'node':[],'edge':[]}
    connection=duckdb.connect(str(database));connection.execute('CREATE SCHEMA carrier')
    observations=[]
    try:
        for name,value in values.items():
            for kind,role in [('node','nodes'),('edge','edges')]:
                table=name.lower()+'_'+role;rows=value[role];names=list(columns(kind))
                if any(set(row)!=set(names) for row in rows):raise ValueError('Closed original carrier required')
                connection.execute('CREATE TABLE carrier.'+table+'('+','.join('"'+n+'" VARCHAR' for n in names)+',carrier_key VARCHAR,carrier_id VARCHAR)')
                entries=[[row[n] for n in names]+[row['graph_id'],row['id']] for row in rows]
                if entries:connection.executemany('INSERT INTO carrier.'+table+' VALUES ('+','.join('?' for _ in entries[0])+')',entries)
                actual=connection.execute('SELECT '+','.join('"'+n+'"' for n in names)+' FROM carrier.'+table).fetchall()
                normalize=lambda xs:sorted(json.dumps(x,ensure_ascii=False,sort_keys=True) for x in xs)
                if normalize([dict(zip(names,r)) for r in actual])!=normalize(rows):raise ValueError('Exact raw carrier parity differs')
                key='node_key' if kind=='node' else 'edge_key'
                fields=[n for n in names if n not in ('id','graph_id')]+['carrier_key','carrier_id']
                mapped=[{'sourceFieldName':n,'targetFieldName':n} for n in fields]+[{'sourceFieldName':'graph_id','targetFieldName':key}]
                entry={'label':name+('Node' if kind=='node' else 'Edge'),'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'ashlar_releases','schema':'carrier','table':table,'mappedField':mapped}},'id':[{'name':key,'type':'STRING'}],'attribute':[{'name':n,'type':'STRING'} for n in fields]}
                if kind=='edge':entry.update(fromNodeLabel=name+'Node',toNodeLabel=name+'Node',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
                model[kind].append(entry)
                observations.append({'release':name,'role':role,'rows':len(rows),'exactCarrierParity':True})
    finally:connection.close()
    raw=database.read_bytes();database.chmod(0o444)
    (output/'model.json').write_text(json.dumps(model,indent=2)+'\n')
    receipt={'profile':'ashlar-puppygraph-release-carrier/0.1','duckdbVersion':duckdb.__version__,'releases':{n:d for n,(_,d) in inputs.items()},'databaseSha256':hashlib.sha256(raw).hexdigest(),'databaseBytes':len(raw),'observations':observations,'qualification':'Two hash-bound whole release exports, all canonical carrier text and nulls preserved. Distinct immutable release labels; no remote activation, direct UC protocol, Truss authority or engine query evidence.'}
    (output/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    return receipt


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('r1','r2'):
        p.add_argument('--'+name,type=Path,required=True);p.add_argument('--'+name+'-sha256',required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    print(json.dumps(prepare({'R1':(a.r1,a.r1_sha256),'R2':(a.r2,a.r2_sha256)},a.output)))
