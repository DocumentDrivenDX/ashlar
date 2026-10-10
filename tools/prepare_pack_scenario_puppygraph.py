"""Source-admitted original17 finite vertex profile -> separate sealed Puppy DB.

Preparation only. All previous databases/models stay independent. The named
profile retains raw carriers and lexical/presence fields; only its seven original
Integer observations additionally have finite LONG columns. No engine acceptance.
"""
import argparse,base64,hashlib,json
from pathlib import Path
from prepare_pack_graph_scenarios import prepare,PROFILE_SCENARIO
from run_graph_release_graphframes import columns,row_bag


def entries(profile):
    if profile['format']!=PROFILE_SCENARIO:raise ValueError('Exact named finite scenario profile required')
    names=list(profile['nodes'][0]);longs={f['signed64_column']for f in profile['fields']if f['signed64_column']}
    nodes=[]
    for row in profile['nodes']:
        if set(row)!=set(names):raise ValueError('Closed complete source-profile row required')
        for n,v in row.items():
            if n in longs:
                if v is not None and(type(v)is not int or not -(2**63)<=v<2**63):raise ValueError('Exact named LONG capacity required')
            elif v is not None and type(v)is not str:raise ValueError('Exact String/null source carrier required')
        nodes.append(dict(row,carrier_id=row['id'],carrier_key=row['graph_id']))
    source=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64']));binding=json.loads(base64.b64decode(profile['source_admission']['original_bindings_base64']))
    assignments={(e['type_id'],e['id']):e['originalKey']for e in binding['entities']if e['kind']=='edge'}
    lookup={e['key']:e for e in source['edges']};edges=[]
    for row in profile['edges']:
        if set(row)!=set(columns('edge'))or any(v is not None and type(v)is not str for v in row.values()):raise ValueError('Exact raw independent edge carrier required')
        key=assignments.get((row['rel_type_id'],row['id']))
        if key not in lookup:raise ValueError('Exact original independent edge binding required')
        edge=lookup[key];typ=json.dumps(edge['relationship'],sort_keys=True,separators=(',',':'))
        edges.append(dict(row,carrier_id=row['id'],carrier_key=row['graph_id'],original_key=key,original_type=typ))
    return nodes,edges,longs


def build(candidates,umf,output):
    if type(candidates)is not list or [c.get('pack')for c in candidates]!=['archaeology','ecology']:raise ValueError('Both original authored scenario packs required')
    profiles=[prepare(c,umf)for c in candidates];projected=[entries(p)for p in profiles]
    output=Path(output)
    if output.exists():raise ValueError('Fresh separate scenario adapter installation required')
    import duckdb
    output.mkdir(mode=0o700);database=output/'scenarios.duckdb';connection=duckdb.connect(str(database))
    model={'catalog':[{'name':'ashlar_scenarios','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:/tmp/scenarios.duckdb'}}],'node':[],'edge':[]};reports=[]
    try:
        connection.execute('CREATE SCHEMA carrier')
        for profile,(nodes,edges,longs)in zip(profiles,projected):
            pack=profile['pack'];label='Scenario'+pack.title();observations=[]
            for kind,rows in [('node',nodes),('edge',edges)]:
                names=list(rows[0]);table=pack+'_'+kind+'s';types={n:'BIGINT'if n in longs else'VARCHAR'for n in names}
                connection.execute('CREATE TABLE carrier.'+table+'('+','.join('"'+n+'" '+types[n]for n in names)+')')
                if rows:connection.executemany('INSERT INTO carrier.'+table+' VALUES('+','.join('?'for n in names)+')',[[r[n]for n in names]for r in rows])
                actual=[dict(zip(names,r))for r in connection.execute('SELECT * FROM carrier.'+table).fetchall()]
                if row_bag(actual)!=row_bag(rows):raise ValueError('Complete lexical/presence/raw/Integer cell parity required')
                attributes=[n for n in names if n not in ('id','graph_id')];key=kind+'_key'
                entry={'label':label+kind.title(),'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'ashlar_scenarios','schema':'carrier','table':table,'mappedField':[{'sourceFieldName':n,'targetFieldName':n}for n in attributes]+[{'sourceFieldName':'graph_id','targetFieldName':key}]}},'id':[{'name':key,'type':'STRING'}],'attribute':[{'name':n,'type':'LONG'if n in longs else'STRING'}for n in attributes]}
                if kind=='edge':entry.update(fromNodeLabel=label+'Node',toNodeLabel=label+'Node',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
                model[kind].append(entry);observations.append({'table':table,'rows':len(rows),'schema':types,'full_cell_parity':True})
            reports.append({'profile':profile,'nodes':nodes,'edges':edges,'observations':observations})
        connection.execute('CHECKPOINT')
    finally:connection.close()
    database.chmod(0o444)
    receipt={'format':'ashlar-original17-puppygraph-preparation/0.1','reports':reports,'database_sha256':hashlib.sha256(database.read_bytes()).hexdigest(),'database_bytes':database.stat().st_size,'duckdb_version':duckdb.__version__,'engine_executed':False,'qualification':__doc__}
    for name,value in [('model.json',model),('receipt.json',receipt)]:
        (output/name).write_text(json.dumps(value,sort_keys=True,ensure_ascii=False,indent=2)+'\n')
    return receipt

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('candidates','umf','output'):p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();build(json.loads(a.candidates.read_bytes()),a.umf,a.output)
