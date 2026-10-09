"""Publicly admitted authored shared fixtures -> sealed exact PuppyGraph carriers."""
import argparse,hashlib,json
from pathlib import Path
from run_graph_augmentations_graphframes import admitted

def source_carriers(fixture,model):
    declaration=json.loads(model);fields=declaration['modules'][0]['elements'][0]['members']
    names=[r['element']for r in fields]
    encoded=lambda v:json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    nodes=[]
    for n in fixture['nodes']:
        members={m['field']['element']:m for m in n['values']}
        if list(members)!=names:raise ValueError('Exact declared ordered member coverage required')
        row={'carrier_key':n['key'],'case_id':n['case'],'record_json':encoded(n)}
        for name in names:
            m=members[name];row[name+'_state']=m['state'];row[name+'_value_json']=encoded(m['value'])if m['state']=='present'else None
            value=m.get('value');row[name+'_token']=None
            if type(value)is dict:
                if len(value)!=1:raise ValueError('Original public scalar carrier required')
                token=next(iter(value.values()));row[name+'_token']=str(token).lower()if type(token)is bool else token
        nodes.append(row)
    edges=[{'carrier_key':e['key'],'src':e['source'],'dst':e['target'],'record_json':encoded(e)}for e in fixture['edges']]
    return nodes,edges,names

def prepare(model,source,receipt,umf,output):
    import duckdb
    fixture,public=admitted(model,source,receipt,umf);nodes,edges,names=source_carriers(fixture,model)
    out=Path(output)
    if out.exists():raise ValueError('Fresh source-profile adapter required')
    out.mkdir(parents=True);database=out/'augmentations.duckdb';c=duckdb.connect(str(database));c.execute('CREATE SCHEMA carrier')
    graph={'catalog':[{'name':'ashlar_authored','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:/tmp/augmentations.duckdb'}}],'node':[],'edge':[]}
    try:
        for kind,role,rows in [('node','nodes',nodes),('edge','edges',edges)]:
            columns=list(rows[0]);c.execute('CREATE TABLE carrier.'+role+'('+','.join('"'+n+'" VARCHAR'for n in columns)+')')
            c.executemany('INSERT INTO carrier.'+role+' VALUES('+','.join('?'for _ in columns)+')',[[r[n]for n in columns]for r in rows])
            actual=[dict(zip(columns,r))for r in c.execute('SELECT * FROM carrier.'+role).fetchall()]
            bag=lambda rows:sorted(json.dumps(r,ensure_ascii=False,sort_keys=True)for r in rows)
            if bag(actual)!=bag(rows):raise ValueError('Exact public source carrier parity differs')
            label='Authored'+('Node'if kind=='node'else'Edge')
            entry={'label':label,'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'ashlar_authored','schema':'carrier','table':role,'mappedField':[{'sourceFieldName':n,'targetFieldName':n}for n in columns]+[{'sourceFieldName':'carrier_key','targetFieldName':kind+'_key'}]}},'id':[{'name':kind+'_key','type':'STRING'}],'attribute':[{'name':n,'type':'STRING'}for n in columns]}
            if kind=='edge':entry.update(fromNodeLabel='AuthoredNode',toNodeLabel='AuthoredNode',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
            graph[kind].append(entry)
    finally:c.close()
    database.chmod(0o444)
    facts={'format':'ashlar-authored-puppygraph-preparation/0.1','model_sha256':hashlib.sha256(model).hexdigest(),'source_sha256':hashlib.sha256(source).hexdigest(),'public_receipt_sha256':hashlib.sha256(receipt).hexdigest(),'database_sha256':hashlib.sha256(database.read_bytes()).hexdigest(),'duckdb_version':duckdb.__version__,'nodes':nodes,'edges':edges,'declaration_member_order':names,'qualification':'Separately authored publicUMF finite source-profile mapping. Exact scalar tokens/Value JSON/state text; no scalar promotion, canonical publication, ACK or productionauthority.'}
    for name,value in [('model.json',graph),('receipt.json',facts)]: (out/name).write_text(json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    return facts
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('model','source','receipt','umf','output'):p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();prepare(a.model.read_bytes(),a.source.read_bytes(),a.receipt.read_bytes(),a.umf,a.output)
