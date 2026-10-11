"""Opt-in finite original-commerce integer/coefficient representation.

Quantity LONG is a backend capability, never an assigned logical Integer width.
Money uses exact signed64 coefficient at authored scale2, with original tokens
and declaration/custody retained. All inputs/intermediates are admitted before
native computation or filters. No binary float or Weft compiler claim.
"""
import argparse,hashlib,json,re,subprocess,tempfile
from decimal import Decimal,localcontext
from pathlib import Path
from run_graph_release_graphframes import load_release,columns,row_bag
from run_commerce_release_graphframes import original
from check_commerce_graphframes_scenarios import scenario_oracle
from private_graph_custody import PROFILE
from ashlar_host.puppy_commerce_projection import signed64,quantity,coefficient,projection,PROFILE_EXACT





def prepare(release,custody,rs,cs,umf,output):
    import duckdb,base64
    value=load_release(release,rs,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=cs);graph,nodes,edges,admission=original(value);model=json.loads(base64.b64decode(admission['original_model_base64']))
    with tempfile.TemporaryDirectory(prefix='ashlar-commerce-exact-public-')as d:
        p=Path(d)/'receipt.json';subprocess.run(['bun',str(Path(__file__).with_name('check_commerce_dataset.ts')),str(umf),str(p)],check=True,capture_output=True);public=json.loads(p.read_bytes())
    if public['sourceSha256']!=admission['source_sha256']or public['graphSha256']!=admission['graph_sha256']:raise ValueError('Actual original public source admission differs')
    rows,edge_rows,guards,intermediate=projection(value,graph,nodes,edges,model);expected,csv_sources,pack=scenario_oracle()
    original_root=Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream'
    if hashlib.sha256((original_root/'pack.json').read_bytes()).hexdigest()!=graph['pack']['sha256']or any(v['sha256']!=graph['source_metadata']['template_'+k]['checksum']['value']for k,v in csv_sources.items()):raise ValueError('Original scenario pack/CSV byte pins differ')
    out=Path(output)
    if out.exists():raise ValueError('Fresh finite adapter required')
    out.mkdir(parents=True);database=out/'commerce-exact.duckdb';c=duckdb.connect(str(database));c.execute('CREATE SCHEMA carrier');schema={'catalog':[{'name':'ashlar_commerce_exact','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:/tmp/commerce-exact.duckdb'}}],'node':[],'edge':[]}
    numeric={'quantity_long','amount_coefficient','unit_price_coefficient'}
    try:
        for kind,role,data in [('node','nodes',rows),('edge','edges',edge_rows)]:
            names=list(data[0]);c.execute('CREATE TABLE carrier.'+role+'('+','.join('"'+n+'" '+('BIGINT'if n in numeric else'VARCHAR')for n in names)+',carrier_key VARCHAR,carrier_id VARCHAR)')
            c.executemany('INSERT INTO carrier.'+role+' VALUES('+','.join('?'for _ in names+['carrier_key','carrier_id'])+')',[[r[n]for n in names]+[r['graph_id'],r['id']]for r in data])
            actual=[dict(zip(names,r))for r in c.execute('SELECT '+','.join('"'+n+'"'for n in names)+' FROM carrier.'+role).fetchall()]
            if row_bag(actual)!=row_bag(data):raise ValueError('Complete exact finite/retained carrier parity differs')
            attrs=[n for n in names if n not in ('id','graph_id')]+['carrier_key','carrier_id'];key=kind+'_key'
            entry={'label':'ExactCommerce'+('Node'if kind=='node'else'Edge'),'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'ashlar_commerce_exact','schema':'carrier','table':role,'mappedField':[{'sourceFieldName':n,'targetFieldName':n}for n in attrs]+[{'sourceFieldName':'graph_id','targetFieldName':key}]}},'id':[{'name':key,'type':'STRING'}],'attribute':[{'name':n,'type':'LONG'if n in numeric else'STRING'}for n in attrs]}
            if kind=='edge':entry.update(fromNodeLabel='ExactCommerceNode',toNodeLabel='ExactCommerceNode',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
            schema[kind].append(entry)
    finally:c.close()
    database.chmod(0o444)
    facts={'format':PROFILE_EXACT,'release_sha256':rs,'custody_sha256':cs,'source_admission':admission,'public_dataset':public,'database_sha256':hashlib.sha256(database.read_bytes()).hexdigest(),'numeric_input_guards':guards,'all_unfiltered_intermediate_guards':intermediate,'original_source_graph':graph,'original_csv_sources':csv_sources,'original_pack':pack,'expected':expected,'nodes':rows,'edges':edge_rows,'qualification':'Opt-in exact finite fixture backend representations; logical Integer remains mathematical/unbounded. Originalmoneytokens/scale retain exactmeaning. No DOUBLE, generaltypedscalarpromotion, canonicalpublication/ACK/UC/Weft or production authority claim.'}
    for name,data in [('model.json',schema),('receipt.json',facts)]: (out/name).write_text(json.dumps(data,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    return facts
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','umf','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('release-sha256','custody-sha256'):p.add_argument('--'+n,required=True)
    a=p.parse_args();prepare(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,a.umf,a.output)
