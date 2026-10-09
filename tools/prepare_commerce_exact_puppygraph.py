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
PROFILE_EXACT='ashlar-commerce-exact-graph-arithmetic/0.1'

def signed64(value):
    if type(value)is not int or not -(2**63)<=value<2**63:raise ValueError('Backend capability: exact signed64 representation/operation refused')
    return value

def quantity(token):
    if type(token)is not str or not re.fullmatch('-?(0|[1-9][0-9]*)',token):raise ValueError('Backend capability: exact integer token form refused')
    return signed64(int(token))

def coefficient(token,facets):
    if facets!={'precision':18,'scale':2}or type(token)is not str:raise ValueError('Backend capability: declared fixed decimal profile refused')
    with localcontext()as context:
        context.prec=max(50,len(token)*2);value=Decimal(token);scaled=value*100
        if not value.is_finite()or scaled!=scaled.to_integral_value():raise ValueError('Backend capability: exact scale2 coefficient refused')
        return signed64(int(scaled))

def projection(value,graph,nodes,edges,model):
    fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']};by_key={o['key']:o for o in graph['objects']};rows=[];guards=[]
    for r in value['nodes']:
        original=by_key[nodes[r['graph_id']]];record=original['type']['element'];values=original['values'];row=dict(r)
        row.update(original_record=record,original_key=original['key'],template_id=None,quantity_long=None,quantity_token=None,amount_coefficient=None,amount_token=None,unit_price_coefficient=None,unit_price_token=None,currency=None,decimal_scale='2')
        if record+'.id'in values:
            identity=json.loads(values[record+'.id'])
            if type(identity)is not list or len(identity)!=3 or identity[:2]!=[42,0]or type(identity[2])is not str:raise ValueError('Explicit original scenario replay identity required')
            row['template_id']=identity[2]
        for field_id,token in values.items():
            field=fields['domain',field_id];name=field_id.split('.')[-1]
            if field.get('scalarType')=='integer':
                if name!='quantity':raise ValueError('Unimplemented selected original integer field')
                row['quantity_long']=quantity(token);row['quantity_token']=token
                guards.append({'original_key':original['key'],'field':field_id,'logical_domain':'original mathematicalInteger (no width assigned)','original_token':token,'backend':'signed64 exact-or-capability-error','exact_integer':str(row['quantity_long'])})
            elif field.get('scalarType')=='decimal':
                if name not in ('amount','unit_price'):raise ValueError('Unimplemented selected decimal field')
                row[name+'_coefficient']=coefficient(token,field.get('facets'));row[name+'_token']=token
                guards.append({'original_key':original['key'],'field':field_id,'original_token':token,'authored_facets':field['facets'],'backend':'signed64 coefficient at scale2 exact-or-capability-error','exact_coefficient':str(row[name+'_coefficient'])})
            elif name=='currency':row['currency']=token
        rows.append(row)
    assigned={r['original_key']:r for r in rows};original_edges=graph['edges'];intermediate=[]
    relation=lambda e:e['relationship']['id']
    for f in original_edges:
        if relation(f)!='fulfillments.line_id':continue
        a,b=assigned[f['source']],assigned[f['target']]
        for returned in original_edges:
            if relation(returned)=='returns.line_id'and returned['target']==f['target']:
                c=assigned[returned['source']];subtract=signed64(b['quantity_long']-a['quantity_long']);add=signed64(subtract+c['quantity_long'])
                intermediate.append({'scenario':'partial-return','source_keys':[a['original_key'],b['original_key'],c['original_key']],'subtract':str(subtract),'add':str(add)})
    for refund in original_edges:
        if relation(refund)!='refunds.return_id':continue
        returned=assigned[refund['target']]
        for line in original_edges:
            if relation(line)!='returns.line_id'or line['source']!=refund['target']:continue
            for product in original_edges:
                if relation(product)=='order_lines.product_id'and product['source']==line['target']:
                    price=assigned[product['target']];multiply=signed64(returned['quantity_long']*price['unit_price_coefficient'])
                    intermediate.append({'scenario':'refund','source_keys':[refund['source'],refund['target'],line['target'],product['target']],'multiply_coefficient':str(multiply),'scale':2})
    projected_edges=[dict(r,original_relationship=next(e['relationship']['id']for e in original_edges if e['key']==edges[r['graph_id']]))for r in value['edges']]
    return rows,projected_edges,guards,intermediate

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
