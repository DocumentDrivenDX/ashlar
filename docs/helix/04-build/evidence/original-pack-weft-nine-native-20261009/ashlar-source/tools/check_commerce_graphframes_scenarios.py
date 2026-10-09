"""Read-only original commerce scenario graph queries with exact finite arithmetic.

Original mathematical Integer declarations remain unbounded; this explicitly
bounded fixture representation is decimal38 exact-or-refuse, not logical typing.
"""
import argparse,base64,csv,hashlib,io,json,re
from decimal import Decimal
from pathlib import Path
from commerce_source_transaction import ROOT
from commerce_graph_oracle import decimal_scenarios
from run_commerce_release_graphframes import original,bag
from run_graph_release_graphframes import load_release,JARS,row_bag
from private_graph_custody import PROFILE

def scenario_oracle(root=ROOT):
    pack_bytes=(root/'pack.json').read_bytes();pack=json.loads(pack_bytes);prepared={'rows':[]};source={}
    for table,columns in pack['execution_profile']['identity_columns'].items():
        raw=(root/'data'/ (table+'.csv')).read_bytes();source[table]={'sha256':hashlib.sha256(raw).hexdigest(),'original_base64':base64.b64encode(raw).decode()}
        for row in csv.DictReader(io.StringIO(raw.decode())):
            prepared['rows'].append({'record':{'element':table},'fields':[{'column':k,'lexical':v} for k,v in row.items()]})
    expected=decimal_scenarios(prepared)
    for s in pack['scenario_checks']:
        if expected[s['id']]!=s['expected']:raise ValueError('Independent original CSV arithmetic differs from authored expected bag')
    return {k:v for k,v in expected.items() if k!='qualification'},source,pack

def run(release,custody,rs,cs,native,jars,output):
    value=load_release(release,rs,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=cs)
    graph,nodes,edges,admission=original(value);model=json.loads(base64.b64decode(admission['original_model_base64']));bindings=json.loads(base64.b64decode(admission['original_bindings_base64']))
    expected,csv_sources,pack=scenario_oracle()
    if hashlib.sha256((ROOT/'pack.json').read_bytes()).hexdigest()!=graph['pack']['sha256']:raise ValueError('Original scenario pack pin differs')
    for table,source in csv_sources.items():
        if source['sha256']!=graph['source_metadata']['template_'+table]['checksum']['value']:raise ValueError('Original CSV template source pin differs')
    native=Path(native);previous=json.loads((native/'report.json').read_text())
    def native_files():return {str(p.relative_to(native)):hashlib.sha256(p.read_bytes()).hexdigest() for role in ('nodes','edges') for p in (native/role).rglob('*') if p.is_file()}
    opening_files=native_files()
    fields={(m['id'],e['id']):e for m in model['modules'] for e in m['elements']};properties={tuple(p['identity'][1:]):p['property_id'] for p in bindings['properties']}
    types={tuple(t['identity']):t['type_id'] for t in bindings['types']}
    # Publicly admitted original source values define every consumed numeric token.
    numeric=[]
    for obj in graph['objects']:
        for name,token in obj['values'].items():
            field=fields['domain',name]
            if field.get('scalarType')=='integer':
                if not re.fullmatch('-?(0|[1-9][0-9]*)',token) or len(token.lstrip('-'))>38:raise ValueError('Finite exact integer capability refused')
                numeric.append({'key':obj['key'],'field':name,'original_token':token,'representation':'decimal(38,0)'})
            elif field.get('scalarType')=='decimal':
                if field.get('facets')!={'precision':18,'scale':2} or not re.fullmatch('-?(0|[1-9][0-9]*)(\.[0-9]{1,2})?',token):raise ValueError('Exact selected fixed decimal capability refused')
                numeric.append({'key':obj['key'],'field':name,'original_token':token,'representation':'decimal(18,2)'})
    output=Path(output)
    if output.exists():raise ValueError('Fresh report required')
    from pyspark.sql import SparkSession,functions as F
    from graphframes import GraphFrame
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original commerce four scenarios')
      .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.sql.ansi.enabled','true')
      .config('spark.sql.decimalOperations.allowPrecisionLoss','false').config('spark.databricks.delta.snapshotPartitions','1')
      .config('spark.ui.enabled','false').config('spark.jars',','.join(str(Path(jars)/n) for n in JARS))
      .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    try:
        frames={};native_snapshots={}
        for role in ('nodes','edges'):
            frame=spark.read.format('delta').option('versionAsOf',0).load(str(Path(native)/role))
            if row_bag(r.asDict() for r in frame.collect())!=row_bag(value[role]):raise ValueError('Existing native complete carrier differs')
            native_uuid=spark.sql("DESCRIBE DETAIL delta.`"+str(native/role)+"`").first()['id']
            if previous['local_snapshots'][role]!={'uuid':native_uuid,'version':0}:raise ValueError('Original materialized native UUID/version differs')
            native_snapshots[role]={'uuid':native_uuid,'version':0}
            frames[role]=frame.withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id')
        gf=GraphFrame(frames['nodes'],frames['edges'])
        def lexical(alias,record,name):return F.get_json_object(F.col(alias+'.props_json'),"$['"+properties['domain',record+'.'+name]+"']")
        def integer(alias,record,name):return lexical(alias,record,name).cast('decimal(38,0)')
        def money(alias,record,name):return lexical(alias,record,name).cast('decimal(18,2)')
        def rel(alias,name):return F.col(alias+'.rel_type_id')==types['edge','domain',name]
        # Verify every numeric extraction/cast matches the exact original token/value.
        extraction=[]
        for item in numeric:
            obj=next(o for o in graph['objects'] if o['key']==item['key']);b=next(b for b in bindings['entities'] if b['kind']=='object' and b['originalKey']==item['key']);property_id=properties['domain',item['field']]
            row=frames['nodes'].filter((F.col('type_id')==b['type_id'])&(F.col('carrier_id')==b['id'])).select(F.get_json_object('props_json',"$['"+property_id+"']").alias('token'),F.get_json_object('props_json',"$['"+property_id+"']").cast(item['representation']).alias('exact')).collect()
            if len(row)!=1 or row[0].token!=item['original_token'] or type(row[0].exact)is not Decimal or row[0].exact!=Decimal(item['original_token']):raise ValueError('Exact native token extraction differs')
            extraction.append(item)
        pair=gf.find('(f)-[fl]->(l)').filter(rel('fl','fulfillments.line_id'))
        over=pair.filter(integer('f','fulfillments','quantity')>integer('l','order_lines','quantity')).select(lexical('f','fulfillments','id').alias('id')).collect()
        partial=gf.find('(f)-[fl]->(l); (r)-[rl]->(l)').filter(rel('fl','fulfillments.line_id')&rel('rl','returns.line_id')).filter(integer('f','fulfillments','quantity')<integer('l','order_lines','quantity'))
        partial_unfiltered=gf.find('(f)-[fl]->(l); (r)-[rl]->(l)').filter(rel('fl','fulfillments.line_id')&rel('rl','returns.line_id'))
        partial_steps=partial_unfiltered.select(F.col('f.id').alias('f'),F.col('l.id').alias('l'),F.col('r.id').alias('r'),(integer('l','order_lines','quantity')-integer('f','fulfillments','quantity')).alias('subtract'),(integer('l','order_lines','quantity')-integer('f','fulfillments','quantity')+integer('r','returns','quantity')).alias('add')).collect()
        by_key={o['key']:o for o in graph['objects']}
        for r in partial_steps:
            f=by_key[nodes[r.f]]['values'];l=by_key[nodes[r.l]]['values'];returned=by_key[nodes[r.r]]['values']
            subtract=int(l['order_lines.quantity'])-int(f['fulfillments.quantity']);added=subtract+int(returned['returns.quantity'])
            if type(r.subtract)is not Decimal or type(r.add)is not Decimal or r.subtract!=subtract or r.add!=added:raise ValueError('Native subtract/add intermediate differs exactly')
        partial_rows=partial.select(integer('f','fulfillments','quantity').alias('fulfilled'),integer('r','returns','quantity').alias('returned'),(integer('l','order_lines','quantity')-integer('f','fulfillments','quantity')+integer('r','returns','quantity')).alias('remaining')).collect()
        settlement=gf.find('(p)-[pi]->(i)').filter(rel('pi','payments.invoice_id')).filter((money('p','payments','amount')==money('i','invoices','amount'))&(lexical('p','payments','currency')==lexical('i','invoices','currency'))).select(lexical('p','payments','id').alias('id')).collect()
        refund_paths=gf.find('(r)-[rr]->(t); (t)-[tl]->(l); (l)-[lp]->(p)').filter(rel('rr','refunds.return_id')&rel('tl','returns.line_id')&rel('lp','order_lines.product_id'))
        product_steps=refund_paths.select(F.col('r.id').alias('r'),F.col('t.id').alias('t'),F.col('p.id').alias('p'),(integer('t','returns','quantity')*money('p','products','unit_price')).alias('product')).collect()
        for r in product_steps:
            returned=by_key[nodes[r.t]]['values'];product=by_key[nodes[r.p]]['values']
            exact=int(returned['returns.quantity'])*Decimal(product['products.unit_price'])
            if type(r.product)is not Decimal or r.product!=exact:raise ValueError('Native multiply intermediate differs exactly')
        refund=gf.find('(r)-[rr]->(t); (t)-[tl]->(l); (l)-[lp]->(p)').filter(rel('rr','refunds.return_id')&rel('tl','returns.line_id')&rel('lp','order_lines.product_id')).filter(money('r','refunds','amount')==integer('t','returns','quantity')*money('p','products','unit_price')).select(lexical('r','refunds','id').alias('id')).collect()
        def original_id(value):
            parsed=json.loads(value)
            if type(parsed)is not list or len(parsed)!=3 or parsed[:2]!=[42,0] or type(parsed[2])is not str:raise ValueError('Original declared scenario identity remapping refused')
            return parsed[2]
        actual={'partial-return':[[int(r.fulfilled),int(r.returned),int(r.remaining)] for r in partial_rows],'fulfillment':[[original_id(r.id)] for r in over],'settlement':[[original_id(r.id)] for r in settlement],'refund':[[original_id(r.id)] for r in refund]}
        for name,rows in expected.items():
            if bag(actual[name])!=bag(rows):raise AssertionError('Native original scenario differs '+name)
        closing_files=native_files()
        if closing_files!=opening_files:raise ValueError('Read-only native table files changed')
        for role in ('nodes','edges'):
            if spark.sql("DESCRIBE DETAIL delta.`"+str(native/role)+"`").first()['id']!=native_snapshots[role]['uuid']:raise ValueError('Closing native UUID changed')
        report={'native_snapshots':native_snapshots,'opening_closing_native_files':opening_files,'native_intermediate_steps':{'partial':[{k:str(v) for k,v in r.asDict().items()} for r in partial_steps],'refund':[{k:str(v) for k,v in r.asDict().items()} for r in product_steps]},'format':'ashlar-commerce-release-graphframes-scenarios/0.1','release_sha256':rs,'custody_sha256':cs,'native_delta_version':0,'numeric_profile':'fixture decimal38 integer and authored decimal18,2; original mathematical Integer unchanged','original_source_admission':admission,'original_csv_sources':csv_sources,'original_pack':pack,'numeric_token_guards':extraction,'expected':expected,'actual':actual,'native_decimal_results':[{k:str(v) for k,v in r.asDict().items()} for r in partial_rows],'scope':'Four actual local GraphFrames scenario queries on unchanged immutable export; exact fixture arithmetic only, no scalar promotion/general unbounded arithmetic/UC/productionauthority.'}
    finally:spark.stop()
    output.write_text(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2)+'\n');return report
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','native','jars','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('release-sha256','custody-sha256'):p.add_argument('--'+n,required=True)
    a=p.parse_args();run(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,a.native,a.jars,a.output)
