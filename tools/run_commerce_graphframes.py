"""Small commerce source-profile GraphFrames check; no native publication claim."""
import argparse
from collections import Counter
import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
from commerce_graph_oracle import decimal_scenarios
from run_graph_release_graphframes import JARS,VERSIONS,row_bag
from domain_pack_inventory import encoded

NODE_COLUMNS=('id','record','source_row_key','values_json','lexical_json','field_receipt_refs_json','key_receipt_ref')
EDGE_COLUMNS=('id','src','dst','relationship_id','relationship_json')

def carriers(receipt):
    prepared=receipt['preparedRequests'];public=receipt['publicUmfOperations'];graph=receipt['graph']
    if receipt.get('format')!='ashlar.commerce-source-graph-oracle' or receipt.get('version')!='0.1':raise ValueError('Original commerce oracle profile required')
    # Recompose original row/key/edge custody; semantic checking belongs to the public producer.
    rows={r['sourceRowKey']:r for r in prepared['rows']}
    if len(rows)!=len(prepared['rows']):raise ValueError('Duplicate original source row')
    keys={k['request']['sourceRowKey']:i for i,k in enumerate(public['keys']) if k['request']['role']=='record'}
    nodes=[];field_start={};offset=0
    for row in prepared['rows']:
        field_start[row['sourceRowKey']]=list(range(offset,offset+len(row['fields'])));offset+=len(row['fields'])
    if offset!=len(public['fields']):raise ValueError('Complete field receipt coverage required')
    for node in graph['nodes']:
        row=rows[node['sourceRowKey']]
        if node['values']!={f['field']['element']:f['value'] for f in row['fields']}:raise ValueError('Original field literal custody differs')
        indices=field_start[row['sourceRowKey']]
        for f,i in zip(row['fields'],indices):
            if public['fields'][i]['request']!={'field':f['field'],'value':f['value']}:raise ValueError('Original field request correspondence differs')
        key_index=keys[row['sourceRowKey']];key=public['keys'][key_index]
        identity=key['request']['identity']
        qualified=[prepared['documentId'],identity['module'],identity['element'],identity['key'],key['receipt']['bytesHex']]
        if node['key']!=json.dumps(qualified,ensure_ascii=False,separators=(',',':')):raise ValueError('Source-qualified public key custody differs')
        nodes.append({'id':node['key'],'record':node['record']['element'],'source_row_key':node['sourceRowKey'],
                      'values_json':encoded(node['values']).decode(),'lexical_json':encoded({f['column']:f['lexical'] for f in row['fields']}).decode(),
                      'field_receipt_refs_json':encoded(indices).decode(),'key_receipt_ref':str(key_index)})
    for e in graph['edges']:
        original_key=json.dumps([e['relationship'],e['source'],e['target']],ensure_ascii=False,sort_keys=True,separators=(',',':'))
        if e['key']!=original_key:raise ValueError('Independent original source-profile edge identity differs')
    edges=[{'id':e['key'],'src':e['source'],'dst':e['target'],'relationship_id':e['relationship']['id'],'relationship_json':encoded(e['relationship']).decode()} for e in graph['edges']]
    if len({n['id'] for n in nodes})!=len(nodes) or len({e['id'] for e in edges})!=len(edges):raise ValueError('Duplicate source-profile graph identity')
    nodekeys={n['id'] for n in nodes}
    if any(e['src'] not in nodekeys or e['dst'] not in nodekeys for e in edges):raise ValueError('Dangling source-profile endpoint')
    if len(nodes)!=len(rows):raise ValueError('Complete original row coverage required')
    return nodes,edges

def topology(nodes,edges):
    incoming=Counter(e['dst'] for e in edges);outgoing=Counter(e['src'] for e in edges)
    return {'nodes':len(nodes),'edges':len(edges),'one_hop':len(edges),
            'two_hop':sum(count*outgoing[key] for key,count in incoming.items()),
            'isolates':sum(n['id'] not in set(incoming)|set(outgoing) for n in nodes)}

def run(umf_repo,source,inventory,inspection,output,jars):
    output=Path(output)
    if output.exists():raise ValueError('Fresh output directory required')
    versions={p:importlib.metadata.version(p) for p in VERSIONS}
    if versions!=VERSIONS:raise ValueError('Qualified existing runtime versions required')
    paths=[Path(jars)/name for name in JARS]
    if not all(p.is_file() for p in paths):raise ValueError('Explicit existing jar inventory required')
    output.mkdir(parents=True)
    oracle_path=output/'original-commerce-oracle.json'
    command=[sys.executable,str(Path(__file__).with_name('commerce_graph_oracle.py')),'--umf-repo',str(umf_repo),'--source',str(source),'--inventory',str(inventory),'--inspection',str(inspection),'--output',str(oracle_path)]
    checked=subprocess.run(command,capture_output=True,check=True)
    (output/'umf-check.log').write_bytes(checked.stdout+checked.stderr)
    original=oracle_path.read_bytes();receipt=json.loads(original);nodes,edges=carriers(receipt)
    expected=decimal_scenarios(receipt['preparedRequests'])
    if receipt['scenarios']!=expected:raise ValueError('Independent source scenarios differ')
    from pyspark.sql import SparkSession,functions as F
    from pyspark.sql.types import StructType,StructField,StringType
    from graphframes import GraphFrame
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original commerce small GraphFrames')
           .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1')
           .config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false')
           .config('spark.jars',','.join(str(p) for p in paths))
           .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
           .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
           .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    try:
        frames={}
        for role,rows,columns in [('nodes',nodes,NODE_COLUMNS),('edges',edges,EDGE_COLUMNS)]:
            schema=StructType([StructField(c,StringType(),True) for c in columns])
            spark.createDataFrame(rows,schema).write.format('delta').save(str(output/role))
            frames[role]=spark.read.format('delta').option('versionAsOf',0).load(str(output/role))
            if row_bag(r.asDict() for r in frames[role].collect())!=row_bag(rows):raise AssertionError('Independent exact carrier parity differs')
        graph=GraphFrame(frames['nodes'],frames['edges'])
        actual_topology={'nodes':graph.vertices.count(),'edges':graph.edges.count(),'one_hop':graph.find('(a)-[e]->(b)').count(),
                         'two_hop':graph.find('(a)-[e]->(b); (b)-[f]->(c)').count(),
                         'isolates':graph.vertices.join(graph.edges.select(F.col('src').alias('id')).union(graph.edges.select(F.col('dst').alias('id'))).distinct(),'id','left_anti').count()}
        if actual_topology!=topology(nodes,edges):raise AssertionError('Independent directed topology differs')
        def lexical(alias,name):return F.get_json_object(F.col(alias+'.lexical_json'),'$.'+name)
        # Explicit bounded commerce query profile: integer scale0 and decimal scale2.
        # Original lexical values and public field receipts remain intact alongside these projections.
        def integer(alias,name):return lexical(alias,name).cast('decimal(38,0)')
        def money(alias,name):return lexical(alias,name).cast('decimal(38,2)')
        def rel(alias,name):return F.col(alias+'.relationship_id')==name
        pair=graph.find('(f)-[fl]->(l)').filter(rel('fl','fulfillments.line_id'))
        over=pair.filter(integer('f','quantity')>integer('l','quantity')).select(lexical('f','id').alias('value')).collect()
        partial=graph.find('(f)-[fl]->(l); (r)-[rl]->(l)').filter(rel('fl','fulfillments.line_id')&rel('rl','returns.line_id')).filter(integer('f','quantity')<integer('l','quantity'))
        partial_rows=partial.select(integer('f','quantity').alias('fulfilled'),integer('r','quantity').alias('returned'),(integer('l','quantity')-integer('f','quantity')+integer('r','quantity')).alias('remaining')).collect()
        settled=graph.find('(p)-[pi]->(i)').filter(rel('pi','payments.invoice_id')).filter((money('p','amount')==money('i','amount'))&(lexical('p','currency')==lexical('i','currency'))).select(lexical('p','id').alias('value')).collect()
        refunds=graph.find('(r)-[rr]->(t); (t)-[tl]->(l); (l)-[lp]->(p)').filter(rel('rr','refunds.return_id')&rel('tl','returns.line_id')&rel('lp','order_lines.product_id')).filter(money('r','amount')==integer('t','quantity')*money('p','unit_price')).select(lexical('r','id').alias('value')).collect()
        actual={'partial-return':sorted([[int(r.fulfilled),int(r.returned),int(r.remaining)] for r in partial_rows]),'fulfillment':sorted([[r.value] for r in over]),'settlement':sorted([[r.value] for r in settled]),'refund':sorted([[r.value] for r in refunds])}
        (output/'scenario-observations.json').write_bytes(encoded({'actual':actual,'independent_expected':expected}))
        if actual!={k:v for k,v in expected.items() if k!='qualification'}:raise AssertionError('Native graph scenarios differ from independent direct CSV integer/Decimal oracle')
        report={'format':'ashlar-commerce-graphframes-check/0.1','source_profile':'original-commerce-csv-public-field-key/0.1',
                'oracle_sha256':hashlib.sha256(original).hexdigest(),'upstream_commit':receipt['upstreamCommit'],
                'field_receipts':len(receipt['publicUmfOperations']['fields']),'key_receipts':len(receipt['publicUmfOperations']['keys']),
                'versions':versions,'counts':actual_topology,'scenarios':actual,'exact_full_row_parity':True,
                'local_delta_versions':{'nodes':0,'edges':0},'qualification':'Original commerce CSV source-profile identities and relationship bindings only. Fresh public UMF selected field/key receipts; no complete Record/relationship-instance admission, accepted Truss IDs, native publication or direct UC access.',
                'jar_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}
        if oracle_path.read_bytes()!=original:raise AssertionError('Original oracle bytes changed')
        (output/'report.json').write_bytes(encoded(report));return report
    finally:spark.stop()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['umf-repo','source','inventory','inspection','output','jars']:p.add_argument('--'+name,required=True,type=Path)
    a=p.parse_args();print(json.dumps(run(a.umf_repo,a.source,a.inventory,a.inspection,a.output,a.jars),sort_keys=True))
