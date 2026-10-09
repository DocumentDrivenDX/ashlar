"""Read-only native limit regression on an existing private Delta0 carrier."""
import argparse,hashlib,json
from pathlib import Path
from collections import Counter
from run_commerce_release_graphframes import original
from run_graph_release_graphframes import load_release,JARS,graph_rows,row_bag
from private_graph_custody import PROFILE

def selection(graph):
    types={o['key']:json.dumps(o['type'],sort_keys=True,separators=(',',':')) for o in graph['objects']}
    counts=Counter(types.values());chosen=min(counts,key=lambda t:(-counts[t],t))
    keys=sorted(k for k,t in types.items() if t==chosen)
    if len(keys)<2:raise ValueError('Original multi-row type required to observe limit')
    return types,chosen,keys

def run(release,custody,release_sha,custody_sha,native_nodes,jars,output):
    v=load_release(release,release_sha,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=custody_sha)
    graph,nodes,_,_=original(v);types,chosen,keys=selection(graph)
    output=Path(output)
    if output.exists():raise ValueError('Fresh report required')
    from pyspark.sql import SparkSession,functions as F
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar native observable commerce limit')
      .config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false')
      .config('spark.jars',','.join(str(Path(jars)/n) for n in JARS))
      .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    try:
        base=spark.read.format('delta').option('versionAsOf',0).load(str(native_nodes))
        if row_bag(r.asDict() for r in base.collect())!=row_bag(v['nodes']):raise ValueError('Existing full native carrier differs')
        metadata=spark.createDataFrame([(gid,key,types[key]) for gid,key in nodes.items()],['graph_id','original_key','original_type'])
        selected=base.join(metadata,'graph_id').filter(F.col('original_type')==chosen)
        total=selected.count();rows=selected.orderBy('original_key').limit(1).collect()
        actual_keys=[r.original_key for r in rows]
        if total!=len(keys) or actual_keys!=keys[:1]:raise AssertionError('Native limit missing/order/total mismatch')
        actual=[{k:x for k,x in r.asDict().items() if k not in ('original_key','original_type')} for r in rows]
        expected=[r for r in v['nodes'] if nodes[r['graph_id']]==keys[0]]
        if row_bag(actual)!=row_bag(expected):raise AssertionError('Complete limited original row differs')
        plan=selected.orderBy('original_key').limit(1)._jdf.queryExecution().executedPlan().toString()
        result={'format':'ashlar-commerce-graphframes-observable-limit/0.1','release_sha256':release_sha,'custody_sha256':custody_sha,'native_nodes':str(native_nodes),'native_version':0,'original_type':chosen,'original_total':len(keys),'actual_total':total,'limit':1,'original_ordered_keys':keys,'actual_keys':actual_keys,'actual_complete_rows':actual,'expected_complete_rows':expected,'executed_plan':plan,'scope':'Read-only existing immutable local export rematerialization; no new source/Delta writes, scalar promotion or ongoing source retention.'}
    finally:spark.stop()
    output.write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+'\n');return result
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','native-nodes','jars','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('release-sha256','custody-sha256'):p.add_argument('--'+n,required=True)
    a=p.parse_args();run(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,a.native_nodes,a.jars,a.output)
