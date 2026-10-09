"""Original archaeology/ecology/historical-medical publication -> immutable export.

Private local held-source/full-native-vector/protected-ACK custody only. Full
original carrier bags are checked independently; no graph engine activation,
scalar promotion, accepted Truss IDs or future native retention is claimed.
"""
import hashlib,importlib.metadata,json,tempfile
from pathlib import Path
from local_delta_custody import encoded
from run_pack_publication_weft import open_pack_reader
from run_commerce_graph_release import read_commerce_export,persist_commerce_export

PACKS=('archaeology','ecology','medical')

def read_after_stop(spark,reader_factory):
    """Return no provisional export after source or Spark cleanup failure."""
    complete=False;result=None
    try:
        result=read_commerce_export(reader_factory)
        complete=True
    finally:spark.stop()
    if not complete or result is None:raise PermissionError('Source reader suppressed incomplete graph export')
    return result

def run(pack,publication,output,jars):
    if pack not in PACKS:raise ValueError('Explicit admitted original pack required')
    publication=Path(publication);output=Path(output);jars=Path(jars)
    if output.exists():raise ValueError('Fresh immutable export output required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Explicit existing Spark4.0.1 required')
    from run_local_weft_typed_spark4 import JAR_SHA
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(not p.is_file()or hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    from pyspark.sql import SparkSession
    with tempfile.TemporaryDirectory(prefix='ashlar-pack-export-runtime-')as runtime:
        spark=(SparkSession.builder.master('local[1]').appName('Read-only original '+pack+' graph export').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(map(str,paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(Path(runtime)/'warehouse')).getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        release,custody,original_native=read_after_stop(spark,lambda:open_pack_reader(spark,publication,pack,graph_only=True))
    pair=persist_commerce_export(output,release,custody)
    report={'format':'ashlar-original-pack-graph-export/0.1','pack':pack,'pair':pair,'sourcePublication':str(publication),'sourcePublicationReportSha256':hashlib.sha256((publication/'report.json').read_bytes()).hexdigest(),'originalNativeFiles':original_native,'originalNativeFilesUnchanged':True,'nodes':len(json.loads(release.payload)['nodes']),'edges':len(json.loads(release.payload)['edges']),'qualification':__doc__,'engineExecuted':False,'qualifiedNativeDatabricks':False}
    (output/'report.json').write_text(encoded(report)+'\n')
    return report

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--pack',required=True,choices=PACKS)
    for name in ('publication','output','jars'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();print(encoded(run(a.pack,a.publication,a.output,a.jars)))
