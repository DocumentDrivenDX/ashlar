"""Tiny unpublished Spark4 infix-collation analyzer observation; no publication qualification."""
import hashlib,json
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession
assert pyspark.__version__=='4.0.1'
output=Path('/private/tmp/ashlar-having-order-collation-smoke-20261009.json')
if output.exists():raise ValueError('Fresh output required')
spark=(SparkSession.builder.master('local[1]').appName('ashlar-infix-order-smoke').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').getOrCreate())
rows=[];complete=False
try:
    base="SELECT x COLLATE UTF8_BINARY AS group_name,COUNT(*) AS n FROM VALUES ('a'),('a '),('é'),('é'),(''),('a') AS t(x) GROUP BY x COLLATE UTF8_BINARY"
    for threshold,expected in [(0,[['',1],['a',2],['a ',1],['é',1],['é',1]]),(9223372036854775807,[])]:
        sql=base+' HAVING COUNT(*)>'+str(threshold)+' ORDER BY group_name COLLATE UTF8_BINARY ASC'
        frame=spark.sql(sql);actual=[list(r)for r in frame.collect()]
        if actual!=expected:raise ValueError('Literal bag/order differs')
        rows.append({'sql':sql,'schema':frame.schema.jsonValue(),'rows':actual,'expected':expected,'plan':frame._jdf.queryExecution().analyzed().toString()})
    complete=True
finally:spark.stop()
if not complete:raise ValueError('No completed scope')
output.write_text(json.dumps({'qualification':__doc__,'runtime':pyspark.__version__,'scriptSha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'observations':rows,'cleanupSucceeded':True},ensure_ascii=False,indent=2)+'\n')
