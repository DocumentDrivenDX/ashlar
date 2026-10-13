"""Bounded one-hop GraphFrames use of an immutable local graph export."""
import importlib.metadata
import json,os,sys
from pathlib import Path
from collections import Counter
from ashlar.graph_release import release_columns
from .delta_custody import encoded
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export
from .lifecycle import owned_context,finish


def query_graphframes_export(spark,graph_frame,release,*,maximum_hops=1):
    """Keep canonical columns and compare actual motif and degree occurrence bags."""
    from pyspark.sql.types import StructType,StructField,StringType
    if type(maximum_hops)is not int or maximum_hops not in (1,2):raise ValueError('One or two GraphFrames hops required')
    graph=json.loads(release.payload);nodes=graph['nodes'];edges=graph['edges']
    if len(nodes)>10000 or len(edges)>10000:raise ValueError('Finite GraphFrames export row bound exceeded')
    original_paths=[]
    if maximum_hops==2:
        adjacency={}
        for edge in edges:adjacency.setdefault(edge['src'],[]).append(edge)
        for first in edges:
            for second in adjacency.get(first['dst'],[]):
                if len(original_paths)==10000:raise ValueError('Complete GraphFrames two-hop witness bound exceeded')
                original_paths.append({'src':first['src'],'edge1':first['graph_id'],'middle':first['dst'],'edge2':second['graph_id'],'dst':second['dst']})
    def frame(rows,required):
        names=sorted(set(required)|{name for row in rows for name in row})
        if any(type(value)is not str and value is not None for row in rows for value in row.values()):raise ValueError('Canonical graph STRING carriers required')
        schema=StructType([StructField(name,StringType(),True)for name in names])
        value=spark.createDataFrame([[row.get(name)for name in names]for row in rows],schema)
        observed=[row.asDict()for row in value.limit(10001).collect()]
        if Counter(map(encoded,observed))!=Counter(encoded({name:row.get(name)for name in names})for row in rows):
            raise ValueError('Complete original GraphFrames carrier values differ')
        return value.withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id')
    vertices=frame(nodes,release_columns('node'));links=frame(edges,release_columns('edge'));native=graph_frame(vertices,links)
    motif=native.find('(a)-[e]->(b)').selectExpr('a.id AS src','e.id AS edge','b.id AS dst').limit(10001).collect()
    observed=[{'src':row['src'],'edge':row['edge'],'dst':row['dst']}for row in motif]
    original=[{'src':row['src'],'edge':row['graph_id'],'dst':row['dst']}for row in edges]
    if Counter(map(encoded,observed))!=Counter(map(encoded,original)):raise ValueError('Complete original GraphFrames one-hop occurrence bag differs')
    degrees=native.outDegrees.limit(10001).collect();actual={}
    for row in degrees:
        if row['id']in actual or type(row['outDegree'])is not int or row['outDegree']<0:raise ValueError('Exact GraphFrames out-degree rows required')
        actual[row['id']]=row['outDegree']
    wanted=dict(Counter(row['src']for row in edges))
    if actual!=wanted:raise ValueError('Complete original GraphFrames out-degree bag differs')
    degree_rows=[{'id':row['graph_id'],'out_degree':wanted.get(row['graph_id'],0)}for row in nodes]
    result={'publication_id':graph['publication']['publication_id'],'nodes':len(nodes),'edges':len(edges),
        'one_hop':observed,'out_degrees':degree_rows}
    if maximum_hops==2:
        from pyspark.sql.functions import count,countDistinct
        paths=native.find('(a)-[e1]->(b); (b)-[e2]->(c)').selectExpr(
            'a.id AS src','e1.id AS edge1','b.id AS middle','e2.id AS edge2','c.id AS dst')
        captured=[{name:row[name]for name in ('src','edge1','middle','edge2','dst')}for row in paths.limit(10001).collect()]
        if Counter(map(encoded,captured))!=Counter(map(encoded,original_paths)):
            raise ValueError('Complete original GraphFrames two-hop occurrence bag differs')
        counts=paths.groupBy('src').agg(count('*').alias('path_count'),countDistinct('dst').alias('distinct_destinations')).limit(10001).collect()
        actual_counts={}
        for row in counts:
            if row['src']in actual_counts or any(type(row[name])is not int or row[name]<0 for name in ('path_count','distinct_destinations')):
                raise ValueError('Exact native GraphFrames path counts required')
            actual_counts[row['src']]=(row['path_count'],row['distinct_destinations'])
        path_counts=Counter(path['src']for path in original_paths);destinations={}
        for path in original_paths:destinations.setdefault(path['src'],set()).add(path['dst'])
        wanted_counts={source:(amount,len(destinations[source]))for source,amount in path_counts.items()}
        if actual_counts!=wanted_counts:raise ValueError('Complete original GraphFrames path/distinct-destination counts differ')
        result.update(two_hop=captured,two_hop_counts=[{'id':row['graph_id'],'path_count':path_counts.get(row['graph_id'],0),
            'distinct_destinations':len(destinations.get(row['graph_id'],()))}for row in nodes])
    return result


def run_graphframes_export_command(configuration,output,*,maximum_hops=1):
    """Own one local runtime; result bytes escape only after Spark/file closure."""
    if type(maximum_hops)is not int or maximum_hops not in (1,2):raise ValueError('One or two GraphFrames hops required')
    config=decode_custody_bytes(snapshot(Path(configuration),65536))
    required={'release_path','custody_path','release_sha256','custody_sha256','jars','java_home'}
    if type(config)is not dict or set(config)!=required:raise ValueError('Exact GraphFrames export configuration required')
    def canonical(value,directory=False):
        if type(value)is not str:raise ValueError('Explicit canonical existing runtime path required')
        path=Path(value)
        if not path.is_absolute()or path.resolve()!=path or not (path.is_dir()if directory else path.is_file())or any(char in value for char in ('\0','\n','\r',',')):
            raise ValueError('Explicit canonical existing runtime path required')
        return path
    release=canonical(config['release_path']);custody=canonical(config['custody_path']);java=canonical(config['java_home'],True)
    if type(config['jars'])is not list or len(config['jars'])!=2:raise ValueError('Two existing GraphFrames JARs required')
    jars=tuple(canonical(value)for value in config['jars'])
    expected_names={'io.graphframes_graphframes-spark3_2.12-0.12.3.jar','io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar'}
    if {path.name for path in jars}!=expected_names:raise ValueError('Selected GraphFrames0.12.3 Spark3 JARs required')
    destination=Path(output)
    if (not destination.is_absolute()or not destination.parent.is_dir()or destination.parent.resolve()!=destination.parent
            or destination.exists()or destination.is_symlink()):raise ValueError('Fresh canonical GraphFrames output required')
    protected=(release,custody,Path(configuration).resolve(),java,*jars)
    if any(destination==path or destination in path.parents or path in destination.parents for path in protected):raise ValueError('GraphFrames output must be separate from selected inputs')
    warehouse=destination.parent/(destination.name+'-warehouse')
    if warehouse.exists()or warehouse.is_symlink():raise ValueError('Fresh GraphFrames warehouse required')
    selection=LocalGraphExportSelection(release,custody,config['release_sha256'],config['custody_sha256'])
    from pyspark.sql import SparkSession
    from pyspark import SparkContext,SparkConf
    from graphframes import GraphFrame
    if importlib.metadata.version('pyspark')!='3.5.3'or importlib.metadata.version('graphframes-py')!='0.12.3':raise ValueError('Existing PySpark3.5.3 and GraphFrames0.12.3 required')
    runtime_environment={'JAVA_HOME':str(java),'PYSPARK_PYTHON':sys.executable,'PYSPARK_DRIVER_PYTHON':sys.executable}
    previous_environment={key:os.environ.get(key)for key in runtime_environment}
    spark_context=None;primary=None
    try:
        os.environ.update(runtime_environment)
        with owned_context(open_local_graph_export(selection))as owner:
            with owned_context(owner.writer(owner.context)):
                owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
                try:
                    conf=SparkConf().setMaster('local[1]').setAppName('Ashlar immutable graph one-hop')
                    for key,value in {'spark.driver.memory':'512m','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'1',
                            'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true','spark.sql.warehouse.dir':str(warehouse),
                            'spark.jars':','.join(map(str,jars)),'spark.pyspark.python':sys.executable,
                            'spark.pyspark.driver.python':sys.executable}.items():conf.set(key,value)
                    # Public constructor refuses an existing context under the
                    # SDK lock, rather than attaching to an ambient runtime.
                    spark_context=SparkContext(conf=conf)
                    spark=SparkSession(spark_context)
                    native_spark_version=spark.version
                    if native_spark_version!='3.5.3':raise ValueError('Actual Spark3.5.3 runtime required')
                    spark.sparkContext.setLogLevel('ERROR')
                    result=query_graphframes_export(spark,GraphFrame,owner.release,maximum_hops=maximum_hops)
                    owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
                except BaseException as error:primary=error
                finish(primary,[spark_context.stop]if spark_context is not None else[])
    finally:
        for key,value in previous_environment.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value
    result.update(profile='ashlar-local-export-graphframes-'+('one-hop'if maximum_hops==1 else'two-hop')+'/0.1',release_sha256=config['release_sha256'],custody_sha256=config['custody_sha256'],
        runtime={'spark':native_spark_version,'pyspark':'3.5.3','graphframes':'0.12.3','master':'local[1]','driver_memory':'512m'},
        qualification='Actual bounded GraphFrames occurrence and degree queries over an immutable local export; two-hop mode also checks native path and distinct-destination counts, permitting repeated vertices/edges and cycles. Original canonical columns and isolated vertices preserved. No live source ACK, source-native grants, Unity Catalog or full authored corpus qualification.')
    raw=(encoded(result)+'\n').encode();descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(raw)!=len(raw):raise OSError('Complete GraphFrames export result write required')
        stream.flush();os.fsync(stream.fileno())
    return result
