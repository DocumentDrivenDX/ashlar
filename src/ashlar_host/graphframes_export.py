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


def query_graphframes_export(spark,graph_frame,release):
    """Keep canonical columns and compare actual motif and degree occurrence bags."""
    from pyspark.sql.types import StructType,StructField,StringType
    graph=json.loads(release.payload);nodes=graph['nodes'];edges=graph['edges']
    if len(nodes)>10000 or len(edges)>10000:raise ValueError('Finite GraphFrames export row bound exceeded')
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
    return {'publication_id':graph['publication']['publication_id'],'nodes':len(nodes),'edges':len(edges),
        'one_hop':observed,'out_degrees':degree_rows}


def run_graphframes_export_command(configuration,output):
    """Own one local runtime; result bytes escape only after Spark/file closure."""
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
                    result=query_graphframes_export(spark,GraphFrame,owner.release)
                    owner.admit_original_export(owner.release,owner.custody_bytes,owner.source,owner.context)
                except BaseException as error:primary=error
                finish(primary,[spark_context.stop]if spark_context is not None else[])
    finally:
        for key,value in previous_environment.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value
    result.update(profile='ashlar-local-export-graphframes-one-hop/0.1',release_sha256=config['release_sha256'],custody_sha256=config['custody_sha256'],
        runtime={'spark':native_spark_version,'pyspark':'3.5.3','graphframes':'0.12.3','master':'local[1]','driver_memory':'512m'},
        qualification='Actual one-hop GraphFrames occurrence and out-degree queries over an immutable local export; original canonical columns and isolated vertices preserved. No live source ACK, source-native grants, Unity Catalog or full authored corpus qualification.')
    raw=(encoded(result)+'\n').encode();descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(raw)!=len(raw):raise OSError('Complete GraphFrames export result write required')
        stream.flush();os.fsync(stream.fileno())
    return result
