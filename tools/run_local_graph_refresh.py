"""Actual small local Delta publications and immutable GraphFrames refresh.

Explicit synthetic canonical source profile, process-local custody only; no UC,
Truss, UMF source admission, remote fencing or production activation qualification.
"""
import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
import hashlib,json
from pathlib import Path
import re,time
from ashlar.graph_release import NODE_INTS,NODE_TEXT,EDGE_INTS,EDGE_TEXT,read_graph_release,persist_graph_release
from ashlar.native import SQLResult
from ashlar.pins import PinVector
from ashlar.publication import Snapshot
from run_graph_release_graphframes import JARS,VERSIONS,load_release,columns,graph_rows,row_bag,oracle

TABLES={'nodes':'local.runtime.object_current','edges':'local.runtime.edge_current'}

def encoded(value):return (json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
def digest(value):return hashlib.sha256(value).hexdigest()
def carrier(kind,identity,*,target='2',source='1',version='1',props=' {"exact":9007199254740993,"decimal":-0.00} '):
    integers=NODE_INTS if kind=='node' else EDGE_INTS;texts=NODE_TEXT if kind=='node' else EDGE_TEXT
    row={k:'1' for k in integers};row.update({k:'exact' for k in texts})
    row.update(id=identity,entity_version=version,source_system='local refresh 雪',schema_revision='synthetic-1',props_json=props,retained_json='{"opaque":18446744073709551615}',source_feed='local-fixture',source_epoch='one',source_position=None,apply_batch_id=None,source_cursor_json=None,source_delivery_id=None,published_at='-1')
    if kind=='node':row['root_id']=None
    else:row.update(source_type='1',source_id=source,target_type='1',target_id=target,order_key=None)
    typed='type_id' if kind=='node' else 'rel_type_id'
    row['lookup_hash']=digest(json.dumps({'source_system':row['source_system'],typed:int(row[typed]),'id':int(identity)},ensure_ascii=False,separators=(',',':')).encode())
    return row

def fixture():
    wide='9223372036854775807'
    return {'R1':{'nodes':[carrier('node',i) for i in ['1','2','3']],
                  'edges':[carrier('edge',i,target=t) for i,t in [('7','2'),('8','2'),('9','1')]]},
            'R2':{'nodes':[carrier('node','1',version='2',props=' {"value":"successor café 雪","decimal":12.5000} '),carrier('node','3'),carrier('node',wide)],
                  'edges':[carrier('edge','7',target='1',version='2'),carrier('edge','8',target=wide,version='2'),carrier('edge','9',target='1'),carrier('edge','10',source=wide,target='1')]}}

@dataclass(frozen=True)
class Handle:
    sha256:str
    payload:bytes
    directory:Path
    # Immutable native rematerialization identity vector (role, UUID, version).
    targets:tuple

def bound_paths(handle):
    if not isinstance(handle,Handle) or digest(handle.payload)!=handle.sha256 or handle.directory.name!=handle.sha256:
        raise ValueError('Single immutable complete release handle required')
    load_release(handle.payload,handle.sha256)
    if len(handle.targets)!=2 or {r for r,u,v in handle.targets}!={'nodes','edges'} or any(type(v) is not int or v!=0 or not isinstance(u,str) or not u for r,u,v in handle.targets):
        raise ValueError('Complete local UUID/version vector required')
    return {r:handle.directory/r for r,u,v in handle.targets}

def validate_native_vector(spark,handle,paths):
    for role,uuid,version in handle.targets:
        if spark.sql('DESCRIBE DETAIL delta.`'+str(paths[role])+'`').first().id!=uuid:
            raise ValueError('Complete local graph native identity vector changed')

class LocalSource:
    """One private process-owned native Delta source and immutable manifest files."""
    def __init__(self,spark,directory):
        self.spark=spark;self.directory=directory;self.context=object();self.held=False
        self.paths={TABLES[r]:directory/r for r in TABLES};self.manifests={};self.expected={}
    def query(self,sql,parameters):
        if parameters:raise ValueError('No unbound local fixture parameters')
        for table,path in self.paths.items():sql=sql.replace('`'+'`.`'.join(table.split('.'))+'`','delta.`'+str(path)+'`')
        frame=self.spark.sql(sql)
        return SQLResult([r.asDict() for r in frame.collect()],tuple((f.name,f.dataType.simpleString().upper()) for f in frame.schema.fields))
    def detail(self,table):return self.spark.sql('DESCRIBE DETAIL delta.`'+str(self.paths[table])+'`').first().asDict()
    def frame(self,table,version):return self.spark.read.format('delta').option('versionAsOf',version).load(str(self.paths[table]))
    def write(self,rows):
        if self.held:raise ValueError('Writer cannot cross local held publication interval')
        from pyspark.sql.types import StructType,StructField,LongType,StringType,TimestampType
        for role,kind in [('nodes','node'),('edges','edge')]:
            ints=NODE_INTS if kind=='node' else EDGE_INTS;texts=NODE_TEXT if kind=='node' else EDGE_TEXT
            schema=StructType([StructField(n,LongType(),True) for n in ints]+[StructField(n,StringType(),True) for n in texts]+[StructField('published_at',TimestampType(),True)])
            native=[{n:(datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(microseconds=int(v))) if n=='published_at' else int(v) if n in ints and v is not None else v for n,v in row.items()} for row in rows[role]]
            self.spark.createDataFrame(native,schema).write.format('delta').mode('overwrite').save(str(self.paths[TABLES[role]]))
    def publish(self,name,rows):
        versions={};targets={}
        for table in self.paths:
            detail=self.detail(table);history=self.spark.sql('DESCRIBE HISTORY delta.`'+str(self.paths[table])+'`').first()
            versions[table]=int(history.version)
            targets[table]={'uuid':detail['id'],'version':int(history.version),'committed_at':str(int(history.timestamp.replace(tzinfo=timezone.utc).timestamp()*1000000)),'readable_until':str(time.time_ns()//1000+3600000000)}
        raw={'publication_id':name,'profile_version':'ashlar-delta/0.3','recorded_at':str(time.time_ns()//1000),'table_versions_json':encoded(versions).decode(),'schema_revisions_json':'{"local-fixture":"synthetic-1"}','source_progress_json':encoded({'local-fixture':{'profile':'explicit-local-refresh-fixture/0.1','publication':name}}).decode(),'validation_report_json':encoded({'complete':True,'scope':'Private local canonical fixture, full independent row oracle; no UMF/Truss/UC or remote policy claim','source_oracle_sha256':digest(encoded(rows)),'retention':{'profile':'private-local-no-cleanup-one-hour/0.1','targets':targets}}).decode()}
        path=self.directory/(name+'.manifest.json')
        with path.open('xb') as file:file.write(encoded(raw))
        self.manifests[name]=(path,encoded(raw));self.expected[name]=rows
        vector=PinVector('private-local-process','manifest',name,digest(encoded(raw)),{t:(o['uuid'],o['version']) for t,o in targets.items()})
        release=read_graph_release(self,self,self,vector,self,publication_id=name,node_table=TABLES['nodes'],edge_table=TABLES['edges'],context=self.context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'local-fixture':['synthetic-1']},max_nodes=10,max_edges=10)
        persist_graph_release(self.directory,release)
        return release
    @contextmanager
    def hold(self,vector,*,context):
        if context is not self.context or self.held:raise ValueError('Private whole-vector local pin interval required')
        self.held=True
        try:
            yield
            self.bind_descriptor(next(iter(self._resolved)),vector,context)
        finally:self.held=False
    def authorize(self,context,publication_id,tables):
        if context is not self.context or not self.held or set(tables)!=set(self.paths):raise ValueError('Private complete read interval required')
    def descriptors(self,publication_id):
        path,raw=self.manifests[publication_id]
        if path.read_bytes()!=raw:raise ValueError('Immutable local original manifest changed')
        return [json.loads(raw)]
    def validate_descriptor(self,descriptor,context):
        if context is not self.context or not self.held:raise ValueError('Private context missing')
        if dict(descriptor.raw)!=self.descriptors(descriptor.publication_id)[0]:raise ValueError('Original descriptor differs')
        for t,target in descriptor.validation_report['retention']['targets'].items():
            if self.detail(t)['id']!=target['uuid'] or int(target['readable_until'])<=time.time_ns()//1000:raise ValueError('Local retention or native identity expired')
        self._resolved=(descriptor,)
    def inspect_snapshot(self,table,version):
        self.frame(table,version).count()
        return Snapshot(table,self.detail(table)['id'],version)
    def bind_descriptor(self,descriptor,vector,context):
        self.validate_descriptor(descriptor,context)
        if vector.custody_digest!=digest(encoded(dict(descriptor.raw))) or dict(vector.targets)!={t:(descriptor.validation_report['retention']['targets'][t]['uuid'],v) for t,v in descriptor.versions.items()}:raise ValueError('Complete original local pin vector differs')
    def authorize_graph_row(self,descriptor,table,row,context):
        role=next(r for r,t in TABLES.items() if t==table)
        if context is not self.context or not self.held or dict(row) not in self.expected[descriptor.publication_id][role]:raise ValueError('Original explicit local row oracle differs')
    def authorize_graph_result(self,descriptor,nodes,edges,context):
        if context is not self.context or not self.held:raise ValueError('Closing local graph interval missing')
        for role,rows in [('nodes',nodes),('edges',edges)]:
            canonical=[{k:v for k,v in r.items() if k not in ('graph_id','src','dst')} for r in rows]
            if row_bag(canonical)!=row_bag(self.expected[descriptor.publication_id][role]):raise ValueError('Complete original local value oracle differs')

class Refresh:
    """Validated process-local handle switch; retained readers hold old handles."""
    def __init__(self,spark,directory):self.spark=spark;self.directory=directory;self.active=None
    def build(self,release,*,after_nodes=None,fail=False):
        from pyspark.sql.types import StructType,StructField,StringType
        value=load_release(release.payload,release.sha256);directory=self.directory/release.sha256;directory.mkdir()
        targets=[]
        for kind,role in [('node','nodes'),('edge','edges')]:
            schema=StructType([StructField(n,StringType(),True) for n in columns(kind)])
            self.spark.createDataFrame(value[role],schema).write.format('delta').save(str(directory/role))
            detail=self.spark.sql('DESCRIBE DETAIL delta.`'+str(directory/role)+'`').first()
            targets.append((role,detail.id,0))
            if role=='nodes' and after_nodes:after_nodes()
            if role=='nodes' and fail:raise ValueError('Injected incomplete successor materialization')
        handle=Handle(release.sha256,release.payload,directory,tuple(targets));self.execute(handle)
        return handle
    def execute(self,handle):
        from graphframes import GraphFrame
        from pyspark.sql import functions as F
        value=load_release(handle.payload,handle.sha256);paths=bound_paths(handle);frames={}
        validate_native_vector(self.spark,handle,paths)
        for role,uuid,version in handle.targets:
            if self.spark.sql('DESCRIBE DETAIL delta.`'+str(paths[role])+'`').first().id!=uuid:raise ValueError('Local graph native table identity changed')
            frame=self.spark.read.format('delta').option('versionAsOf',version).load(str(paths[role]))
            if row_bag(r.asDict() for r in frame.collect())!=row_bag(value[role]):raise ValueError('Exact whole release carrier vector differs')
            frames[role]=frame.withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id')
        graph=GraphFrame(frames['nodes'],frames['edges'])
        result={'nodes':graph.vertices.count(),'edges':graph.edges.count(),'one_hop':graph.find('(a)-[e]->(b)').count(),'two_hop':graph.find('(a)-[e]->(b); (b)-[f]->(c)').count(),'isolates':graph.vertices.join(graph.edges.select(F.col('src').alias('id')).union(graph.edges.select(F.col('dst').alias('id'))).distinct(),'id','left_anti').count()}
        if result!=oracle(value):raise ValueError('Independent directed identity/topology oracle differs')
        pairs=sorted([r.asDict() for r in graph.find('(a)-[e]->(b)').select(F.col('a.id').alias('source'),F.col('e.id').alias('edge'),F.col('b.id').alias('target'),F.col('a.props_json').alias('source_props'),F.col('e.props_json').alias('edge_props'),F.col('b.props_json').alias('target_props')).collect()],key=lambda r:r['edge'])
        nodes={n['graph_id']:n for n in value['nodes']}
        expected=sorted([{'source':e['src'],'edge':e['graph_id'],'target':e['dst'],'source_props':nodes[e['src']]['props_json'],'edge_props':e['props_json'],'target_props':nodes[e['dst']]['props_json']} for e in value['edges']],key=lambda r:r['edge'])
        if pairs!=expected:raise ValueError('Actual graph identities/endpoints/values differ')
        validate_native_vector(self.spark,handle,paths)
        return {'counts':result,'identity_value_rows':pairs,'full_row_parity':True,'release_sha256':handle.sha256}
    def activate(self,handle):
        self.execute(handle);self.active=handle


def run(output,jars):
    import importlib.metadata
    versions={p:importlib.metadata.version(p) for p in VERSIONS}
    if versions!=VERSIONS:raise ValueError('Qualified cached runtimes required')
    paths=[Path(jars)/n for n in JARS]
    if not all(p.is_file() for p in paths):raise ValueError('Existing explicit jar paths required')
    output=Path(output)
    if output.exists():raise ValueError('Fresh private output required')
    output.mkdir(parents=True);(output/'source').mkdir();(output/'materialized').mkdir();(output/'failed').mkdir()
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar bounded local publication refresh').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').config('spark.sql.session.timeZone','UTC').config('spark.jars',','.join(str(p) for p in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    try:
        profile=fixture();(output/'source-profile.json').write_bytes(encoded(profile));source=LocalSource(spark,output/'source')
        source.write(profile['R1']);r1=source.publish('R1',profile['R1']);refresh=Refresh(spark,output/'materialized');h1=refresh.build(r1);refresh.activate(h1);first=refresh.execute(h1)
        source.write(profile['R2']);r2=source.publish('R2',profile['R2'])
        partial=[]
        def old_reader():
            if refresh.active is not h1:raise AssertionError('Partial successor activated')
            partial.append(refresh.execute(refresh.active))
        h2=refresh.build(r2,after_nodes=old_reader)
        # Cross-release UUID vector substitution is refused before GraphFrames execution.
        mixed=Handle(h1.sha256,h1.payload,h1.directory,(h1.targets[0],h2.targets[1]))
        try:refresh.execute(mixed)
        except ValueError:pass
        else:raise AssertionError('Mixed release handle accepted')
        failed=Refresh(spark,output/'failed');failed.active=h1
        try:failed.build(r2,after_nodes=lambda:failed.execute(failed.active),fail=True)
        except ValueError:pass
        else:raise AssertionError('Incomplete successor did not refuse')
        if failed.active is not h1 or failed.execute(h1)!=first:raise AssertionError('Failed refresh replaced retained R1')
        refresh.activate(h2);second=refresh.execute(refresh.active)
        if refresh.execute(h1)!=first or any(p!=first for p in partial):raise AssertionError('Old reader changed across refresh')
        refresh.activate(h1)
        if refresh.execute(refresh.active)!=first:raise AssertionError('Rollback changed R1')
        report={'format':'ashlar-local-graph-refresh-check/0.1','versions':versions,'R1':first,'R2':second,'r1_native_vector':h1.targets,'r2_native_vector':h2.targets,'partial_successor_keeps_r1':True,'mixed_vector_refused':True,'failed_successor_keeps_r1':True,'old_reader_after_activation_unchanged':True,'rollback_r1_verified':True,'scope':__doc__}
        (output/'report.json').write_bytes(encoded(report));return report
    finally:spark.stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--jars',required=True,type=Path);a=parser.parse_args();print(json.dumps(run(a.output,a.jars),sort_keys=True))
