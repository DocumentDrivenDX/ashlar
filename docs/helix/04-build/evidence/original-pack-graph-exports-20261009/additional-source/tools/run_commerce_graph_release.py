"""Exact original commerce publication -> immutable full-value graph release.

Reusable host adapter only. Source, ACK, publication and complete native pin
custody come from the admitted provider; engine activation/scalar promotion do
not follow from this release. The CLI requires an explicit existing local Spark
runtime and publication; it does not acquire cloud endpoints.
"""
from collections import Counter
from contextlib import contextmanager
import json
from ashlar.graph_release import read_graph_release
from ashlar.manifest import manifest_pin_vector
from local_delta_custody import encoded

class CommerceGraphPolicy:
    """Compare every canonical carrier to the independent original source oracle."""
    def __init__(self,provider,driver,vector,context,expected):
        self.provider=provider;self.driver=driver;self.vector=vector;self.context=context
        self.original_manifest=encoded(driver.policy.active['manifest'])
        self.expected=json.loads(encoded(expected))
        self.original_admission=encoded(driver.source_admission.metadata())
        self.roles={driver.tables[role]:role for role in ('object_current','edge_current')}
    def require(self,context):
        if context is not self.context or self.provider.context is not context or not self.provider.active or not self.driver.pin_held:raise PermissionError('Original held complete publication/ACK interval required')
        if encoded(self.driver.source_admission.metadata())!=self.original_admission:raise PermissionError('Original public commerce source custody changed')
    def bind_descriptor(self,descriptor,vector,context):
        self.require(context)
        if vector!=self.vector or encoded(dict(descriptor.raw))!=self.original_manifest:raise PermissionError('Original whole manifest/vector changed')
    def authorize_graph_row(self,descriptor,table,row,context):
        self.bind_descriptor(descriptor,self.vector,context)
        role=self.roles.get(table)
        if role is None or dict(row)not in self.expected[role]:raise PermissionError('Complete original commerce graph row differs')
    def authorize_graph_result(self,descriptor,nodes,edges,context):
        self.bind_descriptor(descriptor,self.vector,context)
        for role,rows in [('object_current',nodes),('edge_current',edges)]:
            canonical=[{k:v for k,v in row.items()if k not in ('graph_id','src','dst')}for row in rows]
            if Counter(map(encoded,canonical))!=Counter(map(encoded,self.expected[role])):raise PermissionError('Complete original graph coverage/value bag differs')

class HeldCommercePins:
    """Reuse the provider's existing pin interval without creating a nested lease."""
    def __init__(self,policy):self.policy=policy
    @contextmanager
    def hold(self,vector,*,context):
        self.policy.require(context)
        if vector!=self.policy.vector:raise PermissionError('Original held vector differs')
        yield
        self.policy.require(context)


def export_commerce_release(provider,driver,expected,*,context):
    """Return complete exact bytes only after resolver/source/ACK/pin closure.

    `expected` is the independently computed original_commerce_oracle, never a
    native read or source projection. Provider owns read-only runtime/authority.
    """
    manifest=driver.policy.active['manifest'];versions=json.loads(manifest['table_versions_json'])
    vector=manifest_pin_vector(manifest,{t:driver.transport.targets[t].uuid for t in versions},authority='private-local-process')
    policy=CommerceGraphPolicy(provider,driver,vector,context,expected)
    release=None;completed=False
    with provider.interval(context):
        release=read_graph_release(driver.transport,driver,HeldCommercePins(policy),vector,policy,
            publication_id=manifest['publication_id'],node_table=driver.tables['object_current'],edge_table=driver.tables['edge_current'],context=context,
            supported_profiles=['ashlar-delta/0.3'],supported_revisions={k:[v]for k,v in json.loads(manifest['schema_revisions_json']).items()},
            max_nodes=len(expected['object_current']),max_edges=len(expected['edge_current']))
        policy.require(context)
        completed=True
    if not completed or release is None:raise PermissionError('Closing provider interval suppressed graph refusal')
    return release



def read_commerce_export(reader_factory):
    """Produce both immutable byte strings after EVERY enclosing reader exits.

    Factory owns outer journal closure/native file parity. Persistence MUST occur
    only after this function returns; no provisional bytes are written inside it.
    """
    from private_graph_custody import build_private_custody
    completed=False;release=None;capture=None;source=None;original_native=None
    with reader_factory() as opened:
        release=export_commerce_release(opened.provider,opened.driver,opened.independent_expected,context=opened.context)
        capture=opened.provider.closed_interval_custody(opened.context)
        required={'format','scope','source_schema','source_signature_sha256','original_request_hex','original_manifest_hex','opening_ack','closing_ack','qualification'}
        if type(capture)is not dict or set(capture)!=required or capture['format']!='ashlar-private-local-protected-ack-interval/0.1':raise PermissionError('Exact authoritative closed ACK producer required')
        for phase in ('opening_ack','closing_ack'):
            session=capture[phase]['session']
            if session['source_schema']!=capture['source_schema']or session['source_signature_sha256']!=capture['source_signature_sha256']:raise PermissionError('Admitted ordinary source session custody differs')
        source=opened.admission.metadata();original_native=dict(opened.original_native_files)
        completed=True
    if not completed or release is None:raise PermissionError('Outer source reader suppressed incomplete export')
    ack={'scope':capture['scope'],'requestHex':capture['original_request_hex'],'manifestHex':capture['original_manifest_hex'],'opening':capture['opening_ack'],'closing':capture['closing_ack']}
    custody=build_private_custody(release,source,ack)
    return release,custody,original_native


def persist_commerce_export(directory,release,custody):
    """Fresh private pair only; never overwrite any previous export handle."""
    import os,shutil
    from pathlib import Path
    from private_graph_custody import PROFILE,sha
    from run_graph_release_graphframes import load_release
    directory=Path(directory)
    # Full paired admission happens before exposing any file.
    load_release(release.payload,release.sha256,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=sha(custody))
    directory.mkdir(mode=0o700)
    try:
        for name,payload in [('release.graph.json',release.payload),('custody.json',custody)]:
            with (directory/name).open('xb')as file:
                os.chmod(file.name,0o600);file.write(payload);file.flush();os.fsync(file.fileno())
        fd=os.open(directory,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    except BaseException:
        shutil.rmtree(directory);raise
    return {'release':str(directory/'release.graph.json'),'releaseSha256':release.sha256,'custody':str(directory/'custody.json'),'custodySha256':sha(custody),'custodyProfile':PROFILE}


def run(publication,output,jars):
    """One bounded read-only existing local Spark4 publication export."""
    import hashlib,importlib.metadata,tempfile
    from pathlib import Path
    from run_commerce_publication_weft import open_commerce_reader
    from run_local_weft_typed_spark4 import JAR_SHA
    publication=Path(publication);output=Path(output);jars=Path(jars)
    if output.exists():raise ValueError('Fresh export output required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Explicit existing Spark4.0.1 required')
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(not p.is_file()or hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    from pyspark.sql import SparkSession
    with tempfile.TemporaryDirectory(prefix='ashlar-commerce-export-runtime-')as runtime:
        spark=(SparkSession.builder.master('local[1]').appName('Read-only original commerce graph export').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(map(str,paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(Path(runtime)/'warehouse')).getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        try:release,custody,original_native=read_commerce_export(lambda:open_commerce_reader(spark,publication))
        finally:spark.stop()
    # Spark and source-reader closure must also succeed before visible bytes.
    pair=persist_commerce_export(output,release,custody)
    report={'format':'ashlar-commerce-graph-export-evidence/0.1','pair':pair,'sourcePublication':str(publication),'sourcePublicationReportSha256':hashlib.sha256((publication/'report.json').read_bytes()).hexdigest(),'originalNativeFiles':original_native,'originalNativeFilesUnchanged':True,'nodes':len(json.loads(release.payload)['nodes']),'edges':len(json.loads(release.payload)['edges']),'qualification':__doc__,'engineExecuted':False,'qualifiedNativeDatabricks':False}
    (output/'report.json').write_text(encoded(report)+'\n')
    return report

if __name__=='__main__':
    import argparse
    from pathlib import Path
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publication',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--jars',type=Path,required=True)
    args=parser.parse_args();report=run(args.publication,args.output,args.jars);print(encoded({'output':str(args.output),'nodes':report['nodes'],'edges':report['edges'],'pair':report['pair']}))
