"""Concrete existing-commerce export using configured ordinary source owners."""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,tempfile
from ashlar.native import quote_table_identifier
from .commerce import runtime_paths
from .commerce_graph_release import read_commerce_export
from .commerce_query_sources import CommerceQuerySourceFactory
from .evolution_cli import InvocationFile
from .lifecycle import finish,owned_context
from .ordinary_query_connection import OrdinaryQueryConnectionFactory
from .original_commerce_local_cli import parse_original_commerce_local
from .original_commerce_local_setup import original_commerce_local_source_ports
from .publication_reader import open_commerce_reader


def _path(value):
    path=Path(value)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical absolute export paths required')
    return path



EXPORT_CONFIGURATION_PROFILE='ashlar-original-commerce-export-configuration/0.1'

@dataclass(frozen=True)
class CommerceExportConfiguration:
    publication:Path
    source:object


def parse_commerce_export_configuration(raw):
    """Select an existing publication and actual ordinary source; no new handoff."""
    from .ack import AckScope
    from .original_commerce_local_configuration import OriginalCommerceLocalSource
    if type(raw)is not bytes or not 0<len(raw)<=262144:raise ValueError('Bounded export configuration required')
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate export configuration member')
            value[key]=item
        return value
    def refuse(_):raise ValueError('Exact export scalars required')
    value=json.loads(raw,object_pairs_hook=pairs,parse_float=refuse,parse_constant=refuse)
    if type(value)is not dict:raise ValueError('Export configuration object required')
    if value.get('profile')!=EXPORT_CONFIGURATION_PROFILE:
        return parse_original_commerce_local(raw,initialize=False)
    if set(value)!={'profile','publication','source'}:raise ValueError('Closed export-only configuration required')
    source=value['source']
    if type(source)is not dict or set(source)!={'host','port','database','authentication','scope','source_schema','source_signature_sha256','writer_role','reader_role','ack_role'}:raise ValueError('Exact ordinary source selection required')
    scope=source['scope']
    if type(scope)is not dict or set(scope)!={'service_schema','scope_id','installation_id','consumer','feed','epoch'}:raise ValueError('Exact original ACK scope required')
    if type(value['publication'])is not str:raise ValueError('Exact publication path text required')
    selected=OriginalCommerceLocalSource(**{**source,'scope':AckScope(**scope)})
    return CommerceExportConfiguration(_path(value['publication']),selected)


def _separate(output,paths):
    if any(output==path or output in path.parents or path in output.parents for path in paths):
        raise ValueError('Export output must be separate from original inputs and publication')


@contextmanager
def open_commerce_export_spark(jars,warehouse):
    """Own a new ordinary Spark context; SDK imports stay lazy."""
    from pyspark import SparkConf,SparkContext
    from pyspark.sql import SparkSession
    context=None;spark=None;primary=None
    try:
        conf=SparkConf(loadDefaults=False).setMaster('local[1]').setAppName('Ashlar commerce graph export')
        for name,value in {'spark.driver.memory':'512m','spark.ui.enabled':'false',
                'spark.sql.shuffle.partitions':'1','spark.databricks.delta.snapshotPartitions':'1',
                'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true',
                'spark.jars':','.join(str(path)for path in jars),
                'spark.sql.extensions':'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog',
                'spark.sql.warehouse.dir':str(warehouse)}.items():conf.set(name,value)
        context=SparkContext(conf=conf);spark=SparkSession(context);yield spark
    except BaseException as error:primary=error
    finish(primary,([spark.stop]if spark is not None else[])+([context.stop]if context is not None else[]))


@contextmanager
def open_registered_commerce_reader(spark,publication,*,ordinary_session,source_factory,destinations=()):
    """Own the actual reader and register ephemeral aliases under its pin interval."""
    with owned_context(open_commerce_reader(spark,publication,ordinary_session=ordinary_session,source_factory=source_factory))as opened:
        protected=[target.path for target in opened.driver.transport.targets.values()]
        journal=getattr(opened.driver.transport,'journal_path',None)
        if journal is not None:protected.append(Path(journal))
        if hasattr(opened,'admission'):
            protected.extend(Path(path)for path,_ in opened.admission.originals)
        for destination in destinations:_separate(destination,protected)
        with owned_context(opened.provider.interval(opened.context)):
            for table,alias in opened.aliases.items():
                if type(alias)is not str or len(alias.split('.'))!=3:
                    raise ValueError('Three-part commerce query alias required')
                target=opened.driver.transport.targets[table]
                if not (target.path/'_delta_log').is_dir():raise ValueError('Existing original Delta target required')
                spark.sql('CREATE DATABASE IF NOT EXISTS '+quote_table_identifier(alias).split('.')[-2]).collect()
                spark.sql('CREATE TABLE '+quote_table_identifier(alias)+" USING DELTA LOCATION '"+str(target.path).replace("'","''")+"'").collect()
        yield opened


def run_export_original_commerce_command(configuration,publication,output,jars):
    captured=InvocationFile.read(configuration,262144)
    config=parse_commerce_export_configuration(captured.raw)
    publication=_path(publication);output=_path(output);jars=_path(jars)
    if type(config)is CommerceExportConfiguration:
        if publication!=config.publication:raise ValueError('Select the configured original publication')
        model=publication/'original-ontology.json';graph=publication/'original-graph.json'
        protected=[captured.path,publication,jars,model,graph]
    else:
        if publication!=config.publication.output:raise ValueError('Select the configured original publication')
        model=config.publication.model;graph=config.publication.graph
        protected=[captured.path,publication,jars,config.installation,model,graph,config.publication.producer.source,config.publication.producer.bun,config.publication.producer.git]
    _separate(output,protected)
    if output.exists():raise FileExistsError('Fresh export output required')
    selected=SimpleNamespace(output=output,jars=jars,model=model,graph=graph)
    jar_paths=runtime_paths(selected,False)
    ports=original_commerce_local_source_ports(config.source);registration=ports.registration
    def connection(role):
        if role==registration.source_role:return registration.source.connection_factory(None)
        if role==registration.ack_role:return registration.metadata_factory(None)
        raise PermissionError('Unselected ordinary query role refused')
    ordinary=OrdinaryQueryConnectionFactory(connection);source_factory=CommerceQuerySourceFactory(registration)
    primary=None;export=None
    try:
        captured.renew(262144)
        with tempfile.TemporaryDirectory(prefix='ashlar-commerce-export-',dir=output.parent)as work:
            with owned_context(open_commerce_export_spark(jar_paths,Path(work)/'warehouse'))as spark:
                def reader_factory():
                    return open_registered_commerce_reader(spark,publication,ordinary_session=ordinary,
                        source_factory=source_factory,destinations=(output,))
                export=read_commerce_export(reader_factory)
    except BaseException as error:primary=error
    finish(primary,[lambda:captured.renew(262144)])
    release,custody,_=export
    # No successful files are exposed before reader, Spark and configuration close.
    output.mkdir(mode=0o700)
    for name,raw in (('release.graph.json',release.payload),('custody.json',custody)):
        with (output/name).open('xb')as stream:
            if stream.write(raw)!=len(raw):raise OSError('Incomplete original export write')
        (output/name).chmod(0o444)
    return {'profile':'ashlar-original-commerce-export/0.1','output':str(output),
        'release':str(output/'release.graph.json'),'release_sha256':release.sha256,
        'custody':str(output/'custody.json'),'custody_sha256':hashlib.sha256(custody).hexdigest(),
        'scope':'Closed original private-local immutable export pair only; no future canonical availability, Fabric deployment or graph query authority.'}
