"""Two original-CQP queries over an authored local Delta fixture, not UC authority.

Run in an explicitly selected Spark4/Delta4 Python environment. No SQL repair,
function registration, network dependency resolution or global SDK setup occurs.
"""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import importlib
import json
import os
from pathlib import Path
import stat
import sys
import time

REQUEST_SHA = '192614d9e7b0dee56364299a9d7cfee0d72b0a7c8778b91640da818cb8ef8ad4'
DELTA_JARS = frozenset(('538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e',
    '9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d'))
PUBLIC_COMMIT = 'ff3695af5ece9d386b3dcbf266cf23d632997733'
MODULE_SHA = 'b3467b40a3b80a33b71f7400cdf513cb064c82bf64cd91c239afa41cbb204e4f'
SHIM_SHA = 'cd0fa27c9580742333d14e24d5ebfd74babb47d31720bb4c577278d05486f11d'
QUERIES = (('count', 'SELECT COUNT(*) AS n FROM Resource r'),
           ('token', 'SELECT r.token FROM Resource r'))
ROWS = ((True, 'k1', 'r1', 9223372036854775807, 'masked', 'opaque-λ\x00'),
        (True, 'k2', 'r2', -9223372036854775808, 'masked', ''),
        (False, 'k3', 'r3', 0, 'masked', 'excluded'))


def text(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def original_binding(packet, relation, *, engine='databricks'):
    """Materialization mapping follows the same-original public smoke fixture.

    The comparison scope labels this authored local example, not a policy grant.
    The native owner separately checks actual local identity/version/schema.
    """
    if type(engine) is not str or engine not in ('databricks','spark'):
        raise ValueError('Explicit supported compiler engine required')
    values = []
    for operation in packet['operations']:
        for operand in operation['operands']:
            field = operand['input']
            if field['kind'] == 'field':
                values.append(dict(path=operand['path'], scan=field['scan'],
                    field=field['identity'], domain=field['identity'],
                    column=field['identity']['element'], comparisonScope=sha(b'ashlar-local-current-fixture'),
                    mode='original-authorized' if field['originalAction'] else 'disclosed',
                    originalAction=field['originalAction'], exposure=None))
    for projection in packet['projections']:
        field = projection['field']
        values.append(dict(path=projection['path'], scan=projection['scan'], field=field,
            domain=field, column=field['element'], comparisonScope=sha(b'ashlar-local-current-fixture'),
            mode=None, originalAction=None, exposure='original'))
    return dict(version='weft.'+engine+'.current-values/0.1-candidate',
        sourceRequestSha256=packet['sourceRequestSha256'], designationSha256=packet['designationSha256'],
        sourceBindingSha256=packet['binding']['sha256'],
        scans=[dict(scan=scan['scan'], target=scan['record'], relation=dict(relation)) for scan in packet['scans']],
        values=values)


def write_owned(path, raw):
    from ashlar_host.lifecycle import finish
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    primary = None
    try:
        if os.write(fd, raw) != len(raw):
            raise OSError('Local example output write refused')
    except BaseException as error:
        primary = error
    finish(primary, [lambda: os.close(fd)])


def execute_example(spark, weft, request_bytes, output, schema, *, engine='databricks'):
    """Use the actual local owner and concrete provider; caller owns Spark close."""
    from ashlar_host.databricks_current_query import execute_databricks_current_query
    from ashlar_host.local_current_development import (LocalCurrentDevelopmentOwner,
        local_current_development_context)
    from ashlar_host.path_capture import PathCaptureConfig
    from pyspark.sql.types import StructType, StructField, BooleanType, StringType, LongType
    source = json.loads(request_bytes)
    qualified = '`spark_catalog`.`'+schema+'`.`current_values`'
    spark.sql('CREATE DATABASE `'+schema+'`')
    native_schema = StructType([StructField(name, kind, False) for name,kind in (
        ('eligible',BooleanType()),('private_key',StringType()),('resourceId',StringType()),
        ('salary',LongType()),('salary_public',StringType()),('token',StringType()))])
    spark.createDataFrame(ROWS,native_schema).write.format('delta').saveAsTable(qualified)
    detail = spark.sql('DESCRIBE DETAIL '+qualified).collect()
    history = spark.sql('DESCRIBE HISTORY '+qualified+' LIMIT 1').collect()
    if len(detail) != 1 or len(history) != 1:
        raise ValueError('Single actual local table identity required')
    native = spark.read.format('delta').option('versionAsOf',str(history[0]['version'])).table(qualified)
    relation = dict(catalog='spark_catalog',schema=schema,table='current_values',
        uuid=detail[0]['id'],version=str(history[0]['version']),
        schemaSha256=sha(native.schema.json().encode()),eligibleColumn='eligible',rowKeyColumn='private_key')
    write_owned(output/'local-source.json',text({'scope':'Authored local fixture, not policy eligibility or UC permission',
        'rows':ROWS,'relation':relation,'location':detail[0]['location'],
        'schema_json':native.schema.json(),'schema_convention':'ashlar.local-spark-schema-json/0.1'}).encode())
    results = []
    for name, sql in QUERIES:
        request = dict(source,sql=sql,parameters={})
        raw_request = text(request)
        parent = weft.prepare_application_lineage(raw_request)
        child = parent.prepare_current_query()
        if child.source_preparation() is not parent:
            raise ValueError('Original actual CQP parent identity required')
        packet = json.loads(child.native_request_json())
        binding = text(original_binding(packet, relation, engine=engine))
        artifact = getattr(type(child),'lower_'+engine+'_current_contributors_candidate_json')(child,binding)
        for suffix, raw in [('request',raw_request),('binding',binding),('artifact',artifact)]:
            write_owned(output/(name+'-'+suffix+'.json'),raw.encode())
        context = local_current_development_context()
        authority = LocalCurrentDevelopmentOwner(spark,child,binding,context)
        backend = authority.backend(capture_limits=PathCaptureConfig(8,65536,1048576),
                                    source_limits=PathCaptureConfig(8,65536,1048576),mode='contributors',engine=engine)
        started=time.monotonic()
        result = execute_databricks_current_query(child,context,backend,refinement=True,engine=engine)
        elapsed=time.monotonic()-started
        public = json.loads(result.verified_result)
        expected = [['2']] if name == 'count' else [[row[-1]] for row in ROWS if row[0]]
        if Counter(map(tuple,public['rows'])) != Counter(map(tuple,expected)):
            raise ValueError('Independent authored source oracle differs')
        write_owned(output/(name+'-refinement.json'),result.refinement_report.encode())
        write_owned(output/(name+'-public.json'),result.verified_result.encode())
        results.append({'case':name,'passed':True,'elapsed_seconds':elapsed,'artifact_sha256':sha(artifact.encode()),
                        'verified_result_sha256':sha(result.verified_result.encode())})
    return results


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('weft-root','request','output','java-home'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--delta-jar',type=Path,action='append',required=True)
    p.add_argument('--schema',required=True)
    p.add_argument('--engine',choices=('databricks','spark'),default='databricks')
    p.add_argument('--weft-module-sha256')
    p.add_argument('--weft-shim-sha256')
    p.add_argument('--weft-source-commit')
    args=p.parse_args(argv)
    import re
    explicit=(args.weft_module_sha256,args.weft_shim_sha256,args.weft_source_commit)
    if args.engine=='spark':
        if any(type(v)is not str or re.fullmatch('[0-9a-f]{'+str(n)+'}',v)is None
               for v,n in zip(explicit,(64,64,40))):
            raise ValueError('Explicit development native/shim/source pins required for Spark')
        expected_module,expected_shim,source_commit=explicit
    else:
        if any(v is not None for v in explicit):
            raise ValueError('Public035 default pins cannot be overridden')
        expected_module,expected_shim,source_commit=MODULE_SHA,SHIM_SHA,PUBLIC_COMMIT
    if re.fullmatch('[a-z][a-z0-9_]{0,62}',args.schema) is None:
        raise ValueError('Explicit safe fresh local schema required')
    for selected in (args.weft_root,args.request,args.java_home,*args.delta_jar):
        if not selected.is_absolute() or selected.resolve()!=selected or not selected.exists():
            raise ValueError('Existing explicit canonical runtime/input paths required')
    parent=args.output.parent
    if (not args.output.is_absolute() or parent.resolve()!=parent or args.output.exists()
            or parent.stat().st_uid!=os.getuid() or stat.S_IMODE(parent.stat().st_mode)!=0o700):
        raise ValueError('Fresh output under private owned canonical parent required')
    if len(args.delta_jar)!=2 or any(not path.is_file() for path in args.delta_jar):
        raise ValueError('Explicit existing Delta Spark/storage JAR pair required')
    from ashlar_host.evolution_producer import snapshot
    request_bytes=snapshot(args.request,1048576)
    if sha(request_bytes)!=REQUEST_SHA:
        raise ValueError('Exact unchanged original public035 fixture required')
    if frozenset(sha(snapshot(path,33554432)) for path in args.delta_jar)!=DELTA_JARS:
        raise ValueError('Exact selected Delta4 Spark/storage JAR bytes required')
    os.umask(0o077);args.output.mkdir(mode=0o700)
    if 'weft' in sys.modules:
        raise ValueError('Fresh explicitly selected Weft process required')
    sys.path.insert(0,str(args.weft_root))
    import weft
    module=Path(importlib.import_module('weft.weft').__file__)
    if (sha(module.read_bytes())!=expected_module or sha(Path(weft.__file__).read_bytes())!=expected_shim
            or args.engine=='databricks' and weft.__version__!='0.4.35'):
        raise ValueError('Exact selected native Weft module and shim required')
    for method in ('lower_'+args.engine+'_current_contributors_candidate_json',
                   'check_'+args.engine+'_current_contributors_candidate_json'):
        if not callable(getattr(weft.CurrentQueryPreparation,method,None)):
            raise ValueError('Selected native compiler engine capability required before Spark startup')
    from pyspark.sql import SparkSession
    from ashlar_host.lifecycle import finish
    if SparkSession.getActiveSession() is not None:
        raise ValueError('Fresh dedicated local Spark session required')
    os.environ['JAVA_HOME']=str(args.java_home)
    os.environ['PYSPARK_PYTHON']=sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON']=sys.executable
    spark=None;primary=None;results=None;started=time.monotonic()
    try:
        spark=(SparkSession.builder.master('local[1]').appName('ashlar-local-current-example')
            .config('spark.driver.memory','512m').config('spark.executor.memory','512m')
            .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
            .config('spark.databricks.delta.snapshotPartitions','1')
            .config('spark.sql.warehouse.dir',str(args.output/'warehouse'))
            .config('spark.jars',','.join(map(str,args.delta_jar)))
            .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.sql.ansi.enabled','true').config('spark.sql.session.timeZone','UTC')
            .getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        if spark.version!='4.0.1':raise ValueError('Selected Spark4.0.1 required')
        results=execute_example(spark,weft,request_bytes,args.output,args.schema,engine=args.engine)
    except BaseException as error:primary=error
    finish(primary,[] if spark is None else [spark.stop])
    write_owned(args.output/'result.json',text({'scope':'Authored local Spark/Delta fixture mechanics only; no UC/security authority',
        'cases':results,'elapsed_seconds':time.monotonic()-started,'spark_closed':True,'weft_source_commit':source_commit,'engine':args.engine,
        'compiler_profile':'weft.'+args.engine+'.current-scalar/0.1-candidate',
        'compiler_selection':'public035' if args.engine=='databricks' else 'explicit-development-build',
        'weft_version':weft.__version__,
        'original_fixture_sha256':sha(request_bytes),'module_sha256':sha(module.read_bytes()),'shim_sha256':sha(Path(weft.__file__).read_bytes())}).encode())


if __name__=='__main__':main()
