"""Explicit installed fresh initialization/existing publication dispatch."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from .config import ProducerConfig
from .evolution_cli import InvocationFile
from .lifecycle import finish
from .original_commerce_local_configuration import (
    OriginalCommerceLocalSource,OriginalCommerceLocalInvocationCustody,OriginalCommerceLocalRuntime,OriginalCommerceLocalInitializationConfig,OriginalCommerceLocalPublicationConfig)
from .original_commerce_publication import OriginalCommercePublicationConfig
from .original_commerce_local_setup import initialize_original_commerce_local,publish_existing_original_commerce_local
from .commerce_publication_sources import OriginalCommerceSourcePorts


def _closed(value,keys):
    if type(value)is not dict or set(value)!=set(keys):raise ValueError('Closed explicit local configuration required')
    return value


def _path(value):
    if type(value)is not str or not value or len(value.encode())>4096 or '\x00'in value:
        raise ValueError('Exact bounded path text required')
    result=Path(value)
    if not result.is_absolute():raise ValueError('Explicit absolute path required')
    return result


def parse_original_commerce_local(raw,*,initialize):
    if type(initialize)is not bool:raise ValueError('Exact selected operation required')
    if type(raw)is not bytes or len(raw)>262144:raise ValueError('Bounded original configuration required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate configuration key')
            result[key]=value
        return result
    def constant(value):raise ValueError('Nonfinite configuration value refused')
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)
    common={'profile','output','model','graph','jars','producer','runtime'}
    _closed(value,common|({'root','installation_id','capacity'}if initialize else{'installation','installation_sha256','source'}))
    expected='ashlar-original-commerce-local-'+('initialization'if initialize else'publication')+'/0.1'
    if value['profile']!=expected:raise ValueError('Exact selected original local operation required')
    p=_closed(value['producer'],{'source','bun','git','timeout_seconds','maximum_output_bytes','maximum_receipt_bytes'})
    producer=ProducerConfig(_path(p['source']),_path(p['bun']),_path(p['git']),p['timeout_seconds'],p['maximum_output_bytes'],p['maximum_receipt_bytes'])
    r=_closed(value['runtime'],{'python','python_sha256','java_home','java_sha256','jvm_config_sha256'})
    runtime=OriginalCommerceLocalRuntime(_path(r['python']),r['python_sha256'],_path(r['java_home']),r['java_sha256'],r['jvm_config_sha256'])
    publication=OriginalCommercePublicationConfig(_path(value['output']),_path(value['model']),_path(value['graph']),_path(value['jars']),producer)
    if initialize:
        return OriginalCommerceLocalInitializationConfig(_path(value['root']),publication.output,value['installation_id'],publication.model,publication.graph,publication.jars,producer,value['capacity'],runtime)
    from .ack import AckScope
    source=_closed(value['source'],{'host','port','database','authentication','scope','source_schema','source_signature_sha256','writer_role','reader_role','ack_role'})
    scope=_closed(source['scope'],{'service_schema','scope_id','installation_id','consumer','feed','epoch'})
    if any(type(v)is not str for v in scope.values()):raise ValueError('Exact original scope strings required')
    selected=OriginalCommerceLocalSource(source['host'],source['port'],source['database'],source['authentication'],AckScope(**scope),source['source_schema'],source['source_signature_sha256'],source['writer_role'],source['reader_role'],source['ack_role'])
    return OriginalCommerceLocalPublicationConfig(_path(value['installation']),value['installation_sha256'],publication,selected,runtime)


def _execute_original_commerce_local(configuration,*,initialize,expected_sha256):
    if type(initialize)is not bool:raise ValueError('Exact selected local operation required')
    file=InvocationFile.read(configuration,262144);config=parse_original_commerce_local(file.raw,initialize=initialize)
    if hashlib.sha256(file.raw).hexdigest()!=expected_sha256:raise PermissionError('Externally selected original configuration required')
    config=replace(config,custody=OriginalCommerceLocalInvocationCustody((file,)))
    primary=None;result=None
    try:
        file.renew(262144)
        if initialize:result=initialize_original_commerce_local(config)
        else:result=publish_existing_original_commerce_local(config)
    except BaseException as error:primary=error
    finish(primary,[lambda:file.renew(262144)])
    return result


def run_original_commerce_local_command(configuration,*,initialize,provider=None,provider_sha256=None):
    """Own a finite installed child; lower setup APIs do not claim hard cancellation."""
    import os
    from .graphframes_release import capture_graphframes_worker
    if type(initialize)is not bool:raise ValueError('Exact selected local operation required')
    if provider is not None or provider_sha256 is not None:raise ValueError('This closed fixture command does not load a provider')
    if any(name.startswith('PG')for name in os.environ):raise PermissionError('Ambient PostgreSQL settings refused')
    file=InvocationFile.read(configuration,262144)
    config=parse_original_commerce_local(file.raw,initialize=initialize)
    def renew():
        file.renew(262144);config.runtime.renew()
    primary=None;raw=None
    try:
        renew()
        environment={name:os.environ[name]for name in ('PATH','HOME','TMPDIR','LANG','LC_ALL')if name in os.environ}
        environment['JAVA_HOME']=str(config.runtime.java_home)
        environment['PYTHONDONTWRITEBYTECODE']='1'
        environment['PYSPARK_PYTHON']=str(config.runtime.python)
        environment['PYSPARK_DRIVER_PYTHON']=str(config.runtime.python)
        environment['SPARK_LOCAL_IP']='127.0.0.1'
        # -I selects the installed package and ignores ambient Python path injection.
        argv=[str(config.runtime.python),'-I','-B','-m','ashlar_host.original_commerce_local_cli',
            '--configuration',str(file.path),'--sha256',hashlib.sha256(file.raw).hexdigest(),
            '--operation','initialize'if initialize else'publish']
        raw,_=capture_graphframes_worker(argv,cwd=file.path.parent,environment=environment,
            renew=renew,timeout_seconds=300,maximum_output_bytes=1048576)
    except BaseException as error:primary=error
    finish(primary,[renew])
    if type(raw)is not bytes or not raw or len(raw)>1048576:raise ValueError('Bounded local result required')
    return json.loads(raw)


def _worker_main():
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--configuration',required=True)
    parser.add_argument('--sha256',required=True)
    parser.add_argument('--operation',required=True,choices=('initialize','publish'))
    args=parser.parse_args()
    if len(args.sha256)!=64 or any(c not in '0123456789abcdef'for c in args.sha256):
        raise ValueError('Exact externally selected configuration digest required')
    result=_execute_original_commerce_local(Path(args.configuration),initialize=args.operation=='initialize',expected_sha256=args.sha256)
    raw=(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n').encode()
    if len(raw)>1048576:raise ValueError('Bounded local result required')
    print(raw.decode(),end='')


if __name__=='__main__':
    try:_worker_main()
    except Exception:
        import sys
        print('Original local operation refused',file=sys.stderr)
        raise SystemExit(2)from None
