"""Explicit installed Paths commerce lifecycle; no fallback or source grant.

Results are provisional until held readers close, Spark stops, and closing source,
JAR, installation and publication custody pass. SDK imports are lazy. This
composition does not change the historical query-commerce profile.
"""
from collections import Counter
from dataclasses import asdict, is_dataclass
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path

from ashlar.weft_paths_distribution import (
    PathsDistributionPaths, compile_paths_distribution, installed_paths_schema_bundle,
)
from .commerce import runtime_paths
from .commerce_path_oracle import commerce_path_cases, original_commerce_path_oracle
from .commerce_path_request import commerce_path_request
from .config import HostError, QueryCommercePathsConfig
from .lifecycle import finish, owned_context
from .path_admission import PathAdmissionConfig
from .path_execution import PathExecutionConfig, execute_commerce_path
from .path_schema import make_offline_path_schema_validation
from .publication_reader import open_commerce_reader
from .source import original_inputs, read_bounded, recompute_dataset
from .source_identity import SOURCE_SHA

UMF_REVISION = 'c7c95e1c4ea5b72541f47fa0350ca467ff02f395'


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)



def _bounded_encoded(value, maximum):
    """Exact ensure-ascii byte preflight before whole serialization."""
    remaining=maximum
    def charge(amount):
        nonlocal remaining
        remaining-=amount
        if remaining<0:raise HostError('paths-retained-bound')
    def string(text):
        charge(2)
        for char in text:
            n=ord(char)
            if 0xD800<=n<=0xDFFF:raise HostError('paths-json-refused')
            charge(2 if char in '\"\\\b\f\n\r\t' else 6 if n<32 or n>=127 and n<=65535 else 12 if n>65535 else 1)
    def visit(item,depth=0):
        if depth>64:raise HostError('paths-retained-bound')
        if type(item) is str:string(item)
        elif item is None:charge(4)
        elif type(item) is bool:charge(4 if item else 5)
        elif type(item) is int:
            if abs(item)>=10**20:raise HostError('paths-json-refused')
            charge(len(str(item)))
        elif type(item) in (tuple,list):
            charge(2+max(0,len(item)-1))
            for entry in item:visit(entry,depth+1)
        elif type(item) is dict:
            charge(2+max(0,len(item)-1))
            for key,entry in item.items():
                if type(key) is not str:raise HostError('paths-json-refused')
                string(key);charge(1);visit(entry,depth+1)
        else:raise HostError('paths-json-refused')
    visit(value)
    return _encoded(value)

def _parse(raw, maximum):
    if type(raw) is not bytes or len(raw) > maximum:
        raise HostError('paths-artifact-bound')
    def pairs(entries):
        result = {}
        for key,value in entries:
            if key in result: raise HostError('paths-json-refused')
            result[key] = value
        return result
    def integer(token):
        if len(token.lstrip('-')) > 20: raise HostError('paths-json-refused')
        return int(token)
    def numeric(token): raise HostError('paths-json-refused')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_int=integer,
                          parse_float=numeric, parse_constant=numeric)
    except (ValueError, UnicodeError, RecursionError):
        raise HostError('paths-json-refused') from None


def _json(value):
    if is_dataclass(value): return _json(asdict(value))
    if isinstance(value,dict):return {key:_json(item) for key,item in value.items()}
    if isinstance(value,(tuple,list)):return [_json(item) for item in value]
    if type(value) is bytes:return {'hex':value.hex()}
    if isinstance(value,Decimal):return {'decimal':str(value)}
    return value



def _publish_report(output: Path, payload: bytes) -> None:
    """Close owned staging before no-clobber final publication.

    Cooperating writers in the exclusively owned output directory are assumed.
    No crash/power-loss durability is claimed. Once the final link succeeds,
    ordinary staging removal failure is housekeeping, not a failed query.
    Fresh cancellation still propagates while that complete report remains.
    """
    stage=output/'.report-stage.json';owned=False;committed=False;primary=None
    try:
        stream=stage.open('xb');owned=True
        with owned_context(stream) as stream:
            if stream.write(payload) != len(payload):raise HostError('paths-report-write-refused')
        os.link(stage,output/'report.json')
        committed=True
    except BaseException as error:primary=error
    cleanup=None
    if owned:
        try:stage.unlink()
        except FileNotFoundError:pass
        except BaseException as error:cleanup=error
    if primary is not None:
        finish(primary, [lambda: (_ for _ in ()).throw(cleanup)] if cleanup is not None else [])
    if cleanup is not None and not (committed and isinstance(cleanup,OSError)):
        raise cleanup

def _source_port(opened, dataset_bytes, maximum):
    """Bind actual public whole-source validity to each held native projection.

    The whole dataset was independently reexecuted through the pinned public UMF
    producer. Every requested Field/Record/property and projected row is checked
    against original carriers before returning an owned receipt; metadata alone
    never supplies source validity or native custody.
    """
    model = _parse(bytes(opened.model), maximum)
    graph = _parse(bytes(opened.graph), maximum)
    dataset = _parse(dataset_bytes, maximum)
    if (dataset.get('umfRevision') != UMF_REVISION
            or dataset.get('sourceSha256') != hashlib.sha256(opened.model).hexdigest()
            or dataset.get('graphSha256') != hashlib.sha256(opened.graph).hexdigest()
            or dataset.get('receipt', {}).get('datasetValidation', {}) != {'valid':True,'complete':True}):
        # Public receipt retains additional diagnostics; do not synthesize them.
        validation=dataset.get('receipt',{}).get('datasetValidation',{})
        if (dataset.get('umfRevision') != UMF_REVISION
                or dataset.get('sourceSha256') != hashlib.sha256(opened.model).hexdigest()
                or dataset.get('graphSha256') != hashlib.sha256(opened.graph).hexdigest()
                or validation.get('valid') is not True or validation.get('complete') is not True):
            raise HostError('paths-public-source-refused')
    bindings = json.loads(_encoded(opened.bindings))
    fields = {(m['id'],e['id']):e for m in model['modules'] for e in m['elements']}
    properties = {tuple(p['identity']):p['property_id'] for p in bindings['properties']}
    entities = {(e['kind'],e['originalKey']):e for e in bindings['entities']}

    def public_source(request, artifact, projections):
        if request['modules'][0]['documentJson'].encode('utf8') != opened.model:
            raise HostError('paths-public-source-refused')
        for projection in projections:
            check=projection['check'];record=check['record'];field=check['field']
            if (record['documentId'] != model['id'] or field['documentId'] != model['id']
                    or record['revision'] != SOURCE_SHA or field['revision'] != SOURCE_SHA
                    or check.get('publicSourceOnly') is not True):
                raise HostError('paths-public-source-refused')
            definition=fields.get((record['module'],record['element']))
            scalar=fields.get((field['module'],field['element']))
            if (not definition or not scalar or definition['kind'] != 'record'
                    or scalar['kind'] != 'field'
                    or {'module':field['module'],'element':field['element']} not in definition['members']
                    or properties.get((model['id'],field['module'],field['element'])) != check['propertyId']):
                raise HostError('paths-public-source-refused')
            logical={'family':scalar['scalarType'],'facets':scalar.get('facets',{}),
                     'nullable':scalar['nullability'] != 'required'}
            if check['logicalType'] != logical:raise HostError('paths-public-source-refused')
            expected=[]
            for obj in graph['objects']:
                if obj['type'] != {'document':model['id'],'module':record['module'],'element':record['element']}:continue
                carrier=entities[('object',obj['key'])]
                props=[]
                for member in definition['members']:
                    authored=fields[(member['module'],member['element'])]
                    key=properties[(model['id'],member['module'],member['element'])]
                    token=obj['values'][member['element']]
                    props.append(json.dumps(key)+':'+(json.dumps(token,ensure_ascii=False,separators=(',',':')) if authored['scalarType']=='string' else token))
                expected.append({'source_system':'private-original-commerce-fixture',
                                 'type_id':carrier['type_id'],'id':carrier['id'],
                                 'schema_revision':SOURCE_SHA,'props_json':'{'+','.join(props)+'}',
                                 'extracted_token':obj['values'][field['element']]})
            if Counter(_encoded(r) for r in projection['rows']) != Counter(_encoded(r) for r in expected):
                raise HostError('paths-public-source-projection-refused')
        original={'sourceText':request['modules'][0]['documentJson'],
                  'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],
                  'checks':projections}
        request_text=_bounded_encoded(original,maximum)
        receipt_text=_bounded_encoded({'originalRequestText':request_text,'umfRevision':UMF_REVISION,
                              'admitted':True,'originalDatasetReceiptText':dataset_bytes.decode('utf8'),
                              'scope':'Actual pinned public whole dataset plus exact original Field and native projection correspondence; no authority grant'},maximum)
        if len(receipt_text.encode('utf8'))>maximum:raise HostError('paths-public-source-bound')
        return {'originalRequestText':request_text,'originalReceiptText':receipt_text,
                'receiptSha256':hashlib.sha256(receipt_text.encode('utf8')).hexdigest()}
    return public_source


def _query_commerce_paths(config) -> dict:
    """Run all ten declared intents through the fixed installed Paths profile.

    A refused required case withholds the whole report. This is not a capability
    qualification claim until actual native results and custody are reviewed.
    """
    from .config import QueryCommercePathsConfig
    if type(config) is not QueryCommercePathsConfig:raise HostError('invalid-configuration')
    jars=runtime_paths(config,False)
    original_model,original_graph=original_inputs(config.model,config.graph)
    report_bytes=read_bounded(config.publication/'report.json',4*1024*1024)
    paths=PathsDistributionPaths(config.index,config.installation)
    schemas=dict(installed_paths_schema_bundle(paths))
    selected={name:schemas[name] for name in ('compile-request-v0.4.schema.json',
              'compile-response-v0.4.schema.json','logical-plan-v0.4.schema.json')}
    schema_port=make_offline_path_schema_validation(selected)
    opening_jars={str(path):hashlib.sha256(read_bounded(path,32*1024*1024)).hexdigest() for path in jars}
    config.output.mkdir(parents=False,exist_ok=False)
    receipt_path=config.output/'fresh-public-dataset.json'
    producer=recompute_dataset(config.producer,config.model,config.graph,receipt_path)
    dataset_bytes=read_bounded(receipt_path,config.producer.maximum_receipt_bytes)
    if dataset_bytes != read_bounded(config.publication/'public-dataset.json',config.producer.maximum_receipt_bytes):
        raise HostError('public-dataset-correspondence-refused')
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.master('local[1]').appName('Installed Paths original commerce').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false').config('spark.jars',','.join(str(path) for path in jars)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(config.output/'warehouse')).getOrCreate()
    result=None;primary=None
    try:
        if spark.version != '4.0.1':raise HostError('qualified-runtime-required')
        with owned_context(open_commerce_reader(spark,config.publication)) as opened:
            if (opened.original_report != _parse(report_bytes,4*1024*1024)
                    or opened.model != original_model or opened.graph != original_graph):
                raise HostError('paths-opening-source-refused')
            with owned_context(opened.provider.interval(opened.context)):
                for table,alias in opened.aliases.items():
                    if len(alias.split('.')) != 3 or not all(part.replace('_','').isalnum() for part in alias.split('.')):
                        raise HostError('paths-alias-refused')
                    spark.sql('CREATE DATABASE IF NOT EXISTS '+alias.split('.')[-2]).collect()
                    native=opened.driver.transport.targets[table].path
                    spark.sql('CREATE TABLE '+alias+" USING DELTA LOCATION '"+str(native).replace("'","''")+"'").collect()
                if opened.native_files() != opened.original_native_files:raise HostError('paths-native-drift')
            alias_interval=opened.provider.closed_interval_custody(opened.context)
            execution=PathExecutionConfig(PathAdmissionConfig(config.maximum_artifact_bytes,schema_port),
                config.capture,config.decoder,_source_port(opened,dataset_bytes,config.maximum_artifact_bytes),UMF_REVISION)
            cases=[]
            for name,sql in commerce_path_cases():
                request=commerce_path_request(sql,opened.model,opened.bindings,opened.manifest,
                    opened.original_report['table_registry'],opened.aliases)
                raw_request=(_encoded(request)+'\n').encode('utf8')
                if len(raw_request)>config.maximum_artifact_bytes:raise HostError('paths-request-bound')
                raw_response=compile_paths_distribution(paths,raw_request)
                artifact=_parse(raw_response,config.maximum_artifact_bytes)
                raw_recompiled=compile_paths_distribution(paths,raw_request)
                recompiled=_parse(raw_recompiled,config.maximum_artifact_bytes)
                if opened.provider.active:raise HostError('paths-active-binding-refused')
                opened.provider.expected_binding=_encoded(json.loads(request['target']['bindingJson']))
                provisional=execute_commerce_path(opened,request,artifact,recompiled,
                    config=execution,original_oracle=original_commerce_path_oracle)
                cases.append({'case':name,'originalRequestText':raw_request.decode('utf8'),
                    'originalResponseText':raw_response.decode('utf8'),
                    'originalRecompiledText':raw_recompiled.decode('utf8'),'provisional':provisional})
            closing_native=dict(opened.native_files())
            if closing_native != opened.original_native_files:raise HostError('paths-native-drift')
            result={'format':'ashlar-installed-commerce-paths/0.1','cases':cases,'alias_interval':alias_interval,
                'original_publication_report_sha256':hashlib.sha256(report_bytes).hexdigest(),
                'original_manifest':opened.manifest,'opening_native_files':dict(opened.original_native_files),
                'closing_native_files':closing_native,'public_source':producer,
                'source_hashes':{'model':hashlib.sha256(original_model).hexdigest(),'graph':hashlib.sha256(original_graph).hexdigest()},
                'umfVersion':'0.8.0','schema_hashes':{name:hashlib.sha256(raw).hexdigest() for name,raw in schemas.items()},
                'scope':__doc__}
        if read_bounded(config.publication/'report.json',4*1024*1024)!=report_bytes:raise HostError('paths-report-drift')
    except BaseException as error:primary=error;result=None
    finish(primary,[spark.stop])
    runtime_paths(config,False,require_fresh=False)
    if original_inputs(config.model,config.graph)!=(original_model,original_graph):raise HostError('paths-source-drift')
    if {str(path):hashlib.sha256(read_bounded(path,32*1024*1024)).hexdigest() for path in jars}!=opening_jars:raise HostError('paths-jar-drift')
    if dict(installed_paths_schema_bundle(paths))!=schemas:raise HostError('paths-installation-drift')
    result['opening_closing_jars']=opening_jars;result['cleanup']={'readerClosed':True,'sparkStopped':True}
    payload=(_bounded_encoded(_json(result),96*1024*1024-1)+'\n').encode('utf8')
    _publish_report(config.output,payload)
    return result


def query_commerce_paths(config: QueryCommercePathsConfig) -> dict:
    """Return complete provisional-to-closed evidence or a payload-free refusal.

    Cancellation remains the original BaseException, including cleanup markers.
    Ordinary component/SDK/I/O refusal is presented through the public host type.
    """
    try:
        return _query_commerce_paths(config)
    except HostError:
        raise
    except Exception as error:
        refusal=HostError('paths-query-refused')
        if getattr(error,'cleanup_failed',False):refusal.cleanup_failed=True
        raise refusal from None
