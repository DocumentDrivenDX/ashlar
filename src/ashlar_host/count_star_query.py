"""Explicit installed CountStar commerce lifecycle; no fallback or source grant.

Results are provisional until held readers close, Spark stops, and closing source,
JAR, installation and publication custody pass. SDK imports are lazy. This
composition does not change the historical query-commerce profile.
"""
from dataclasses import asdict, is_dataclass
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path

from ashlar.weft_count_star_distribution import (
    CountStarDistributionPaths,compile_count_star_distribution,installed_count_star_schema_bundle,
)
from .commerce import runtime_paths
from .commerce_path_oracle import commerce_path_cases, original_commerce_path_oracle
from .count_star_oracle import commerce_count_star_cases,original_commerce_count_star_oracle
from .commerce_path_request import commerce_path_request as original_commerce_path_request
from .config import HostError
from .count_star_configuration import QueryCommerceCountStarConfig
from .lifecycle import finish, owned_context
from .count_star_admission import CountStarAdmissionConfig,BACKEND
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .count_star_schema import make_offline_count_star_schema_validation
from .publication_reader import open_commerce_reader, ReaderCleanupState
from .source import original_inputs, read_bounded, recompute_dataset

from .commerce_source_receipt import (UMF_REVISION, commerce_public_source_port,
    encode_commerce_json as _encoded, encode_bounded_commerce_json as _bounded_encoded,
    parse_commerce_json as _parse)


def commerce_count_star_request(sql,model,bindings,manifest,registry,aliases):
    """Preserve all original SQL/model/bindings; select only explicit 041 markers."""
    request=original_commerce_path_request(sql,model,bindings,manifest,registry,aliases,profile='paths-keys')
    request.update(interfaceVersion='weft-compile/0.4.1',dialect='weft-sql/0.4.1')
    request['target'].update({key:BACKEND[key]for key in ('backendId','backendVersion','targetProfile')})
    return request



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
    """Compatibility port; complete correspondence belongs to its public owner."""
    return commerce_public_source_port(model=opened.model, graph=opened.graph,
        bindings=opened.bindings, dataset_receipt=dataset_bytes, maximum_bytes=maximum)


class _Observation:
    """Fixed host observations; ordinary sink errors confer no work authority."""
    def __init__(self, run):
        self.run, self.attempt, self.category = run, None, 'configuration'
        self.owner_phase = 'configuration'
        self.native_cleanup_failed = False
        self.reader_cleanup = ReaderCleanupState() if run is not None else None

    def begin(self):
        if self.run is not None:
            try:
                self.attempt = self.run.begin_attempt('held-read')
            except Exception:
                pass

    def phase(self, phase, state):
        self.owner_phase = phase
        self.category = {'admission': 'source', 'prepare': 'configuration',
                         'commit': 'io'}.get(phase, phase)
        if self.attempt is not None:
            try:
                self.run.phase(self.attempt, phase, state)
            except Exception:
                pass

    def stop(self, spark, primary):
        try:
            spark.stop()
        except BaseException:
            self.native_cleanup_failed = True
            if primary is None:
                self.category = self.owner_phase = 'cleanup'
            raise

    def finish(self, primary=None):
        if self.attempt is None:
            return primary
        if self.reader_cleanup is not None and self.reader_cleanup.failed:
            self.native_cleanup_failed = True
            if self.reader_cleanup.cleanup_only:
                self.category = self.owner_phase = 'cleanup'
        outcome = ('succeeded' if primary is None else
                   'cancelled' if not isinstance(primary, Exception) else
                   'refused' if isinstance(primary, HostError)
                       and self.owner_phase in ('configuration', 'admission', 'guard')
                       and not self.native_cleanup_failed
                       and not getattr(primary, 'cleanup_failed', False) else 'failed')
        try:
            self.run.finish_attempt(self.attempt, outcome,
                error_category=None if primary is None else self.category,
                cleanup_failed=self.native_cleanup_failed
                    or bool(getattr(primary, 'cleanup_failed', False)))
        except Exception:
            pass
        except BaseException as cancellation:
            if primary is None or isinstance(primary, Exception):
                return cancellation
        return primary


def _query_commerce_count_star(config, observation) -> dict:
    """Run all ten original intents and four explicit COUNT star intents through the explicitly selected profile.

    A refused required case withholds the whole report. This is not a capability
    qualification claim until actual native results and custody are reviewed.
    """
    observation.phase('configuration', 'started')
    from .count_star_configuration import QueryCommerceCountStarConfig
    if type(config) is not QueryCommerceCountStarConfig:raise HostError('invalid-configuration')
    profile='count-star'
    paths=CountStarDistributionPaths(config.index,config.installation)
    schema_bundle=installed_count_star_schema_bundle
    compile_distribution=compile_count_star_distribution
    observation.phase('configuration', 'completed')
    observation.phase('admission', 'started')
    jars=runtime_paths(config,False)
    original_model,original_graph=original_inputs(config.model,config.graph)
    report_bytes=read_bounded(config.publication/'report.json',4*1024*1024)
    schemas=dict(schema_bundle(paths))
    selected={name:schemas[name] for name in ('compile-request-v0.4.1.schema.json',
              'compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')}
    schema_port=make_offline_count_star_schema_validation(selected)
    opening_jars={str(path):hashlib.sha256(read_bounded(path,32*1024*1024)).hexdigest() for path in jars}
    observation.phase('admission', 'completed')
    observation.phase('prepare', 'started')
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
        observation.phase('prepare', 'completed')
        observation.phase('guard', 'started')
        if spark.version != '4.0.1':raise HostError('qualified-runtime-required')
        reader = (open_commerce_reader(spark, config.publication)
                  if observation.reader_cleanup is None else
                  open_commerce_reader(spark, config.publication,
                                       cleanup_state=observation.reader_cleanup))
        with owned_context(reader) as opened:
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
            execution=CountStarExecutionConfig(CountStarAdmissionConfig(config.maximum_artifact_bytes,schema_port,profile=profile),
                config.capture,config.decoder,_source_port(opened,dataset_bytes,config.maximum_artifact_bytes),UMF_REVISION)
            observation.phase('guard', 'completed')
            observation.phase('capture', 'started')
            cases=[]
            for name,sql in commerce_path_cases()+commerce_count_star_cases():
                request=commerce_count_star_request(sql,opened.model,opened.bindings,opened.manifest,
                    opened.original_report['table_registry'],opened.aliases)
                raw_request=(_encoded(request)+'\n').encode('utf8')
                if len(raw_request)>config.maximum_artifact_bytes:raise HostError('paths-request-bound')
                raw_response=compile_distribution(paths,raw_request)
                artifact=_parse(raw_response,config.maximum_artifact_bytes)
                raw_recompiled=compile_distribution(paths,raw_request)
                recompiled=_parse(raw_recompiled,config.maximum_artifact_bytes)
                if opened.provider.active:raise HostError('paths-active-binding-refused')
                opened.provider.expected_binding=_encoded(json.loads(request['target']['bindingJson']))
                provisional=execute_commerce_count_star(opened,request,artifact,recompiled,
                    config=execution,original_oracle=(original_commerce_count_star_oracle if name in dict(commerce_count_star_cases()) else original_commerce_path_oracle))
                cases.append({'case':name,'originalRequestText':raw_request.decode('utf8'),
                    'originalResponseText':raw_response.decode('utf8'),
                    'originalRecompiledText':raw_recompiled.decode('utf8'),'provisional':provisional})
            observation.phase('capture', 'completed')
            observation.phase('closing', 'started')
            closing_native=dict(opened.native_files())
            if closing_native != opened.original_native_files:raise HostError('paths-native-drift')
            result={'format':'ashlar-installed-commerce-count-star/0.1','cases':cases,'alias_interval':alias_interval,
                'original_publication_report_sha256':hashlib.sha256(report_bytes).hexdigest(),
                'original_manifest':opened.manifest,'opening_native_files':dict(opened.original_native_files),
                'closing_native_files':closing_native,'public_source':producer,
                'source_hashes':{'model':hashlib.sha256(original_model).hexdigest(),'graph':hashlib.sha256(original_graph).hexdigest()},
                'umfVersion':'0.8.0','schema_hashes':{name:hashlib.sha256(raw).hexdigest() for name,raw in schemas.items()},
                'scope':__doc__}
        if read_bounded(config.publication/'report.json',4*1024*1024)!=report_bytes:raise HostError('paths-report-drift')
    except BaseException as error:primary=error;result=None
    failure_category, failure_phase = observation.category, observation.owner_phase
    try:
        observation.phase('cleanup', 'started')
    except BaseException as cancellation:
        if primary is None or isinstance(primary, Exception):
            primary = cancellation
            failure_category, failure_phase = observation.category, observation.owner_phase
    if primary is not None:
        observation.category, observation.owner_phase = failure_category, failure_phase
    finish(primary,[lambda: observation.stop(spark, primary)])
    observation.phase('cleanup', 'completed')
    observation.phase('closing', 'started')
    runtime_paths(config,False,require_fresh=False)
    if original_inputs(config.model,config.graph)!=(original_model,original_graph):raise HostError('paths-source-drift')
    if {str(path):hashlib.sha256(read_bounded(path,32*1024*1024)).hexdigest() for path in jars}!=opening_jars:raise HostError('paths-jar-drift')
    if dict(schema_bundle(paths))!=schemas:raise HostError('paths-installation-drift')
    observation.phase('closing', 'completed')
    observation.phase('commit', 'started')
    result['opening_closing_jars']=opening_jars;result['cleanup']={'readerClosed':True,'sparkStopped':True}
    payload=(_bounded_encoded(_json(result),96*1024*1024-1)+'\n').encode('utf8')
    _publish_report(config.output,payload)
    observation.phase('commit', 'completed')
    return result


def query_commerce_count_star(config: QueryCommerceCountStarConfig, *, diagnostics=None) -> dict:
    """Return complete provisional-to-closed evidence or a payload-free refusal.

    Cancellation remains the original BaseException, including cleanup markers.
    Ordinary component/SDK/I/O refusal is presented through the public host type.
    """
    observation = _Observation(diagnostics)
    try:
        observation.begin()
        result = _query_commerce_count_star(config, observation)
    except BaseException as primary:
        winning = observation.finish(primary)
        if winning is primary:
            try:
                raise
            except HostError:
                raise
            except Exception as error:
                refusal = HostError('paths-query-refused')
                if getattr(error, 'cleanup_failed', False):
                    refusal.cleanup_failed = True
                raise refusal from None
        raise winning
    winning = observation.finish()
    if winning is not None:
        raise winning
    return result
