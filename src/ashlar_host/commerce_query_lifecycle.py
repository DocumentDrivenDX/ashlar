"""Closed installed041/042 commerce lifecycle; no fallback or source grant.

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
from .commerce_query_selection import select_commerce_query_cases
from .count_star_oracle import commerce_count_star_cases,original_commerce_count_star_oracle
from .commerce_path_request import commerce_path_request as original_commerce_path_request
from .config import HostError
from .count_star_configuration import QueryCommerceCountStarConfig
from .lifecycle import finish, owned_context
from .count_star_admission import CountStarAdmissionConfig,BACKEND
from .count_star_request import commerce_count_star_request
from .implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
from .implicit_arithmetic_profile import ImplicitArithmeticAdmissionConfig, ImplicitArithmeticExecutionConfig
from .commerce_implicit_arithmetic import execute_original_commerce_implicit_arithmetic_cases
from .commerce_original_scenarios import commerce_original_cases
from .count_star_schema import make_offline_implicit_arithmetic_schema_validation
from ashlar.weft_implicit_arithmetic_distribution import (ImplicitArithmeticDistributionPaths,
    compile_implicit_arithmetic_distribution, installed_implicit_arithmetic_schema_bundle)
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .count_star_schema import make_offline_count_star_schema_validation
from .publication_reader import open_commerce_reader, ReaderCleanupState
from .source import original_inputs, read_bounded, recompute_dataset

from .commerce_source_receipt import (UMF_REVISION, commerce_public_source_port,
    encode_commerce_json as _encoded, encode_bounded_commerce_json as _bounded_encoded,
    parse_commerce_json as _parse)



COUNT_STAR_SCOPE='Explicit installed CountStar commerce lifecycle; no fallback or source grant.\n\nResults are provisional until held readers close, Spark stops, and closing source,\nJAR, installation and publication custody pass. SDK imports are lazy. This\ncomposition does not change the historical query-commerce profile.\n'
IMPLICIT_ARITHMETIC_SCOPE='Fixed indexed042 original four commerce cases; current ordinary source ACK and full publication holds; provisional until complete shared reader/runtime/source/compiler closing checks. No production or remote authority claim.'

def commerce_query_report_value(value):
    if is_dataclass(value): return commerce_query_report_value(asdict(value))
    if isinstance(value,dict):return {key:commerce_query_report_value(item) for key,item in value.items()}
    if isinstance(value,(tuple,list)):return [commerce_query_report_value(item) for item in value]
    if type(value) is bytes:return {'hex':value.hex()}
    if isinstance(value,Decimal):return {'decimal':str(value)}
    return value



def publish_commerce_query_report(output: Path, payload: bytes) -> None:
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

def commerce_query_source_port(opened, dataset_bytes, maximum):
    """Compatibility port; complete correspondence belongs to its public owner."""
    return commerce_public_source_port(model=opened.model, graph=opened.graph,
        bindings=opened.bindings, dataset_receipt=dataset_bytes, maximum_bytes=maximum)


class CommerceQueryObservation:
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


def _query_commerce(config, observation, *, ordinary_session=None, source_factory=None, pack_bytes=None, invocation_custody=None, selected_case_ids=()) -> dict:
    """Run the complete fixed041 or original042 scenario inventory.

    A refused required case withholds the whole report. This is not a capability
    qualification claim until actual native results and custody are reviewed.
    """
    observation.phase('configuration', 'started')
    if type(config) is QueryCommerceCountStarConfig:
        profile='count-star'
        paths=CountStarDistributionPaths(config.index,config.installation)
        schema_bundle=installed_count_star_schema_bundle
        compile_distribution=compile_count_star_distribution
        schema_version='0.4.1'
        make_schema=make_offline_count_star_schema_validation
    elif type(config) is QueryCommerceImplicitArithmeticConfig:
        from .ordinary_query_connection import OrdinaryQueryConnectionFactory
        if type(ordinary_session) is not OrdinaryQueryConnectionFactory:
            raise HostError('ordinary-query-session-required')
        ordinary_session.__post_init__()
        from .commerce_query_sources import CommerceQuerySourceFactory
        if type(source_factory) is not CommerceQuerySourceFactory:raise HostError('query-source-factory-required')
        source_factory.__post_init__()
        if invocation_custody is not None:
            from .query_invocation_custody import CommerceQueryInvocationCustody
            if type(invocation_custody) is not CommerceQueryInvocationCustody:raise HostError('invalid-query-invocation-custody')
            invocation_custody.__post_init__()
            invocation_custody.renew()
        commerce_original_cases(pack_bytes)
        profile='implicit-arithmetic'
        paths=ImplicitArithmeticDistributionPaths(config.index,config.installation)
        schema_bundle=installed_implicit_arithmetic_schema_bundle
        compile_distribution=compile_implicit_arithmetic_distribution
        schema_version='0.4.2'
        make_schema=make_offline_implicit_arithmetic_schema_validation
    else:raise HostError('invalid-configuration')
    selected_cases=select_commerce_query_cases(profile,selected_case_ids)
    original_cases=select_commerce_query_cases(profile)
    if profile=='count-star':
        from .implicit_arithmetic_configuration import ExplicitCommercePostgresEndpoint
        if type(config.postgres)is ExplicitCommercePostgresEndpoint and (ordinary_session is None or source_factory is None):
            raise HostError('complete-ordinary-query-ports-required')
    if profile=='count-star' and any(port is not None for port in (ordinary_session,source_factory,invocation_custody)):
        from .ordinary_query_connection import OrdinaryQueryConnectionFactory
        from .commerce_query_sources import CommerceQuerySourceFactory
        if type(ordinary_session)is not OrdinaryQueryConnectionFactory or type(source_factory)is not CommerceQuerySourceFactory:
            raise HostError('complete-ordinary-query-ports-required')
        ordinary_session.__post_init__();source_factory.__post_init__()
        if invocation_custody is not None:
            from .query_invocation_custody import CommerceQueryInvocationCustody
            if type(invocation_custody)is not CommerceQueryInvocationCustody:raise HostError('invalid-query-invocation-custody')
            invocation_custody.__post_init__();invocation_custody.renew()
    config.__post_init__()
    observation.phase('configuration', 'completed')
    observation.phase('admission', 'started')
    jars=runtime_paths(config,False)
    original_model,original_graph=original_inputs(config.model,config.graph)
    report_bytes=read_bounded(config.publication/'report.json',4*1024*1024)
    schemas=dict(schema_bundle(paths))
    selected={name:schemas[name] for name in (f'compile-request-v{schema_version}.schema.json',
              f'compile-response-v{schema_version}.schema.json',f'logical-plan-v{schema_version}.schema.json')}
    schema_port=make_schema(selected)
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
        reader_options={}
        if observation.reader_cleanup is not None:reader_options['cleanup_state']=observation.reader_cleanup
        if ordinary_session is not None:reader_options.update(ordinary_session=ordinary_session,source_factory=source_factory)
        reader=open_commerce_reader(spark,config.publication,**reader_options)
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
            if profile=='count-star':
                execution=CountStarExecutionConfig(CountStarAdmissionConfig(config.maximum_artifact_bytes,schema_port,profile=profile),
                    config.capture,config.decoder,commerce_query_source_port(opened,dataset_bytes,config.maximum_artifact_bytes),UMF_REVISION)
                observation.phase('guard', 'completed')
                observation.phase('capture', 'started')
            else:
                execution=ImplicitArithmeticExecutionConfig(ImplicitArithmeticAdmissionConfig(config.maximum_artifact_bytes,schema_port),
                    config.capture,config.decoder,commerce_query_source_port(opened,dataset_bytes,config.maximum_artifact_bytes),UMF_REVISION)
                observation.phase('guard', 'completed')
                observation.phase('capture', 'started')
            if profile=='implicit-arithmetic':
                cases=list(execute_original_commerce_implicit_arithmetic_cases(opened,paths,config=execution,pack_bytes=pack_bytes,selected_case_ids=selected_case_ids))
            else:
                cases=[]
                for name,sql in selected_cases:
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
            result={'format':('ashlar-installed-commerce-count-star/0.1' if profile=='count-star' else 'ashlar-installed-commerce-implicit-arithmetic/0.1'),'cases':cases,'alias_interval':alias_interval,
                'original_publication_report_sha256':hashlib.sha256(report_bytes).hexdigest(),
                'original_manifest':opened.manifest,'opening_native_files':dict(opened.original_native_files),
                'closing_native_files':closing_native,'public_source':producer,
                'source_hashes':{'model':hashlib.sha256(original_model).hexdigest(),'graph':hashlib.sha256(original_graph).hexdigest()},
                'umfVersion':'0.8.0','schema_hashes':{name:hashlib.sha256(raw).hexdigest() for name,raw in schemas.items()},
                'original_case_ids':[name for name,sql in original_cases],'selected_case_ids':[name for name,sql in selected_cases],
                'scope':(COUNT_STAR_SCOPE if profile=='count-star' else IMPLICIT_ARITHMETIC_SCOPE)+(' Selected authored cases only; full corpus execution is not claimed.'if selected_case_ids else'')}
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
    finish(primary,[lambda: observation.stop(spark, primary)] + ([invocation_custody.renew] if invocation_custody is not None else []))
    observation.phase('cleanup', 'completed')
    observation.phase('closing', 'started')
    runtime_paths(config,False,require_fresh=False)
    if original_inputs(config.model,config.graph)!=(original_model,original_graph):raise HostError('paths-source-drift')
    if {str(path):hashlib.sha256(read_bounded(path,32*1024*1024)).hexdigest() for path in jars}!=opening_jars:raise HostError('paths-jar-drift')
    if dict(schema_bundle(paths))!=schemas:raise HostError('paths-installation-drift')
    observation.phase('closing', 'completed')
    observation.phase('commit', 'started')
    result['opening_closing_jars']=opening_jars;result['cleanup']={'readerClosed':True,'sparkStopped':True}
    payload=(_bounded_encoded(commerce_query_report_value(result),96*1024*1024-1)+'\n').encode('utf8')
    publish_commerce_query_report(config.output,payload)
    observation.phase('commit', 'completed')
    return result


def _run_commerce_query(config, configuration_type, *, diagnostics=None, ordinary_session=None, source_factory=None, pack_bytes=None, invocation_custody=None, selected_case_ids=()) -> dict:
    """Return complete provisional-to-closed evidence or a payload-free refusal.

    Cancellation remains the original BaseException, including cleanup markers.
    Ordinary component/SDK/I/O refusal is presented through the public host type.
    """
    observation = CommerceQueryObservation(diagnostics)
    try:
        observation.begin()
        if type(config) is not configuration_type:raise HostError('invalid-configuration')
        result = _query_commerce(config, observation, ordinary_session=ordinary_session, source_factory=source_factory, pack_bytes=pack_bytes, invocation_custody=invocation_custody,selected_case_ids=selected_case_ids)
    except BaseException as primary:
        if invocation_custody is not None:
            from .query_invocation_custody import CommerceQueryInvocationCustody
            if type(invocation_custody) is CommerceQueryInvocationCustody:
                try:finish(primary,[invocation_custody.renew])
                except BaseException as renewed:primary=renewed
        winning = observation.finish(primary)
        if winning is primary:
            try:
                raise primary
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


def run_count_star_query(config, *, diagnostics=None,selected_case_ids=(),ordinary_session=None,source_factory=None,invocation_custody=None):
    """Closed legacy041 entry; no other configuration is selected implicitly."""
    return _run_commerce_query(config,QueryCommerceCountStarConfig,diagnostics=diagnostics,selected_case_ids=selected_case_ids,
        ordinary_session=ordinary_session,source_factory=source_factory,invocation_custody=invocation_custody)


def run_implicit_arithmetic_query(config, *, ordinary_session, source_factory, pack_bytes, diagnostics=None, invocation_custody=None,selected_case_ids=()):
    """Closed042 entry; ordinary sessions and original scenario pack are explicit."""
    return _run_commerce_query(config,QueryCommerceImplicitArithmeticConfig,
        ordinary_session=ordinary_session,source_factory=source_factory,pack_bytes=pack_bytes,diagnostics=diagnostics,invocation_custody=invocation_custody,selected_case_ids=selected_case_ids)
