"""Explicit local three-case path composition; no indexed release authority.

A separately reviewed private compiler, real original public source admission and
ordinary protected PG publication holds are required. The caller injects the
qualified Python/JDK/Spark environment; this tool installs or downloads nothing.
"""
import argparse
from dataclasses import dataclass, asdict, is_dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess

from commerce_path_request import commerce_path_request
from commerce_path_oracle import COLLECTION, COUNT, DISTINCT, original_commerce_path_oracle
from run_commerce_publication_weft import open_commerce_reader
from run_commerce_path_weft import PathExecutionConfig, execute_commerce_path
from run_indexed_commerce_weft import preflight
from weft_path_compiler import PathCompilerConfig, compile_path_request
from weft_path_plan import PathAdmissionConfig, SCHEMA_SHA256
from weft_path_schema import make_offline_path_schema_validation
from weft_path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig


class PathCompositionError(ValueError):
    """Safe composition refusal without native payloads or credentials."""


@dataclass(frozen=True)
class PathCompositionConfig:
    publication: Path
    output: Path
    jars: Path
    umf_source: Path
    dataset_producer: Path
    bun: Path
    schemas: tuple
    compiler: PathCompilerConfig
    capture: PathCaptureConfig
    decoder: PathDecodeConfig
    maximum_artifact_bytes: int
    maximum_input_bytes: int
    ack: tuple

    def __post_init__(self):
        paths = (self.publication, self.output, self.jars, self.umf_source,
                 self.dataset_producer, self.bun)
        if any(not isinstance(p, Path) or not p.is_absolute() for p in paths):
            raise PathCompositionError('Explicit absolute operator paths required')
        if any(p.is_symlink() for p in (self.output, *self.output.parents)):
            raise PathCompositionError('Original output parent custody required')
        if (type(self.schemas) is not tuple or len(self.schemas) != 3
                or any(not isinstance(p, Path) or not p.is_absolute() for p in self.schemas)
                or {p.name for p in self.schemas} != set(SCHEMA_SHA256)):
            raise PathCompositionError('Exact explicit public schema paths required')
        if (type(self.compiler) is not PathCompilerConfig or type(self.capture) is not PathCaptureConfig
                or type(self.decoder) is not PathDecodeConfig or type(self.ack) is not tuple):
            raise PathCompositionError('Explicit typed composition ports required')
        for bound in (self.maximum_artifact_bytes, self.maximum_input_bytes):
            if type(bound) is not int or not 1 <= bound <= 16 * 1024 * 1024:
                raise PathCompositionError('Explicit finite artifact and input limits required')


def _read(path, maximum):
    if any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file():
        raise PathCompositionError('Original regular-file custody required')
    if path.stat().st_size > maximum:
        raise PathCompositionError('Original file capacity exceeded')
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise PathCompositionError('Original file capacity exceeded')
    return raw


def _parse(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise PathCompositionError('Duplicate protocol member')
            result[key] = value
        return result
    def integer(token):
        if len(token.lstrip('-')) > 20: raise PathCompositionError('Protocol integer capacity exceeded')
        return int(token)
    def floating(token): raise PathCompositionError('Protocol numeric atom refused')
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_int=integer,
                          parse_float=floating, parse_constant=floating)
    except (ValueError, UnicodeError):
        raise PathCompositionError('Original protocol encoding refused') from None


def _encoded(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode('utf8') + b'\n'


def _json(value):
    if is_dataclass(value): return _json(asdict(value))
    if isinstance(value, dict): return {k: _json(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)): return [_json(v) for v in value]
    if type(value) is bytes: return {'hex': value.hex()}
    if isinstance(value, Decimal): return {'decimal': str(value)}
    return value


def create_path_spark(config: PathCompositionConfig, jars):
    """No fallback runtime; preflight and native command custody are required."""
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master('local[1]').appName('Original commerce paths')
            .config('spark.driver.memory', '512m').config('spark.ui.enabled', 'false')
            .config('spark.sql.shuffle.partitions', '1')
            .config('spark.databricks.delta.snapshotPartitions', '1')
            .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true')
            .config('spark.jars', ','.join(str(p) for p in jars))
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.sql.warehouse.dir', str(config.output/'warehouse')).getOrCreate())


def run_commerce_paths(config: PathCompositionConfig) -> dict:
    """Persist success only after original reader closure and mandatory Spark stop."""
    if type(config) is not PathCompositionConfig:
        raise PathCompositionError('Explicit composition configuration required')
    # Bound selected legacy-reader inputs before it can open any engine/resource.
    names = ('report.json', 'original-ontology.json', 'original-graph.json', 'development-bindings.json',
             'source.jsonl', 'public-dataset.json')
    originals = {name: _read(config.publication/name, config.maximum_input_bytes) for name in names}
    schemas = {p.name: _read(p, config.maximum_artifact_bytes) for p in config.schemas}
    schema_port = make_offline_path_schema_validation(schemas)
    for jar in config.jars.glob('*.jar'): _read(jar, 32 * 1024 * 1024)
    jars = preflight(config.umf_source, config.publication, config.jars, config.ack)
    opening_jars = {str(p): hashlib.sha256(_read(p, 32*1024*1024)).hexdigest() for p in jars}
    producer = _read(config.dataset_producer, config.maximum_input_bytes)
    # Exact original script source bytes; dependent source/resources are separately
    # bound by the required executable command review, not inferred from this hash.
    if hashlib.sha256(producer).hexdigest() != '69075c1c89fd7a352ec8c7037db0baecd85bb6a0ec844a8129df9e64abdbdacf':
        raise PathCompositionError('Original dataset producer differs')
    config.output.mkdir(parents=False, exist_ok=False)
    receipt = config.output/'fresh-public-dataset.json'
    # Producer payloads/credentials are never copied into diagnostics. The exact
    # output receipt and return status are retained; child streams are discarded.
    call = subprocess.run([str(config.bun), str(config.dataset_producer),
                           str(config.umf_source), str(receipt)],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
    if call.returncode or _read(receipt, config.maximum_input_bytes) != originals['public-dataset.json']:
        raise PathCompositionError('Fresh original public dataset admission differs')
    spark = create_path_spark(config, jars)
    result = None
    primary_failure = None
    body_failure = None
    try:
        if spark.version != '4.0.1': raise PathCompositionError('Actual qualified Spark runtime required')
        with open_commerce_reader(spark, config.publication) as opened:
            try:
                if opened.original_report != json.loads(originals['report.json']):
                    raise PathCompositionError('Original opened publication report differs')
                if bytes(opened.model) != originals['original-ontology.json'] or bytes(opened.graph) != originals['original-graph.json']:
                    raise PathCompositionError('Original opened model/graph differs')
                with opened.provider.interval(opened.context):
                    for table, alias in opened.aliases.items():
                        if len(alias.split('.')) != 3 or not all(part.replace('_', '').isalnum() for part in alias.split('.')):
                            raise PathCompositionError('Original catalog alias refused')
                        spark.sql('CREATE DATABASE IF NOT EXISTS ' + alias.split('.')[-2]).collect()
                        native = opened.driver.transport.targets[table].path
                        spark.sql('CREATE TABLE ' + alias + " USING DELTA LOCATION '" + str(native).replace("'", "''") + "'").collect()
                    if opened.native_files() != opened.original_native_files:
                        raise PathCompositionError('Held alias registration changed native originals')
                alias_interval = opened.provider.closed_interval_custody(opened.context)
                results = []
                execution = PathExecutionConfig(PathAdmissionConfig(config.maximum_artifact_bytes, schema_port),
                                                config.capture, config.decoder, None, None)
                for name, sql in (('collection', COLLECTION), ('path-count', COUNT), ('target-count', DISTINCT)):
                    request = commerce_path_request(sql, opened.model, opened.bindings, opened.manifest,
                                                    opened.original_report['table_registry'], opened.aliases)
                    raw_request = _encoded(request)
                    raw_response = compile_path_request(raw_request, config=config.compiler)
                    artifact = _parse(raw_response)
                    raw_recompiled = compile_path_request(raw_request, config=config.compiler)
                    # Updating the inactive provider's exact source-derived mapping is
                    # explicit composition, not a bypass of admit_binding or source policy.
                    if opened.provider.active: raise PathCompositionError('Inactive binding composition required')
                    opened.provider.expected_binding = json.dumps(json.loads(request['target']['bindingJson']),
                        ensure_ascii=False, sort_keys=True, separators=(',', ':'))
                    provisional = execute_commerce_path(opened, request, artifact, _parse(raw_recompiled),
                        config=execution, original_oracle=original_commerce_path_oracle)
                    results.append({'case': name, 'original_request': raw_request,
                                    'original_response': raw_response, 'original_recompiled': raw_recompiled,
                                    'provisional': provisional})
                closing_native = dict(opened.native_files())
                if closing_native != opened.original_native_files:
                    raise PathCompositionError('Original complete closing native files differ')
                for name, raw in originals.items():
                    if _read(config.publication/name, config.maximum_input_bytes) != raw:
                        raise PathCompositionError('Original closing source/report differs')
                result = {'scope': 'Three original local commerce path cases; private compiler and ordinary protected PG only.',
                          'cases': results, 'alias_interval': alias_interval,
                          'original_publication_report_sha256': hashlib.sha256(originals['report.json']).hexdigest(),
                          'original_manifest': opened.manifest,
                          'opening_native_files': dict(opened.original_native_files), 'closing_native_files': closing_native,
                          'source_hashes': {name: hashlib.sha256(raw).hexdigest() for name, raw in originals.items()},
                          'public_source_status': {'returncode': call.returncode, 'streams_retained': False},
                          'schema_hashes': {name: hashlib.sha256(raw).hexdigest() for name, raw in schemas.items()},
                          'runtime': {'spark': '4.0.1', 'delta': '4.0.0',
                                      'jars': [{'path': str(p), 'sha256': hashlib.sha256(_read(p, 32*1024*1024)).hexdigest()} for p in jars]}}
            except BaseException as error:
                # Capture before context-manager exit can replace cancellation.
                body_failure = error
                raise
        # Reader exit may refuse; no success report exists at this point.
    except BaseException as error:
        primary_failure = body_failure if body_failure is not None else error
        result = None
        if body_failure is not None and error is not body_failure:
            primary_failure.reader_cleanup_failed = True
            primary_failure.cleanup_failed = True
            try:
                with (config.output/'reader-cleanup-failure.json').open('xb') as stream:
                    stream.write(b'{"scope":"local-path-composition","reader_close_failed":true,"success_report_withheld":true}\n')
            except Exception:
                pass
            raise primary_failure from None
        raise
    finally:
        try:
            spark.stop()
        except BaseException:
            if primary_failure is None:
                raise
            # Preserve cancellation/body failure; cleanup failure is separate.
            primary_failure.cleanup_failed = True
            try:
                with (config.output/'cleanup-failure.json').open('xb') as stream:
                    stream.write(b'{"scope":"local-path-composition","spark_stop_failed":true,"success_report_withheld":true}\n')
            except Exception:
                pass  # A denied diagnostic write must not mask the primary failure.
    if result is None: raise PathCompositionError('Complete cleanup and results required')
    for name, raw in originals.items():
        if _read(config.publication/name, config.maximum_input_bytes) != raw:
            raise PathCompositionError('Original post-cleanup source/report differs')
    if any(_read(path, config.maximum_artifact_bytes) != schemas[path.name] for path in config.schemas):
        raise PathCompositionError('Original closing schema bytes differ')
    if _read(config.dataset_producer, config.maximum_input_bytes) != producer:
        raise PathCompositionError('Original closing producer bytes differ')
    if {str(p): hashlib.sha256(_read(p, 32*1024*1024)).hexdigest() for p in jars} != opening_jars:
        raise PathCompositionError('Original closing JAR bytes differ')
    result['opening_jar_hashes'] = opening_jars
    result['closing_jar_hashes'] = dict(opening_jars)
    result['cleanup'] = {'reader_closed': True, 'spark_stopped': True}
    # Fresh output belongs exclusively to this invocation. Report publication is
    # after cleanup; no crash durability or atomic multi-file publication claimed.
    with (config.output/'report.json').open('xb') as stream:
        stream.write(_encoded(_json(result)))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('publication', 'output', 'jars', 'umf-source', 'dataset-producer', 'bun', 'compiler'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--schema', type=Path, action='append', required=True)
    for name in ('maximum-artifact-bytes', 'maximum-input-bytes', 'maximum-rows',
                 'maximum-cell-bytes', 'maximum-total-cell-bytes', 'maximum-request-bytes',
                 'maximum-response-bytes', 'compiler-timeout-seconds', 'ack-port'):
        parser.add_argument('--'+name, type=int, required=True)
    for name in ('ack-container', 'ack-host', 'ack-database'):
        parser.add_argument('--'+name, required=True)
    args = parser.parse_args()
    config = PathCompositionConfig(args.publication,args.output,args.jars,args.umf_source,args.dataset_producer,
        args.bun,tuple(args.schema),PathCompilerConfig(args.compiler,args.maximum_request_bytes,
        args.maximum_response_bytes,args.compiler_timeout_seconds),PathCaptureConfig(args.maximum_rows,
        args.maximum_cell_bytes,args.maximum_total_cell_bytes),PathDecodeConfig(args.maximum_cell_bytes),
        args.maximum_artifact_bytes,args.maximum_input_bytes,
        (args.ack_container,args.ack_host,args.ack_port,args.ack_database))
    try: run_commerce_paths(config)
    except Exception:
        raise SystemExit('Local path composition refused; no success report published') from None


if __name__ == '__main__': main()
