"""Selected original commerce native publication composition; no CLI discovery."""
from .lifecycle import owned_connection
import hashlib, importlib.metadata, json
from pathlib import Path
from ashlar.apply import empty_state
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.outbox import PostgresOutbox, publish_outbox_transaction
from ashlar.stored_publisher import StoredPublisherBackend
from .source_identity import build_transaction, SOURCE_SHA, GRAPH_SHA
from .source import recompute_dataset
from .commerce_admission import CommerceAdmission, original_commerce_oracle
from .schema_rows import fixture_columns
from .delta_custody import DeltaTarget, LocalDeltaTransport, encoded
from .connection import connect
from .postgres import Session
from .runtime import JARS, VERSIONS
from .driver import ROOT, CLOCK, FIELDS, PrivatePolicy, NativeDriver, AttemptExecutor, CarrierPolicy, ManifestPort, local_effect_plan, progress_union, request_for, provision_sources
from .graph_sql import graph_sql_plan
UMF_PIN = 'c7c95e1c4ea5b72541f47fa0350ca467ff02f395'
SOURCE_SYSTEM = 'private-original-commerce-fixture'

def publish(config):
    output = config.output
    jars = config.jars
    umf_source = config.producer.source
    output = Path(output)
    if output.exists():
        raise ValueError('Exclusive fresh local output required; retain every original attempted installation')
    if {k: importlib.metadata.version(k) for k in VERSIONS} != VERSIONS:
        raise ValueError('Exact qualified existing Spark runtime required')
    paths = [Path(jars) / name for name in JARS]
    if not all((p.is_file() for p in paths)):
        raise ValueError('Explicit existing jars required')
    output.mkdir(parents=True, mode=448)
    model_bytes = config.model.read_bytes()
    graph_bytes = config.graph.read_bytes()
    (batch, bindings) = build_transaction(model_bytes, graph_bytes, source_system=SOURCE_SYSTEM)
    model_path = output / 'original-ontology.json'
    graph_path = output / 'original-graph.json'
    binding_path = output / 'development-bindings.json'
    receipt_path = output / 'public-dataset.json'
    model_path.write_bytes(model_bytes)
    graph_path.write_bytes(graph_bytes)
    binding_path.write_text(encoded(bindings) + '\n')
    raw = batch.begin + b''.join((r.raw for r in batch.records)) + batch.commit
    (output / 'source.jsonl').write_bytes(raw)
    public_call = recompute_dataset(config.producer, config.model, config.graph, receipt_path)
    (output / 'public-dataset-call.json').write_text(encoded(public_call) + '\n')
    admission = CommerceAdmission(batch, bindings, model_path, graph_path, binding_path, receipt_path)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master('local[1]').appName('Ashlar original commerce local publication').config('spark.driver.memory', '512m').config('spark.sql.shuffle.partitions', '1').config('spark.databricks.delta.snapshotPartitions', '1').config('spark.ui.enabled', 'false').config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true').config('spark.jars', ','.join((str(p) for p in paths))).config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate()
    transport = None
    report = None
    primary = None
    try:
        graph_columns = fixture_columns(ROOT)
        columns = {**graph_columns, 'attempts': tuple(((name, 'STRING') for name in ('stream', 'batch_id', 'phase', 'request_digest', 'payload_json', 'payload_digest'))), 'manifest': tuple(((name, 'TIMESTAMP' if name == 'recorded_at' else 'STRING') for name in FIELDS))}
        tables = {role: 'local.commerce.' + role for role in columns}
        targets = []
        for (role, schema) in columns.items():
            path = output / role
            spark.sql('CREATE TABLE delta.`' + str(path) + '` (' + ','.join((name + ' ' + family for (name, family) in schema)) + ') USING DELTA')
            targets.append(DeltaTarget(tables[role], path, spark.sql('DESCRIBE DETAIL delta.`' + str(path) + '`').first().id))
        context = object()
        policy = PrivatePolicy(context, tuple(targets))
        transport = LocalDeltaTransport.initialize(spark, output / 'operations.sqlite', 'private-original-commerce', tuple(targets), policy, context=context)
        policy.initializing = False
        ports = provision_sources(output, [{'label': 'commerce', 'feed': batch.feed, 'epoch': batch.epoch}])
        port = ports[batch.feed]
        connection = connect(port['writer'])
        with owned_connection(connection):
            position = connection.execute('SELECT "' + port['source'] + '".append(%s,%s)', (batch.batch_id, raw.decode())).fetchone()[0]
            if position != 1:
                raise ValueError('Fresh original source must start at zero')
            connection.commit()
        connection = connect(port['reader'])
        with owned_connection(connection):
            (transaction,) = PostgresOutbox(Session(connection), feed=batch.feed, epoch=batch.epoch, schema=port['source']).read('0', limit=1)
            connection.rollback()
        if transaction.batch != batch:
            raise ValueError('Actual original source bytes differ')
        driver = NativeDriver(transport, policy, context, tables, ports, admission.changes, graph_columns, source_admission=admission)
        graph_tables = {role: tables[role] for role in graph_columns}
        (state, generated) = graph_sql_plan(empty_state(), batch, graph_tables, materialized_at=CLOCK, schema_policy=driver.schema_admit)
        empty = {role: [] for role in graph_columns}
        (steps, elisions) = local_effect_plan(generated, empty, graph_tables)
        expected = original_commerce_oracle(model_bytes, graph_bytes, bindings, batch, graph_columns)
        request = request_for('private-original-commerce', transaction, 'explicit-original-commerce-origin', {SOURCE_SYSTEM: SOURCE_SHA})
        checkpoint = json.loads(request['source_checkpoint_json'])
        progress = progress_union({}, checkpoint)
        policy.active = {'request': request, 'steps': steps, 'expected': expected, 'publication_id': 'original-commerce-publication-1', 'previous_progress': {}, 'progress': progress, 'previous_expected': empty, 'generated_steps': generated, 'elisions': elisions}
        target = transport.targets[tables['attempts']]
        backend = StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(driver), CarrierPolicy(driver, 'attempts'), target.table, target.uuid), driver, lambda original, supplied: ManifestPort(driver, original))
        descriptor = publish_outbox_transaction(backend, 'private-original-commerce', transaction, predecessor=request['predecessor'], schema_revisions_json=request['schema_revisions_json'], context=context)
        history_before = {table: transport.original_history(transport.targets[table]) for table in descriptor.versions}
        repeated = publish_outbox_transaction(backend, 'private-original-commerce', transaction, predecessor=request['predecessor'], schema_revisions_json=request['schema_revisions_json'], context=context)
        if repeated != descriptor or any((transport.original_history(transport.targets[table]) != history_before[table] for table in descriptor.versions)):
            raise ValueError('Exact commerce replay changed native publication')
        connection = connect(port['role'])
        with owned_connection(connection):
            (observation,) = Session(connection).query('SELECT * FROM "' + port['scope'].service_schema + '".observe(CAST(:scope AS uuid),CAST(:position AS bigint))', {'scope': port['scope'].scope_id, 'position': '1'}).rows
            if observation['position'] != '1' or bytes.fromhex(observation['request_hex']) != encoded(request).encode() or json.loads(bytes.fromhex(observation['manifest_hex'])) != dict(descriptor.raw):
                raise ValueError('Actual protected original ACK bytes differ')
            connection.rollback()
        report = {'format': 'ashlar-original-commerce-publication/0.1', 'runtime_versions': VERSIONS, 'public_umf_revision': UMF_PIN, 'public_dataset_receipt_sha256': hashlib.sha256(receipt_path.read_bytes()).hexdigest(), 'original_model_sha256': SOURCE_SHA, 'original_graph_sha256': GRAPH_SHA, 'bindings_sha256': hashlib.sha256(binding_path.read_bytes()).hexdigest(), 'source_transaction_sha256': hashlib.sha256(raw).hexdigest(), 'native_manifest': dict(descriptor.raw), 'table_registry': [{'table': t.table, 'uuid': t.uuid, 'path': str(t.path)} for t in targets], 'complete_original_oracle': expected, 'record_count': 11, 'edge_count': 10, 'history_count': 21, 'protected_ack_scope': port['scope'].__dict__, 'source_schema': port['source'], 'source_signature_sha256': port['signature'], 'protected_ack_observation': observation, 'exact_replay_unchanged': True, 'qualification': __doc__}
    except BaseException as error:
        report = None
        primary = error
    finally:
        failures = []
        if transport is not None:
            try:
                transport.close()
            except BaseException as error:
                failures.append(error)
        try:
            spark.stop()
        except BaseException as error:
            failures.append(error)
        if primary is not None:
            if failures:
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass
            raise primary
        if failures:
            raise failures[0]
    from .commerce import runtime_paths
    runtime_paths(config, True, require_fresh=False)
    if report is not None:
        payload = encoded(report) + '\n'
        with (output / 'report.json').open('x') as f:
            f.write(payload)
    return report
