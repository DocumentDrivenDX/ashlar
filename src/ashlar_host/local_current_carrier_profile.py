# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh local current Delta installation for an explicit carrier profile."""
import json
import hashlib
import re
from urllib.parse import unquote, urlsplit
from pathlib import Path

from .delta_custody import DeltaTarget, encoded
from .managed_current_carriers import load_current_managed_carriers
from .schema_rows import LOCAL_CURRENT_ROLE_TO_UMF
from .local_current_delta4_umf import (
    BRIDGE_SHA256, BUILD_INPUTS_SHA256, LOCAL_TARGET_PROFILE, ROLE_SOURCE_SHA256,
    UMF_CANDIDATE_COMMIT, UMF_REALIZATION_PROFILE, REALIZATION_MANIFEST_SHA256,
    LocalUmfDelta4Bridge,
)

PROFILE = 'ashlar-local-current-installation-profile/0.1'
XXHASH64_PROFILE = 'ashlar-delta/0.4-xxhash64'
_TYPE_NAMES = {'STRING': 'string', 'BIGINT': 'long', 'TIMESTAMP': 'timestamp'}


def _require(value, reason='Exact local current carrier installation required'):
    if not value:
        raise ValueError(reason)


def _detail(spark, path):
    rows = spark.sql('DESCRIBE DETAIL delta.`' + str(path) + '`').limit(2).collect()
    _require(len(rows) == 1, 'Unique local Delta detail required')
    return rows[0].asDict(recursive=True)


def _protocol(detail, uuid):
    from .configured_local_runtime import PROFILE as RUNTIME_PROTOCOL
    from .protocol import ReaderProtocolProfile, validate_protocol_detail
    profile = ReaderProtocolProfile(RUNTIME_PROTOCOL, (1, 2, 3), (2, 5, 7), {
        'appendOnly', 'invariants', 'checkConstraints', 'changeDataFeed',
        'generatedColumns', 'columnMapping', 'deletionVectors', 'timestampNtz',
        'domainMetadata', 'rowTracking', 'clustering',
    })
    normalized = dict(detail)
    normalized['minReaderVersion'] = str(normalized.get('minReaderVersion'))
    normalized['minWriterVersion'] = str(normalized.get('minWriterVersion'))
    features = normalized.get('tableFeatures')
    _require(type(features) in (list, tuple), 'Complete native Delta feature inventory required')
    normalized['tableFeatures'] = json.dumps(list(features), separators=(',', ':'))
    return validate_protocol_detail(normalized, uuid=uuid, profile=profile)


def _observed_role(spark, carrier_role, table, path, fields, layout, field_metadata=None):
    detail = _detail(spark, path)
    uuid = detail.get('id')
    _require(type(uuid) is str and uuid, 'Native local Delta UUID required')
    native = spark.read.format('delta').load(str(path)).schema
    actual_fields = tuple((field.name, field.dataType.simpleString(), field.nullable)
                          for field in native.fields)
    expected_fields = tuple((name, _TYPE_NAMES[kind], nullable)
                            for name, kind, nullable in fields)
    _require(actual_fields == expected_fields, 'Exact UMF-generated local schema required')
    if field_metadata is not None:
        _require(type(field_metadata) in (list, tuple) and len(field_metadata) == len(native.fields)
                 and all(type(item) is dict for item in field_metadata)
                 and tuple(dict(field.metadata) for field in native.fields) == tuple(field_metadata),
                 'Exact UMF-authored local Delta column metadata required')
    protocol = _protocol(detail, uuid)
    clustering = detail.get('clusteringColumns')
    partitioning = detail.get('partitionColumns')
    properties = detail.get('properties')
    expected_clustering = layout.get('clusterBy', [])
    expected_partitioning = layout.get('partitionBy', [])
    expected_properties = layout.get('properties', {})
    _require(type(expected_clustering)is list and type(clustering) in (list, tuple)
             and list(clustering)==expected_clustering
             and type(expected_partitioning)is list and type(partitioning) in (list, tuple)
             and list(partitioning)==expected_partitioning
             and type(properties)is dict and type(expected_properties)is dict
             and all(properties.get(key)==value for key,value in expected_properties.items()),
             'Native local UMF layout observation differs')
    return DeltaTarget(table, path, uuid), {
        'carrier_role': carrier_role,
        'table': table,
        'path': str(path),
        'uuid': uuid,
        'fields': [[name, kind, nullable] for name, kind, nullable in fields],
        'field_metadata': [dict(item) for item in field_metadata] if field_metadata is not None else None,
        'protocol': protocol,
        'clustering_columns': list(clustering or ()),
        'partition_columns': list(partitioning or ()),
        'layout_properties': expected_properties,
    }


def create_local_current_xxhash64_tables(spark, output, tables):
    """Create one fresh six-role `.4` carrier set and freeze its native binding."""
    output = Path(output)
    _require(output.is_absolute() and output.resolve() == output,
             'Canonical fresh local current output required')
    _require(type(tables) is dict and set(tables) == set(LOCAL_CURRENT_ROLE_TO_UMF),
             'Exact six-role local table map required')
    _require(all(character not in str(output) for character in ('`', '\0', '\n', '\r')),
             'Safe local Delta target path required')
    carriers = load_current_managed_carriers(XXHASH64_PROFILE)
    source_bytes = dict(carriers.inputs)
    targets = []
    role_bindings = {}
    proposals = {}
    # Preflight every source, typed target and complete UMF result before the
    # first native DDL. No managed SQL is rewritten in the Ashlar adapter.
    with LocalUmfDelta4Bridge.open() as bridge:
        for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
            table = tables[local_role]
            path = output / local_role
            _require(not path.exists() and not path.is_symlink(),
                     'Fresh local Delta role path required')
            _require(type(table) is str and table == 'local.trussgraph.' + local_role,
                     'Exact local current table binding required')
            source = source_bytes.get(carrier_role + '.umf.json')
            _require(type(source) is bytes and hashlib.sha256(source).hexdigest() ==
                     ROLE_SOURCE_SHA256[carrier_role],
                     'Original `.4` UMF source bytes required: ' + carrier_role)
            admitted = bridge.realize(source, str(path), carrier_role)
            _require(admitted['fields'] == carriers.ordered_fields(carrier_role),
                     'UMF local Delta schema differs from the original `.4` carrier: ' + carrier_role)
            proposals[local_role] = admitted
    apply_local_writer_settings(spark)
    for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
        table = tables[local_role]
        path = output / local_role
        admitted = proposals[local_role]
        proposal = admitted['proposal']
        spark.sql(proposal['sql'])
        target, observed = _observed_role(spark, carrier_role, table, path,
            admitted['fields'], admitted['layout'], admitted['field_metadata'])
        observed.update({
            'umf_source_bytes_sha256': admitted['source_bytes_sha256'],
            'umf_source_sha256': proposal['sourceSha256'],
            'umf_schema_sha256': proposal['schemaSha256'],
            'umf_target_sha256': proposal['target']['identitySha256'],
            'umf_proposal_sha256': admitted['proposal_sha256'],
            'umf_ddl_sha256': hashlib.sha256(proposal['sql'].encode('utf-8')).hexdigest(),
            'umf_dispositions': proposal['dispositions'],
            'umf_writer_settings': proposal['writerSettings'],
            'umf_proposal': proposal,
        })
        targets.append(target)
        role_bindings[local_role] = observed
    profile = {
        'profile': PROFILE,
        'physical_profile': XXHASH64_PROFILE,
        'lookup_hash_profile': carriers.lookup_hash_profile,
        'generated_carriers_sha256': 'cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa',
        'umf_revision': '2ede4c879bea5db13794a6fa30e67c821306c17f',
        'local_realization': LOCAL_TARGET_PROFILE,
        'umf_realization_profile': UMF_REALIZATION_PROFILE,
        'umf_candidate_commit': UMF_CANDIDATE_COMMIT,
        'umf_realization_manifest_sha256': REALIZATION_MANIFEST_SHA256,
        'umf_bridge_sha256': BRIDGE_SHA256,
        'umf_build_inputs_sha256': BUILD_INPUTS_SHA256,
        'writer_settings': [{'key': 'spark.sql.parquet.compression.codec', 'value': 'zstd'}],
        'roles': role_bindings,
    }
    _require(len(encoded(profile).encode()) <= 65536)
    return targets, profile, {role: tuple((name, kind) for name, kind, _ in fields)
                              for role, fields in ((local, carriers.ordered_fields(umf))
                                  for local, umf in LOCAL_CURRENT_ROLE_TO_UMF.items())}


def verify_local_current_profile(spark, profile, targets, carrier_profile):
    """Refuse any profile, UUID, schema, protocol or layout drift before work."""
    _require(carrier_profile == XXHASH64_PROFILE
             and type(profile) is dict and profile.get('profile') == PROFILE
             and profile.get('physical_profile') == XXHASH64_PROFILE
             and profile.get('lookup_hash_profile') == 'spark-xxhash64-typed-key/0.1'
             and profile.get('generated_carriers_sha256') ==
                 'cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa'
             and profile.get('umf_revision') == '2ede4c879bea5db13794a6fa30e67c821306c17f'
             and profile.get('local_realization') == LOCAL_TARGET_PROFILE
             and profile.get('umf_realization_profile') == UMF_REALIZATION_PROFILE
             and profile.get('umf_candidate_commit') == UMF_CANDIDATE_COMMIT
             and profile.get('umf_realization_manifest_sha256') == REALIZATION_MANIFEST_SHA256
             and profile.get('umf_bridge_sha256') == BRIDGE_SHA256
             and profile.get('umf_build_inputs_sha256') == BUILD_INPUTS_SHA256
             and profile.get('writer_settings') ==
                 [{'key': 'spark.sql.parquet.compression.codec', 'value': 'zstd'}]
             and set(profile.get('roles', {})) == set(LOCAL_CURRENT_ROLE_TO_UMF),
             'Exact original local `.4` profile custody required')
    verify_local_writer_settings(spark)
    by_table = {target.table: target for target in targets}
    _require(len(by_table) == 6)
    carriers = load_current_managed_carriers(XXHASH64_PROFILE)
    source_bytes = dict(carriers.inputs)
    with LocalUmfDelta4Bridge.open() as bridge:
        for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
            binding = profile['roles'][local_role]
            table = binding.get('table')
            path = binding.get('path')
            _require(type(table) is str and type(path) is str and type(binding.get('uuid')) is str,
                     'Exact original local table binding required')
            target = by_table.get(table)
            _require(target is not None and target.path == Path(path)
                     and target.uuid == binding.get('uuid')
                     and binding.get('carrier_role') == carrier_role,
                     'Exact original six-role table and UUID custody required')
            admitted = bridge.realize(source_bytes[carrier_role + '.umf.json'], path, carrier_role)
            _require(admitted['fields'] == carriers.ordered_fields(carrier_role)
                     and binding.get('umf_source_bytes_sha256') == admitted['source_bytes_sha256']
                     and binding.get('umf_source_sha256') == admitted['proposal']['sourceSha256']
                     and binding.get('umf_schema_sha256') == admitted['proposal']['schemaSha256']
                     and binding.get('umf_target_sha256') == admitted['proposal']['target']['identitySha256']
                     and binding.get('umf_proposal_sha256') == admitted['proposal_sha256']
                     and binding.get('umf_ddl_sha256') ==
                         hashlib.sha256(admitted['proposal']['sql'].encode('utf-8')).hexdigest()
                     and binding.get('umf_dispositions') == admitted['proposal']['dispositions']
                     and binding.get('umf_writer_settings') == admitted['proposal']['writerSettings']
                     and binding.get('field_metadata') == [dict(item) for item in admitted['field_metadata']]
                     and binding.get('umf_proposal') == admitted['proposal'],
                     'Local `.4` UMF target, source or layout disposition changed')
            _target, observed = _observed_role(spark, carrier_role, table,
                target.path, admitted['fields'], admitted['layout'], admitted['field_metadata'])
            observed.update({key: binding[key] for key in (
                'field_metadata',
                'umf_source_bytes_sha256', 'umf_source_sha256', 'umf_schema_sha256', 'umf_target_sha256',
                'umf_proposal_sha256', 'umf_ddl_sha256', 'umf_dispositions',
                'umf_writer_settings', 'umf_proposal')})
            _require(encoded(observed) == encoded(binding),
                     'Local `.4` role schema, protocol, layout or UMF custody changed')
    return None


def apply_local_writer_settings(spark):
    """Apply UMF-required Spark writer settings before local DDL/DML."""
    key = 'spark.sql.parquet.compression.codec'
    spark.conf.set(key, 'zstd')
    verify_local_writer_settings(spark)


def verify_local_writer_settings(spark):
    """Verify previously applied writer settings without repairing drift."""
    key = 'spark.sql.parquet.compression.codec'
    _require(spark.conf.get(key) == 'zstd',
             'Local `.4` ZSTD Spark writer setting must remain effective')


def _admit_zstd_footer_inventory(reports, *, expected_rows):
    """Admit the bounded footer facts read for one exact pinned local snapshot."""
    _require(type(expected_rows) is int and 0 <= expected_rows < (1 << 63)
             and type(reports) is list
             and len(reports) <= 4096, 'Bounded pinned Parquet footer inventory required')
    total_rows = 0
    nonempty_files = 0
    column_chunks = 0
    for report in reports:
        _require(type(report) is dict and set(report) == {'path_sha256', 'row_groups'}
                 and re.fullmatch('[0-9a-f]{64}', report['path_sha256']) is not None
                 and type(report['row_groups']) is list and len(report['row_groups']) <= 4096,
                 'Exact bounded Parquet footer report required')
        file_rows = 0
        for group in report['row_groups']:
            _require(type(group) is dict and set(group) == {'rows', 'codecs'}
                     and type(group['rows']) is int and 0 <= group['rows'] < (1 << 63)
                     and type(group['codecs']) is list and len(group['codecs']) <= 16384,
                     'Exact bounded Parquet row-group report required')
            _require(all(codec == 'ZSTD' for codec in group['codecs']),
                     'Every pinned Parquet column chunk must use ZSTD')
            if group['rows']:
                _require(bool(group['codecs']),
                         'Every nonempty pinned Parquet row group must expose column chunks')
                file_rows += group['rows']
            column_chunks += len(group['codecs'])
            _require(column_chunks <= 262144,
                     'Bounded pinned Parquet column-chunk inventory required')
        if file_rows:
            nonempty_files += 1
            total_rows += file_rows
    _require(total_rows == expected_rows,
             'Pinned Parquet footer rows must equal the complete verified Delta snapshot row count')
    _require(expected_rows == 0 or nonempty_files > 0,
             'Nonempty expected local publication requires nonempty pinned ZSTD data files')
    return {'active_files': len(reports), 'nonempty_files': nonempty_files,
            'row_groups_rows': total_rows, 'zstd_column_chunks': column_chunks}


def verify_local_xxhash64_zstd_snapshot(spark, target, version, *, expected_rows):
    """Read every active Parquet footer at one Delta version before source ACK."""
    from urllib.parse import urlsplit

    _require(type(version) is int and 0 <= version < (1 << 63)
             and type(expected_rows) is int and 0 <= expected_rows < (1 << 63),
             'Exact pinned local Delta version required')
    root = target.path.resolve()
    frame = (spark.read.format('delta').option('versionAsOf', version)
             .load(str(root)))
    files = frame.inputFiles()
    _require(type(files) in (list, tuple) and len(files) <= 4096
             and len(set(files)) == len(files),
             'Complete bounded pinned local Delta file set required')
    jvm = spark._jvm
    configuration = spark.sparkContext._jsc.hadoopConfiguration()
    reader_type = jvm.org.apache.parquet.hadoop.ParquetFileReader
    input_type = jvm.org.apache.parquet.hadoop.util.HadoopInputFile
    hadoop_path_type = jvm.org.apache.hadoop.fs.Path
    reports = []
    for uri in sorted(files):
        _require(type(uri) is str and 0 < len(uri.encode('utf-8')) <= 8192,
                 'Bounded active local Delta file URI required')
        parsed = urlsplit(uri)
        _require(parsed.scheme in ('', 'file') and parsed.netloc in ('', 'localhost'),
                 'Local pinned Delta data file required')
        original_path = Path(unquote(parsed.path if parsed.scheme else uri))
        _require(root in original_path.parents and original_path.is_file()
                 and not any(path.is_symlink() for path in (original_path, *original_path.parents)
                             if path == root or root in path.parents),
                 'Pinned Delta data file must remain within its exact installation target')
        local_path = original_path.resolve()
        _require(root in local_path.parents,
                 'Pinned Delta data file resolved outside its exact installation target')
        path_arg = hadoop_path_type(uri if parsed.scheme else str(local_path))
        reader = reader_type.open(input_type.fromPath(path_arg, configuration))
        try:
            footer = reader.getFooter()
            blocks = footer.getBlocks()
            _require(blocks is not None and blocks.size() <= 4096,
                     'Bounded Parquet row-group footer required')
            row_groups = []
            for index in range(blocks.size()):
                block = blocks.get(index)
                chunks = block.getColumns()
                _require(chunks is not None and chunks.size() <= 16384,
                         'Bounded Parquet column-chunk footer required')
                codecs = [str(chunks.get(chunk).getCodec().name())
                          for chunk in range(chunks.size())]
                row_groups.append({'rows': int(block.getRowCount()), 'codecs': codecs})
            reports.append({'path_sha256': hashlib.sha256(uri.encode('utf-8')).hexdigest(),
                            'row_groups': row_groups})
        finally:
            reader.close()
    return _admit_zstd_footer_inventory(reports, expected_rows=expected_rows)


def verify_local_xxhash64_snapshot(spark, path, version, role):
    """Recompute every stored data-role hash natively at its pinned version."""
    from pyspark.sql import functions as F
    _require(role in ('object_current', 'edge_current') and type(version) is int
             and 0 <= version < (1 << 63), 'Exact pinned `.4` snapshot required')
    typed = 'type_id' if role == 'object_current' else 'rel_type_id'
    frame = spark.read.format('delta').option('versionAsOf', version).load(str(path))
    selected = frame.select('source_system', typed, 'id', 'lookup_hash',
        F.xxhash64(F.col('source_system').cast('string'), F.col(typed).cast('long'),
                   F.col('id').cast('long')).alias('_native_lookup_hash'))
    rows = selected.limit(4097).collect()
    _require(len(rows) <= 4096, 'Bounded complete local `.4` hash snapshot required')
    identities = set()
    for row in rows:
        value = row.asDict(recursive=True)
        key = (value['source_system'], value[typed], value['id'])
        stored = value['lookup_hash']; actual = value['_native_lookup_hash']
        _require(type(key[0]) is str and type(key[1]) is int and type(key[2]) is int
                 and -(1 << 63) <= key[1] < (1 << 63)
                 and -(1 << 63) <= key[2] < (1 << 63)
                 and type(stored) is int and type(actual) is int
                 and -(1 << 63) <= stored < (1 << 63)
                 and stored == actual and key not in identities,
                 'Pinned local `.4` hash/full-key identity validation failed')
        identities.add(key)
    return len(rows)
