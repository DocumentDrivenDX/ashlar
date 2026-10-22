# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh local current Delta installation for an explicit carrier profile."""
import json
from pathlib import Path

from .delta_custody import DeltaTarget, encoded
from .managed_current_carriers import load_current_managed_carriers
from .schema_rows import LOCAL_CURRENT_ROLE_TO_UMF

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


def _observed_role(spark, carrier_role, table, path, fields, layout):
    detail = _detail(spark, path)
    uuid = detail.get('id')
    _require(type(uuid) is str and uuid, 'Native local Delta UUID required')
    native = spark.read.format('delta').load(str(path)).schema
    actual_fields = tuple((field.name, field.dataType.simpleString(), field.nullable)
                          for field in native.fields)
    expected_fields = tuple((name, _TYPE_NAMES[kind], nullable)
                            for name, kind, nullable in fields)
    _require(actual_fields == expected_fields, 'Exact UMF-generated local schema required')
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
    carriers = load_current_managed_carriers(XXHASH64_PROFILE)
    targets = []
    role_bindings = {}
    generated_resources=json.loads(carriers.generated_bytes)
    for local_role, carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
        table = tables[local_role]
        path = output / local_role
        _require(not path.exists() and not path.is_symlink(),
                 'Fresh local Delta role path required')
        ddl = carriers.create_sql(carrier_role, ('local', 'trussgraph', carrier_role))
        prefix = 'CREATE TABLE `local`.`trussgraph`.`' + carrier_role + '`'
        _require(ddl.startswith(prefix) and ddl.endswith(';\n'),
                 'UMF DDL must match the closed local substitution point')
        sql = 'CREATE TABLE delta.`' + str(path) + '`' + ddl[len(prefix):-2]
        spark.sql(sql)
        fields = carriers.ordered_fields(carrier_role)
        layout=generated_resources['carriers'][carrier_role]['layout']
        target, binding = _observed_role(spark, carrier_role, table, path, fields,layout)
        targets.append(target)
        role_bindings[local_role] = binding
    profile = {
        'profile': PROFILE,
        'physical_profile': XXHASH64_PROFILE,
        'lookup_hash_profile': carriers.lookup_hash_profile,
        'generated_carriers_sha256': 'cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa',
        'umf_revision': '2ede4c879bea5db13794a6fa30e67c821306c17f',
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
             and set(profile.get('roles', {})) == set(LOCAL_CURRENT_ROLE_TO_UMF),
             'Exact original local `.4` profile custody required')
    by_table = {target.table: target for target in targets}
    _require(len(by_table) == 6)
    carriers = load_current_managed_carriers(XXHASH64_PROFILE)
    generated=json.loads(carriers.generated_bytes)
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
        _target, observed = _observed_role(spark, carrier_role, table,
            target.path, carriers.ordered_fields(carrier_role),
            generated['carriers'][carrier_role]['layout'])
        _require(encoded(observed) == encoded(binding),
                 'Local `.4` role schema, protocol or layout changed')
    return None


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
