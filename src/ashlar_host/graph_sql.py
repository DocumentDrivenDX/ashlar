# SPDX-License-Identifier: MIT OR Apache-2.0
"""Selected local host implementation; native qualification remains version-scoped."""
from dataclasses import asdict
import base64
import datetime
import hashlib
import json
from ashlar.apply import plan_apply
from ashlar.native import quote_table_identifier
from ashlar.whole_entity import changes_from_batch
TABLES = {'object_current', 'edge_current', 'tombstone', 'whole_source_history'}

def graph_sql_plan(prior, batch, tables, *, materialized_at, schema_policy, schema_transition_policy=None,
                   native_scalar_profile='string', lookup_hash_profile='sha256-json-v1'):
    if type(native_scalar_profile)is not str or native_scalar_profile not in ('string','typed'):raise ValueError('Closed native scalar profile required')
    if type(lookup_hash_profile)is not str or lookup_hash_profile not in ('sha256-json-v1','spark-xxhash64-typed-key/0.1'):
        raise ValueError('Closed current lookup-hash profile required')
    if set(tables) != TABLES or len(set(tables.values())) != 4:
        raise ValueError('Four distinct complete graph target names required')
    quoted = {k: quote_table_identifier(v) for (k, v) in tables.items()}
    stamp = datetime.datetime.fromisoformat(materialized_at)
    if stamp.tzinfo is None or stamp.utcoffset() != datetime.timedelta(0):
        raise ValueError('Explicit UTC materialization metadata clock required')
    native_original = None
    native_edge_orders = {}
    if batch.profile == 'truss-native-feed/0.1':
        from ashlar.staging import batch_row
        from ashlar.truss_native_feed import scalar_entity_changes,typed_scalar_entity_changes
        batch_row(batch)
        parsed = (typed_scalar_entity_changes if native_scalar_profile=='typed'else scalar_entity_changes)(batch.begin, sha256=hashlib.sha256(batch.begin).hexdigest(),
            prior=base64.b64decode(batch.cursor_before, validate=True),
            next=base64.b64decode(batch.cursor_after, validate=True), feed=batch.feed)
        changes = parsed.changes
        native_edge_orders = {(key,version):order for key,version,order in parsed.edge_order_keys}
        native_original = {}
        for raw in parsed.event_bytes:
            event = json.loads(raw)
            native_original[event['xid'] + ':' + event['seq']] = raw
        cursor = json.dumps({'profile': batch.profile, 'cursorBase64': batch.cursor_after}, separators=(',', ':'))
    else:
        changes = changes_from_batch(batch)
        cursor = json.dumps({'profile': batch.profile, 'offset': batch.cursor_after}, separators=(',', ':'))
    after = plan_apply(prior, changes, schema_policy=schema_policy, schema_transition_policy=schema_transition_policy)
    fresh = [x for x in changes if (x.feed, x.epoch, x.delivery_id) not in prior.deliveries]
    steps = []
    typed_json_staging = lookup_hash_profile == 'spark-xxhash64-typed-key/0.1'

    def insert(table, rows, types, *, hash_typed_field=None):
        if not rows:
            return
        fields = list(rows[0])
        def stage_type(field):
            target_type = types.get(field, 'STRING')
            return target_type if typed_json_staging and target_type == 'BIGINT' else 'STRING'
        definition = 'ARRAY<STRUCT<' + ','.join((k + ':' + stage_type(k) for k in fields)) + '>>'
        def select_field(field):
            target_type = types.get(field, 'STRING')
            if typed_json_staging and target_type == 'BIGINT':
                return 'r.' + field + ' AS ' + field
            return 'cast(r.' + field + ' AS ' + target_type + ') AS ' + field
        casts = ','.join((select_field(k) for k in fields))
        output_fields = list(fields)
        if hash_typed_field is not None:
            casts += (',xxhash64(CAST(r.source_system AS STRING),CAST(r.' + hash_typed_field
                       + ' AS BIGINT),CAST(r.id AS BIGINT)) AS lookup_hash')
            output_fields.append('lookup_hash')
        steps.append({'statement': 'INSERT INTO ' + quoted[table] + ' (' + ','.join(output_fields) + ') SELECT ' + casts + " FROM (SELECT explode(from_json(:rows,'" + definition + "')) r)", 'parameters': {'rows': json.dumps(rows, separators=(',', ':'))}})
    numeric = {k: 'BIGINT' for k in ['type_id', 'rel_type_id', 'id', 'entity_version', 'source_type', 'source_id', 'target_type', 'target_id']}
    latest_change_by_key = {}
    for change in fresh:
        latest_change_by_key[change.state.key] = change
    for (table, kind, typed) in [('object_current', 'object', 'type_id'), ('edge_current', 'edge', 'rel_type_id')]:
        touched = sorted({x.state.key for x in fresh if x.state.key.kind == kind})
        if not touched:
            continue
        if typed_json_staging:
            keys = [{'source_system': key.source, 'type_id': key.type_id, 'id': key.id} for key in touched]
            merge_key_schema = 'ARRAY<STRUCT<source_system:STRING,type_id:BIGINT,id:BIGINT>>'
            merge_type = 's.type_id'
            merge_id = 's.id'
        else:
            keys = [{'source_system': key.source, 'type_id': str(key.type_id), 'id': str(key.id)} for key in touched]
            merge_key_schema = 'ARRAY<STRUCT<source_system:STRING,type_id:STRING,id:STRING>>'
            merge_type = 'cast(s.type_id AS BIGINT)'
            merge_id = 'cast(s.id AS BIGINT)'
        merge_sql = ('MERGE INTO ' + quoted[table] + " t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'"
                     + merge_key_schema + "')) k)) s ON t.source_system=s.source_system AND t." + typed
                     + '=' + merge_type + ' AND t.id=' + merge_id + ' WHEN MATCHED THEN DELETE')
        steps.append({'statement': merge_sql, 'parameters': {'keys': json.dumps(keys, separators=(',', ':'))}})
        rows = []
        for key in touched:
            if key not in after.current:
                continue
            entity = after.current[key]
            change = latest_change_by_key[key]
            identity = {'source_system': key.source, typed: key.type_id, 'id': key.id}
            row = dict(identity) if typed_json_staging else {k: str(v) for (k, v) in identity.items()}
            row.update(schema_revision=entity.schema_revision,
                       entity_version=entity.version if typed_json_staging else str(entity.version),
                       props_json=entity.props_json, retained_json=entity.retained_json,
                       source_feed=change.feed, source_epoch=change.epoch,
                       source_cursor_json=cursor, source_delivery_id=change.delivery_id,
                       published_at=materialized_at, apply_batch_id=batch.batch_id)
            if lookup_hash_profile == 'sha256-json-v1':
                row['lookup_hash'] = hashlib.sha256(json.dumps(identity, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
            if kind == 'object':
                row['logical_key_json'] = '[]'
            else:
                (a, b) = entity.endpoints
                if typed_json_staging:
                    row.update(source_type=a.type_id, source_id=a.id, target_type=b.type_id, target_id=b.id)
                else:
                    row.update(source_type=str(a.type_id), source_id=str(a.id), target_type=str(b.type_id), target_id=str(b.id))
                if native_original is not None:row['order_key'] = native_edge_orders[(key,entity.version)]
            rows.append(row)
        insert(table, rows, dict(numeric, published_at='TIMESTAMP'),
               hash_typed_field=typed if lookup_hash_profile == 'spark-xxhash64-typed-key/0.1' else None)
    original = native_original if native_original is not None else {r.delivery_id: r.raw for r in batch.records}
    history = [{'feed': x.feed, 'epoch': x.epoch, 'delivery_id': x.delivery_id, 'digest': x.raw_digest, 'change_json': json.dumps(asdict(x), separators=(',', ':')), 'raw_base64': base64.b64encode(original[x.delivery_id]).decode()} for x in fresh]
    insert('whole_source_history', history, {})
    if typed_json_staging:
        deletes = [{'source_system': x.state.key.source, 'entity_kind': x.state.key.kind,
                    'type_id': x.state.key.type_id, 'id': x.state.key.id,
                    'entity_version': x.state.version, 'source_feed': x.feed,
                    'source_epoch': x.epoch, 'source_cursor_json': cursor,
                    'source_delivery_id': x.delivery_id}
                   for x in fresh if x.operation == 'delete']
    else:
        deletes = [{'source_system': x.state.key.source, 'entity_kind': x.state.key.kind,
                    'type_id': str(x.state.key.type_id), 'id': str(x.state.key.id),
                    'entity_version': str(x.state.version), 'source_feed': x.feed,
                    'source_epoch': x.epoch, 'source_cursor_json': cursor,
                    'source_delivery_id': x.delivery_id}
                   for x in fresh if x.operation == 'delete']
    insert('tombstone', deletes, numeric)
    return (after, steps)
