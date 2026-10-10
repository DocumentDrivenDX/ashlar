"""Selected local host implementation; native qualification remains version-scoped."""
from .lifecycle import finish, owned_context
import hashlib, json
from contextlib import contextmanager
from ashlar.publication import resolve_publication
from .source_identity import SOURCE_SHA
from .delta_custody import encoded
LAYOUT_SHA = 'ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e'

class ReaderCleanupState:
    """One reader's actual cleanup failure facts; no exception payload retention."""
    __slots__ = ('_failed', '_cleanup_only', '_claimed')

    def __init__(self):
        object.__setattr__(self, '_failed', False)
        object.__setattr__(self, '_cleanup_only', False)
        object.__setattr__(self, '_claimed', False)

    def __setattr__(self, name, value):
        raise AttributeError('read-only-cleanup-state')

    def __delattr__(self, name):
        raise AttributeError('read-only-cleanup-state')

    @property
    def failed(self):
        return self._failed

    @property
    def cleanup_only(self):
        return self._cleanup_only


def _close_observed(action, state, primary):
    try:
        return action()
    except BaseException:
        if state is not None and not state.failed:
            object.__setattr__(state, '_failed', True)
            object.__setattr__(state, '_cleanup_only', primary is None)
        raise


def compiler_request(sql, model, bindings, manifest, registry, aliases, *, fields=False):
    """Bind original qualified model identities to explicitly admitted carriers."""
    if hashlib.sha256(model).hexdigest() != SOURCE_SHA or json.loads(model)['umf'] != '0.8.0':
        raise ValueError('Exact original model bytes required')
    versions = json.loads(manifest['table_versions_json'])
    native = {r['table']: r for r in registry}
    if len(native) != len(registry) or {t.split('.')[-1] for t in versions} != {'object_current', 'edge_current', 'tombstone', 'whole_source_history'} or set(json.loads(manifest['source_progress_json'])) != {'private-original-commerce-fixture'}:
        raise ValueError('Complete original commerce publication/source inventory required')
    if set(aliases) != set(versions) or len(set(aliases.values())) != len(aliases):
        raise ValueError('Complete injective original catalog alias inventory required')
    if any((name not in native for name in versions)):
        raise ValueError('Original publication registry incomplete')
    tables = [{'name': aliases[t].split('.'), 'uuid': native[t]['uuid'], 'version': v} for (t, v) in sorted(versions.items())]
    object_table = next((t for t in versions if t.endswith('.object_current')))
    index = sorted(versions).index(object_table)
    pin = {'documentId': 'urn:umf:domain:commerce', 'revision': SOURCE_SHA, 'sha256': SOURCE_SHA, 'umfVersion': '0.8.0'}
    records = []
    for element in ('products', 'suppliers'):
        entry = next((b for b in bindings['types'] if b['identity'] == ['object', 'domain', element]))
        logical = {'documentId': pin['documentId'], 'module': 'domain', 'element': element, 'revision': SOURCE_SHA}
        properties = []
        if fields:
            if bindings['profile'] != 'ashlar-commerce-development-bindings/0.2':
                raise ValueError('Field homes require explicit canonical development property-ID bindings')
            document = json.loads(model)
            record = next((e for m in document['modules'] if m['id'] == 'domain' for e in m['elements'] if e['id'] == element))
            property_inventory = {tuple(p['identity']): p['property_id'] for p in bindings['properties']}
            if len(property_inventory) != len(bindings['properties']) or len(set(property_inventory.values())) != len(property_inventory):
                raise ValueError('Injective explicit qualified Field carrier map required')
            for member in record['members']:
                identity = (pin['documentId'], member['module'], member['element'])
                if identity not in property_inventory:
                    raise ValueError('Missing original qualified Field carrier binding')
                properties.append({'logical': {'documentId': identity[0], 'module': identity[1], 'element': identity[2], 'revision': SOURCE_SHA}, 'home': {'kind': 'props', 'propertyId': property_inventory[identity]}})
        records.append({'kind': 'object', 'logical': logical, 'sourceSystem': 'private-original-commerce-fixture', 'typeId': entry['type_id'], 'table': index, 'schemaRevision': SOURCE_SHA, 'properties': properties})
    binding = {'profile': 'ashlar-databricks-candidate/0.1.0', 'layoutRevision': 'ashlar-delta/0.3', 'layoutSha256': LAYOUT_SHA, 'modelPins': [pin], 'publication': {'id': manifest['publication_id'], 'manifestUuid': native[next((t for t in native if t.endswith('.manifest')))]['uuid'], 'tables': tables}, 'records': records}
    raw = encoded(binding)
    return {'interfaceVersion': 'weft-compile/0.2.0', 'dialect': 'weft-sql/0.2.0', 'sql': sql, 'modules': [{'documentJson': model.decode(), 'pin': pin, 'selectedModuleIds': ['domain']}], 'target': {'backendId': 'ashlar.databricks', 'backendVersion': '0.1.0-candidate', 'targetProfile': 'dbsql-candidate', 'bindingJson': raw, 'bindingSha256': hashlib.sha256(raw.encode()).hexdigest()}, 'options': {'allowCandidate': True}}

def execute_guarded(provider, request, artifact, *, context):
    """Full resolver/pin closure encloses checks, SQL and buffered-result release.

    The provider must own the private native authorization/retention policy;
    an original compiler receipt never supplies that authority by itself.
    """
    binding = json.loads(request['target']['bindingJson'])
    if artifact.get('status') != 'compiled' or artifact.get('bindingSha256') != request['target']['bindingSha256'] or artifact.get('modelPins') != binding['modelPins']:
        raise ValueError('Exact compiled request custody required')
    obligations = artifact.get('obligations', [])
    if len(obligations) != 2 or {o['id'] for o in obligations} != {'ashlar.candidate.publication', 'ashlar.candidate.scalarIntegrity'}:
        raise ValueError('Unknown, missing or duplicate consumer obligation')
    publication = next((o for o in obligations if o['id'] == 'ashlar.candidate.publication'))['parameters']
    integrity = next((o for o in obligations if o['id'] == 'ashlar.candidate.scalarIntegrity'))['parameters']
    if publication['publication'] != binding['publication'] or publication['modelPins'] != binding['modelPins'] or publication['layoutRevision'] != binding['layoutRevision'] or (publication['layoutSha256'] != binding['layoutSha256']):
        raise ValueError('Original publication obligation differs')
    if integrity['phase'] != 'before-user-query' or integrity['samePublicationRequired'] is not True or integrity['noPartialPublication'] is not True:
        raise ValueError('Original integrity lifecycle differs')
    params = {'p' + str(p['position']): p['value'] for p in artifact['parameters']}
    checks = []
    answer = None
    completed = False
    with provider.interval(context):
        resolved = provider.resolve(context)
        provider.admit_binding(binding, resolved, context)
        provider.runtime(context)
        for check in integrity['checks']:
            rows = provider.sql(check['sql'], params)
            if rows != [{'violations': '0'}]:
                raise ValueError('Original scalar integrity refused')
            checks.append({'check': check, 'rows': rows})
        answer = provider.sql(artifact['sql'], params)
        provider.runtime(context)
        closed = provider.resolve(context)
        provider.admit_binding(binding, closed, context)
        if dict(closed.descriptor.raw) != dict(resolved.descriptor.raw) or closed.snapshots != resolved.snapshots:
            raise ValueError('Complete original publication changed before result release')
        completed = True
    if not completed:
        raise ValueError('Consumer interval suppressed failure')
    return {'rows': answer, 'integrity': checks, 'qualification': __doc__}

class ReadOnlyTransport:
    """Construct the reviewed native reader with an original read-only journal."""

    @staticmethod
    def open(spark, journal, installation, targets, policy, *, capacity=None):
        import sqlite3
        from urllib.parse import quote
        from .delta_custody import LocalDeltaTransport, LocalDeltaError

        class Reader(LocalDeltaTransport):

            def _journal(self, registry, *, create):
                if create:
                    raise LocalDeltaError('Read-only consumer cannot initialize')
                self.db = sqlite3.connect('file:' + quote(str(self.journal_path.resolve())) + '?mode=ro', uri=True)
                if self.db.execute('SELECT original FROM local_installation WHERE id=1').fetchall() != [(registry,)]:
                    raise LocalDeltaError('Original journal registration differs')

            def mutation(self, *args, **kwargs):
                raise LocalDeltaError('Read-only publication consumer')

            def recover(self, *args, **kwargs):
                raise LocalDeltaError('Read-only publication consumer')
        return Reader(spark, journal, installation, targets, policy, capacity=capacity)

class PublicationProvider:

    def __init__(self, driver, aliases, scope_port, request_bytes, manifest_bytes, expected_binding, *, context, cleanup_state=None):
        if cleanup_state is not None and type(cleanup_state) is not ReaderCleanupState:
            raise ValueError('invalid-reader-cleanup-state')
        self.cleanup_state = cleanup_state
        self.driver = driver
        self.aliases = dict(aliases)
        self.port = scope_port
        self.context = context
        self.request_bytes = request_bytes
        self.manifest_bytes = manifest_bytes
        self.expected_binding = encoded(expected_binding)
        self.active = False
        self._closed_custody = None
        self._reader_closed = False

    def runtime(self, context):
        if context is not self.context or not self.active:
            raise PermissionError('Explicit held private query context required')
        spark = self.driver.transport.spark
        if spark.version != '4.0.1':
            raise ValueError('Experimental Spark4.0.1 only; not a Databricks native-profile admission')
        self.driver.transport._profile()
        if spark.sql("SELECT ('a' COLLATE UTF8_BINARY) = ('A' COLLATE UTF8_BINARY) AS equal").first()['equal'] is not False:
            raise ValueError('Original binary comparison profile differs')

    def _ack(self, session):
        from .ack import receipt_bytes
        scope = self.port['scope']
        self.driver.admit_scope(scope, session, self.context)
        position = json.loads(self.driver.policy.active['request']['source_checkpoint_json'])['position']
        rows = session.query('SELECT * FROM "' + scope.service_schema + '".observe(CAST(:scope AS uuid),CAST(:position AS bigint))', {'scope': scope.scope_id, 'position': position}).rows
        expected = receipt_bytes(scope, self.request_bytes, self.manifest_bytes)[-1].hex()
        if len(rows) != 1 or type(rows[0].get('position')) is not str or (not rows[0]['position'].isdigit()) or (str(int(rows[0]['position'])) != rows[0]['position']) or (not int(position) <= int(rows[0]['position']) < 2 ** 63) or (rows[0]['request_hex'] != self.request_bytes.hex()) or (rows[0]['manifest_hex'] != self.manifest_bytes.hex()) or (rows[0]['receipt_hex'] != expected):
            raise PermissionError('Original protected native source ACK differs')
        (identity,) = session.query('SELECT current_user,session_user,current_database() AS database,CAST(inet_server_addr() AS TEXT) AS server_address,CAST(inet_server_port() AS TEXT) AS server_port,CAST(pg_backend_pid() AS TEXT) AS backend_pid', {}).rows
        if identity['current_user'] != self.port['role'] or identity['session_user'] != self.port['role'] or identity['database'] != 'truss_e2e':
            raise PermissionError('Original ordinary native query session differs')
        identity.update(source_schema=self.port['source'], source_signature_sha256=self.port['signature'], connection_route='private-local-postgresql')
        return {'session': identity, 'observation': json.loads(encoded(rows[0]))}

    @contextmanager
    def interval(self, context):
        from ashlar.manifest import manifest_pin_vector
        from .connection import connect
        from .postgres import Session
        if context is not self.context or self.active or self._reader_closed:
            raise PermissionError('Original nonnested private consumer required')
        self._closed_custody = None
        completed = False
        opening = None
        closing = None
        row = self.driver.policy.active['manifest']
        versions = json.loads(row['table_versions_json'])
        vector = manifest_pin_vector(row, {t: self.driver.transport.targets[t].uuid for t in versions}, authority='private-local-process')
        with owned_context(self.driver.writer('read-only-compiler-consumer', context)):
            with owned_context(self.driver.hold(vector, context=context)):
                connection = connect(self.port['role'])
                primary = None
                try:
                    session = Session(connection)
                    opening = self._ack(session)
                    self.active = True
                    yield
                    closing = self._ack(session)
                    completed = True
                except BaseException as error:
                    primary = error
                finally:
                    self.active = False
                    finish(primary, [lambda: _close_observed(connection.rollback, self.cleanup_state, primary),
                                     lambda: _close_observed(connection.close, self.cleanup_state, primary)])
        if not completed:
            raise PermissionError('Original consumer closing checks suppressed')
        from dataclasses import asdict
        self._closed_custody = {'format': 'ashlar-private-local-protected-ack-interval/0.1', 'scope': asdict(self.port['scope']), 'source_schema': self.port['source'], 'source_signature_sha256': self.port['signature'], 'original_request_hex': self.request_bytes.hex(), 'original_manifest_hex': self.manifest_bytes.hex(), 'opening_ack': opening, 'closing_ack': closing, 'qualification': 'Fresh ordinary private PG observations and original full native publication pin policy completed before this receipt. Private local host custody only; not UC/Truss or remotely authenticated source authority.'}

    def closed_interval_custody(self, context):
        if context is not self.context or self.active or self._closed_custody is None:
            raise PermissionError('Original complete closed interval custody unavailable')
        return json.loads(encoded(self._closed_custody))

    def resolve(self, context):
        versions = json.loads(self.driver.policy.active['manifest']['table_versions_json'])
        return resolve_publication(self.driver, self.driver.policy.active['publication_id'], {t: self.driver.transport.targets[t].uuid for t in versions}, context=context, supported_profiles=['ashlar-delta/0.3'], supported_revisions={k: [v] for (k, v) in json.loads(self.driver.policy.active['manifest']['schema_revisions_json']).items()})

    def admit_binding(self, binding, resolved, context):
        if context is not self.context or not self.active:
            raise PermissionError('Current held original publication required')
        if encoded(binding) != self.expected_binding:
            raise ValueError('Original qualified model/source/property carrier binding changed')
        tables = [{'name': self.aliases[t].split('.'), 'uuid': s.uuid, 'version': s.version} for (t, s) in sorted(resolved.snapshots.items())]
        manifest = self.driver.transport.targets[self.driver.tables['manifest']]
        expected = {'id': resolved.descriptor.publication_id, 'manifestUuid': manifest.uuid, 'tables': tables}
        if binding['publication'] != expected:
            raise ValueError('Compiler full vector/catalog alias binding differs')
        for native in self.aliases:
            detail = self.driver.transport.spark.sql('DESCRIBE DETAIL ' + self.aliases[native]).first().asDict()
            target = self.driver.transport.targets[native]
            if detail['id'] != target.uuid or detail['location'].removeprefix('file:').rstrip('/') != str(target.path):
                raise ValueError('Alias replaced or points at another native original')

    def native_table_schema(self, table, resolved, context):
        """Observe complete native metadata at a resolved, held original version.

        Physical nullable metadata is retained; row non-nullness is a separate
        mandatory compiler guard. This callback never samples source rows.
        """
        if context is not self.context or not self.active:
            raise PermissionError('Original active full-publication schema context required')
        if type(table) is not dict or set(table) != {'name', 'uuid', 'version'} or type(table['name']) is not list or (len(table['name']) != 3) or any((type(part) is not str or not part for part in table['name'])) or (type(table['uuid']) is not str) or (not table['uuid']) or (type(table['version']) is not int) or (table['version'] < 0):
            raise ValueError('Exact original native table reference required')
        matches = [native for (native, alias) in self.aliases.items() if alias.split('.') == table['name']]
        if len(matches) != 1 or matches[0] not in resolved.snapshots:
            raise ValueError('Schema table outside original resolved alias vector')
        native = matches[0]
        snapshot = resolved.snapshots[native]
        target = self.driver.transport.targets[native]
        if snapshot.uuid != table['uuid'] or snapshot.version != table['version'] or target.uuid != snapshot.uuid:
            raise ValueError('Schema UUID/version differs from original pinned source')
        frame = self.driver.transport.spark.read.format('delta').option('versionAsOf', snapshot.version).load(str(target.path))
        native_schema = frame.schema
        schema = json.loads(native_schema.json())
        if type(schema) is not dict or schema.get('type') != 'struct' or type(schema.get('fields')) is not list or any((type(field) is not dict or type(field.get('name')) is not str or type(field.get('nullable')) is not bool for field in schema['fields'])) or (len({field['name'] for field in schema['fields']}) != len(schema['fields'])):
            raise ValueError('Actual complete native schema is ambiguous or malformed')
        return {'table': json.loads(encoded(table)), 'schema': schema, 'nativeTypes': [[field.name, field.dataType.simpleString().upper()] for field in native_schema.fields]}

    def sql(self, sql, params):
        if not self.active:
            raise PermissionError('No user SQL outside full held native publication')
        return [r.asDict() for r in self.driver.transport.spark.sql(sql, args=params).collect()]

    def sql_ordered(self, sql, params):
        if not self.active:
            raise PermissionError('No user SQL outside full held native publication')
        frame = self.driver.transport.spark.sql(sql, args=params)
        schema = [[f.name, f.dataType.simpleString().upper()] for f in frame.schema.fields]
        return {'schema': schema, 'rows': [list(row) for row in frame.collect()]}

@contextmanager
def open_commerce_reader(spark, publication, *, cleanup_state=None):
    """Open original custody/authority ports without submitting native writes.

    Caller owns Spark lifetime. The private reader retains original journal,
    source/model admission, independent full rows, protected ACK and complete
    pin policy. Consumers must use provider.interval(context); no alias setup
    or user SQL is performed by this factory.
    """
    if cleanup_state is not None:
        if type(cleanup_state) is not ReaderCleanupState or cleanup_state._claimed:
            raise ValueError('invalid-reader-cleanup-state')
        object.__setattr__(cleanup_state, '_claimed', True)
    from pathlib import Path
    from types import SimpleNamespace
    from .source_identity import build_transaction
    from .schema_rows import fixture_columns
    from .delta_custody import DeltaTarget
    from .ack import AckScope
    from .commerce_admission import CommerceAdmission, original_commerce_oracle
    from .driver import NativeDriver, PrivatePolicy, ROOT
    publication = Path(publication)
    original_report = json.loads((publication / 'report.json').read_bytes())
    model = (publication / 'original-ontology.json').read_bytes()
    graph = (publication / 'original-graph.json').read_bytes()
    bindings = json.loads((publication / 'development-bindings.json').read_bytes())
    (batch, rebuilt) = build_transaction(model, graph, source_system='private-original-commerce-fixture', binding_profile=bindings['profile'])
    if bindings != rebuilt or batch.begin + b''.join((r.raw for r in batch.records)) + batch.commit != (publication / 'source.jsonl').read_bytes():
        raise ValueError('Original source/binding bytes differ')
    admission = CommerceAdmission(batch, bindings, publication / 'original-ontology.json', publication / 'original-graph.json', publication / 'development-bindings.json', publication / 'public-dataset.json')
    expected = original_commerce_oracle(model, graph, bindings, batch, fixture_columns(ROOT))
    targets = [DeltaTarget(r['table'], Path(r['path']), r['uuid']) for r in original_report['table_registry']]

    def native_files():
        return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for t in targets for p in t.path.rglob('*') if p.is_file()}
    original_native = native_files()
    transport = None
    primary = None
    try:
        context = object()
        policy = PrivatePolicy(context, targets)
        policy.initializing = False
        transport = ReadOnlyTransport.open(spark, publication / 'operations.sqlite', 'private-original-commerce', targets, policy)
        (request_raw, artifact_raw) = transport.db.execute('SELECT request,artifact FROM local_publication_artifact').fetchone()
        request = json.loads(request_raw)
        artifact = json.loads(artifact_raw)
        manifest = artifact['manifest']
        if manifest != original_report['native_manifest']:
            raise ValueError('Original immutable artifact/report manifest differs')
        scope = AckScope(**original_report['protected_ack_scope'])
        source = original_report['source_schema']
        role = scope.service_schema.replace('pipeline_', 'operator_')
        port = {'scope': scope, 'source': source, 'reader': source + '_reader', 'role': role, 'signature': original_report['source_signature_sha256']}
        tables = {t.table.split('.')[-1]: t.table for t in targets}
        driver = NativeDriver(transport, policy, context, tables, {batch.feed: port}, admission.changes, fixture_columns(ROOT), source_admission=admission)
        plan = json.loads(transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?', (request['request_digest'],)).fetchone()[0])
        policy.active = {'request': request, 'steps': plan['selected_steps'], 'expected': expected, 'manifest': manifest, 'publication_id': manifest['publication_id'], 'previous_progress': {}, 'progress': json.loads(manifest['source_progress_json']), 'previous_expected': plan['complete_prior_oracle'], 'generated_steps': plan['generated_steps'], 'elisions': plan['zero_match_elisions']}
        aliases = {t: t.replace('local.', 'spark_catalog.', 1) for t in json.loads(manifest['table_versions_json'])}
        req = compiler_request('SELECT COUNT(*) AS n FROM products p', model, bindings, manifest, original_report['table_registry'], aliases)
        provider = PublicationProvider(driver, aliases, port, request_raw.encode(), encoded(manifest).encode(), json.loads(req['target']['bindingJson']), context=context, cleanup_state=cleanup_state)
        yield SimpleNamespace(provider=provider, driver=driver, admission=admission, independent_expected=expected, context=context, original_native_files=original_native, native_files=native_files, aliases=aliases, original_report=original_report, model=model, graph=graph, bindings=bindings, manifest=manifest, request_bytes=request_raw.encode(), manifest_bytes=encoded(manifest).encode())
        if native_files() != original_native:
            raise ValueError('Read-only consumer changed original native bytes')
    except BaseException as error:
        primary = error
    finally:
        actions = []
        if transport is not None:
            if 'provider' in locals():
                provider._reader_closed = True
            actions.append(lambda: _close_observed(transport.close, cleanup_state, primary))
        finish(primary, actions)
