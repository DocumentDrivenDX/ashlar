# SPDX-License-Identifier: MIT OR Apache-2.0
"""Selected local host implementation; native qualification remains version-scoped."""
from .lifecycle import finish, owned_context
import hashlib, json
from contextlib import contextmanager
from dataclasses import dataclass, field
from ashlar.publication import resolve_publication
from .source_identity import SOURCE_SHA
from .delta_custody import encoded
from .read_only_transport import ReadOnlyTransport, original_read_capacity
from .commerce_binding import compiler_request, LAYOUT_SHA
from .ordinary_query_connection import OrdinaryQueryConnectionFactory, acquire_ordinary_query_connection

@dataclass(frozen=True)
class OpenedCommerceReader:
    """Original port correspondence only; every use needs the active owner."""
    provider: object
    driver: object
    admission: object
    independent_expected: dict
    context: object
    original_native_files: dict
    native_files: object
    aliases: dict
    original_report: dict
    model: bytes
    graph: bytes
    bindings: dict
    manifest: dict
    request_bytes: bytes
    manifest_bytes: bytes
    publication: object
    _original: str = field(init=False, repr=False)
    _ports: tuple = field(init=False, repr=False)

    def __post_init__(self):
        from .driver import NativeDriver
        from .commerce_admission import CommerceAdmission
        from .supply_chain_request import source_snapshot
        from pathlib import Path
        if (type(self.provider) is not PublicationProvider or type(self.driver) is not NativeDriver
                or type(self.admission) is not CommerceAdmission
                or self.provider.driver is not self.driver or self.provider.context is not self.context
                or self.driver.context is not self.context or self.driver.source_admission is not self.admission
                or any(type(raw) is not bytes for raw in (self.model,self.graph,self.request_bytes,self.manifest_bytes))
                or not callable(self.native_files) or type(self.publication)is not type(Path())
                or not self.publication.is_absolute() or self.publication.is_symlink()):
            raise ValueError('Exact original opened reader port correspondence required')
        for name in ('independent_expected','original_native_files','aliases','original_report','bindings','manifest'):
            object.__setattr__(self,name,source_snapshot(getattr(self,name)))
        object.__setattr__(self,'_ports',self._port_identity())
        object.__setattr__(self,'_original',self._correspondence())

    def _port_identity(self):
        return (self.provider,self.driver,self.admission,self.context,self.driver.transport,
                self.driver.policy,self.provider.ordinary_session,getattr(self.driver,'query_source_factory',None),self.driver.source_sessions)

    def _correspondence(self):
        from .supply_chain_request import source_snapshot
        from .evolution_producer import snapshot
        from .delta_custody import DeltaTarget
        if any(type(t)is not DeltaTarget for t in self.driver.transport.targets.values()):raise PermissionError('Exact original native target carriers required')
        if self.publication.is_symlink()or not self.publication.is_dir():raise PermissionError('Original publication directory required')
        info=self.publication.stat()
        if json.loads(snapshot(self.publication/'report.json',16777216))!=self.original_report:raise PermissionError('Original retained publication report changed')
        return encoded(source_snapshot({'model':self.model.hex(),'graph':self.graph.hex(),
            'request':self.request_bytes.hex(),'manifest_text':self.manifest_bytes.hex(),
            'manifest':self.manifest,'bindings':self.bindings,'report':self.original_report,
            'aliases':self.aliases,'native':self.original_native_files,'expected':self.independent_expected,
            'targets':[{ 'table':t.table,'uuid':t.uuid,'path':str(t.path)} for t in self.driver.transport.targets.values()],
            'admission':self.admission.metadata(),'publication':str(self.publication),
            'publication_identity':[info.st_dev,info.st_ino],
            'registered_source':None if getattr(self.driver,'query_source_factory',None)is None else self.driver.query_source_factory.metadata()}))

    def _require_owned_correspondence(self, context):
        from .driver import NativeDriver
        from .commerce_admission import CommerceAdmission
        from .commerce_query_sources import CommerceQuerySourceFactory
        if (context is not self.context or type(self.provider) is not PublicationProvider
                or type(self.driver) is not NativeDriver or type(self.admission) is not CommerceAdmission
                or self.provider.driver is not self.driver or self.provider.context is not context
                or self.driver.source_admission is not self.admission or self.provider._reader_closed
                or not self.provider.active or not self.driver.pin_held
                or type(self.provider.ordinary_session)is not OrdinaryQueryConnectionFactory
                or type(getattr(self.driver,'query_source_factory',None))is not CommerceQuerySourceFactory):
            raise PermissionError('Current explicit ordinary source and full reader interval required')
        if any(current is not original for current,original in zip(self._port_identity(),self._ports)):
            raise PermissionError('Original actual reader ports replaced')
        self.driver.require(context)
        if self._correspondence()!=self._original or self.provider.request_bytes!=self.request_bytes or self.provider.manifest_bytes!=self.manifest_bytes:
            raise PermissionError('Original opened reader correspondence changed')
    def require_current(self, context):
        self._require_owned_correspondence(context)
        self.driver.validate_manifest(self.manifest,context)
        self.driver.source_admit(json.loads(self.request_bytes))
        self.provider.renew_current_ack(context)
        if self.native_files()!=self.original_native_files:
            raise PermissionError('Original complete native carrier bytes changed')
        return None

    def renew_original_graph_read(self, vector, *, context):
        """Current complete original bytes and ordinary authority, not a grant.

        The commerce GraphFrames owner performs full snapshot admission at both
        held boundaries. This separate closed port never alters query admission.
        """
        self._require_owned_correspondence(context)
        if self.native_files()!=self.original_native_files:
            raise PermissionError('Original complete graph native bytes changed')
        self.driver.renew_original_graph_read_source(vector,context=context)
        self.provider.renew_current_ack(context)
        self._require_owned_correspondence(context)
        if self.native_files()!=self.original_native_files:
            raise PermissionError('Closing original complete graph native bytes changed')
        return None

def original_commerce_reader_roles(report,source_factory):
    """Explicit selected role carriers; historical reports retain old spelling."""
    import re
    if type(report) is not dict:raise ValueError('Exact retained original report required')
    scope=report['protected_ack_scope']
    if type(scope) is not dict or type(scope.get('service_schema')) is not str or type(report.get('source_schema')) is not str:
        raise ValueError('Exact original source namespaces required')
    if report.get('source_ports_profile') == 'ashlar-explicit-original-commerce-source/0.1':
        from .commerce_query_sources import CommerceQuerySourceFactory
        if type(source_factory) is not CommerceQuerySourceFactory:raise ValueError('Explicit original source factory required')
        source_factory.__post_init__()
        reader=report.get('source_role');ack=report.get('ack_role')
        if any(type(value) is not str or not re.fullmatch('[a-z][a-z0-9_]{0,62}',value) for value in (reader,ack)) or reader==ack:
            raise ValueError('Explicit original source/ACK role registration required')
        selected=source_factory.registration
        if selected.source_role!=reader or selected.ack_role!=ack:
            raise ValueError('Original explicit role registration differs')
        return reader,ack
    if any(key in report for key in ('source_ports_profile','source_role','ack_role')):
        raise ValueError('Unknown or incomplete original source role profile')
    return report['source_schema']+'_reader',scope['service_schema'].replace('pipeline_','operator_')

def original_commerce_reader_installation(report,publication):
    """Select original existing installation; opening validates retained registry."""
    from pathlib import Path
    import re
    if type(report) is not dict or type(publication) is not type(Path()):raise ValueError('Exact original layout carriers required')
    if report.get('source_ports_profile')=='ashlar-explicit-original-commerce-source/0.1':
        value=report.get('original_journal_path');installation=report.get('installation_id')
        if type(value) is not str or type(installation) is not str or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9:_-]{0,1023}',installation):raise ValueError('Exact original journal/installation required')
        journal=Path(value)
        if not journal.is_absolute() or str(journal)!=value or journal.resolve()!=journal or journal.is_symlink() or not journal.is_file():raise ValueError('Original existing canonical journal required')
        return journal,installation
    if any(key in report for key in ('source_ports_profile','original_journal_path','installation_id')):raise ValueError('Unknown or incomplete original installation profile')
    return publication/'operations.sqlite','private-original-commerce'

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

class PublicationProvider:

    def renew_current_ack(self, context):
        """Observe the current ordinary ACK inside the already held interval."""
        from .postgres import Session
        if context is not self.context or not self.active or self._reader_closed or self.ordinary_session is None:
            raise PermissionError('Active explicit ordinary ACK interval required')
        self.driver.require(context)
        if not self.driver.pin_held:raise PermissionError('Original complete pin interval required')
        connection=acquire_ordinary_query_connection(self.ordinary_session,self.port['role']);primary=None
        try:self._ack(Session(connection))
        except BaseException as error:primary=error
        finish(primary,[connection.rollback,connection.close])
        return None

    def __init__(self, driver, aliases, scope_port, request_bytes, manifest_bytes, expected_binding, *, context, cleanup_state=None, original_bindings=(), ordinary_session=None):
        if ordinary_session is not None:
            if type(ordinary_session) is not OrdinaryQueryConnectionFactory:
                raise ValueError('ordinary-query-connection-factory-required')
            ordinary_session.__post_init__()
        self.ordinary_session = ordinary_session
        if cleanup_state is not None and type(cleanup_state) is not ReaderCleanupState:
            raise ValueError('invalid-reader-cleanup-state')
        self.cleanup_state = cleanup_state
        self.driver = driver
        self.aliases = dict(aliases)
        self.port = scope_port
        self.context = context
        self.request_bytes = request_bytes
        self.manifest_bytes = manifest_bytes
        self._expected_binding = encoded(expected_binding)
        if type(original_bindings) is not tuple or any(type(raw) is not bytes or len(raw)>1024*1024 for raw in original_bindings):
            raise ValueError('Exact bounded original binding inventory required')
        self._original_bindings = original_bindings
        self._binding_selected = False
        self.active = False
        self._closed_custody = None
        self._reader_closed = False

    @property
    def expected_binding(self):
        return self._expected_binding

    @expected_binding.setter
    def expected_binding(self, raw):
        # Compatibility for existing 0.4.1 callers; never a native authority port.
        if self._binding_selected:
            raise PermissionError('Public original binding selection cannot be replaced')
        self._expected_binding = raw

    def select_original_binding(self, request, *, context):
        """Select captured original carriers; current interval admission remains mandatory.

        The reader factory reconstructs both original supported carrier shapes.
        This method grants no writer, source ACK, schema or native pin authority.
        """
        if context is not self.context or self.active or self._reader_closed:
            raise PermissionError('Original inactive reader context required')
        from .commerce_source_receipt import encode_bounded_commerce_json, parse_commerce_json
        # Exact carriers are checked before serialization or executable equality.
        nodes=0
        def own(value, depth=0):
            nonlocal nodes
            nodes+=1
            if nodes>250000:raise ValueError('Original request node bound exceeded')
            if type(value) is str and len(value)>1024*1024:raise ValueError('Original request text bound exceeded')
            if depth>64:raise ValueError('Original request depth exceeded')
            if type(value) in (str, int, bool) or value is None:return value
            if type(value) is list:return [own(v,depth+1) for v in value]
            if type(value) is dict:
                if any(type(k) is not str for k in value):raise ValueError('Exact request keys required')
                return {k:own(v,depth+1) for k,v in value.items()}
            raise ValueError('Exact original request carriers required')
        snapshot=own(request)
        encode_bounded_commerce_json(snapshot,1024*1024)
        if type(snapshot) is not dict or type(snapshot.get('target')) is not dict:
            raise ValueError('Original request target required')
        target=snapshot['target'];raw=target.get('bindingJson');digest=target.get('bindingSha256')
        if type(raw) is not str or type(digest) is not str:
            raise ValueError('Original binding bytes and digest required')
        body=raw.encode('utf8')
        parse_commerce_json(body,1024*1024)
        if hashlib.sha256(body).hexdigest()!=digest or body not in self._original_bindings:
            raise ValueError('Binding differs from captured original source carriers')
        self._expected_binding=raw
        self._binding_selected=True
        return None

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
                if self.ordinary_session is None:
                    from .connection import connect
                    connection = connect(self.port['role'])
                else:
                    connection = acquire_ordinary_query_connection(self.ordinary_session, self.port['role'])
                primary = None
                admitted = False
                def close_ack():
                    nonlocal closing
                    closing = self._ack(session)
                try:
                    session = Session(connection)
                    opening = self._ack(session)
                    admitted = True
                    self.active = True
                    yield
                except BaseException as error:
                    primary = error
                finally:
                    try:
                        finish(primary, ([close_ack] if admitted else []) + [
                            lambda: _close_observed(connection.rollback, self.cleanup_state, primary),
                            lambda: _close_observed(connection.close, self.cleanup_state, primary)])
                        completed = True
                    finally:
                        self.active = False
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

def original_commerce_reader_bindings(model, bindings, manifest, registry, aliases):
    """Reconstruct closed original reader carrier shapes; bytes grant no authority."""
    from .commerce_path_request import commerce_path_request
    request=compiler_request('',model,bindings,manifest,registry,aliases)
    originals=[request['target']['bindingJson'].encode('utf8')]
    if bindings['profile']=='ashlar-commerce-development-bindings/0.2':
        field_request=compiler_request('',model,bindings,manifest,registry,aliases,fields=True)
        originals.append(field_request['target']['bindingJson'].encode('utf8'))
        complete_request=commerce_path_request('',model,bindings,manifest,registry,aliases,profile='paths-keys')
        originals.append(complete_request['target']['bindingJson'].encode('utf8'))
    return tuple(originals)

@contextmanager
def open_commerce_reader(spark, publication, *, cleanup_state=None, ordinary_session=None, source_factory=None):
    """Open original custody/authority ports without submitting native writes.

    Caller owns Spark lifetime. The private reader retains original journal,
    source/model admission, independent full rows, protected ACK and complete
    pin policy. Consumers must use provider.interval(context); no alias setup
    or user SQL is performed by this factory.
    """
    if ordinary_session is not None:
        if type(ordinary_session) is not OrdinaryQueryConnectionFactory:
            raise ValueError('ordinary-query-connection-factory-required')
        ordinary_session.__post_init__()
    if source_factory is not None:
        from .commerce_query_sources import CommerceQuerySourceFactory
        if type(source_factory) is not CommerceQuerySourceFactory:
            raise ValueError('exact-query-source-factory-required')
        source_factory.__post_init__()
    if cleanup_state is not None:
        if type(cleanup_state) is not ReaderCleanupState or cleanup_state._claimed:
            raise ValueError('invalid-reader-cleanup-state')
        object.__setattr__(cleanup_state, '_claimed', True)
    from pathlib import Path
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
        journal,installation=original_commerce_reader_installation(original_report,publication)
        capacity = original_read_capacity(journal, installation, targets)
        transport = ReadOnlyTransport.open(spark, journal, installation, targets, policy, capacity=capacity)
        (request_raw, artifact_raw) = transport.db.execute('SELECT request,artifact FROM local_publication_artifact').fetchone()
        request = json.loads(request_raw)
        artifact = json.loads(artifact_raw)
        manifest = artifact['manifest']
        if manifest != original_report['native_manifest']:
            raise ValueError('Original immutable artifact/report manifest differs')
        scope = AckScope(**original_report['protected_ack_scope'])
        source = original_report['source_schema']
        reader_role,role=original_commerce_reader_roles(original_report,source_factory)
        port = {'scope': scope, 'source': source, 'reader': reader_role, 'role': role, 'signature': original_report['source_signature_sha256']}
        tables = {t.table.split('.')[-1]: t.table for t in targets}
        driver = NativeDriver(transport, policy, context, tables, {batch.feed: port}, admission.changes, fixture_columns(ROOT), source_admission=admission)
        plan = json.loads(transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?', (request['request_digest'],)).fetchone()[0])
        policy.active = {'request': request, 'steps': plan['selected_steps'], 'expected': expected, 'manifest': manifest, 'publication_id': manifest['publication_id'], 'previous_progress': {}, 'progress': json.loads(manifest['source_progress_json']), 'previous_expected': plan['complete_prior_oracle'], 'generated_steps': plan['generated_steps'], 'elisions': plan['zero_match_elisions']}
        if source_factory is not None:driver.bind_query_sources(source_factory)
        aliases = {t: t.replace('local.', 'spark_catalog.', 1) for t in json.loads(manifest['table_versions_json'])}
        req = compiler_request('SELECT COUNT(*) AS n FROM products p', model, bindings, manifest, original_report['table_registry'], aliases)
        original_bindings = original_commerce_reader_bindings(model, bindings, manifest, original_report['table_registry'], aliases)
        provider = PublicationProvider(driver, aliases, port, request_raw.encode(), encoded(manifest).encode(), json.loads(req['target']['bindingJson']), context=context, cleanup_state=cleanup_state, original_bindings=tuple(original_bindings), ordinary_session=ordinary_session)
        yield OpenedCommerceReader(provider=provider, driver=driver, admission=admission, independent_expected=expected, context=context, original_native_files=original_native, native_files=native_files, aliases=aliases, original_report=original_report, model=model, graph=graph, bindings=bindings, manifest=manifest, request_bytes=request_raw.encode(), manifest_bytes=encoded(manifest).encode(),publication=publication.resolve())
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
