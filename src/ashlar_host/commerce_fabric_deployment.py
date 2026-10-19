# SPDX-License-Identifier: MIT OR Apache-2.0
"""Held original-commerce staging/creation recovery; no loaded-policy or CAS grant.

Actual source/ACK/native release custody spans projection admission, OneLake
bytes, native creation and explicit load observations. A first portal opening is
an external owned step: no callback or elapsed time can declare it successful.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import base64
import hashlib
import json
from pathlib import Path

from ashlar.graph_release import release_columns
from .commerce_graph_release import hold_original_commerce_graph_release
from .evolution_cli import InvocationFile
from .fabric_deployment_http import (FabricAcceptedCreation, FabricCreatedGraph, open_fabric_deployment_http)
from .fabric_graph_definition import make_fabric_graph_definition
from .fabric_pack_delta import metadata, readback
from .fabric_release_journal import FabricReleaseJournal, JournalRefusal, ReleaseIntent
from .lifecycle import finish, owned_context
from .onelake_release_upload import (OneLakeUploadConfig, OneLakeReleaseFile, OneLakeReleaseTable,
                                     open_onelake_release_upload)

_COMMIT = '_delta_log/00000000000000000000.json'


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class CommerceFabricDeploymentConfig:
    upload: OneLakeUploadConfig
    node_projection: Path
    edge_projection: Path
    node_table: str
    edge_table: str
    display_name: str
    ownership_profile: str

    def __post_init__(self):
        if type(self.upload) is not OneLakeUploadConfig:
            raise ValueError('Exact OneLake upload selection required')
        self.upload.__post_init__()
        for path in (self.node_projection, self.edge_projection):
            if type(path) is not type(Path('/')) or not path.is_absolute() or path.resolve() != path:
                raise ValueError('Canonical existing projection directories required')
        if self.node_projection == self.edge_projection or self.node_projection in self.edge_projection.parents or self.edge_projection in self.node_projection.parents:
            raise ValueError('Separate projection directories required')
        if (type(self.display_name) is not str or not self.display_name.strip() or len(self.display_name) > 256
                or any(ord(c) < 32 or ord(c) == 127 for c in self.display_name)):
            raise ValueError('Explicit bounded display name required')
        if self.ownership_profile != 'private-cooperative-sole-creator/0.1':
            raise ValueError('Explicit private cooperative ownership assumption required')


def _location(config, path):
    return ('abfss://' + config.upload.authentication.workspace_id + '@onelake.dfs.fabric.microsoft.com/'
            + config.upload.lakehouse_id + '/' + path)


def _projection(spark, path, role, expected, maximum):
    before = metadata(spark, str(path))
    columns = release_columns('node' if role == 'nodes' else 'edge')
    properties = [{'name': name, 'type': 'STRING'} for name in columns]
    readback(spark, str(path), properties, [tuple(row[name] for name in columns) for row in expected])
    commit = InvocationFile.read(path / _COMMIT, maximum)
    records = [json.loads(line) for line in commit.raw.splitlines()]
    additions = [item['add']['path'] for item in records if 'add' in item]
    ids = [item['metaData']['id'] for item in records if 'metaData' in item]
    if (not 1 <= len(additions) <= 63 or len(set(additions)) != len(additions) or ids != [before['uuid']]
            or any('remove' in item for item in records)):
        raise ValueError('Original fresh version-zero projection inventory required')
    # OneLakeReleaseFile validates every relative name before local file access.
    for name in additions:
        OneLakeReleaseFile(name, b'validate-name')
        if name == _COMMIT:
            raise ValueError('Data addition cannot name the commit')
    pins = [InvocationFile.read(path / name, maximum) for name in additions] + [commit]
    if metadata(spark, str(path)) != before:
        raise PermissionError('Native projection identity changed during byte acquisition')
    return before, pins, tuple(OneLakeReleaseFile(pin.path.relative_to(path).as_posix(), pin.raw) for pin in pins)


class HeldCommerceFabricDeployment:
    def __init__(self, *args, **kwargs):
        raise PermissionError('Use open_commerce_fabric_deployment')

    def __setattr__(self, name, value):
        if name == '_anchor' and hasattr(self, name):
            raise AttributeError('Original held deployment selection is immutable')
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name == '_anchor':
            raise AttributeError('Original held deployment selection is immutable')
        object.__delattr__(self, name)

    def renew(self):
        if not self._live:
            raise PermissionError('Original held deployment has closed')
        config, facts, held, mutation, selected, tables, pins, maximum = self._anchor
        def intent():
            current = (tuple(vars(config.upload.authentication).items()), config.upload.lakehouse_id,
                config.upload.maximum_file_bytes, config.upload.maximum_table_bytes,
                config.node_projection, config.edge_projection, config.node_table, config.edge_table,
                config.display_name, config.ownership_profile)
            if (self._config is not config or current != facts or self._held is not held
                    or self._mutation is not mutation or self.intent != selected or self.tables != tables
                    or self._pins != pins or self._maximum != maximum
                    or mutation.candidate(selected.candidate_id)[0] != selected):
                raise PermissionError('Original deployment intent/owners changed')
        finish(None, [held.renew, mutation.renew, intent]
               + [lambda pin=pin: pin.renew(maximum) for pin in pins])

    @contextmanager
    def admission_interval(self):
        """Expose nominal actual source/mutation custody without reacquiring it."""
        self.renew()
        interval = object.__new__(HeldCommerceFabricAdmission)
        object.__setattr__(interval, '_deployment', self)
        object.__setattr__(interval, '_live', True)
        primary = None
        try:
            yield interval
        except BaseException as error:
            primary = error
        try:
            finish(primary, [self.renew])
        finally:
            object.__setattr__(interval, '_live', False)

    def stage(self):
        """Write both fresh tables once; partial output survives a refusal."""
        self.renew()
        with owned_context(open_onelake_release_upload(self._config.upload, self._mutation, self.intent.candidate_id)) as upload:
            result = tuple(upload.upload(table) for table in self.tables)
        self.renew()
        return result

    def staging_readback(self):
        """Explicit recovery of exact existing bytes, with no overwrite/repair."""
        self.renew()
        with owned_context(open_onelake_release_upload(self._config.upload, self._mutation, self.intent.candidate_id)) as upload:
            result = tuple(upload.readback(table) for table in self.tables)
        self.renew()
        return result

    def submit_creation(self):
        """Verify staged bytes, commit unknown, submit once, retain real response."""
        self.renew()
        if self._mutation.candidate(self.intent.candidate_id)[1] != 'recorded':
            raise PermissionError('Original submission already attempted; recover its handle')
        self.staging_readback()
        self._mutation.begin_creation(self.intent.candidate_id)
        # A failure at acquisition/submission/closing leaves unknown or retained
        # original response. No creation is retried, including across restart.
        with owned_context(open_fabric_deployment_http(self._config.upload.authentication)) as http:
            response = http.create_graph_model(self.intent.mapping_json.encode())
            if type(response) is FabricAcceptedCreation:
                self._mutation.record_creation_operation(self.intent.candidate_id, response.operation_id)
            elif type(response) is FabricCreatedGraph:
                if json.loads(response.response.body).get('displayName') != self._config.display_name:
                    raise PermissionError('Original created display name differs')
                self._mutation.record_created_synchronous(self.intent.candidate_id, response.graph_model_id, response.response.body)
            else:
                raise PermissionError('Actual native creation response required')
        self.renew()
        return response

    def observe_creation(self):
        """One explicit original-operation observation; never recreate unknown work."""
        self.renew()
        retained = self._mutation.creation(self.intent.candidate_id)
        if retained.graph_model_id is not None:
            return retained
        if retained.kind != 'asynchronous' or retained.operation_handle is None:
            raise PermissionError('No original creation handle; unknown outcome refuses')
        with owned_context(open_fabric_deployment_http(self._config.upload.authentication)) as http:
            state = http.creation_state(retained.operation_handle)
            value = json.loads(state.body)
            if value['status'] == 'Succeeded':
                result = http.creation_result(retained.operation_handle)
                item = json.loads(result.body)
                if item.get('displayName') != self._config.display_name:
                    raise PermissionError('Original created display name differs')
                self._mutation.record_created(self.intent.candidate_id, item['id'], retained.operation_handle)
        self.renew()
        return state

    def observe_initial_load(self):
        """Retain the sole observed original job before one explicit status read.

        Missing first-portal initialization or ambiguous jobs refuses. This method
        never opens a portal, starts refresh, or declares loaded bag admission.
        """
        self.renew()
        created = self._mutation.creation(self.intent.candidate_id)
        if created.graph_model_id is None:
            raise PermissionError('Original created graph item required')
        with owned_context(open_fabric_deployment_http(self._config.upload.authentication)) as http:
            listing = http.item_jobs(created.graph_model_id)
            value = json.loads(listing.body)
            jobs = value['value']
            if (value.get('continuationToken') or value.get('continuationUri') or len(jobs) != 1
                    or jobs[0].get('jobType') != 'Refresh' or jobs[0].get('invokeType') != 'Manual'):
                raise PermissionError('Original sole initial load not observed; no refresh fallback')
            original = self._mutation.first_load(self.intent.candidate_id)
            if original is None:
                self._mutation.record_first_load(self.intent.candidate_id, jobs[0]['id'])
                original = jobs[0]['id']
            if original != jobs[0]['id']:
                raise PermissionError('Unexpected load/refresh differs from original job')
            result = http.item_job(created.graph_model_id, original)
        self.renew()
        return result


@contextmanager
def open_commerce_fabric_deployment(spark, opened, journal, config, candidate_id):
    """Hold genuine source/ACK/native custody and mutation lease through all steps.

    The original local Delta projections must already exist. They are independently
    admitted against the current held release; archived export metadata cannot
    substitute. Closing rechecks complete native projection bags and identities.
    No activation or loaded-graph policy is minted by this preparation interval.
    """
    if type(journal) is not FabricReleaseJournal or type(config) is not CommerceFabricDeploymentConfig:
        raise ValueError('Exact deployment journal/configuration required')
    config.__post_init__()
    protected = [Path(target.path).resolve() for target in opened.driver.transport.targets.values()]
    protected += [journal.root, Path(opened.publication).resolve()]
    canonical_journal = getattr(opened.driver.transport, 'journal_path', None)
    if canonical_journal is not None:
        protected.append(Path(canonical_journal).resolve())
    protected += [Path(path).resolve() for path, _ in opened.admission.originals]
    for projection in (config.node_projection, config.edge_projection):
        if any(projection == path or projection in path.parents or path in projection.parents for path in protected):
            raise ValueError('Projection paths must be separate from original custody')
    with owned_context(hold_original_commerce_graph_release(opened)) as held:
        with journal.mutation() as mutation:
            value = json.loads(held.release.payload)
            maximum = min(config.upload.maximum_file_bytes, config.upload.authentication.maximum_request_bytes)
            facts, pins, tables = [], [], []
            for role, path, remote in [('nodes', config.node_projection, config.node_table), ('edges', config.edge_projection, config.edge_table)]:
                held.renew()
                identity, captured, files = _projection(spark, path, role, value[role], maximum)
                facts.append((path, role, identity))
                pins.extend(captured)
                tables.append(OneLakeReleaseTable(remote, files))
            if facts[0][2]['uuid'] == facts[1][2]['uuid']:
                raise PermissionError('Distinct native projection UUIDs required')
            locations = [_location(config, table.path) for table in tables]
            documents = make_fabric_graph_definition(*locations)
            definition = {'format': 'json', 'parts': [
                {'path': name, 'payloadType': 'InlineBase64', 'payload': base64.b64encode(_json(document).encode()).decode()}
                for name, document in sorted(documents.items())]}
            payload = _json({'displayName': config.display_name, 'definition': definition})
            intent = ReleaseIntent(candidate_id, config.upload.authentication.workspace_id, held.release.sha256,
                _json({'publication_id': value['publication']['publication_id'],
                       'manifest_sha256': hashlib.sha256(_json(value['publication']).encode()).hexdigest(),
                       'snapshots': value['snapshots']}), payload,
                tuple((location, table.sha256()) for location, table in zip(locations, tables)))
            try:
                prior = mutation.candidate(candidate_id)[0]
            except JournalRefusal as error:
                if error.args != ('missing original intent',):
                    raise
                mutation.record_intent(intent)
            else:
                if prior != intent:
                    raise PermissionError('Original deployment recovery intent differs')
            owner = object.__new__(HeldCommerceFabricDeployment)
            owner._live = True
            owner._config, owner._held, owner._mutation = config, held, mutation
            owner.intent, owner.tables = intent, tuple(tables)
            owner._pins, owner._maximum = tuple(pins), maximum
            selected = (tuple(vars(config.upload.authentication).items()), config.upload.lakehouse_id,
                config.upload.maximum_file_bytes, config.upload.maximum_table_bytes,
                config.node_projection, config.edge_projection, config.node_table, config.edge_table,
                config.display_name, config.ownership_profile)
            owner._anchor = (config, selected, held, mutation, intent, tuple(tables), tuple(pins), maximum)
            primary = None
            try:
                owner.renew()
                yield owner
            except BaseException as error:
                primary = error
            def projection(fact):
                path, role, identity = fact
                observed, _, _ = _projection(spark, path, role, value[role], maximum)
                if observed != identity:
                    raise PermissionError('Closing original native projection identity differs')
            try:
                finish(primary, [owner.renew] + [lambda fact=fact: projection(fact) for fact in facts])
            finally:
                owner._live = False


class HeldCommerceFabricAdmission:
    """Nominal, expiring view of the real enclosing deployment custody."""
    def __init__(self, *args, **kwargs):
        raise PermissionError('Use deployment.admission_interval()')

    def __setattr__(self, name, value):
        raise AttributeError('Original admission interval is immutable')

    def __delattr__(self, name):
        raise AttributeError('Original admission interval is immutable')

    def renew(self):
        if not self._live:
            raise PermissionError('Original admission interval expired')
        self._deployment.renew()

    @property
    def release(self):
        self.renew()
        return self._deployment._anchor[2].release

    @property
    def mutation(self):
        self.renew()
        return self._deployment._anchor[3]

    @property
    def configuration(self):
        self.renew()
        return self._deployment._anchor[0]

    @property
    def intent(self):
        self.renew()
        return self._deployment._anchor[4]
