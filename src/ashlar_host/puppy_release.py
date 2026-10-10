"""Immutable release-specific PuppyGraph staging under supplied native authority.

This cooperating process owns handle selection, not a durable remote latest alias.
The adapter must execute real schema activation and exact native observations;
inert ports establish ownership controls only.
"""
from dataclasses import dataclass
from contextlib import contextmanager
import hashlib
import json
from typing import Protocol
from threading import RLock
from ashlar.graph_release import GraphRelease
from .lifecycle import owned_context

class PuppyReleaseError(ValueError):
    pass


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PuppyReleaseError('Duplicate original graph member')
        result[key] = value
    return result


def _decode(raw):
    if type(raw) is not bytes or len(raw) > 16 * 1024 * 1024:
        raise PuppyReleaseError('Bounded exact graph bytes required')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=_object,
        parse_constant=lambda value: (_ for _ in ()).throw(PuppyReleaseError('Nonfinite graph carrier')))


def _encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode('utf-8')


@dataclass(frozen=True)
class PuppyReleaseActivation:
    release_sha256: str
    engine_id: str
    schema_sha256: str
    carrier_sha256: str
    catalog_visibility: str = "present"

    def __post_init__(self):
        if (any(type(value) is not str or not value for value in
                (self.release_sha256, self.engine_id, self.schema_sha256, self.carrier_sha256))
                or any(len(value) != 64 or any(c not in "0123456789abcdef" for c in value)
                       for value in (self.release_sha256, self.schema_sha256, self.carrier_sha256))
                or type(self.catalog_visibility) is not str or self.catalog_visibility not in ("present", "omitted")
):
            raise PuppyReleaseError("Exact engine/carrier/schema activation custody required")


@dataclass(frozen=True)
class PuppyReleaseHandle:
    release: GraphRelease
    activation: PuppyReleaseActivation = None

    def __post_init__(self):
        if (type(self.release) is not GraphRelease or type(self.release.payload) is not bytes
                or len(self.release.payload) > 16 * 1024 * 1024
                or type(self.release.sha256) is not str
                or hashlib.sha256(self.release.payload).hexdigest() != self.release.sha256):
            raise PuppyReleaseError('Exact original release custody required')
        if self.activation is not None:
            if type(self.activation) is not PuppyReleaseActivation:
                raise PuppyReleaseError("Exact typed native activation required")
            self.activation.__post_init__()
            if self.activation.release_sha256 != self.release.sha256:
                raise PuppyReleaseError("Original release activation differs")
        value = _decode(self.release.payload)
        if type(value) is not dict or value.get('format') != 'ashlar-graph-release/0.1':
            raise PuppyReleaseError('Original graph release profile required')
        for role in ('nodes', 'edges'):
            rows = value.get(role)
            if type(rows) is not list or len(rows) > 10000 or any(type(row) is not dict for row in rows):
                raise PuppyReleaseError('Complete bounded original graph rows required')
        # Complete canonical and publication lineage admission belongs to the
        # mandatory independent release policy, not byte hashes or this handle.

    @property
    def node_label(self): return 'Ashlar' + self.release.sha256 + 'Node'
    @property
    def edge_label(self): return 'Ashlar' + self.release.sha256 + 'Edge'


class PuppyReleaseAdapter(Protocol):
    def stage(self, handle: PuppyReleaseHandle, context: object) -> PuppyReleaseActivation:
        """Activate only this release's immutable labels/carrier; no latest alias."""
    def observe(self, handle: PuppyReleaseHandle, context: object) -> bytes:
        """Return complete exact native rows and incident identities as JSON."""


class PuppyReleasePolicy(Protocol):
    def writer(self, context: object): ...
    def admit(self, handle: PuppyReleaseHandle, context: object) -> None:
        """Independently admit original publication/custody and native authority."""


class PuppyReleaseOwner:
    def __init__(self, adapter: PuppyReleaseAdapter, policy: PuppyReleasePolicy):
        if (not all(callable(getattr(adapter, name, None)) for name in ('stage', 'observe'))
                or not all(callable(getattr(policy, name, None)) for name in ('writer', 'admit'))):
            raise PuppyReleaseError('Explicit native adapter and independent release policy required')
        self.adapter = adapter
        self.policy = policy
        self._active = None
        self._lock = RLock()

    def _admit(self, handle, context):
        if type(handle) is not PuppyReleaseHandle:
            raise PuppyReleaseError('Exact immutable original handle required')
        handle.__post_init__()
        if self.policy.admit(handle, context) is not None:
            raise PuppyReleaseError('Independent release admission incomplete')

    def _read(self, handle, context):
        self._admit(handle, context)
        if handle.activation is None:
            raise PuppyReleaseError('Actual complete native activation required')
        raw = self.adapter.observe(handle, context)
        actual = _decode(raw)
        required = {'engine', 'version', 'engine_id', 'schema_json', 'carrier_sha256',
                    'catalog_visibility', 'node_label', 'edge_label', 'nodes', 'edges'}
        if (type(actual) is not dict or set(actual) != required or actual['engine'] != 'PuppyGraph'
                or type(actual['version']) is not str or not actual['version']
                or type(actual['engine_id']) is not str or actual['engine_id'] != handle.activation.engine_id
                or type(actual['carrier_sha256']) is not str or actual['carrier_sha256'] != handle.activation.carrier_sha256
                or type(actual['schema_json']) is not str
                or hashlib.sha256(actual['schema_json'].encode('utf-8')).hexdigest() != handle.activation.schema_sha256
                or type(actual['catalog_visibility']) is not str
                or actual['catalog_visibility'] != handle.activation.catalog_visibility
                or actual['node_label'] != handle.node_label or actual['edge_label'] != handle.edge_label):
            raise PuppyReleaseError('Exact named engine and release observation required')
        schema = _decode(actual["schema_json"].encode("utf-8"))
        expected_keys = {"node", "edge"} | ({"catalog"} if handle.activation.catalog_visibility == "present" else set())
        if (type(schema) is not dict or set(schema) != expected_keys
                or ("catalog" in schema and (type(schema["catalog"]) is not list or not schema["catalog"]))
                or type(schema["node"]) is not list or len(schema["node"]) != 1
                or type(schema["edge"]) is not list or len(schema["edge"]) != 1
                or type(schema["node"][0]) is not dict or type(schema["edge"][0]) is not dict
                or schema["node"][0].get("label") != handle.node_label
                or schema["edge"][0].get("label") != handle.edge_label
                or schema["edge"][0].get("fromNodeLabel") != handle.node_label
                or schema["edge"][0].get("toNodeLabel") != handle.node_label):
            raise PuppyReleaseError("Original release-specific native schema differs")
        original = _decode(handle.release.payload)
        for role, label in (('nodes', handle.node_label), ('edges', handle.edge_label)):
            rows = actual[role]
            if type(rows) is not list or len(rows) != len(original[role]):
                raise PuppyReleaseError('Complete native graph inventory differs')
            values = []
            for item in rows:
                fields = {'row', 'native_id'} | ({'native_source', 'native_target'} if role == 'edges' else set())
                if type(item) is not dict or set(item) != fields or type(item['row']) is not dict:
                    raise PuppyReleaseError('Exact native identity/value observation required')
                row = item['row']
                if (type(row.get('graph_id')) is not str or type(item['native_id']) is not str
                        or item['native_id'] != label + '[' + row['graph_id'] + ']'):
                    raise PuppyReleaseError('Native release identity differs')
                if role == 'edges' and (any(type(row.get(key)) is not str for key in ('src', 'dst'))
                        or any(type(item[key]) is not str for key in ('native_source', 'native_target'))
                        or item['native_source'] != handle.node_label + '[' + row['src'] + ']'
                        or item['native_target'] != handle.node_label + '[' + row['dst'] + ']'):
                    raise PuppyReleaseError('Native incident endpoint differs')
                values.append(_encode(row))
            if sorted(values) != sorted(_encode(row) for row in original[role]):
                raise PuppyReleaseError('Complete native identity/value oracle differs')
        self._admit(handle, context)
        return raw

    def refresh(self, release, *, context):
        candidate = PuppyReleaseHandle(release)
        with self._lock:
            with owned_context(self.policy.writer(context)) as permit:
                if permit is not None:
                    raise PuppyReleaseError('Writer admission incomplete')
                self._admit(candidate, context)
                if self._active is not None:
                    self._read(self._active, context)
                activation = self.adapter.stage(candidate, context)
                if type(activation) is not PuppyReleaseActivation:
                    raise PuppyReleaseError('Native staging outcome incomplete')
                candidate = PuppyReleaseHandle(release, activation)
                self._read(candidate, context)
                if self._active is not None:
                    self._read(self._active, context)
            # Selection remains locally serialized after native writer cleanup.
            self._active = candidate
            return candidate

    def read(self, handle, *, context):
        with self._lock:
            with owned_context(self.policy.writer(context)) as permit:
                if permit is not None:
                    raise PuppyReleaseError('Writer admission incomplete')
                raw = self._read(handle, context)
            return raw

    def current(self, *, context):
        with self._lock:
            if self._active is None:
                raise PuppyReleaseError('No complete admitted native release selected')
            self.read(self._active, context=context)
            return self._active
