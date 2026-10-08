"""Pure graph apply planning for explicitly versioned whole-entity sources.

Truss property-feed reconstruction is a separate adapter. Nothing here writes,
publishes, acknowledges or infers an entity version from property positions.
"""
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, Optional, Tuple
from .schema import _json

class ApplyError(ValueError):
    pass

@dataclass(frozen=True, order=True)
class EntityKey:
    source: str
    kind: str
    type_id: int
    id: int

@dataclass(frozen=True)
class EntityState:
    key: EntityKey
    version: int
    schema_revision: str
    props_json: str
    retained_json: str
    endpoints: Optional[Tuple[EntityKey,EntityKey]]=None

@dataclass(frozen=True)
class Change:
    feed: str
    epoch: str
    delivery_id: str
    raw_digest: str
    operation: str
    state: EntityState

@dataclass(frozen=True)
class ApplyState:
    current: Mapping[EntityKey,EntityState]
    history: Mapping[Tuple[EntityKey,int],Change]
    tombstones: Mapping[EntityKey,Change]
    deliveries: Mapping[Tuple[str,str,str],Change]


def empty_state():
    return ApplyState(*[MappingProxyType({}) for _ in range(4)])


def _key(key):
    if not isinstance(key,EntityKey) or not isinstance(key.source,str) or not key.source or key.kind not in ('object','edge'):
        raise ApplyError('Invalid complete entity identity')
    if any(type(v) is not int or not -(2**63)<=v<2**63 for v in (key.type_id,key.id)):
        raise ApplyError('Identity outside selected signed64 profile')


def _change(change):
    state=change.state;_key(state.key)
    if any(not isinstance(x,str) or not x or '\x00' in x for x in [change.feed,change.epoch,change.delivery_id,state.schema_revision]):
        raise ApplyError('Explicit source/revision custody required')
    if not isinstance(change.raw_digest,str) or len(change.raw_digest)!=64 or any(x not in '0123456789abcdef' for x in change.raw_digest):
        raise ApplyError('Exact original source digest required')
    if type(state.version) is not int or not 0<=state.version<2**63 or change.operation not in ('create','replace','delete'):
        raise ApplyError('Explicit supported operation/entity version required')
    for text in [state.props_json,state.retained_json]:
        if not isinstance(text,str) or not isinstance(_json(text.encode('utf-8')),dict):raise ApplyError('Exact property/retained object text required')
    if state.key.kind=='edge':
        if not isinstance(state.endpoints,tuple) or len(state.endpoints)!=2:raise ApplyError('Typed edge endpoints required')
        for endpoint in state.endpoints:
            _key(endpoint)
            if endpoint.kind!='object' or endpoint.source!=state.key.source:raise ApplyError('Invalid endpoint authority')
    elif state.endpoints is not None:raise ApplyError('Object cannot have edge endpoints')


def plan_apply(prior:ApplyState,changes,*,schema_policy):
    """Plan a complete source transaction; return defensive immutable state.

    schema_policy(change) must authorize/admit the exact schema, identity,
    operation and carrier/endpoint constraints, returning None or raising.
    It must be independently qualified; there is no permissive default.
    prior is a trusted complete retained state snapshot. Native integration
    must serialize, revalidate and persist all effects before publication.
    """
    current=dict(prior.current);history=dict(prior.history);tombstones=dict(prior.tombstones);deliveries=dict(prior.deliveries)
    for change in changes:
        _change(change)
        if schema_policy(change) is not None:raise ApplyError('Schema admission policy did not complete')
        key=change.state.key;delivery=(change.feed,change.epoch,change.delivery_id)
        if delivery in deliveries:
            if deliveries[delivery]!=change:raise ApplyError('Source delivery conflict')
            continue
        version_key=(key,change.state.version)
        if version_key in history:raise ApplyError('Entity version has a different original delivery')
        previous=current.get(key);deleted=tombstones.get(key)
        high=previous.version if previous is not None else deleted.state.version if deleted is not None else -1
        if change.state.version<=high:raise ApplyError('Stale or conflicting entity version')
        if change.operation=='create':
            if previous is not None or deleted is not None:raise ApplyError('Create cannot overwrite or resurrect an existing identity')
            current[key]=change.state
        elif change.operation=='replace':
            if previous is None:raise ApplyError('Replace requires a live entity')
            current[key]=change.state
        else:
            if previous is None:raise ApplyError('Delete requires a live entity')
            # The source delete carries the exact prior payload with its new
            # version/revision, so retained current meaning survives removal.
            if (change.state.props_json,change.state.retained_json,change.state.endpoints)!=(previous.props_json,previous.retained_json,previous.endpoints):
                raise ApplyError('Delete carrier differs from exact prior state')
            del current[key];tombstones[key]=change
        deliveries[delivery]=change;history[version_key]=change
    # Validate the complete transaction boundary, allowing endpoints created
    # later in the same transaction without exposing a dangling intermediate.
    for state in current.values():
        if state.key.kind=='edge' and any(endpoint not in current for endpoint in state.endpoints):
            raise ApplyError('Dangling typed endpoint at completed transaction')
    return ApplyState(*[MappingProxyType(x) for x in (current,history,tombstones,deliveries)])
