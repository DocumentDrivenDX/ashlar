"""Complete original finite graph release inside the existing read owner.

No engine, source, publisher or installation lease is created here. Returned
bytes remain subject to the caller's outer source/runtime/installation closing.
"""
from collections import Counter
from contextlib import contextmanager
import json

from ashlar.graph_release import GraphRelease, read_graph_release
from ashlar.manifest import manifest_pin_vector
from .delta_custody import encoded
from .lifecycle import finish, owned_context
from .supply_chain_common_read import OpenedSupplyChainCommonRead
from .supply_chain_projection import original_supply_chain_oracle
from .supply_chain_request import source_snapshot


def _columns(opened):
    # Public original carrier schema, no SDK inspection or caller validator.
    return {role:[list(pair)for pair in columns]for role,columns in opened.columns.items()}


def _vector_targets(opened):
    value=opened.vector
    if (type(value)is not dict or any(type(pair)is not tuple or len(pair)!=2
            or type(pair[0])is not str or type(pair[1])is not int for pair in value.values())):
        raise PermissionError('Exact original common UUID/version vector required')
    return dict(value)


class _OriginalGraphPolicy:
    def __init__(self, opened):
        if type(opened)is not OpenedSupplyChainCommonRead:
            raise ValueError('Actual original common read owner required')
        self.opened=opened
        self.driver,self.provider,self.context=opened.driver,opened.provider,opened.context
        self.transport,self.source=self.driver.transport,opened.source
        self.manifest=source_snapshot(opened.manifest)
        self.aliases=source_snapshot(opened.aliases)
        self.model,self.graph=opened.model,opened.graph
        self.bindings=source_snapshot(opened.bindings)
        self.columns=_columns(opened)
        versions=json.loads(self.manifest['table_versions_json'])
        self.vector=manifest_pin_vector(self.manifest,
            {table:self.transport.targets[table].uuid for table in versions},
            authority='private-local-process')
        if _vector_targets(opened)!=dict(self.vector.targets):
            raise PermissionError('Original complete common vector differs')
        self.roles={self.driver.tables[role]:role for role in ('object_current','edge_current')}
        self.expected=source_snapshot(original_supply_chain_oracle(
            self.model,self.graph,self.bindings,self.source.batch,self.columns))
        if (len(self.expected['object_current']),len(self.expected['edge_current']))!=(20,23):
            raise ValueError('Complete original twenty-node twenty-three-edge graph required')
        self.bags={role:Counter(map(encoded,self.expected[role]))for role in ('object_current','edge_current')}

    def require(self, context):
        if context is not self.context:
            raise PermissionError('Original common read context required')
        if self.opened.require_current(context)is not None:
            raise PermissionError('Current original common read admission incomplete')
        self._correspondence(context)
        if self.provider.authorize(context,self.manifest['publication_id'],tuple(self.vector.targets))is not None:
            raise PermissionError('Original native graph authorization incomplete')

    def _correspondence(self, context):
        if context is not self.context:
            raise PermissionError('Original common read context required')
        if (self.opened.driver is not self.driver or self.opened.provider is not self.provider
                or self.opened.source is not self.source or self.driver.transport is not self.transport
                or self.opened.context is not self.context or _vector_targets(self.opened)!=dict(self.vector.targets)
                or source_snapshot(self.opened.manifest)!=self.manifest
                or source_snapshot(self.opened.aliases)!=self.aliases
                or source_snapshot(self.opened.bindings)!=self.bindings
                or (self.opened.model,self.opened.graph)!=(self.model,self.graph)
                or _columns(self.opened)!=self.columns):
            raise PermissionError('Original graph read owning inputs changed')
        current=manifest_pin_vector(self.manifest,
            {table:self.transport.targets[table].uuid for table in self.vector.targets},
            authority='private-local-process')
        if current!=self.vector:
            raise PermissionError('Original complete native graph vector changed')

    def bind_descriptor(self, descriptor, vector, context):
        self.require(context)
        if vector!=self.vector or source_snapshot(dict(descriptor.raw))!=self.manifest:
            raise PermissionError('Original descriptor and whole vector required')

    def authorize_graph_row(self, descriptor, table, row, context):
        self.opened.require_held_correspondence(context)
        self._correspondence(context)
        if source_snapshot(dict(descriptor.raw))!=self.manifest:
            raise PermissionError('Original graph row descriptor differs')
        role=self.roles.get(table)
        if role is None or encoded(dict(row))not in self.bags[role]:
            raise PermissionError('Original complete canonical graph row differs')

    def authorize_graph_result(self, descriptor, nodes, edges, context):
        self.bind_descriptor(descriptor,self.vector,context)
        for role,rows in (('object_current',nodes),('edge_current',edges)):
            canonical=[{k:v for k,v in row.items()if k not in ('graph_id','src','dst')}for row in rows]
            if Counter(map(encoded,canonical))!=self.bags[role]:
                raise PermissionError('Original full graph occurrence bag differs')


class _OriginalHeldPins:
    """Use only this owner's actual acquired read interval; register no grant."""
    def __init__(self,policy):self.policy=policy
    @contextmanager
    def hold(self,vector,*,context):
        self.policy.require(context)
        if vector!=self.policy.vector:
            raise PermissionError('Original complete held graph vector required')
        primary=None
        try:yield
        except BaseException as error:primary=error
        finish(primary,[lambda:self.policy.require(context)])


def _read(policy):
    policy.require(policy.context)
    return read_graph_release(policy.transport,policy.provider,
        _OriginalHeldPins(policy),policy.vector,policy,
        publication_id=policy.manifest['publication_id'],
        node_table=next(table for table,role in policy.roles.items()if role=='object_current'),
        edge_table=next(table for table,role in policy.roles.items()if role=='edge_current'),
        context=policy.context,supported_profiles=['ashlar-delta/0.3'],
        supported_revisions={k:[v]for k,v in json.loads(policy.manifest['schema_revisions_json']).items()},
        max_nodes=20,max_edges=23)


def read_original_supply_chain_graph_release(opened):
    """Return only after this full original opening/closing interval succeeds."""
    if type(opened)is not OpenedSupplyChainCommonRead:
        raise ValueError('Actual original common read owner required')
    context=opened.context
    if opened.require_current(context)is not None:
        raise PermissionError('Current original common read admission incomplete')
    policy=_OriginalGraphPolicy(opened)
    release=None;primary=None
    with owned_context(opened.read_interval(context)):
        def closing_graph():
            closing=_read(policy)
            if release is not None and closing!=release:
                raise PermissionError('Original closing whole graph release changed')
        try:
            release=_read(policy)
            if type(release)is not GraphRelease:
                raise ValueError('Exact core graph release required')
        except BaseException as error:primary=error
        finish(primary,[lambda:policy.require(context),closing_graph])
    # The provider's original full native/source interval is closed here. The
    # separately held outer installation/runtime/source is still caller-owned.
    policy.provider.closed_interval_custody(context)
    if opened.require_current(context)is not None:
        raise PermissionError('Closing original common read admission incomplete')
    return release
