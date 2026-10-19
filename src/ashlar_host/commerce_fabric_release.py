# SPDX-License-Identifier: MIT OR Apache-2.0
"""Compose actual commerce source/ACK custody with an owned Fabric graph interval.

Fabric HTTP and loaded-graph mutation/refresh exclusion belong to their actual
owners. This bridge cannot mint a native snapshot lease from metadata or hashes.
"""
from contextlib import contextmanager, nullcontext
from .commerce_graph_release import hold_original_commerce_graph_release
from .fabric_gql import FabricGraphBinding, execute_gql
from .lifecycle import finish, owned_context


class _CommerceFabricPolicy:
    def __init__(self, held, binding, query, graph_policy, graph_context, http_context):
        self.held = held
        self.binding = binding
        self.query = query
        self.graph_policy = graph_policy
        self.graph_context = graph_context
        self.http_context = http_context
        self.active = False

    def same(self, binding, context):
        if binding != self.binding or context is not self.http_context:
            raise PermissionError('Original Fabric binding and HTTP context required')
        if self.held.release.sha256 != self.binding.release_sha256:
            raise PermissionError('Original held publication release differs')

    def renew_graph(self):
        if self.graph_policy.admit(self.binding, self.query, self.graph_context) is not None:
            raise PermissionError('Actual loaded Fabric graph admission must complete')

    @contextmanager
    def hold(self, binding, context):
        self.same(binding, context)
        if self.active:
            raise PermissionError('Overlapping Fabric query interval refused')
        self.held.renew()
        primary = None
        try:
            with owned_context(self.graph_policy.hold(binding, self.graph_context)) as permit:
                if permit is not None:
                    raise PermissionError('Actual loaded Fabric hold must complete')
                self.active = True
                body = None
                try:
                    self.admit(binding, self.query, context)
                    yield
                except BaseException as error:
                    body = error
                finally:
                    self.active = False
                finish(body, [self.held.renew, self.renew_graph])
        except BaseException as error:
            primary = error
        finish(primary, [self.held.renew])

    def admit(self, binding, query, context):
        self.same(binding, context)
        if not self.active or query != self.query:
            raise PermissionError('Original active Fabric query required')
        # Independently attempt both owners, including on a refusal path.
        finish(None, [self.held.renew, self.renew_graph])


def execute_commerce_fabric_gql(opened, binding, query, *, transport,
                               transport_context, loaded_graph_policy,
                               loaded_graph_context, **limits):
    """Return only after the actual source/ACK and loaded-graph holds close.

    loaded_graph_policy must implement the existing Fabric hold/admit ports and
    independently own this exact loaded release throughout the query. Metadata
    equality, a Python lock or a no-op policy cannot supply that native authority.
    The caller owns ordinary HTTP acquisition and must close it before releasing
    a final workflow report. This function creates no roles, grants or resources.
    """
    if type(binding) is not FabricGraphBinding:
        raise ValueError('Exact selected Fabric binding required')
    binding.__post_init__()
    if not all(callable(getattr(loaded_graph_policy, name, None)) for name in ('hold', 'admit')):
        raise ValueError('Actual independent loaded Fabric graph owner required')
    result = None
    with owned_context(hold_original_commerce_graph_release(opened)) as held:
        if held.release.sha256 != binding.release_sha256:
            raise PermissionError('Fabric graph must match the actual held publication release')
        source_interval = getattr(loaded_graph_policy, 'source_interval', None)
        with owned_context(source_interval(held) if callable(source_interval) else nullcontext()):
            policy = _CommerceFabricPolicy(held, binding, query, loaded_graph_policy,
                                          loaded_graph_context, transport_context)
            result = execute_gql(binding, query, transport=transport, policy=policy,
                                 context=transport_context, **limits)
    return result
