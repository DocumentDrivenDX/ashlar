# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual original commerce publication custody for conditional current queries.

This bridge preserves the ordinary source/ACK/full-vector reader interval. It
never converts accepted source mappings into UMF Subject/action/disclosure rights.
"""
from __future__ import annotations
from contextlib import contextmanager
import json
from ashlar.manifest import manifest_pin_vector
from .publication_reader import OpenedCommerceReader
from .lifecycle import finish, owned_context
from .delta_custody import encoded


class HeldPublicationCurrent:
    """One actual acquired publication reader interval, not imported evidence."""
    def __init__(self, opened):
        if type(opened) is not OpenedCommerceReader:
            raise TypeError('Actual original opened commerce reader required')
        opened.require_current(opened.context)
        self.opened = opened
        self.context = opened.context
        self.original_ports = (opened.provider, opened.driver, opened.admission,
            opened.driver.transport, opened.driver.policy)
        self.original_manifest = encoded(opened.manifest)
        self.original_request = opened.request_bytes
        self.original_model = opened.model
        self.original_graph = opened.graph
        self.original_vector = self._current_vector()
        self.active = True

    def _current_vector(self):
        opened = self.opened
        versions = json.loads(opened.manifest['table_versions_json'])
        return manifest_pin_vector(opened.manifest,
            {table: opened.driver.transport.targets[table].uuid for table in versions},
            authority='private-local-process')

    def _require_correspondence(self, context):
        opened = self.opened
        if (not self.active or context is not self.context or opened.context is not context
                or type(opened) is not OpenedCommerceReader
                or any(current is not original for current, original in zip(
                    (opened.provider, opened.driver, opened.admission,
                     opened.driver.transport, opened.driver.policy), self.original_ports))):
            raise PermissionError('Original held publication/current source ports required')
        if (encoded(opened.manifest) != self.original_manifest
                or opened.request_bytes != self.original_request
                or opened.model != self.original_model or opened.graph != self.original_graph
                or self._current_vector() != self.original_vector):
            raise PermissionError('Original complete publication/source correspondence changed')


    def require_current(self, context):
        """Full snapshot admission, retained at both publication boundaries."""
        self._require_correspondence(context)
        self.opened.require_current(context)
        self._require_correspondence(context)

    def renew_current(self, context):
        """Fresh original-byte/source/ACK/native identity renewal, never a grant.

        Only valid inside the same fully admitted reader interval. The existing
        graph renewal port checks complete native bytes, all six installation
        identities and the exact held data vector without redecoding snapshots.
        """
        self._require_correspondence(context)
        self.opened.renew_original_graph_read(self.original_vector, context=context)
        self._require_correspondence(context)


@contextmanager
def open_publication_current_interval(opened):
    """Hold the real reader, complete native vector and current read-only ACK.

    Staging must be minted inside this interval. The source reader remains the
    owner of native source signatures, roles, current ACK and complete population
    admission; this helper creates no replacement source receipt or boolean grant.
    """
    if type(opened) is not OpenedCommerceReader:
        raise TypeError('Actual original opened commerce reader required')
    with owned_context(opened.provider.interval(opened.context)):
        held = HeldPublicationCurrent(opened)
        primary = None
        try: yield held
        except BaseException as error: primary = error
        def close():
            try: held.require_current(opened.context)
            finally: held.active = False
        finish(primary, [close])


class PublicationCurrentOwner:
    """Compose one actual publication/staging/native interval without new grants.

    Callers must close open_publication_current_interval before exposing a final
    workflow report. Accepted product mappings are source custody, not Subject,
    transformed-disclosure or original-action authorization.
    """
    def __init__(self, held_publication, stage):
        from .publication_current_stage import PublicationCurrentStage
        from .local_current_development import (LocalCurrentDevelopmentOwner,
            local_current_development_context)
        if type(held_publication) is not HeldPublicationCurrent or type(stage) is not PublicationCurrentStage:
            raise TypeError('Actual held publication and factory-owned source stage required')
        self.publication = held_publication; self.stage = stage
        self.child = stage.original_child; self.original_binding = stage.original_binding
        self.context = held_publication.context; self.spark = stage.spark
        self.local_context = local_current_development_context()
        self._require_stage()
        self.local = LocalCurrentDevelopmentOwner(self.spark, self.child,
            self.original_binding, self.local_context)

    def _require_stage(self):
        stage = self.stage
        if (stage.held_publication is not self.publication or stage.spark is not self.spark
                or stage.original_child is not self.child
                or stage.original_binding != self.original_binding):
            raise PermissionError('Original publication/stage/query correspondence differs')
        # The nominal stage's public admission performs this exact publication
        # renew_current before its native metadata/full-bag observations. Do
        # not duplicate that same source/ACK/vector check in this wrapper.
        if stage.require_current(self.publication, self.child, self.original_binding, self.context) is not None:
            raise ValueError('Actual staged source admission must complete or raise')

    def backend(self, *, capture_limits, source_limits):
        from .databricks_current_spark import SparkCurrentQueryBackend
        self._require_stage()
        return SparkCurrentQueryBackend(self.spark, self, capture_limits,
            source_limits, mode='contributors', engine='spark')

    @contextmanager
    def open_current_query(self, original_child, authenticated_context):
        if original_child is not self.child or authenticated_context is not self.context:
            raise PermissionError('Original actual publication query/context required')
        self._require_stage()
        with owned_context(self.local.open_current_query(self.child, self.local_context)) as local:
            held = _HeldPublicationQuery(self, local)
            primary = None
            try: yield held
            except BaseException as error: primary = error
            finish(primary, [lambda: held.renew(self.child, self.original_binding, self.context), held.close])


class _HeldPublicationQuery:
    def __init__(self, owner, local):
        self.owner = owner; self.local = local; self.active = True

    def close(self): self.active = False

    def binding(self):
        if not self.active or not self.owner.publication.active:
            raise PermissionError('Original actual publication query interval is closed')
        if self.local.binding() != self.owner.original_binding:
            raise PermissionError('Original local native binding differs')
        return self.owner.original_binding

    def renew(self, child, binding, context):
        if child is not self.owner.child or context is not self.owner.context or binding != self.binding():
            raise PermissionError('Original publication query/context required')
        finish(None, [self.owner._require_stage,
            lambda: self.local.renew(child, binding, self.owner.local_context)])

    def admit(self, child, binding, context):
        self.renew(child, binding, context)
