# SPDX-License-Identifier: MIT OR Apache-2.0
"""Truss coordinator retains original plans before a durable prepared phase."""
from ashlar.stored_publisher import StoredPublisherBackend


class TrussPublisherBackend(StoredPublisherBackend):
    def __init__(self, attempts, driver, manifest_factory, *, context):
        super().__init__(attempts, driver, manifest_factory)
        self.truss_context = context

    def prepare(self, stream, request):
        self.observe(stream, request['batch_id'])
        self.driver.prepare_truss_apply(dict(request), self.truss_context)
        return super().prepare(stream, request)
