"""Canonical graph bytes from the original live Truss publication owner.

No protected ACK custody or fresh post-ACK source reopening is implied.
"""
from collections import Counter
import json
from ashlar.graph_release import read_graph_release
from ashlar.manifest import manifest_pin_vector
from .delta_custody import encoded
from .driver import NativeDriver
from .lifecycle import owned_context
from .truss_feed_sessions import TrussFeedSessions
from .truss_graph_admission import TrussGraphAdmission


class _TrussGraphPolicy:
    def __init__(self, driver, vector):
        self.driver = driver
        self.vector = vector
        self.manifest = encoded(driver.policy.active['manifest'])
        # These are independently projected source rows, including UTC microsecond
        # timestamp strings, rather than values inferred from generated SQL.
        self.expected = json.loads(encoded(driver.policy.active['expected']))
        self.roles = {driver.tables[role]: role for role in ('object_current', 'edge_current')}
        self.rows = {role: {encoded(row) for row in self.expected[role]} for role in self.roles.values()}

    def bind_descriptor(self, descriptor, vector, context):
        self.driver.authorize(context, self.driver.policy.active['publication_id'], tuple(self.vector.targets))
        if vector != self.vector or encoded(dict(descriptor.raw)) != self.manifest:
            raise PermissionError('Original complete Truss graph manifest/vector differs')
        if encoded(self.driver.policy.active['expected']) != encoded(self.expected):
            raise PermissionError('Original independent Truss graph rows changed')

    def authorize_graph_row(self, descriptor, table, row, context):
        self.bind_descriptor(descriptor, self.vector, context)
        role = self.roles.get(table)
        if role is None or encoded(dict(row)) not in self.rows[role]:
            raise PermissionError('Independent original Truss graph row differs')

    def authorize_graph_result(self, descriptor, nodes, edges, context):
        self.bind_descriptor(descriptor, self.vector, context)
        for role, rows in (('object_current', nodes), ('edge_current', edges)):
            canonical = [{k: v for k, v in row.items() if k not in ('graph_id', 'src', 'dst')} for row in rows]
            if Counter(map(encoded, canonical)) != Counter(map(encoded, self.expected[role])):
                raise PermissionError('Independent complete Truss graph occurrence bag differs')


def read_original_truss_graph_export(driver, *, with_custody=False):
    """Read final immutable bytes while the actual source/transport remain owned.

    The caller must retain outer resources and write only after their successful
    closure. Truss delivery ACK is caller asserted, not protected outbox custody.
    """
    if (type(driver) is not NativeDriver or type(driver.source_admission) is not TrussGraphAdmission
            or type(driver.source_sessions) is not TrussFeedSessions
            or driver.ack_sessions is not driver.source_sessions):
        raise PermissionError('Original actual Truss graph publication owner required')
    context = driver.context
    with owned_context(driver.writer(driver.policy.active['request']['stream'], context)):
        manifest = driver.policy.active['manifest']
        vector = manifest_pin_vector(manifest, {table: driver.transport.targets[table].uuid
            for table in json.loads(manifest['table_versions_json'])}, authority='private-local-process')
        policy = _TrussGraphPolicy(driver, vector)
        release = read_graph_release(driver.transport, driver, driver, vector, policy,
            publication_id=manifest['publication_id'], node_table=driver.tables['object_current'],
            edge_table=driver.tables['edge_current'], context=context, supported_profiles=['ashlar-delta/0.3'],
            supported_revisions={k: [v] for k, v in json.loads(manifest['schema_revisions_json']).items()},
            max_nodes=len(policy.expected['object_current']), max_edges=len(policy.expected['edge_current']))

        if with_custody:
            from .truss_graph_custody import build_truss_graph_custody
            source = driver.admission_facts()
            checkpoint = json.loads(manifest['source_progress_json'])[source['feed']]
            return release, build_truss_graph_custody(release, source=source, vector=vector, checkpoint=checkpoint)
        return release
