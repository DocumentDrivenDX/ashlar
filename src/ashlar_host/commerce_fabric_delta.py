"""Exact commerce release carriers on fresh Delta targets under source/ACK custody.

All columns remain STRING; scalar properties remain their original JSON carrier.
This materialization supplies no Fabric load, query or immutable graph authority.
"""
import json
from pathlib import Path
from ashlar.graph_release import release_columns
from .commerce_graph_release import hold_original_commerce_graph_release
from .fabric_pack_delta import destination, metadata, readback
from .lifecycle import owned_context


def _overlap(left, right):
    return left == right or left.startswith(right.rstrip('/') + '/') or right.startswith(left.rstrip('/') + '/')


def materialize_commerce_fabric_delta(spark, opened, *, node_path, edge_path):
    """Return only after full publication/source/ACK closing succeeds.

    The caller owns Spark and destination access. Writes use errorifexists and
    partial output survives any refusal or cancellation. Fresh version-zero
    identity and complete ordered schemas/bags are checked independently of the
    canonical source hold. Targets are projections, never publication authority.
    """
    nodes, edges = destination(node_path), destination(edge_path)
    if _overlap(nodes, edges):
        raise ValueError('Separate nonoverlapping destinations required')
    observations = {}
    with owned_context(hold_original_commerce_graph_release(opened)) as held:
        protected = [str(Path(target.path).resolve()) for target in opened.driver.transport.targets.values()]
        protected += [str(Path(path).resolve()) for path, _ in opened.admission.originals]
        protected.append(str(opened.publication.resolve()))
        journal = getattr(opened.driver.transport, 'journal_path', None)
        if journal is not None:
            protected.append(str(Path(journal).resolve()))
        if any(_overlap(path, original) for path in (nodes, edges) for original in protected):
            raise ValueError('Destinations must be separate from canonical targets and original custody files')
        release = held.release
        value = json.loads(release.payload)
        for role, kind, path in [('nodes', 'node', nodes), ('edges', 'edge', edges)]:
            held.renew()
            names = release_columns(kind)
            properties = [{'name': name, 'type': 'STRING'} for name in names]
            rows = [tuple(row[name] for name in names) for row in value[role]]
            schema = ','.join(name + ' STRING' for name in names)
            spark.createDataFrame(rows, schema=schema).write.format('delta').mode('errorifexists').save(path)
            observed = metadata(spark, path)
            readback(spark, path, properties, rows)
            observations[role] = {**observed, 'rows': len(rows), 'schema': properties}
            held.renew()
        if observations['nodes']['uuid'] == observations['edges']['uuid']:
            raise ValueError('Distinct fresh native Delta targets required')
        for role, path in [('nodes', nodes), ('edges', edges)]:
            names = release_columns('node' if role == 'nodes' else 'edge')
            readback(spark, path, observations[role]['schema'], [tuple(row[name] for name in names) for row in value[role]])
        # Recheck both identities after both bags, including a replacement during
        # the final edge readback. This observation grants no immutable target hold.
        for role, path in [('nodes', nodes), ('edges', edges)]:
            if metadata(spark, path) != {key: observations[role][key] for key in ('uuid', 'version', 'location', 'path')}:
                raise ValueError('Written Delta identity changed before closing')
        held.renew()
    return {'profile': 'ashlar-commerce-release-delta-materialization/0.1',
            'release_sha256': release.sha256, 'publication': value['publication'],
            'snapshots': value['snapshots'], 'targets': observations,
            'mapping': {'columns': 'Canonical release columns, all STRING',
                        'properties': 'Original props_json and retained_json verbatim; no scalar property promotion',
                        'limits': 'Fresh Delta carrier projection/readback only; no Fabric load/query, immutable loaded graph, or managed Unity Catalog qualification.'}}
