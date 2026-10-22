# SPDX-License-Identifier: MIT OR Apache-2.0
"""Selected local host implementation; native qualification remains version-scoped."""
import re

def fixture_columns(root):
    columns = {}
    baseline = (root / 'sql/ashlar-delta-v03/01-baseline.sql').read_text()
    for table in ['object_current', 'edge_current', 'tombstone']:
        body = re.search('CREATE TABLE ' + table + ' \\((.*?)\\) USING', baseline, re.S).group(1)
        columns[table] = tuple(((m.group(1), m.group(2)) for part in body.split(',') for m in [re.match('\\s*(\\w+) (STRING|BIGINT|TIMESTAMP)', part)] if m))
    columns['whole_source_history'] = tuple(((k, 'STRING') for k in ['feed', 'epoch', 'delivery_id', 'digest', 'change_json', 'raw_base64']))
    return columns


LOCAL_CURRENT_ROLE_TO_UMF = {
    'object_current': 'object_current',
    'edge_current': 'edge_current',
    'tombstone': 'tombstone',
    'whole_source_history': 'whole_source_history',
    'attempts': 'publication_attempt_phase',
    'manifest': 'publication_manifest',
}


def local_current_columns(root, carrier_profile='ashlar-delta/0.3'):
    """Return the exact local current carrier schemas for a selected profile."""
    if carrier_profile == 'ashlar-delta/0.3':
        data = fixture_columns(root)
        from ashlar.manifest import FIELDS
        return {**data,
            'attempts': tuple((name, 'STRING') for name in
                ('stream', 'batch_id', 'phase', 'request_digest', 'payload_json', 'payload_digest')),
            'manifest': tuple((name, 'TIMESTAMP' if name == 'recorded_at' else 'STRING') for name in FIELDS)}
    if carrier_profile != 'ashlar-delta/0.4-xxhash64':
        raise ValueError('Exact supported local current carrier profile required')
    from .managed_current_carriers import load_current_managed_carriers
    carriers = load_current_managed_carriers(carrier_profile)
    return {local_role: tuple((name, kind) for name, kind, _nullable in
                carriers.ordered_fields(umf_role))
            for local_role, umf_role in LOCAL_CURRENT_ROLE_TO_UMF.items()}
