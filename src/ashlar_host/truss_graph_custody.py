# SPDX-License-Identifier: MIT OR Apache-2.0
"""Immutable Truss graph capture correspondence, never protected ACK authority."""
import base64
import hashlib
import json
from .delta_custody import encoded
from .truss_graph_admission import (
    PROFILE as SOURCE_PROFILE, TYPED_PROFILE, CURRENT_052_CANDIDATE_PROFILE,
)

PROFILE = 'ashlar-immutable-truss-graph-capture/0.1'
SCOPE = 'immutable-truss-export-bytes-only'
QUALIFICATION = 'Caller-asserted Truss delivery checkpoint; no protected ACK receipt, future source authority or native availability.'
_CURRENT_052_FIELDS = {
    'profile', 'source_profile', 'feed', 'epoch', 'source_transaction_sha256',
    'accepted_reports', 'catalog_views', 'declared_umf', 'qualification',
}


def _validate_current_052_source(source):
    """Preserve the complete versioned `.52` graph-admission carrier."""
    from .private_graph_custody import require_custody_digest
    if (type(source) is not dict or set(source) != _CURRENT_052_FIELDS
            or source.get('profile') != CURRENT_052_CANDIDATE_PROFILE
            or source.get('source_profile') != 'truss-native-feed/0.1'
            or any(type(source.get(name)) is not str or not source[name]
                   for name in ('feed', 'epoch', 'qualification'))):
        raise ValueError('Exact current Truss .52 source-admission metadata required')
    require_custody_digest(source['source_transaction_sha256'])
    for name in ('accepted_reports', 'catalog_views'):
        values = source[name]
        if (type(values) is not dict or not values
                or any(type(revision) is not str or not revision for revision in values)
                or any(type(digest) is not str or len(digest) != 64
                       or any(char not in '0123456789abcdef' for char in digest)
                       for digest in values.values())):
            raise ValueError('Complete current Truss .52 source revision digests required')
    declared = source['declared_umf']
    if (type(declared) is not dict or not declared
            or any(type(revision) is not str or not revision
                   or type(documents) is not dict or not documents
                   or any(type(document) is not str or not document
                          or type(version) is not str or not version
                          for document, version in documents.items())
                   for revision, documents in declared.items())):
        raise ValueError('Complete current Truss .52 declared-model inventory required')


def admit_truss_graph_custody(release, payload, trusted_receipt_sha256, trusted_release_sha256):
    from .private_graph_custody import decode_custody_bytes, require_custody_digest
    require_custody_digest(trusted_receipt_sha256); require_custody_digest(trusted_release_sha256)
    if type(payload) is not bytes or hashlib.sha256(payload).hexdigest() != trusted_receipt_sha256:
        raise ValueError('Trusted Truss capture digest differs')
    value = decode_custody_bytes(payload)
    fields = {'format', 'scope', 'qualification', 'releaseSha256', 'originalManifest', 'snapshots', 'roles', 'source', 'checkpoint'}
    if (type(value) is not dict or set(value) != fields or value['format'] != PROFILE
            or value['scope'] != SCOPE or value['qualification'] != QUALIFICATION
            or (encoded(value)+'\n').encode() != payload):
        raise ValueError('Named canonical immutable Truss capture required')
    if (value['releaseSha256'] != trusted_release_sha256 or value['originalManifest'] != release['publication']
            or value['snapshots'] != release['snapshots'] or value['roles'] != release['roles']):
        raise ValueError('Original Truss graph manifest/vector differs')
    source = json.loads(release['publication']['validation_report_json'])['source_admission']
    checkpoint = value['checkpoint']
    if (type(source) is not dict or source != value['source'] or source.get('profile') not in (SOURCE_PROFILE,TYPED_PROFILE,CURRENT_052_CANDIDATE_PROFILE)
            or source.get('source_profile') != 'truss-native-feed/0.1'):
        raise ValueError('Actual Truss scalar graph metadata required')
    if source['profile'] == CURRENT_052_CANDIDATE_PROFILE:
        _validate_current_052_source(source)
    if (type(checkpoint) is not dict or set(checkpoint) != {'profile', 'feed', 'epoch', 'previous', 'position', 'payload_digest', 'batch_id'}
            or checkpoint['profile'] != source['source_profile'] or checkpoint['feed'] != source['feed']
            or checkpoint['epoch'] != source['epoch'] or checkpoint['payload_digest'] != source['source_transaction_sha256']
            or json.loads(release['publication']['source_progress_json']).get(source['feed']) != checkpoint):
        raise ValueError('Original native Truss checkpoint differs')
    require_custody_digest(checkpoint['payload_digest'])
    if type(checkpoint['batch_id']) is not str or not checkpoint['batch_id'] or len(checkpoint['batch_id']) > 256:
        raise ValueError('Original bounded Truss writer identity required')
    for field in ('previous', 'position'):
        text = checkpoint[field]
        if type(text) is not str or not 0 < len(text) <= 16384:
            raise ValueError('Bounded opaque Truss checkpoint required')
        raw = base64.b64decode(text, validate=True)
        if base64.b64encode(raw).decode() != text:
            raise ValueError('Canonical opaque Truss checkpoint required')
    if len(value['snapshots']) != 4 or {table.split('.')[-1] for table in value['snapshots']} != {'object_current', 'edge_current', 'tombstone', 'whole_source_history'}:
        raise ValueError('Complete original four-role Truss graph vector required')
    if value['roles']['nodes'].split('.')[-1] != 'object_current' or value['roles']['edges'].split('.')[-1] != 'edge_current':
        raise ValueError('Original Truss graph role binding differs')
    return value


def build_truss_graph_custody(release, *, source, vector, checkpoint):
    """Build after actual live capture closes; pure correspondence grants no authority."""
    from ashlar.graph_release import GraphRelease
    from ashlar.pins import PinVector
    from .private_graph_custody import decode_custody_bytes, load_private_graph_release
    if type(release) is not GraphRelease or type(vector) is not PinVector:
        raise ValueError('Exact original Truss GraphRelease and manifest PinVector required')
    value = decode_custody_bytes(release.payload)
    if {table: {'uuid': item[0], 'version': item[1]} for table, item in vector.targets.items()} != value['snapshots']:
        raise ValueError('Actual complete Truss capture vector differs')
    receipt = {'format': PROFILE, 'scope': SCOPE, 'qualification': QUALIFICATION,
        'releaseSha256': release.sha256, 'originalManifest': value['publication'], 'snapshots': value['snapshots'],
        'roles': value['roles'], 'source': source, 'checkpoint': checkpoint}
    payload = (encoded(receipt)+'\n').encode()
    load_private_graph_release(release.payload, release.sha256, payload, hashlib.sha256(payload).hexdigest())
    return payload
