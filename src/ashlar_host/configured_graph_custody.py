# SPDX-License-Identifier: MIT OR Apache-2.0
"""Configured JSONL saved-export correspondence, never live or protected authority."""
import hashlib,json
from .delta_custody import encoded
from .finite_graph_custody import decode

PROFILE='ashlar-configured-local-graph-custody/0.1'
SCOPE='immutable-configured-jsonl-export-bytes-only'


def admit_configured_graph_custody(release,payload,trusted_receipt_sha256,trusted_release_sha256):
    from .private_graph_custody import require_custody_digest
    for digest in (trusted_receipt_sha256,trusted_release_sha256):require_custody_digest(digest)
    if type(payload)is not bytes or hashlib.sha256(payload).hexdigest()!=trusted_receipt_sha256:
        raise ValueError('Trusted configured custody digest differs')
    value=decode(payload)
    keys={'format','scope','releaseSha256','originalManifest','snapshots','roles','source','bindingSha256','ordinal','request','interval'}
    if (type(value)is not dict or set(value)!=keys or value['format']!=PROFILE or value['scope']!=SCOPE
            or (encoded(value)+'\n').encode()!=payload or value['releaseSha256']!=trusted_release_sha256
            or value['originalManifest']!=release['publication']or value['snapshots']!=release['snapshots']or value['roles']!=release['roles']):
        raise ValueError('Exact configured graph custody required')
    report=json.loads(release['publication']['validation_report_json'])
    if value['source']!=report.get('source_admission')or type(value['source'])is not dict:
        raise ValueError('Original configured source admission differs')
    source=value['source']
    if source.get('profile')not in ('ashlar-configured-local-source/0.1','ashlar-configured-local-typed-source/0.1','ashlar-configured-local-typed-graph-source/0.1')or source.get('checkpoint_profile')!='ashlar-immutable-jsonl/0.1':
        raise ValueError('Explicit configured immutable-file source required')
    for name in ('configuration_sha256','source_sha256','model_sha256','admission_report_sha256'):require_custody_digest(source.get(name))
    require_custody_digest(value['bindingSha256'])
    if type(source.get('batches'))is not int or type(value['ordinal'])is not int or not 1<=value['ordinal']<=source['batches']<=4:
        raise ValueError('Original selected configured ordinal required')
    from ashlar.attempt_store import verify_request_digest
    request=value['request'];verify_request_digest(request)
    if report.get('request_digest')!=request['request_digest']or request['schema_revisions_json']!=release['publication']['schema_revisions_json']:
        raise ValueError('Original configured request correspondence differs')
    checkpoint=json.loads(request['source_checkpoint_json'])
    if json.loads(release['publication']['source_progress_json']).get(checkpoint['feed'])!=checkpoint or checkpoint.get('profile')!='ashlar-immutable-jsonl/0.1':
        raise ValueError('Original configured progress differs')
    interval=value['interval']
    if (type(interval)is not dict or set(interval)!={'wholeVectorHeld','closingSucceeded','scope'}
            or interval['wholeVectorHeld']is not True or interval['closingSucceeded']is not True
            or interval['scope']!='cooperative-local-installation'):
        raise ValueError('Complete configured owner closing required')
    return value


def build_configured_graph_custody(release,*,source,binding_sha256,ordinal,request):
    """Called only after every live owner closes; structural proof is not authority."""
    graph=json.loads(release.payload)
    value={'format':PROFILE,'scope':SCOPE,'releaseSha256':release.sha256,
        'originalManifest':graph['publication'],'snapshots':graph['snapshots'],'roles':graph['roles'],
        'source':source,'bindingSha256':binding_sha256,'ordinal':ordinal,'request':request,
        'interval':{'wholeVectorHeld':True,'closingSucceeded':True,'scope':'cooperative-local-installation'}}
    raw=(encoded(value)+'\n').encode()
    from .private_graph_custody import load_private_graph_release
    load_private_graph_release(release.payload,release.sha256,raw,hashlib.sha256(raw).hexdigest())
    return raw
