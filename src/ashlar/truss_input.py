"""Complete candidate Truss acceptance-input custody, not acceptance authority.

Host-supplied archives, original UMF receipts and dependency order must come from
trusted producers. This verifies bytes and correspondence; it does not register
profiles, qualify callbacks, authorize an actor or admit target semantics.
"""
import base64
from dataclasses import dataclass
import hashlib
import re
from .schema import _json, SchemaIntake
from .lineage import _quote

MAX_BYTES = 1048576
MAX_ITEMS = 1000
DOMAIN = 'truss-acceptance-input/0.1.0'

class AcceptanceInputError(ValueError):
    pass


def _shape(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise AcceptanceInputError('Incomplete or unknown input members')


def _text(value):
    if type(value) is not str or not value or '\x00' in value:
        raise AcceptanceInputError('Nonempty scalar text required')
    try: value.encode('utf-8')
    except UnicodeError as exc: raise AcceptanceInputError('Unpaired surrogate') from exc
    return value


def _hash(value):
    if type(value) is not str or not re.fullmatch('[0-9a-f]{64}',value):
        raise AcceptanceInputError('Exact SHA256 text required')
    return value


def exact_artifact(value):
    _shape(value, ['identity','bytesBase64','sha256']);_text(value['identity']);_hash(value['sha256'])
    encoded=value['bytesBase64']
    if type(encoded) is not str or len(encoded)>MAX_BYTES*4//3+4:
        raise AcceptanceInputError('Artifact bound exceeded')
    try: raw=base64.b64decode(encoded,validate=True)
    except (ValueError,TypeError) as exc: raise AcceptanceInputError('Invalid base64') from exc
    if base64.b64encode(raw).decode('ascii')!=encoded or len(raw)>MAX_BYTES or hashlib.sha256(raw).hexdigest()!=value['sha256']:
        raise AcceptanceInputError('Artifact spelling/digest mismatch')
    return raw


def artifact(identity, raw):
    _text(identity)
    if type(raw) is not bytes or len(raw)>MAX_BYTES:
        raise AcceptanceInputError('Bounded original bytes required')
    return {'identity':identity,'bytesBase64':base64.b64encode(raw).decode('ascii'),'sha256':hashlib.sha256(raw).hexdigest()}


def _pin(value, archives):
    _shape(value,['identity','version','sha256'])
    key=(_text(value['identity']),_text(value['version']));_hash(value['sha256'])
    raw=archives.get(key)
    if type(raw) is not bytes or len(raw)>MAX_BYTES or hashlib.sha256(raw).hexdigest()!=value['sha256']:
        raise AcceptanceInputError('Original profile archive unavailable or changed')


def _canonical(value, depth=0):
    if depth>64:raise AcceptanceInputError('Canonical depth bound exceeded')
    if value is None:return 'null'
    if type(value) is bool:return 'true' if value else 'false'
    if type(value) is str:
        try:value.encode('utf-8')
        except UnicodeError as exc:raise AcceptanceInputError('Unpaired surrogate') from exc
        # Selected PostgreSQL-bound input refuses NUL; other controls use long escapes.
        if '\x00' in value:raise AcceptanceInputError('PostgreSQL-bound NUL unsupported')
        return _quote(value)
    if type(value) is list:
        if len(value)>MAX_ITEMS:raise AcceptanceInputError('Canonical item bound exceeded')
        return '['+','.join(_canonical(v,depth+1) for v in value)+']'
    if type(value) is dict:
        if len(value)>MAX_ITEMS:raise AcceptanceInputError('Canonical item bound exceeded')
        for key in value:
            if type(key) is not str:raise AcceptanceInputError('String object key required')
            _canonical(key,depth+1)
        return '{'+','.join(_quote(k)+':'+_canonical(value[k],depth+1) for k in sorted(value,key=lambda k:k.encode('utf-8')))+'}'
    raise AcceptanceInputError('Host JSON numbers or unsupported canonical values')


@dataclass(frozen=True)
class AcceptanceInputCustody:
    original: bytes
    canonical_preimage: bytes
    sha256: str
    document_sources: tuple

    def exact_repeat(self, other):
        # Digest is only a route; full original admitted meaning defines equality.
        return isinstance(other,AcceptanceInputCustody) and self.canonical_preimage==other.canonical_preimage


def verify_acceptance_input_custody(raw, *, profile_archives, intakes,
                                    trusted_validator_revision, document_order):
    if type(raw) is not bytes or len(raw)>MAX_BYTES:
        raise AcceptanceInputError('Bounded original acceptance input required')
    value=_json(raw)
    _shape(value,['interfaceVersion','layoutProfile','acceptanceProfile','validatorProfile','supportProfile','documents','binding','policy','transforms'])
    if value['interfaceVersion']!=DOMAIN:raise AcceptanceInputError('Unsupported input interface')
    # Validate the full bounded number-free tree, including every transform parameter.
    canonical=_canonical(value)
    for field in ['layoutProfile','acceptanceProfile','validatorProfile','supportProfile']:_pin(value[field],profile_archives)
    receipts={}
    for intake in intakes:
        if not isinstance(intake,SchemaIntake):raise AcceptanceInputError('Original UMF receipt required')
        recovered=SchemaIntake.read(intake.artifact,intake.document_revision,trusted_validator_revision=trusted_validator_revision)
        if recovered!=intake or intake.document_id in receipts:raise AcceptanceInputError('Altered or duplicate original intake')
        receipts[intake.document_id]=intake
    docs=value['documents']
    if type(docs) is not list or not 1<=len(docs)<=MAX_ITEMS:raise AcceptanceInputError('Nonempty bounded document set required')
    seen=set();sources=[];order=[]
    for doc in docs:
        _shape(doc,['documentId','documentRevision','artifact','umfProfile','ingress'])
        did=_text(doc['documentId']);revision=_text(doc['documentRevision'])
        if did in seen or did not in receipts:raise AcceptanceInputError('Duplicate or unobserved document')
        seen.add(did);order.append(did);source=exact_artifact(doc['artifact']);intake=receipts[did]
        if source!=intake.source or revision!=intake.document_revision:raise AcceptanceInputError('Original document/revision differs')
        _pin(doc['umfProfile'],profile_archives);sources.append(source)
        ingress=doc['ingress']
        if type(ingress) is not dict:raise AcceptanceInputError('Exact ingress required')
        if ingress.get('kind')=='native':_shape(ingress,['kind'])
        elif ingress.get('kind')=='converted':
            _shape(ingress,['kind','adapterProfile','source','lossReport']);_pin(ingress['adapterProfile'],profile_archives)
            exact_artifact(ingress['source']);exact_artifact(ingress['lossReport'])
        else:raise AcceptanceInputError('Unknown ingress profile')
    if seen!=set(receipts) or tuple(order)!=tuple(document_order) or len(set(document_order))!=len(document_order):
        raise AcceptanceInputError('Incomplete document set or original dependency order differs')
    binding=value['binding']
    if type(binding) is not dict:raise AcceptanceInputError('Exact binding state required')
    if binding.get('state')=='absent':_shape(binding,['state'])
    elif binding.get('state')=='present':
        _shape(binding,['state','vocabulary','artifact']);_pin(binding['vocabulary'],profile_archives);exact_artifact(binding['artifact'])
    else:raise AcceptanceInputError('Unknown binding state')
    policy=value['policy'];_shape(policy,['unknownEndpoint','loss','profile']);_pin(policy['profile'],profile_archives)
    if policy['unknownEndpoint'] not in ['reject','provisional','skip'] or policy['loss'] not in ['strict','report']:
        raise AcceptanceInputError('Unknown policy')
    if type(value['transforms']) is not list or len(value['transforms'])>MAX_ITEMS:
        raise AcceptanceInputError('Bounded ordered transform inventory required')
    for transform in value['transforms']:
        _shape(transform,['registration','targetDefinitionIdentity','parameters']);_pin(transform['registration'],profile_archives)
        _text(transform['targetDefinitionIdentity'])
    preimage=('truss-canonical/0.1.0\n'+DOMAIN+'\n'+canonical).encode('utf-8')
    if len(preimage)>MAX_BYTES:raise AcceptanceInputError('Canonical input bound exceeded')
    return AcceptanceInputCustody(raw,preimage,hashlib.sha256(preimage).hexdigest(),tuple(sources))
