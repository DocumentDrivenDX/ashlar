"""Exact-byte UMF intake custody; target acceptance is a separate operation."""
import base64
import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

class SchemaIntakeError(ValueError):
    pass

def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SchemaIntakeError('Duplicate JSON member')
        result[key] = value
    return result

def decode_retained_json(raw: bytes) -> Any:
    """Decode UTF-8 JSON without duplicate members or nonfinite numbers.

    Fractions retain Decimal precision; every JSON root is permitted. Callers
    own input byte/work bounds and domain validation; this is no UMF admission.
    """
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                          parse_float=Decimal,
                          parse_constant=lambda _: (_ for _ in ()).throw(SchemaIntakeError('Non-finite JSON')))
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise SchemaIntakeError('Invalid retained JSON') from exc

# Retained compatibility name for existing internal consumers.
_json = decode_retained_json

@dataclass(frozen=True)
class SchemaIntake:
    document_id: str
    document_revision: str
    source: bytes
    artifact: bytes
    source_sha256: str
    artifact_sha256: str
    validator_revision: str
    complete_interpretation: bool

    @classmethod
    def read(cls, artifact: bytes, document_revision: str, *, trusted_validator_revision: str):
        if not isinstance(document_revision, str) or not document_revision or '\x00' in document_revision:
            raise SchemaIntakeError('Explicit document revision required')
        if not re.fullmatch('[0-9a-f]{40}', trusted_validator_revision):
            raise SchemaIntakeError('Pinned validator revision required')
        value = _json(artifact)
        if not isinstance(value, dict) or value.get('format') != 'ashlar-schema-intake/0.1':
            raise SchemaIntakeError('Unsupported intake format')
        if value.get('validatorRevision') != trusted_validator_revision:
            raise SchemaIntakeError('Untrusted validator revision')
        try:
            source = base64.b64decode(value['sourceBase64'], validate=True)
        except (KeyError, ValueError, TypeError) as exc:
            raise SchemaIntakeError('Invalid retained source encoding') from exc
        digest = hashlib.sha256(source).hexdigest()
        if value.get('sourceSha256') != digest or type(value.get('sourceBytes')) is not int or value['sourceBytes'] != len(source):
            raise SchemaIntakeError('Retained source digest/length mismatch')
        document = _json(source)
        if not isinstance(document, dict) or not isinstance(document.get('id'), str) or not document['id'] or '\x00' in document['id']:
            raise SchemaIntakeError('Missing source identity')
        if value.get('documentId') != document['id'] or value.get('umfCoreVersion') != document.get('umf'):
            raise SchemaIntakeError('Intake/source identity mismatch')
        validation = value.get('validation')
        if not isinstance(validation, dict) or value.get('validatedStructure') is not True or validation.get('valid') is not True:
            raise SchemaIntakeError('No successful structural validation')
        complete = value.get('completeInterpretation')
        if type(complete) is not bool or validation.get('complete') is not complete or not isinstance(validation.get('diagnostics'), list):
            raise SchemaIntakeError('Inconsistent validation result')
        return cls(document['id'], document_revision, source, artifact, digest,
                   hashlib.sha256(artifact).hexdigest(), trusted_validator_revision, complete)

    def row(self):
        return {'document_id': self.document_id, 'document_revision': self.document_revision,
                'source_base64': base64.b64encode(self.source).decode('ascii'),
                'artifact_base64': base64.b64encode(self.artifact).decode('ascii'),
                'source_sha256': self.source_sha256, 'artifact_sha256': self.artifact_sha256,
                'validator_revision': self.validator_revision,
                'complete_interpretation': self.complete_interpretation}
