"""Durable publication orchestration; native authority/recovery are mandatory ports."""
from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping, Optional, Protocol
from types import MappingProxyType
from .schema import _json, SchemaIntakeError
from .source import SourceBatch
from .staging import batch_row

class PublicationError(ValueError):
    pass

@dataclass(frozen=True)
class Attempt:
    request_digest: str
    phase: str
    apply_result: Any=None
    descriptor: Any=None

class PublisherBackend(Protocol):
    def writer(self, stream: str, context: Any):
        """Authorized exclusive writer/fence context; yield None, retain uncertain custody."""
    def observe(self, stream: str, batch_id: str) -> Optional[Attempt]:
        """Return None only for proven absence; unresolved original submission raises."""
    def prepare(self, stream: str, request: Mapping[str,str]) -> Attempt:
        """Durably retain complete original intent with predecessor/source/revision custody."""
    def start_apply(self, stream: str, batch_id: str, digest: str) -> None:
        """Persist applying before any graph effects; no replacement attempt on uncertainty."""
    def apply(self, stream: str, request: Mapping[str,str]) -> Any:
        """Apply only the original admitted retained plan; return completed actual effects."""
    def recover_apply(self, stream: str, request: Mapping[str,str]) -> Any:
        """Inspect original native handles/receipts/effects, never blindly rerun writes."""
    def retain_applied(self, stream: str, batch_id: str, digest: str, result: Any) -> Attempt: ...
    def validate(self, stream: str, request: Mapping[str,str], result: Any, context: Any) -> None:
        """Complete native effect/source/schema/pin/custody parity under current authority."""
    def start_commit(self, stream: str, batch_id: str, digest: str) -> None:
        """Durably mark committing before descriptor submission."""
    def recover_commit(self, stream: str, request: Mapping[str,str], result: Any, context: Any) -> Attempt:
        """Resolve original descriptor submission/handle; never create a replacement."""
    def commit(self, stream: str, request: Mapping[str,str], result: Any) -> Attempt:
        """Commit one immutable descriptor; recover original commit outcome if uncertain."""
    def acknowledge(self, stream: str, request: Mapping[str,str], descriptor: Any, context: Any) -> None:
        """Advance only complete descriptor-bound progress; exact replay is idempotent."""


def publish_batch(backend: PublisherBackend, stream: str, batch: SourceBatch, *,
                  predecessor: str, schema_revisions_json: str, context: Any,
                  source_checkpoint_json=None):
    """Resume one durable attempt; acknowledge only the original committed descriptor.

    Ports must implement native durable state, complete admission, source fencing,
    original-handle recovery and immutable publication. No permissive native
    implementation is provided by this coordinator.
    """
    if any(not isinstance(x,str) or not x for x in [stream,predecessor,schema_revisions_json]):
        raise PublicationError('Explicit stream/predecessor/schema revision custody required')
    try: revisions=_json(schema_revisions_json.encode('utf-8'))
    except SchemaIntakeError as exc:raise PublicationError('Malformed exact schema revision inventory') from exc
    if not isinstance(revisions,dict) or not revisions or any(not key or not isinstance(value,str) or not value for key,value in revisions.items()):
        raise PublicationError('Complete named schema revision strings required')
    row=batch_row(batch)
    request={'stream':stream,'batch_id':batch.batch_id,'predecessor':predecessor,
             'schema_revisions_json':schema_revisions_json,'source_batch_json':row['batch_json'],
             'source_batch_digest':row['batch_digest']}
    if source_checkpoint_json is not None:
        from .source_checkpoint import validate_outbox_checkpoint
        validate_outbox_checkpoint(source_checkpoint_json,batch)
        request['source_checkpoint_json']=source_checkpoint_json
    encoded=json.dumps(request,separators=(',',':'),sort_keys=True).encode('utf-8')
    digest=hashlib.sha256(encoded).hexdigest();request['request_digest']=digest
    request=MappingProxyType(request)
    def check(attempt,phases):
        if not isinstance(attempt,Attempt) or attempt.request_digest!=digest or attempt.phase not in phases:
            raise PublicationError('Conflicting or invalid original publication attempt')
        if attempt.phase in ('applied','committing') and attempt.apply_result is None:
            raise PublicationError('Missing original complete apply result')
        return attempt
    with backend.writer(stream,context) as permit:
        if permit is not None:raise PublicationError('Writer policy did not complete')
        attempt=backend.observe(stream,batch.batch_id)
        if attempt is None:attempt=backend.prepare(stream,request)
        check(attempt,('prepared','applying','applied','committing','committed'))
        if attempt.phase=='prepared':
            if backend.start_apply(stream,batch.batch_id,digest) is not None:
                raise PublicationError('Apply-start custody did not complete')
            result=backend.apply(stream,request)
            attempt=check(backend.retain_applied(stream,batch.batch_id,digest,result),('applied',))
        elif attempt.phase=='applying':
            result=backend.recover_apply(stream,request)
            attempt=check(backend.retain_applied(stream,batch.batch_id,digest,result),('applied',))
        if attempt.phase=='applied':
            if backend.validate(stream,request,attempt.apply_result,context) is not None:
                raise PublicationError('Complete publication validation did not succeed')
            if backend.start_commit(stream,batch.batch_id,digest) is not None:
                raise PublicationError('Commit-start custody did not complete')
            attempt=check(backend.commit(stream,request,attempt.apply_result),('committed',))
        elif attempt.phase=='committing':
            attempt=check(backend.recover_commit(stream,request,attempt.apply_result,context),('committed',))
        if attempt.descriptor is None:raise PublicationError('Missing original committed descriptor')
        # A committed replay still requires current source/checkpoint authority;
        # acknowledge must verify descriptor/source correspondence independently.
        if backend.acknowledge(stream,request,attempt.descriptor,context) is not None:
            raise PublicationError('Source acknowledgement did not complete')
        return attempt.descriptor
