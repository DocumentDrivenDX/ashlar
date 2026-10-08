"""Streaming, exact-byte source admission. A complete batch is not publication."""
from dataclasses import dataclass
import hashlib
from typing import Iterable, Iterator
from .schema import SchemaIntakeError, _json

class SourceError(ValueError):
    pass

@dataclass(frozen=True)
class SourceRecord:
    delivery_id: str
    cursor: str
    raw: bytes
    sha256: str

@dataclass(frozen=True)
class SourceBatch:
    profile: str
    feed: str
    epoch: str
    batch_id: str
    cursor_before: str
    cursor_after: str
    records: tuple
    begin: bytes
    commit: bytes
    records_sha256: str


def records_digest(records: Iterable[bytes]) -> str:
    """SHA256 over ordered (uint64 big-endian byte length, exact record bytes)."""
    digest=hashlib.sha256()
    for raw in records:
        digest.update(len(raw).to_bytes(8,'big'));digest.update(raw)
    return digest.hexdigest()


def jsonl_batches(lines: Iterable[bytes], *, feed: str, epoch: str,
                  cursor_before: str='0', max_records: int=1000,
                  max_transaction_bytes: int=1024*1024) -> Iterator[SourceBatch]:
    """One explicit transaction at a time from an already positioned binary stream.

    The host must establish trusted feed/epoch/cursor/file custody. This profile
    has no implicit begin/commit, synthetic safe watermark or acknowledgement.
    Unknown event payloads remain raw; consumers must separately admit meaning.
    """
    if any(not isinstance(x,str) or not x or '\x00' in x for x in [feed,epoch]):raise SourceError('Explicit source namespace required')
    if not isinstance(cursor_before,str) or not cursor_before.isascii() or not cursor_before.isdecimal() or str(int(cursor_before))!=cursor_before:
        raise SourceError('Canonical byte-offset cursor required')
    if type(max_records) is not int or max_records<1 or type(max_transaction_bytes) is not int or max_transaction_bytes<1:
        raise SourceError('Finite positive transaction bounds required')
    offset=int(cursor_before);before=cursor_before;begin=None;events=[];batch_id=None;size=0
    for raw in lines:
        if not isinstance(raw,bytes) or not raw.endswith(b'\n') or b'\n' in raw[:-1]:raise SourceError('One complete binary JSONL line required')
        if len(raw)>max_transaction_bytes:raise SourceError('Transaction byte limit exceeded')
        try: item=_json(raw)
        except SchemaIntakeError as exc: raise SourceError('Invalid source JSON') from exc
        if not isinstance(item,dict):raise SourceError('Source envelope must be an object')
        offset+=len(raw);size+=len(raw)
        if size>max_transaction_bytes:raise SourceError('Transaction byte limit exceeded')
        kind=item.get('kind')
        if begin is None:
            if kind!='begin' or set(item)!={'kind','batch_id'} or not isinstance(item.get('batch_id'),str) or not item['batch_id'] or '\x00' in item['batch_id']:
                raise SourceError('Explicit begin envelope required')
            begin=raw;batch_id=item['batch_id'];continue
        if kind=='event':
            if len(events)>=max_records:raise SourceError('Transaction record limit exceeded')
            if not isinstance(item.get('delivery_id'),str) or not item['delivery_id'] or '\x00' in item['delivery_id']:
                raise SourceError('Explicit delivery identity required')
            if any(e.delivery_id==item['delivery_id'] for e in events):raise SourceError('Duplicate delivery identity in transaction')
            events.append(SourceRecord(item['delivery_id'],str(offset),raw,hashlib.sha256(raw).hexdigest()));continue
        if kind!='commit' or set(item)!={'kind','batch_id','record_count','records_sha256'}:
            raise SourceError('Unknown transaction control or nested begin')
        digest=records_digest(e.raw for e in events)
        if item.get('batch_id')!=batch_id or type(item.get('record_count')) is not int or item['record_count']!=len(events) or item.get('records_sha256')!=digest:
            raise SourceError('Incomplete or mismatched commit manifest')
        yield SourceBatch('ashlar-jsonl-transactions/0.1',feed,epoch,batch_id,before,str(offset),tuple(events),begin,raw,digest)
        before=str(offset);begin=None;events=[];batch_id=None;size=0
    if begin is not None:raise SourceError('Incomplete source transaction; no checkpoint admission')
