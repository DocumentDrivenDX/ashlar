"""Bounded ordered whole-entity recovery from complete retained source rows.

Caller admission must establish the original stage UUID/snapshot, complete ordered
scope, trusted prior state and both checkpoint boundaries. This is pure recovery,
not an acknowledgement, publication, or repair of uncertain native effects.
"""
from dataclasses import dataclass
import hashlib
from .apply import plan_apply
from .staging import batch_from_row
from .whole_entity import changes_from_batch


class RecoveryError(ValueError):
    pass


@dataclass(frozen=True)
class RecoveredState:
    state: object
    cursor_after: str
    batches: int
    events: int
    ordered_batch_digest: str


def recover_whole_entity_state(rows, *, prior, feed, epoch, cursor_before,
                               cursor_after, schema_policy, schema_transition_policy=None,
                               max_batches=128, max_events=10000,
                               max_artifact_bytes=16 * 1024 * 1024):
    """Reconstruct only the explicitly admitted contiguous checkpoint interval.

    No sorting or missing-batch inference occurs. The digest frames each original
    retained batch artifact length plus bytes in observed source order.
    """
    for value in (feed, epoch):
        if not isinstance(value, str) or not value or '\x00' in value:
            raise RecoveryError('Explicit source scope required')
    for value in (cursor_before, cursor_after):
        if (not isinstance(value, str) or not value.isascii() or not value.isdecimal()
                or len(value) > 1024 or str(int(value)) != value):
            raise RecoveryError('Bounded canonical checkpoint text required')
    for limit in (max_batches, max_events, max_artifact_bytes):
        if type(limit) is not int or limit < 1:
            raise RecoveryError('Finite positive recovery limits required')
    state = prior
    cursor = cursor_before
    count = events = size = 0
    seen = set()
    digest = hashlib.sha256()
    for row in rows:
        count += 1
        if count > max_batches:
            raise RecoveryError('Recovery batch budget exceeded')
        if not isinstance(row, dict) or not isinstance(row.get('batch_json'), str):
            raise RecoveryError('Complete original staged rows required')
        if len(row['batch_json']) > max_artifact_bytes - size:
            raise RecoveryError('Recovery artifact budget exceeded')
        artifact = row['batch_json'].encode('utf-8')
        size += len(artifact)
        if size > max_artifact_bytes:
            raise RecoveryError('Recovery artifact budget exceeded')
        batch = batch_from_row(row)
        if (batch.feed, batch.epoch, batch.cursor_before) != (feed, epoch, cursor):
            raise RecoveryError('Source recovery interval is mixed, reordered or incomplete')
        if batch.batch_id in seen:
            raise RecoveryError('Duplicate source batch identity in recovery interval')
        seen.add(batch.batch_id)
        events += len(batch.records)
        if events > max_events:
            raise RecoveryError('Recovery event budget exceeded')
        state = plan_apply(state, changes_from_batch(batch), schema_policy=schema_policy,
                           schema_transition_policy=schema_transition_policy)
        cursor = batch.cursor_after
        digest.update(len(artifact).to_bytes(8, 'big'))
        digest.update(artifact)
    if cursor != cursor_after:
        raise RecoveryError('Recovery does not reach admitted terminal checkpoint')
    return RecoveredState(state, cursor, count, events, digest.hexdigest())
