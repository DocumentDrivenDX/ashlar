"""Original evolution plan custody; this owner grants no native or ACK authority.

The publication coordinator and native driver's held admission remain mandatory.
This prerequisite stores complete intent before those owners submit any operation;
resume reads only the original file and never derives a replacement from tables.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import stat
from typing import Any, Protocol

PROFILE = 'ashlar-evolution-attempt-plan/0.1'
MAXIMUM_BYTES = 8 * 1024 * 1024
FIELDS = frozenset(('profile', 'request', 'generated_steps', 'selected_steps',
    'zero_match_elisions', 'observed_native_prior', 'publication_id',
    'materialized_at', 'recorded_at', 'previous_expected', 'expected',
    'previous_progress', 'progress', 'schema_state', 'resource_registry',
    'source_admission', 'operations'))


class EvolutionPlanError(ValueError):
    """Missing, changed or incomplete original custody; no replacement permitted."""


def pairs(entries):
    result = {}
    for key, value in entries:
        if key in result:
            raise EvolutionPlanError('Duplicate original plan member')
        result[key] = value
    return result


def integer(token):
    if len(token) > 20:
        raise EvolutionPlanError('Bounded original plan integer required')
    return int(token)


def number(token):
    value = float(token)
    if not math.isfinite(value):
        raise EvolutionPlanError('Finite original plan number required')
    return value


def decode(raw):
    if type(raw) is not bytes or not 0 < len(raw) <= MAXIMUM_BYTES:
        raise EvolutionPlanError('Bounded original plan bytes required')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_int=integer, parse_float=number,
            parse_constant=lambda _: (_ for _ in ()).throw(EvolutionPlanError('Nonfinite plan value')))
    except (UnicodeError, ValueError, RecursionError) as error:
        raise EvolutionPlanError('Invalid original plan JSON') from error


def validate(raw):
    value = decode(raw)
    if type(value) is not dict or set(value) != FIELDS or value['profile'] != PROFILE:
        raise EvolutionPlanError('Complete original plan inventory required')
    for name in ('publication_id', 'materialized_at', 'recorded_at'):
        if type(value[name]) is not str or not value[name]:
            raise EvolutionPlanError('Original publication identity and clocks required')
    for name in ('request', 'observed_native_prior', 'expected', 'schema_state',
                 'resource_registry', 'source_admission'):
        if type(value[name]) is not dict or not value[name]:
            raise EvolutionPlanError('Complete original plan mappings required')
    for name in ('previous_expected', 'previous_progress', 'progress'):
        if type(value[name]) is not dict:
            raise EvolutionPlanError('Original prefix and progress mappings required')
    request = value['request']
    required = {'stream', 'batch_id', 'predecessor', 'schema_revisions_json',
                'source_batch_json', 'source_batch_digest', 'source_checkpoint_json', 'request_digest'}
    if set(request) != required or any(type(x) is not str or not x for x in request.values()):
        raise EvolutionPlanError('Complete source-qualified original request required')
    unsigned = {k: v for k, v in request.items() if k != 'request_digest'}
    digest = hashlib.sha256(json.dumps(unsigned, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if request['request_digest'] != digest:
        raise EvolutionPlanError('Original request digest differs')
    for name in ('generated_steps', 'selected_steps'):
        if type(value[name]) is not list:
            raise EvolutionPlanError('Original ordered effect steps required')
        for step in value[name]:
            if (type(step) is not dict or set(step) != {'statement', 'parameters'}
                    or type(step['statement']) is not str or not step['statement']
                    or type(step['parameters']) is not dict
                    or any(type(k) is not str or type(v) is not str for k, v in step['parameters'].items())):
                raise EvolutionPlanError('Closed original effect step required')
    elisions = value['zero_match_elisions']
    if type(elisions) is not list:
        raise EvolutionPlanError('Original no-op anchors required')
    omitted = set()
    for anchor in elisions:
        if (type(anchor) is not dict or type(anchor.get('ordinal')) is not int
                or not 0 <= anchor['ordinal'] < len(value['generated_steps'])
                or anchor['ordinal'] in omitted
                or anchor.get('original_step') != value['generated_steps'][anchor['ordinal']]):
            raise EvolutionPlanError('Unique original no-op ordinals required')
        omitted.add(anchor['ordinal'])
    if value['selected_steps'] != [step for n, step in enumerate(value['generated_steps']) if n not in omitted]:
        raise EvolutionPlanError('Selected steps must retain original order and exact elisions')
    operations = value['operations']
    if (type(operations) is not list or len(operations) != len(value['selected_steps'])
            or any(type(x) is not str or not x for x in operations)
            or len(set(operations)) != len(operations)):
        raise EvolutionPlanError('Unique ordered original operation identities required')
    return value


@dataclass(frozen=True)
class EvolutionAttemptPlan:
    """Immutable original bytes; decoded copies are never the retained authority."""
    raw: bytes

    def __post_init__(self):
        validate(self.raw)

    @property
    def sha256(self):
        return hashlib.sha256(self.raw).hexdigest()

    def document(self):
        return validate(self.raw)


class EvolutionPlanPolicy(Protocol):
    def admit(self, plan: EvolutionAttemptPlan, context: Any) -> None:
        """Admit complete original semantics and current held authority; raise on refusal.

        Validate every resource registry binding/UUID, observed prior native anchor,
        complete source-qualified request and schema/prefix/progress/oracle mapping,
        effect/elision correspondence and exact operation identities. Structural
        envelope validation and SHA256 equality never replace this native owner.
        This port must bind bytes to the original installation/attempt; it cannot
        trust an arbitrary caller's expected digest as durable authority.
        """


@contextmanager
def owned_fd(fd):
    """Close every descriptor; original cancellation outranks cleanup failures."""
    primary = None
    try:
        yield fd
    except BaseException as error:
        primary = error
    try:
        os.close(fd)
    except BaseException as error:
        if primary is None or (isinstance(primary, Exception) and not isinstance(error, Exception)):
            raise
        try:
            setattr(primary, 'cleanup_failed', True)
        except BaseException:
            pass
    if primary is not None:
        raise primary


@dataclass(frozen=True)
class EvolutionPlanJournal:
    """Explicit existing parent and mandatory current policy; never creates a directory.

    Retention is an exclusive file creation, fsync and parent-directory fsync on
    the selected local POSIX filesystem. Interrupted creation is retained and
    refused; no rename, deletion, automatic repair or phase-absence inference.
    The enclosing caller owns the exclusive writer interval. This local custody
    receipt does not prove native submission, durable engine state or source ACK.
    """
    path: Path
    policy: EvolutionPlanPolicy

    def admit(self, plan, context):
        if not isinstance(plan, EvolutionAttemptPlan) or not callable(getattr(self.policy, 'admit', None)):
            raise EvolutionPlanError('Explicit original plan and current policy required')
        if self.policy.admit(plan, context) is not None:
            raise EvolutionPlanError('Original plan admission did not complete')

    def load(self, *, expected_sha256, context):
        """Resume exact retained bytes using the original owner's trusted digest.

        Missing or partial custody raises. A caller-supplied hash is only a byte
        comparison; the mandatory policy must independently admit original intent.
        """
        with owned_fd(os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)) as parent:
            with owned_fd(os.open(self.path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                  dir_fd=parent)) as fd:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAXIMUM_BYTES:
                    raise EvolutionPlanError('Bounded regular original plan required')
                chunks = []
                size = 0
                while size <= MAXIMUM_BYTES:
                    chunk = os.read(fd, min(65536, MAXIMUM_BYTES + 1 - size))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    size += len(chunk)
                raw = b''.join(chunks)
                after = os.fstat(fd)
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    raise EvolutionPlanError('Original plan changed during read')
        plan = EvolutionAttemptPlan(raw)
        if type(expected_sha256) is not str or plan.sha256 != expected_sha256:
            raise EvolutionPlanError('Trusted original plan digest differs')
        self.admit(plan, context)
        return plan

    def retain(self, plan, *, context):
        """Fresh only: retain admitted full bytes before any submission; never upsert."""
        self.admit(plan, context)
        with owned_fd(os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)) as parent:
            with owned_fd(os.open(self.path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                  0o600, dir_fd=parent)) as fd:
                written = 0
                while written < len(plan.raw):
                    count = os.write(fd, plan.raw[written:])
                    if count <= 0:
                        raise EvolutionPlanError('Original plan write incomplete')
                    written += count
                os.fsync(fd)
            os.fsync(parent)
        retained = self.load(expected_sha256=plan.sha256, context=context)
        if retained.raw != plan.raw:
            raise EvolutionPlanError('Original retained plan differs')
        return retained

