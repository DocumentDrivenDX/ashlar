"""Installed explicit eight-step invocation and trusted operator extension boundary.

The provider is Python code selected by the operator, not a sandbox. Its pin
proves byte correspondence only; ordinary source/ACK and original reservation
policies establish authority. This owner never discovers credentials or runtimes.
"""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from types import ModuleType
from typing import ContextManager, Protocol, Union
from .config import (HostError, EvolutionAdmissionConfig, FreshCommerceEvolutionConfig,
                     ResumeCommerceEvolutionConfig)
from .evolution_composition import NativeEvolutionRunPolicy
from .driver import NativeDriver
from .source_sessions import RegisteredOutboxSources
from .ack_sessions import RegisteredOutboxAcks
from .evolution_run import EvolutionRunRequest, request_from_document
from .commerce_evolution import publish_commerce_evolution, resume_commerce_evolution
from .lifecycle import finish, owned_context

PROFILE = 'ashlar-commerce-evolution-invocation/0.1'
MAX_CONFIG = 65536
MAX_PROVIDER = 262144


def absolute(value):
    if type(value) is not str or not 0 < len(value) <= 4096 or not Path(value).is_absolute():
        raise HostError('evolution-invocation-configuration')
    return Path(value)


@dataclass(frozen=True, repr=False)
class FreshEvolutionInvocation:
    ledger_path: Path
    receipt_path: Path
    producer: EvolutionAdmissionConfig
    request: EvolutionRunRequest

    def __post_init__(self):
        validate_invocation_base(self.ledger_path, self.receipt_path, self.producer)
        if type(self.request) is not EvolutionRunRequest:
            raise HostError("evolution-invocation-configuration")
        self.request.__post_init__()


@dataclass(frozen=True, repr=False)
class ResumeEvolutionInvocation:
    ledger_path: Path
    receipt_path: Path
    producer: EvolutionAdmissionConfig
    expected_sha256: str

    def __post_init__(self):
        validate_invocation_base(self.ledger_path, self.receipt_path, self.producer)
        if type(self.expected_sha256) is not str or not re.fullmatch("[0-9a-f]{64}", self.expected_sha256):
            raise HostError("evolution-invocation-configuration")


def validate_invocation_base(ledger_path, receipt_path, producer):
    for path in (ledger_path, receipt_path):
        if type(path) is not type(Path('/')) or not path.is_absolute():
            raise HostError('evolution-invocation-configuration')
    if ledger_path == receipt_path or type(producer) is not EvolutionAdmissionConfig:
        raise HostError('evolution-invocation-configuration')
    for path in (producer.source, producer.bun, producer.git):
        if type(path) is not type(Path('/')) or not path.is_absolute():
            raise HostError('evolution-invocation-configuration')
    producer.__post_init__()


Invocation = Union[FreshEvolutionInvocation, ResumeEvolutionInvocation]
RunConfig = Union[FreshCommerceEvolutionConfig, ResumeCommerceEvolutionConfig]


class EvolutionOperatorProvider(Protocol):
    def open_evolution(self, invocation: Invocation) -> ContextManager[RunConfig]:
        """Own actual runtime/driver, reservation authority and ordinary sessions."""


def parse_evolution_invocation(raw, mode):
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_CONFIG or type(mode) is not str or mode not in ('fresh', 'resume'):
        raise HostError('evolution-invocation-configuration')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise HostError('evolution-invocation-configuration')
            result[key] = value
        return result
    def refuse(value): raise HostError('evolution-invocation-configuration')
    def integer(value):
        # Only the three producer bounds are numeric; their largest admitted
        # value is 4194304. Refuse oversized tokens before bigint conversion.
        if len(value) > 7 or not re.fullmatch('0|[1-9][0-9]*', value):
            raise HostError('evolution-invocation-configuration')
        return int(value)
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_int=integer, parse_float=refuse, parse_constant=refuse)
        specific = 'request' if mode == 'fresh' else 'expected_sha256'
        if (type(value) is not dict or set(value) != {'profile', 'mode', 'ledger_path', 'receipt_path', 'producer', specific}
                or value['profile'] != PROFILE or value['mode'] != mode):
            raise HostError('evolution-invocation-configuration')
        producer = value['producer']
        if type(producer) is not dict or set(producer) != {'source', 'bun', 'git', 'timeout_seconds',
                'maximum_output_bytes', 'maximum_receipt_bytes'}:
            raise HostError('evolution-invocation-configuration')
        selected = EvolutionAdmissionConfig(*(absolute(producer[name]) for name in ('source', 'bun', 'git')),
            *(producer[name] for name in ('timeout_seconds', 'maximum_output_bytes', 'maximum_receipt_bytes')))
        ledger = absolute(value['ledger_path']); receipt = absolute(value['receipt_path'])
        if ledger == receipt: raise HostError('evolution-invocation-configuration')
        if mode == 'fresh':
            return FreshEvolutionInvocation(ledger, receipt, selected, request_from_document(value['request']))
        digest = value['expected_sha256']
        if type(digest) is not str or not re.fullmatch('[0-9a-f]{64}', digest):
            raise HostError('evolution-invocation-configuration')
        return ResumeEvolutionInvocation(ledger, receipt, selected, digest)
    except (ValueError, TypeError, KeyError, RecursionError, UnicodeError):
        raise HostError('evolution-invocation-configuration') from None


@dataclass(frozen=True, repr=False)
class InvocationFile:
    path: Path
    raw: bytes
    identity: tuple[int, int]

    def __post_init__(self):
        if (type(self.path) is not type(Path('/')) or not self.path.is_absolute()
                or type(self.raw) is not bytes or not 0 < len(self.raw) <= MAX_PROVIDER
                or type(self.identity) is not tuple or len(self.identity) != 2
                or any(type(value) is not int or value < 0 for value in self.identity)):
            raise HostError('evolution-invocation-file')

    @classmethod
    def read(cls, path, maximum):
        if type(path) is str:
            path = absolute(path)
        elif type(path) is not type(Path('/')) or not path.is_absolute():
            raise HostError('evolution-invocation-file')
        if type(maximum) is not int or not 0 < maximum <= MAX_PROVIDER:
            raise HostError('evolution-invocation-file')
        if path.parent.resolve() != path.parent:
            raise HostError('evolution-invocation-file')
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        primary = None; result = None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= maximum:
                raise HostError('evolution-invocation-file')
            chunks = []; count = 0
            while True:
                block = os.read(fd, min(65536, maximum + 1 - count))
                if not block: break
                chunks.append(block); count += len(block)
                if count > maximum: raise HostError('evolution-invocation-file')
            raw = b''.join(chunks); closing = os.fstat(fd); named = path.lstat()
            if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) != (
                    closing.st_dev, closing.st_ino, closing.st_size, closing.st_mtime_ns) or (
                    info.st_dev, info.st_ino) != (named.st_dev, named.st_ino) or stat.S_ISLNK(named.st_mode):
                raise HostError('evolution-invocation-file')
            result = cls(path, raw, (info.st_dev, info.st_ino))
        except BaseException as error: primary = error
        finish(primary, [lambda: os.close(fd)])
        return result

    def renew(self, maximum):
        current = self.read(self.path, maximum)
        if current.identity != self.identity or current.raw != self.raw:
            raise HostError('evolution-invocation-custody')


def load_evolution_provider(snapshot, expected_sha256):
    if type(snapshot) is not InvocationFile or type(snapshot.raw) is not bytes or not 0 < len(snapshot.raw) <= MAX_PROVIDER:
        raise HostError('evolution-provider-custody')
    if type(expected_sha256) is not str or not re.fullmatch('[0-9a-f]{64}', expected_sha256) or (
            hashlib.sha256(snapshot.raw).hexdigest() != expected_sha256):
        raise HostError('evolution-provider-custody')
    snapshot.__post_init__()
    # Compile the admitted bytes, rather than importing/reopening a mutable path.
    module = ModuleType('ashlar_operator_evolution_' + expected_sha256)
    module.__file__ = str(snapshot.path)
    exec(compile(snapshot.raw, str(snapshot.path), 'exec'), module.__dict__)
    if not callable(getattr(module, 'open_evolution', None)):
        raise HostError('evolution-provider-configuration')
    return module


def run_evolution_invocation(invocation, provider):
    """Use actual public full-run ports; release success after provider cleanup."""
    if type(invocation) not in (FreshEvolutionInvocation, ResumeEvolutionInvocation):
        raise HostError('evolution-invocation-configuration')
    invocation.__post_init__()
    answer = None
    with owned_context(provider.open_evolution(invocation)) as config:
        expected = FreshCommerceEvolutionConfig if type(invocation) is FreshEvolutionInvocation else ResumeCommerceEvolutionConfig
        if (type(config) is not expected or type(config.policy) is not NativeEvolutionRunPolicy
                or type(config.driver) is not NativeDriver
                or type(config.driver.source_sessions) is not RegisteredOutboxSources
                or type(config.driver.ack_sessions) is not RegisteredOutboxAcks):
            raise HostError('evolution-provider-configuration')
        config.__post_init__()
        validate_invocation_base(config.ledger_path, invocation.receipt_path, config.producer)
        if config.policy.driver is not config.driver or config.ledger_path != invocation.ledger_path or config.producer != invocation.producer:
            raise HostError('evolution-provider-configuration')
        if type(invocation) is FreshEvolutionInvocation:
            if config.request != invocation.request: raise HostError('evolution-provider-configuration')
            answer = publish_commerce_evolution(config)
        else:
            if config.expected_sha256 != invocation.expected_sha256: raise HostError('evolution-provider-configuration')
            answer = resume_commerce_evolution(config)
    if answer is None: raise HostError('evolution-invocation-incomplete')
    return answer


def run_evolution_command(configuration, provider_path, provider_sha256, mode):
    """Return closed operator receipt only after original input and cleanup gates."""
    config_file = InvocationFile.read(configuration, MAX_CONFIG)
    invocation = parse_evolution_invocation(config_file.raw, mode)
    if not invocation.receipt_path.parent.is_dir() or invocation.receipt_path.parent.resolve() != invocation.receipt_path.parent:
        raise HostError('evolution-receipt-path')
    if invocation.receipt_path.exists() or invocation.receipt_path.is_symlink():
        raise HostError('evolution-receipt-exists')
    # The provider boundary is explicitly trusted code. Input validation precedes it.
    source = InvocationFile.read(provider_path, MAX_PROVIDER)
    primary = None; answer = None
    try:
        provider = load_evolution_provider(source, provider_sha256)
        answer = run_evolution_invocation(invocation, provider)
    except BaseException as error: primary = error
    finish(primary, [lambda: source.renew(MAX_PROVIDER), lambda: config_file.renew(MAX_CONFIG)])
    if type(answer) is not dict or set(answer) != {'profile', 'run_sha256', 'publications'} or answer['profile'] != 'ashlar-commerce-evolution-run-result/0.1':
        raise HostError('evolution-result-refused')
    digest = answer['run_sha256']; publications = answer['publications']
    if type(digest) is not str or not re.fullmatch('[0-9a-f]{64}', digest) or type(publications) is not tuple or len(publications) != 8:
        raise HostError('evolution-result-refused')
    identifiers = [item.publication_id for item in publications]
    if any(type(value) is not str or not 0 < len(value) <= 1024 for value in identifiers) or len(set(identifiers)) != 8:
        raise HostError('evolution-result-refused')
    receipt = {'profile': 'ashlar-commerce-evolution-receipt/0.1', 'run_sha256': digest, 'publication_ids': identifiers}
    raw = (json.dumps(receipt, sort_keys=True, separators=(',', ':')) + '\n').encode()
    if len(raw) > 16384: raise HostError('evolution-result-refused')
    with owned_context(invocation.receipt_path.open('xb')) as stream:
        if stream.write(raw) != len(raw): raise HostError('evolution-receipt-write')
        stream.flush(); os.fsync(stream.fileno())
    return receipt
