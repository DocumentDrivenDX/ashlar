"""Bounded installed verification dispatch; an explicit external supervisor owns time/streams."""
import hashlib
import json
import os
from pathlib import Path
from contextlib import contextmanager
import re
import stat
from types import ModuleType
from .config import EvolutionAdmissionConfig, HostError
from .evolution_cli import InvocationFile, absolute, validate_invocation_base
from .evolution_verification_types import VerificationBounds, VerificationSupervision, VerifyEvolutionInvocation, VerifyCommerceEvolutionConfig
from .evolution_verification import verify_commerce_evolution
from .lifecycle import finish, owned_context
from .delta_custody import bounded_local_json_bytes

PROFILE = 'ashlar-evolution-verification-invocation/0.1'
MAXIMUM_INPUT_BYTES = 262144


def parse_verification_invocation(raw):
    if type(raw) is not bytes or not 0 < len(raw) <= MAXIMUM_INPUT_BYTES:
        raise HostError('evolution-verification-configuration')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate field')
            result[key] = value
        return result
    def refuse(value): raise ValueError('Exact integers required')
    def integer(value):
        if len(value) > 10: raise ValueError('Bounded integer required')
        return int(value)
    def closed(value, keys):
        if type(value) is not dict or set(value) != set(keys): raise ValueError('Closed configuration required')
        return value
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_int=integer, parse_float=refuse, parse_constant=refuse)
        closed(value, ('profile','ledger_path','receipt_path','producer','expected_sha256','ordinal','bounds','supervision'))
        if value['profile'] != PROFILE: raise ValueError('Exact verification profile required')
        producer = closed(value['producer'], ('source','bun','git','timeout_seconds','maximum_output_bytes','maximum_receipt_bytes'))
        selected = EvolutionAdmissionConfig(*(absolute(producer[name]) for name in ('source','bun','git')),
            *(producer[name] for name in ('timeout_seconds','maximum_output_bytes','maximum_receipt_bytes')))
        bounds = VerificationBounds(**closed(value['bounds'], ('input_bytes','rows','cell_bytes','total_bytes','receipt_bytes','capture_bytes')))
        supervision = VerificationSupervision(**closed(value['supervision'], ('deadline_seconds','capture_bytes','owner')))
        result = VerifyEvolutionInvocation(absolute(value['ledger_path']), absolute(value['receipt_path']), selected,
            value['expected_sha256'], value['ordinal'], bounds, supervision)
        validate_invocation_base(result.ledger_path, result.receipt_path, selected)
        if len(raw) > bounds.input_bytes: raise ValueError('Selected input bound exceeded')
        return result
    except (ValueError, TypeError, KeyError, RecursionError, UnicodeError):
        raise HostError('evolution-verification-configuration') from None


def run_verification_invocation(invocation, provider):
    if type(invocation) is not VerifyEvolutionInvocation:
        raise HostError('evolution-verification-configuration')
    invocation.__post_init__()
    answer = None
    with owned_context(provider.open_verification(invocation)) as config:
        if type(config) is not VerifyCommerceEvolutionConfig or any(getattr(config, name) != getattr(invocation, name)
                for name in ('ledger_path','producer','expected_sha256','ordinal','bounds','supervision')):
            raise HostError('evolution-verification-provider')
        config.__post_init__()
        answer = verify_commerce_evolution(config)
    return answer


def run_verification_command(configuration, provider_path, provider_sha256):
    """Run under the operator's explicitly selected finite process supervisor.

    This function has no implicit watchdog. The selection is bound to the actual
    trusted provider before any native owner is acquired; operator cancellation
    remains BaseException-transparent. Success follows all provider cleanup.
    """
    captured = InvocationFile.read(configuration, MAXIMUM_INPUT_BYTES)
    invocation = parse_verification_invocation(captured.raw)
    if type(provider_sha256) is not str or re.fullmatch('[0-9a-f]{64}', provider_sha256) is None:
        raise HostError('evolution-verification-provider')
    provider = None; parent_fd = None; primary = None; answer = None
    try:
        parent = invocation.receipt_path.parent
        parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        identity = os.fstat(parent_fd)
        def renew_parent():
            current = parent.lstat(); held = os.fstat(parent_fd)
            if (current.st_dev,current.st_ino,current.st_mode,current.st_uid) != (identity.st_dev,identity.st_ino,identity.st_mode,identity.st_uid) or (held.st_dev,held.st_ino) != (identity.st_dev,identity.st_ino):
                raise HostError('evolution-verification-output-custody')
        renew_parent()
        if invocation.receipt_path.exists() or invocation.receipt_path.is_symlink():
            raise HostError('evolution-verification-existing-receipt')
        provider = InvocationFile.read(provider_path, invocation.bounds.input_bytes)
        if provider.path in (captured.path, invocation.ledger_path, invocation.receipt_path) or captured.path in (invocation.ledger_path, invocation.receipt_path):
            raise HostError('evolution-verification-distinct-inputs')
        if hashlib.sha256(provider.raw).hexdigest() != provider_sha256:
            raise HostError('evolution-verification-provider')
        module = ModuleType('ashlar_verification_provider_' + provider_sha256)
        module.__file__ = str(provider.path)
        exec(compile(provider.raw, str(provider.path), 'exec'), module.__dict__)
        if not callable(getattr(module,'open_verification',None)):
            raise HostError('evolution-verification-provider')
        def renew_inputs():
            captured.renew(MAXIMUM_INPUT_BYTES)
            provider.renew(invocation.bounds.input_bytes)
            renew_parent()
        renew_inputs()
        class CapturedProvider:
            @contextmanager
            def open_verification(self, selected):
                with owned_context(module.open_verification(selected)) as config:
                    if type(config) is not VerifyCommerceEvolutionConfig or any(getattr(config, name) != getattr(invocation, name)
                            for name in ('ledger_path','producer','expected_sha256','ordinal','bounds','supervision')):
                        raise HostError('evolution-verification-provider')
                    config.__post_init__()
                    renew_inputs()
                    yield config
        answer = run_verification_invocation(invocation, CapturedProvider())
    except BaseException as error:
        primary = error
    callbacks = [lambda: captured.renew(MAXIMUM_INPUT_BYTES)]
    if provider is not None: callbacks.append(lambda: provider.renew(invocation.bounds.input_bytes))
    if parent_fd is not None: callbacks.append(renew_parent)
    try:
        finish(primary, callbacks)
        if type(answer) is not dict or answer.get('profile') != 'ashlar-evolution-publication-verification/0.1' or answer.get('original_sha256') != invocation.expected_sha256 or answer.get('ordinal') != invocation.ordinal:
            raise HostError('evolution-verification-result')
        bounded_local_json_bytes(answer, invocation.bounds.receipt_bytes - 1)
        raw = (json.dumps(answer, ensure_ascii=False, sort_keys=True, separators=(',',':'), allow_nan=False)+'\n').encode()
        if len(raw) > invocation.bounds.receipt_bytes:
            raise HostError('evolution-verification-receipt-bound')
        renew_parent()
        fd = os.open(invocation.receipt_path.name, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600, dir_fd=parent_fd)
        error = None
        try:
            held = os.fstat(fd)
            view = memoryview(raw)
            while view:
                written = os.write(fd, view)
                if type(written) is not int or not 0 < written <= len(view):
                    raise HostError('evolution-verification-output-write')
                view = view[written:]
            os.fsync(fd); os.fsync(parent_fd)
            named = os.stat(invocation.receipt_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISREG(named.st_mode) or (named.st_dev,named.st_ino,named.st_size) != (held.st_dev,held.st_ino,len(raw)):
                raise HostError('evolution-verification-output-custody')
        except BaseException as failure: error = failure
        finally:
            try:
                finish(error, [renew_parent, lambda: captured.renew(MAXIMUM_INPUT_BYTES),
                    lambda: provider.renew(invocation.bounds.input_bytes), lambda: os.close(fd)])
            except BaseException as output_error:
                def remove_owned_failed_receipt():
                    current = os.stat(invocation.receipt_path.name, dir_fd=parent_fd, follow_symlinks=False)
                    if (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
                        raise HostError('evolution-verification-foreign-output-preserved')
                    os.unlink(invocation.receipt_path.name, dir_fd=parent_fd)
                    os.fsync(parent_fd)
                finish(output_error, [remove_owned_failed_receipt])
    except BaseException as error:
        primary = error
    finally:
        finish(primary, [lambda: os.close(parent_fd)] if parent_fd is not None else [])
    return answer
