"""Prepared only: actual filesystem/producer owners, no native/source authority."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time

p = argparse.ArgumentParser()
p.add_argument('--installed-target', required=True)
p.add_argument('--configuration', required=True)
a = p.parse_args()
target = Path(a.installed_target)
configuration = Path(a.configuration)
if any(not x.is_absolute() or x.resolve() != x for x in (target, configuration)):
    raise ValueError('Canonical explicit installed target/configuration required')
sys.path.insert(0, str(target))
from ashlar_host.lifecycle import finish, owned_context
root = Path(tempfile.mkdtemp(prefix='ashlar-medical-owner-diagnostic-', dir='/private/tmp'))
os.chmod(root, 0o700)
records = []
report = {'scope': 'Actual non-Spark runtime/held dataset/admission renewals only',
          'plan_excluded': 'Public source read_original/renew_original require bound actual NativeDriver and LocalDeltaTransport; neither is acquired',
          'outbox_source_access': False, 'records': records,
          'deadline_scope': '30-second soft Python-body alarm; required owner cleanup may extend elapsed time'}

def save():
    raw = (json.dumps(report, sort_keys=True, separators=(',', ':'))+'\n').encode()
    if len(raw) > 16384:
        raise ValueError('Bounded diagnostic report required')
    path = root / 'report.json'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    try:
        offset = 0
        while offset < len(raw):
            count = os.write(fd, raw[offset:])
            if count <= 0:
                raise OSError('Complete diagnostic report required')
            offset += count
        os.fsync(fd)
    finally:
        os.close(fd)

def timed(name, call):
    started = time.monotonic_ns()
    try:
        answer = call()
    except BaseException as error:
        records.append({'phase': name, 'nanoseconds': time.monotonic_ns()-started,
                        'outcome': 'failed', 'exception_type': type(error).__name__})
        finish(error, [save])
    records.append({'phase': name, 'nanoseconds': time.monotonic_ns()-started, 'outcome': 'completed'})
    save()
    return answer

def deadline(_signum, _frame):
    raise TimeoutError('Thirty-second diagnostic deadline')

signal.signal(signal.SIGALRM, deadline)
signal.alarm(30)
primary = None
invocation = None
try:
    from ashlar_host.original_medical_local_configuration import parse_original_medical_local
    from ashlar_host.original_medical_runtime import OriginalMedicalRuntimeCustody
    from ashlar_host.medical_producer import HeldMedicalDataset
    from ashlar_host.original_medical_publication import MedicalAdmission
    from ashlar_host.evolution_cli import InvocationFile
    from ashlar.medical_source import build_transaction
    from ashlar_host.medical_publication_plan import FEED
    import ashlar_host.original_medical_runtime as selected
    if not Path(selected.__file__).resolve().is_relative_to(target):
        raise PermissionError('Selected installed module required')
    invocation = InvocationFile.read(configuration, 262144)
    raw = invocation.raw
    config = parse_original_medical_local(raw)
    if (Path(sys.executable).resolve() != config.runtime.python.path
            or '.'.join(map(str, sys.version_info[:3])) != config.runtime.python_version):
        raise PermissionError('Exact configured interpreter process required')
    report['configuration_sha256'] = hashlib.sha256(raw).hexdigest()
    report['runtime_module_sha256'] = hashlib.sha256(Path(selected.__file__).read_bytes()).hexdigest()
    with owned_context(OriginalMedicalRuntimeCustody.open(config.runtime, config.inputs, config.bounds, config.producer)) as custody:
        timed('runtime.renew', custody.renew)
        dataset = timed('dataset.acquire', lambda: HeldMedicalDataset(config.producer,
            config.inputs.model.path, config.inputs.graph.path, config.inputs.baseline.path,
            root/'public-revalidation.json'))
        inner = None
        try:
            timed('dataset.renew', dataset.renew)
            batch, bindings = build_transaction(*custody.input_bytes, source_system=FEED,
                binding_profile='ashlar-medical-development-bindings/0.2')
            admission = timed('admission.acquire', lambda: MedicalAdmission(batch, bindings,
                config.inputs.model.path, config.inputs.graph.path, config.inputs.baseline.path, dataset))
            metadata = timed('admission.metadata', admission.metadata)
            report['counts'] = {key: metadata[key] for key in
                ('record_count', 'edge_count', 'history_count', 'boolean_receipt_count')}
        except BaseException as error:
            inner = error
        finish(inner, [lambda: timed('dataset.closing', dataset.renew)])
        save()  # Persist before actual runtime owner closing.
        closing_started = time.monotonic_ns()
    records.append({'phase': 'runtime.closing', 'nanoseconds': time.monotonic_ns()-closing_started,
                    'outcome': 'completed'})
    report['runtime_closing_completed'] = True
except BaseException as error:
    primary = error
    report['exception_type'] = type(error).__name__
finally:
    signal.alarm(0)
    report['completed'] = primary is None
    callbacks = ([] if invocation is None else [lambda: invocation.renew(262144)]) + [save]
    finish(primary, callbacks)
    print(str(root/'report.json'), flush=True)
