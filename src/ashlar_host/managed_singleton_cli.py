# SPDX-License-Identifier: MIT OR Apache-2.0
"""Installed managed read with an owned finite worker and closed JSON result."""
from contextlib import contextmanager,redirect_stdout
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
from .evolution_cli import InvocationFile
from .lifecycle import finish
from .managed_singleton_configuration import parse_managed_singleton
from .managed_publication_selection import open_managed_publication_selection
from .managed_pin_reader import ManagedPinReader
from .managed_sql_read import open_managed_sql_read,managed_sql_child_context
from .managed_singleton import read_managed_singleton
from .process_capture import capture_process

RESULT_PROFILE='ashlar-managed-singleton-result/0.1'
MAX_CONFIGURATION=262144

def _encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

def _write(path,raw):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);primary=None
    try:
        if os.write(fd,raw)!=len(raw):raise OSError('managed-read-short-write')
        os.fsync(fd)
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)])

@contextmanager
def _diagnostics():
    sys.stdout.flush();sys.stderr.flush();saved=os.dup(1);primary=None
    try:
        os.dup2(2,1)
        with redirect_stdout(sys.stderr):yield
    except BaseException as error:primary=error
    finish(primary,[sys.stderr.flush,lambda:os.dup2(saved,1),lambda:os.close(saved)])

def _report(config,row):
    return dict(profile=RESULT_PROFILE,publication_id=config.selection.publication_id,
        source_system=config.selector.source,type_id=config.selector.type_id,
        entity_id=config.selector.entity_id,row=row)

def _worker(configuration,expected_sha256):
    selected=InvocationFile.read(configuration,MAX_CONFIGURATION)
    if hashlib.sha256(selected.raw).hexdigest()!=expected_sha256:raise PermissionError('managed-read-selection-changed')
    config=parse_managed_singleton(selected.raw);result=None;primary=None
    previous=signal.getsignal(signal.SIGALRM)
    def expired(*_):
        signal.setitimer(signal.ITIMER_REAL,0)
        raise TimeoutError('managed-read-work-deadline')
    signal.signal(signal.SIGALRM,expired)
    signal.setitimer(signal.ITIMER_REAL,config.whole_seconds-config.cleanup_seconds)
    def restore():
        signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)
    try:
        with _diagnostics():
            evidence=config.output/'evidence';evidence.mkdir(mode=0o700)
            context=object()
            with open_managed_publication_selection(config.selection)as selection:
                pins=ManagedPinReader(config.pins,context)
                with open_managed_sql_read(config.sql,context=managed_sql_child_context())as sql:
                    row=read_managed_singleton(selection,sql,pins,producer=config.producer,
                        selector=config.selector,context=context,capture_directory=evidence)
            selected.renew(MAX_CONFIGURATION);config.__post_init__()
            result=_report(config,row)
    except BaseException as error:primary=error
    finish(primary,[restore,lambda:selected.renew(MAX_CONFIGURATION)])
    raw=_encoded(result)
    if len(raw)>config.receipt_bytes:raise ValueError('managed-read-result-capacity')
    return raw

def run_managed_singleton_command(configuration):
    """Return only after actual worker termination and original selection closing."""
    file=InvocationFile.read(configuration,MAX_CONFIGURATION);config=parse_managed_singleton(file.raw)
    config.output.mkdir(mode=0o700)
    os.chmod(config.output,0o700)
    primary=None;result=None
    try:
        # HOME is the explicit current operator home for the selected CLI's
        # ordinary credential provider. Authentication settings come from its
        # selected profile/file, never ambient DATABRICKS/Azure settings.
        environment={'PATH':'/usr/bin:/bin','HOME':str(Path.home()),'LANG':'C.UTF-8','LC_ALL':'C.UTF-8'}
        raw,diagnostics=capture_process([sys.executable,'-I','-B','-m','ashlar_host.managed_singleton_cli',
            '--configuration',str(file.path),'--expected-sha256',hashlib.sha256(file.raw).hexdigest()],
            cwd=config.output,environment=environment,timeout_seconds=config.whole_seconds,
            maximum_output_bytes=config.stream_bytes)
        file.renew(MAX_CONFIGURATION);config.__post_init__()
        if len(raw)>config.receipt_bytes:raise ValueError('managed-read-result-capacity')
        from ashlar.schema import decode_retained_json
        result=decode_retained_json(raw)
        if (type(result)is not dict or set(result)!=set(_report(config,None))
                or {k:v for k,v in result.items()if k!='row'}!={k:v for k,v in _report(config,None).items()if k!='row'}
                or type(result['type_id'])is not int or type(result['entity_id'])is not int
                or result['row']is not None and type(result['row'])is not dict):
            raise ValueError('managed-read-result-correspondence')
        _write(config.output/'diagnostics.log',diagnostics)
        file.renew(MAX_CONFIGURATION)
        _write(config.output/'report.json',_encoded(result))
    except BaseException as error:primary=error
    def failure():
        if primary is not None:_write(config.output/'failure.json',_encoded({'profile':RESULT_PROFILE,'status':'refused'}))
    finish(primary,[failure])
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--configuration',type=Path,required=True)
    parser.add_argument('--expected-sha256',required=True)
    args=parser.parse_args()
    try:
        raw=_worker(args.configuration,args.expected_sha256)
    except Exception:
        print('managed singleton worker refused',file=sys.stderr);return 1
    sys.stdout.buffer.write(raw);sys.stdout.buffer.flush();return 0

if __name__=='__main__':raise SystemExit(main())
