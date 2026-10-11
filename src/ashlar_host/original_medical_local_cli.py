"""Installed original medical operations with explicit runtime and worker custody."""
from contextlib import contextmanager, redirect_stdout
import hashlib
import json
import os
from pathlib import Path
import sys

from .evolution_cli import InvocationFile
from .lifecycle import finish, owned_context
from .medical_process_supervision import OriginalMedicalNativePhase
from .medical_worker_capture import capture_original_medical_worker
from .original_medical_local_configuration import (
    OriginalMedicalInitializationConfig, OriginalMedicalPublicationConfig, OriginalMedicalReplayConfig, OriginalMedicalReadConfig, OriginalMedicalRecoveryConfig, parse_original_medical_local)
from .original_medical_local_setup import initialize_original_medical_local
from .original_medical_local_publication import publish_original_medical_local, replay_original_medical_local, recover_original_medical_local
from .original_medical_runtime import OriginalMedicalRuntimeCustody
from .original_medical_local_read import read_original_medical_local

MAX_CONFIGURATION=262144


def _configuration(raw,operation):
    types={'initialization':OriginalMedicalInitializationConfig,'publication':OriginalMedicalPublicationConfig,'replay':OriginalMedicalReplayConfig,'read':OriginalMedicalReadConfig,'recovery':OriginalMedicalRecoveryConfig}
    if type(operation)is not str or operation not in types:raise ValueError('Closed medical operation required')
    config=parse_original_medical_local(raw)
    if type(config)is not types[operation]:raise ValueError('Exact selected medical operation configuration required')
    return config


def _initialization(raw):return _configuration(raw,'initialization')


def _execute(configuration, *, expected_sha256, phase, operation):
    """Worker dispatch retains its original config before and after the host entry."""
    if (type(expected_sha256)is not str or len(expected_sha256)!=64
            or any(c not in '0123456789abcdef' for c in expected_sha256)
            or type(phase)is not OriginalMedicalNativePhase):
        raise ValueError('Exact selected medical worker digest and phase required')
    file=InvocationFile.read(configuration,MAX_CONFIGURATION)
    if hashlib.sha256(file.raw).hexdigest()!=expected_sha256:
        raise PermissionError('Original selected medical worker configuration differs')
    config=_configuration(file.raw,operation);result=None;primary=None
    try:
        file.renew(MAX_CONFIGURATION)
        entry={'initialization':initialize_original_medical_local,'publication':publish_original_medical_local,'replay':replay_original_medical_local,'read':read_original_medical_local,'recovery':recover_original_medical_local}[operation]
        result=entry(config,supervision=phase)
    except BaseException as error:primary=error
    finish(primary,[lambda:file.renew(MAX_CONFIGURATION)])
    return result


def _run(configuration,operation):
    """Parent retains actual runtime custody and fixed isolated installed dispatch."""
    file=InvocationFile.read(configuration,MAX_CONFIGURATION)
    config=_configuration(file.raw,operation);raw=None;primary=None
    try:
        with owned_context(OriginalMedicalRuntimeCustody.open(
                config.runtime,config.inputs,config.bounds,config.producer))as custody:
            def renew():
                finish(None,[lambda:file.renew(MAX_CONFIGURATION),custody.renew])
            argv=[str(config.runtime.python.path),'-I','-B','-m',
                  'ashlar_host.original_medical_local_cli',
                  '--operation',operation,'--configuration',str(file.path),'--sha256',hashlib.sha256(file.raw).hexdigest()]
            raw,_=capture_original_medical_worker(argv,cwd=file.path.parent,
                environment=custody.native_environment(),renew=renew,bounds=config.bounds)
    except BaseException as error:primary=error
    finish(primary,[lambda:file.renew(MAX_CONFIGURATION)])
    if type(raw)is not bytes or not 0<len(raw)<=config.bounds.receipt_bytes:
        raise ValueError('Bounded medical operation result required')
    # Reuse the configuration parser's strict JSON conventions for the protocol.
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate medical result member')
            result[key]=value
        return result
    def constant(value):raise ValueError('Nonfinite medical result refused')
    result=json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)
    if type(result)is not dict:raise ValueError('Medical operation object required')
    return result


def execute_original_medical_initialization(configuration,*,expected_sha256,phase):
    return _execute(configuration,expected_sha256=expected_sha256,phase=phase,operation='initialization')

def execute_original_medical_publication(configuration,*,expected_sha256,phase):
    return _execute(configuration,expected_sha256=expected_sha256,phase=phase,operation='publication')

def execute_original_medical_replay(configuration,*,expected_sha256,phase):
    return _execute(configuration,expected_sha256=expected_sha256,phase=phase,operation='replay')

def execute_original_medical_read(configuration,*,expected_sha256,phase):
    return _execute(configuration,expected_sha256=expected_sha256,phase=phase,operation='read')

def execute_original_medical_recovery(configuration,*,expected_sha256,phase):
    return _execute(configuration,expected_sha256=expected_sha256,phase=phase,operation='recovery')


def run_original_medical_recovery_command(configuration):return _run(configuration,'recovery')


def medical_failure_metadata(error):
    """Closed public cause/status/phase only; private stream prefixes stay private."""
    if not isinstance(error,BaseException):return None
    attributes=BaseException.__dict__['__dict__'].__get__(error,type(error))
    capture=attributes.get('medical_capture')
    if type(capture)is not dict:return None
    failure=capture.get('failure')
    if type(failure)is not dict or set(failure)!={'cause','status','native_phase'}:return None
    if (type(failure['cause'])is not str or failure['cause']not in
            ('native-deadline','whole-deadline','child-exit','capture-capacity','phase-protocol','custody-refusal','closing-refusal','cancel','unknown')
            or type(failure['native_phase'])is not str or failure['native_phase']not in ('ready','begun','ended')
            or failure['status']is not None and (type(failure['status'])is not int or not -255<=failure['status']<=255)):
        return None
    return {key:failure[key]for key in ('cause','status','native_phase')}


def run_original_medical_read_command(configuration):return _run(configuration,'read')

def run_original_medical_initialization_command(configuration):return _run(configuration,'initialization')
def run_original_medical_publication_command(configuration):return _run(configuration,'publication')
def run_original_medical_replay_command(configuration):return _run(configuration,'replay')


@contextmanager
def _worker_diagnostic_interval():
    """Keep Python and inherited native fd-one writes outside the JSON protocol."""
    descriptor=None;primary=None
    try:
        sys.stdout.flush();sys.stderr.flush()
        descriptor=os.dup(1)
        os.dup2(2,1)
        with redirect_stdout(sys.stderr):yield
    except BaseException as error:primary=error
    finish(primary,[sys.stderr.flush]+([] if descriptor is None else
        [lambda:os.dup2(descriptor,1),lambda:os.close(descriptor)]))


def _worker_main():
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--operation',choices=['initialization','publication','replay','read','recovery'],default='initialization')
    parser.add_argument('--configuration',required=True)
    parser.add_argument('--sha256',required=True)
    parser.add_argument('--native-phase-fd',type=int,required=True)
    args=parser.parse_args()
    phase=OriginalMedicalNativePhase(args.native_phase_fd)
    file=InvocationFile.read(Path(args.configuration),MAX_CONFIGURATION)
    config=_configuration(file.raw,args.operation)
    if hashlib.sha256(file.raw).hexdigest()!=args.sha256:
        raise PermissionError('Original selected medical worker configuration differs')
    # Native/producer diagnostics cannot enter the result protocol.
    with _worker_diagnostic_interval():
        result=_execute(Path(args.configuration),
            expected_sha256=args.sha256,phase=phase,operation=args.operation)
    file.renew(MAX_CONFIGURATION)
    parts=[];size=0
    for part in json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':')).iterencode(result):
        piece=part.encode();size+=len(piece)
        if size+1>config.bounds.receipt_bytes:raise ValueError('Bounded medical operation result required')
        parts.append(piece)
    file.renew(MAX_CONFIGURATION)
    sys.stdout.buffer.write(b''.join(parts)+b'\n');sys.stdout.buffer.flush()


if __name__=='__main__':
    try:_worker_main()
    except Exception:
        print('Original medical operation refused',file=sys.stderr)
        raise SystemExit(2)from None
