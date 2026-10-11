"""Installed medical initialization with explicit runtime and worker custody."""
from contextlib import redirect_stdout
import hashlib
import json
from pathlib import Path
import sys

from .evolution_cli import InvocationFile
from .lifecycle import finish, owned_context
from .medical_process_supervision import OriginalMedicalNativePhase
from .medical_worker_capture import capture_original_medical_worker
from .original_medical_local_configuration import (
    OriginalMedicalInitializationConfig, parse_original_medical_local)
from .original_medical_local_setup import initialize_original_medical_local
from .original_medical_runtime import OriginalMedicalRuntimeCustody

MAX_CONFIGURATION=262144


def _initialization(raw):
    config=parse_original_medical_local(raw)
    if type(config)is not OriginalMedicalInitializationConfig:
        raise ValueError('Exact medical initialization configuration required')
    return config


def execute_original_medical_initialization(configuration, *, expected_sha256, phase):
    """Worker dispatch retains its original config before and after the host entry."""
    if (type(expected_sha256)is not str or len(expected_sha256)!=64
            or any(c not in '0123456789abcdef' for c in expected_sha256)
            or type(phase)is not OriginalMedicalNativePhase):
        raise ValueError('Exact selected medical worker digest and phase required')
    file=InvocationFile.read(configuration,MAX_CONFIGURATION)
    if hashlib.sha256(file.raw).hexdigest()!=expected_sha256:
        raise PermissionError('Original selected medical worker configuration differs')
    config=_initialization(file.raw);result=None;primary=None
    try:
        file.renew(MAX_CONFIGURATION)
        result=initialize_original_medical_local(config,supervision=phase)
    except BaseException as error:primary=error
    finish(primary,[lambda:file.renew(MAX_CONFIGURATION)])
    return result


def run_original_medical_initialization_command(configuration):
    """Parent retains actual runtime custody and fixed isolated installed dispatch."""
    file=InvocationFile.read(configuration,MAX_CONFIGURATION)
    config=_initialization(file.raw);raw=None;primary=None
    try:
        with owned_context(OriginalMedicalRuntimeCustody.open(
                config.runtime,config.inputs,config.bounds,config.producer))as custody:
            def renew():
                finish(None,[lambda:file.renew(MAX_CONFIGURATION),custody.renew])
            argv=[str(config.runtime.python.path),'-I','-B','-m',
                  'ashlar_host.original_medical_local_cli',
                  '--configuration',str(file.path),'--sha256',hashlib.sha256(file.raw).hexdigest()]
            raw,_=capture_original_medical_worker(argv,cwd=file.path.parent,
                environment=custody.native_environment(),renew=renew,bounds=config.bounds)
    except BaseException as error:primary=error
    finish(primary,[lambda:file.renew(MAX_CONFIGURATION)])
    if type(raw)is not bytes or not 0<len(raw)<=config.bounds.receipt_bytes:
        raise ValueError('Bounded medical initialization result required')
    # Reuse the configuration parser's strict JSON conventions for the protocol.
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate medical result member')
            result[key]=value
        return result
    def constant(value):raise ValueError('Nonfinite medical result refused')
    result=json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)
    if type(result)is not dict:raise ValueError('Medical initialization object required')
    return result


def _worker_main():
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--configuration',required=True)
    parser.add_argument('--sha256',required=True)
    parser.add_argument('--native-phase-fd',type=int,required=True)
    args=parser.parse_args()
    phase=OriginalMedicalNativePhase(args.native_phase_fd)
    file=InvocationFile.read(Path(args.configuration),MAX_CONFIGURATION)
    config=_initialization(file.raw)
    # Native/producer diagnostics cannot enter the result protocol.
    with redirect_stdout(sys.stderr):
        result=execute_original_medical_initialization(Path(args.configuration),
            expected_sha256=args.sha256,phase=phase)
    file.renew(MAX_CONFIGURATION)
    parts=[];size=0
    for part in json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':')).iterencode(result):
        piece=part.encode();size+=len(piece)
        if size+1>config.bounds.receipt_bytes:raise ValueError('Bounded medical initialization result required')
        parts.append(piece)
    file.renew(MAX_CONFIGURATION)
    sys.stdout.buffer.write(b''.join(parts)+b'\n');sys.stdout.buffer.flush()


if __name__=='__main__':
    try:_worker_main()
    except Exception:
        print('Original medical initialization refused',file=sys.stderr)
        raise SystemExit(2)from None
