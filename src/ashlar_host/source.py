"""Bounded actual public UMF producer transport; no native authority inference."""
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time
from .config import HostError, ProducerConfig
from .resources import RESOURCE_ROOT
from ashlar.commerce_source import SOURCE_SHA, GRAPH_SHA


def read_bounded(path: Path, maximum: int) -> bytes:
    if path.is_symlink() or not path.is_file():raise HostError('regular-input-required')
    with path.open('rb') as f: raw=f.read(maximum+1)
    if len(raw)>maximum:raise HostError('input-bound')
    return raw


def original_inputs(model: Path, graph: Path) -> tuple[bytes, bytes]:
    a=read_bounded(model,4*1024*1024);b=read_bounded(graph,4*1024*1024)
    if hashlib.sha256(a).hexdigest()!=SOURCE_SHA or hashlib.sha256(b).hexdigest()!=GRAPH_SHA:raise HostError('original-commerce-inputs-required')
    return a,b


def _capture(argv: list[str], timeout: int, maximum: int) -> tuple[int,bytes,bytes]:
    proc=None; selector=None; primary=None; cleanup=False; out=bytearray();err=bytearray()
    try:
        proc=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        selector=selectors.DefaultSelector()
        for stream,dest in ((proc.stdout,out),(proc.stderr,err)):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,dest)
        deadline=time.monotonic()+timeout
        while selector.get_map() or proc.poll() is None:
            left=deadline-time.monotonic()
            if left<=0:raise HostError('public-producer-timeout')
            for key,_ in selector.select(min(left,.05)):
                dest=key.data;chunk=os.read(key.fd,min(65536,maximum+1-len(dest)))
                if not chunk:selector.unregister(key.fileobj);continue
                dest.extend(chunk)
                if len(dest)>maximum:raise HostError('public-producer-output-bound')
        code=proc.returncode
    except BaseException as e:primary=e
    finally:
        if proc is not None:
            try:os.killpg(proc.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            except BaseException:cleanup=True
            try:proc.wait(timeout=1)
            except BaseException:cleanup=True
            for stream in (proc.stdout,proc.stderr):
                try:stream.close()
                except BaseException:cleanup=True
        if selector is not None:
            try:selector.close()
            except BaseException:cleanup=True
    if primary is not None:
        if cleanup:
            try:primary.cleanup_failed=True
            except BaseException:pass
        raise primary
    if cleanup:raise HostError('public-producer-cleanup-failed')
    return code,bytes(out),bytes(err)


def recompute_dataset(config: ProducerConfig, model: Path, graph: Path, output: Path) -> dict:
    originals=original_inputs(model,graph)
    script=RESOURCE_ROOT/'check_commerce_dataset.ts';script_bytes=read_bounded(script,1024*1024)
    for program in (config.bun,config.git):
        if not program.is_file() or not os.access(program,os.X_OK):raise HostError('explicit-producer-executable-required')
    if output.exists():raise HostError('fresh-public-receipt-required')
    code,stdout,stderr=_capture([str(config.bun),str(script),str(config.source),str(output),str(model),str(graph),str(config.git)],config.timeout_seconds,config.maximum_output_bytes)
    if code:raise HostError('public-dataset-refused')
    raw=read_bounded(output,config.maximum_receipt_bytes)
    if original_inputs(model,graph)!=originals or read_bounded(script,1024*1024)!=script_bytes:raise HostError('public-source-custody-drift')
    return {'exit':code,'stdout':stdout.decode('utf-8','strict'),'stderr':stderr.decode('utf-8','strict'),'umf_pin':'c7c95e1c4ea5b72541f47fa0350ca467ff02f395','umf_source':str(config.source),'receipt_sha256':hashlib.sha256(raw).hexdigest()}
