#!/usr/bin/env python3
"""Reach the R40 worker's LocalPostgres entry seam without starting PG."""
from __future__ import annotations
import hashlib, importlib.util, json, os
from pathlib import Path

HERE=Path(__file__).resolve().parent
SCOPE_PATH=HERE/"r40-host-scope.json"
WORKER_PATH=HERE/"r40-local-worker.py"
RECEIPT_PATH=HERE/"r40-native-entry-shape-selftest.json"

class NativeEntryReached(RuntimeError): pass

def sha(raw): return hashlib.sha256(raw).hexdigest()

def remove_test_root(path, allowed_names):
    if not path.exists(): return
    if path.is_symlink() or path.stat().st_uid!=os.getuid():
        raise RuntimeError("refusing to clean a non-owned self-test root")
    names={p.name for p in path.iterdir()}
    if not names <= set(allowed_names):
        raise RuntimeError("self-test root has unexpected entries; preserving it")
    for child in path.iterdir():
        if child.is_symlink() or child.stat().st_uid!=os.getuid():
            raise RuntimeError("refusing to clean a foreign/symlink self-test entry")
        if child.is_dir():
            if child.name!="receipts" or {p.name for p in child.iterdir()}!={"failure.json"}:
                raise RuntimeError("unexpected worker self-test output; preserving it")
            receipt=child/"failure.json"
            if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_uid!=os.getuid():
                raise RuntimeError("refusing to clean an unexpected worker self-test receipt")
            receipt.unlink(); child.rmdir()
        else: child.unlink()
    path.rmdir()

def main():
    scope_raw=SCOPE_PATH.read_bytes(); scope=json.loads(scope_raw)
    out=Path(scope["paths"]["output_root"]); sup=Path(scope["paths"]["supervisor_root"])
    if out.exists() or sup.exists(): raise RuntimeError("fresh R40 roots must be absent")
    spec=importlib.util.spec_from_file_location("ashlar_r40_native_entry_worker",WORKER_PATH)
    worker=importlib.util.module_from_spec(spec); spec.loader.exec_module(worker)
    import truss
    reached={"value":False}
    class SentinelLocalPostgres:
        def __init__(self,data_directory):
            if Path(data_directory)!=Path(scope["paths"]["data_directory"]):
                raise AssertionError("worker selected unexpected PGDATA")
        def __enter__(self):
            reached["value"]=True
            raise NativeEntryReached("sentinel: stopped immediately before PostgreSQL startup")
        def __exit__(self,*_args): return False
    sup.mkdir(mode=0o700); os.chmod(sup,0o700)
    for name in ("child.stdout.log","child.stderr.log"):
        fd=os.open(sup/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        os.fchmod(fd,0o600); os.close(fd)
    old=truss.LocalPostgres; truss.LocalPostgres=SentinelLocalPostgres
    failure=None
    try:
        try: worker.native(scope,sup)
        except NativeEntryReached as e: failure=str(e)
        if not reached["value"] or failure is None:
            raise RuntimeError("native worker did not reach sentinel LocalPostgres.__enter__")
    finally:
        truss.LocalPostgres=old
        remove_test_root(out,{"receipts"})
        remove_test_root(sup,{"child.stdout.log","child.stderr.log"})
    if out.exists() or sup.exists(): raise RuntimeError("self-test did not restore R39 roots absent")
    receipt={"profile":"ashlar-r40-native-entry-shape-selftest/0.1","status":"passed_effect_free",
        "scope_sha256":sha(scope_raw),"worker_sha256":sha(WORKER_PATH.read_bytes()),
        "test_script_sha256":sha(Path(__file__).read_bytes()),
        "resume_evidence_projection":"validate_resume_evidence()['evidence'] accepted",
        "sentinel":"LocalPostgres.__enter__ reached and refused before startup",
        "postgres_started":False,"spark_started":False,"delta_written":False,"ack_attempted":False,
        "cloud":False,"output_and_supervisor_roots_absent_after_test":True}
    RECEIPT_PATH.write_text(json.dumps(receipt,sort_keys=True,indent=2)+"\n")
    return receipt

if __name__=="__main__": print(json.dumps(main(),sort_keys=True,separators=(",",":")))
