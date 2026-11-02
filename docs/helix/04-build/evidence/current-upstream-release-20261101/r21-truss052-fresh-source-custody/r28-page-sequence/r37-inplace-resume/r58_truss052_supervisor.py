#!/usr/bin/env python3
"""Bounded R58 committed-page recovery supervisor; no source/destination copying."""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, os, subprocess, sys
from pathlib import Path
PROFILE="ashlar-r58-truss052-committed-page-supervisor/0.1"
MAX_WALL=900; MAX_RSS=2*1024**3; MAX_BYTES=512*1024**2
HERE=Path(__file__).resolve().parent
BASE_SUPERVISOR=HERE.parent/"r36_truss052_supervisor.py"

def sha(raw): return hashlib.sha256(raw).hexdigest()
def load_base(path):
 spec=importlib.util.spec_from_file_location("ashlar_r36_supervisor_helpers",path)
 mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); return mod
def strict(raw):
 def pairs(items):
  d={}
  for k,v in items:
   if k in d: raise ValueError("duplicate JSON key")
   d[k]=v
  return d
 x=json.loads(raw,object_pairs_hook=pairs); assert type(x) is dict; return x
def admit(scope_path,worker_path,*,allow_dispatch=False):
 raw=scope_path.read_bytes(); scope=strict(raw); need= lambda c,m: (_ for _ in ()).throw(ValueError(m)) if not c else None
 need(scope.get("profile")=="ashlar-r58-committed-page-recovery/0.1" and scope.get("authority_class")=="local_development_authority" and scope.get("cloud") is False and scope.get("production_authority") is False,"local R58 scope required")
 need(scope.get("status")==("sol_reviewed_and_dispatchable" if allow_dispatch else "pending_sol_review"),"R58 scope status does not match preflight/dispatch mode")
 limits=scope.get("limits",{}); need(limits.get("wall_seconds")==MAX_WALL and limits.get("process_rss_bytes")==MAX_RSS and limits.get("attempt_root_bytes")==MAX_BYTES and limits.get("attempts")==1 and limits.get("automatic_retries")==0 and limits.get("cloud") is False and limits.get("network_listen") is False,"R58 bounded local limits mismatch")
 worker=Path(scope["worker"]["path"]); sup_entry=scope.get("supervisor",{})
 need(worker==worker_path.resolve() and sha(worker.read_bytes())==scope["worker"]["sha256"],"worker binding mismatch")
 need(Path(sup_entry.get("path",""))==Path(__file__).resolve() and sup_entry.get("sha256")==sha(Path(__file__).read_bytes()),"supervisor binding mismatch")
 base_path=Path(scope["retained_r36"]["base_supervisor_path"]); need(base_path.is_file() and sha(base_path.read_bytes())==scope["retained_r36"]["base_supervisor_sha256"],"R36 monitor helper hash mismatch")
 base=load_base(base_path); paths=scope["paths"]
 out=Path(paths["output_root"]); sup=Path(paths["supervisor_root"]); pg=Path(paths["data_directory"]); delta=Path(paths["publication_directory"])
 need(not out.exists() and not sup.exists() and out.is_absolute() and sup.is_absolute(),"fresh R58 roots required")
 need(pg.is_dir() and delta.is_dir() and not pg.is_symlink() and not delta.is_symlink() and pg.resolve()==pg and delta.resolve()==delta,"existing R36 roots must remain canonical")
 need(not (pg/"postmaster.pid").exists(),"R36 PGDATA must be stopped before preflight")
 py=base._selected_python(scope["runtime_candidate"]["local_runtime_base_python"])
 return scope,raw,base,py,out,sup,pg,delta

def main():
 p=argparse.ArgumentParser(); p.add_argument("--scope",required=True,type=Path); p.add_argument("--worker",required=True,type=Path); p.add_argument("--preflight-only",action="store_true"); p.add_argument("--dispatch-receipt",type=Path); a=p.parse_args()
 try:
  scope,raw,base,py,out,sup,pg,delta=admit(a.scope.resolve(),a.worker.resolve(),allow_dispatch=a.dispatch_receipt is not None)
  cmd=[str(py),str(a.worker.resolve()),"--scope",str(a.scope.resolve()),"--attempt-root",str(out),"--preflight-only"]
  result=subprocess.run(cmd,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=90,check=False,env={**os.environ,"ASHLAR_R36_CLOUD":"0"})
  if result.returncode: raise ValueError("worker effect-free preflight refused: "+result.stderr.decode("utf-8","replace")[:1200])
  worker_receipt=strict(result.stdout)
  if worker_receipt.get("status")!="passed_effect_free_inplace_preflight" or worker_receipt["scope_sha256"]!=sha(raw): raise ValueError("worker preflight receipt mismatch")
  if a.preflight_only:
   if a.dispatch_receipt is not None: raise ValueError("preflight cannot include a dispatch receipt")
   print(json.dumps({"profile":PROFILE,"status":"preflight_ok","worker_preflight":worker_receipt,"attempt_root":str(out),"supervisor_root":str(sup),"monitored_existing_roots":[str(pg),str(delta)],"native_effects":False},sort_keys=True,separators=(",",":"))); return 0
  if a.dispatch_receipt is None: raise ValueError("native dispatch requires exact Sol-reviewed receipt")
  dispatch=strict(a.dispatch_receipt.read_bytes())
  expected={"profile":"ashlar-r58-inplace-dispatch/0.1","status":"sol_reviewed_and_dispatchable","scope_sha256":sha(raw),"worker_sha256":sha(Path(scope["worker"]["path"]).read_bytes()),"supervisor_sha256":sha(Path(__file__).read_bytes()),
            "page_match_helper_sha256":sha(Path(scope["page_match_helper"]["path"]).read_bytes()),
            "native_dispatch":True,"native_run_performed":False,"source_ack_attempted":False,
            "mandatory_pre_ack_gate":"independent read-only PostgreSQL request_receipt and source-page SQL confirmation"}
  if dispatch!=expected: raise ValueError("R58 dispatch receipt does not bind the exact reviewed files")
  original_tree_bytes=base._tree_bytes
  def monitored_tree_bytes(_attempt_root):
   total=sum(original_tree_bytes(root) for root in (out,pg,delta))
   if total>MAX_BYTES: raise ValueError("R58 aggregate output, PGDATA and Delta bytes exceed 512 MiB")
   return total
  base.PROFILE=PROFILE; base.DISPATCH_PROFILE="ashlar-r58-inplace-dispatch/0.1"; base.MAX_ATTEMPT_BYTES=MAX_BYTES
  base._tree_bytes=monitored_tree_bytes
  return base._dispatch(scope,raw,Path(scope["worker"]["path"]),py,out,pg,sup,Path(__file__).resolve(),a.scope.resolve())
 except Exception as e:
  print(json.dumps({"profile":PROFILE,"status":"refused","reason":f"{type(e).__name__}: {e}"[:1500],"native_effects":False},sort_keys=True,separators=(",",":")),file=sys.stderr); return 2
if __name__=="__main__": raise SystemExit(main())
