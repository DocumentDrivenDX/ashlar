#!/usr/bin/env python3
"""Build an effect-free inventory index for an official Weft 0.4.56 install."""
from __future__ import annotations
import argparse,hashlib,importlib,importlib.metadata,json,os,platform,stat,sys
from pathlib import Path
sys.dont_write_bytecode=True

VERSION='0.4.56'; SOURCE='2f62e1148d1b2030558550e4f6f2ffe518bac62d'
WHEELS={('Darwin','arm64'):('68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d',),
        ('Linux','x86_64'):('32f4c38d9a614dd4c3a367f8b24d5cd4ba41ec80c2a4a2e3423e2082a294dbdd',)}
SCHEMAS=('compile-request-v0.4.1.schema.json','compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')
def sha(raw): return hashlib.sha256(raw).hexdigest()
def digest(path):
    path=Path(path); st=path.lstat()
    if not stat.S_ISREG(st.st_mode) or path.is_symlink(): raise SystemExit('index input is not a regular file')
    raw=path.read_bytes(); return sha(raw),len(raw)
def refuse_bytecode_caches(root):
    for package in (root/'weft',root/'umf_weft-0.4.56.dist-info'):
        if not package.is_dir() or package.is_symlink(): raise SystemExit('installed Weft package tree is missing')
        for parent,dirs,names in os.walk(package,followlinks=False):
            if any(name=='__pycache__' for name in dirs) or any(name.endswith(('.pyc','.pyo')) for name in names):
                raise SystemExit('installed Weft bytecode cache is refused')
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--wheel',type=Path,required=True); ap.add_argument('--schemas',type=Path,required=True); ap.add_argument('--output',type=Path,required=True); a=ap.parse_args()
    expected=WHEELS.get((platform.system(),platform.machine()))
    dist=importlib.metadata.distribution('umf-weft')
    if expected is None or dist.version!=VERSION: raise SystemExit('selected Weft version/platform differs')
    wheel=a.wheel.absolute(); wsha,_=digest(wheel)
    if wsha not in expected: raise SystemExit('selected Weft wheel digest differs')
    direct=dist.read_text('direct_url.json')
    direct_value=json.loads(direct) if direct else None
    recorded_hash=direct_value.get('archive_info',{}).get('hashes',{}).get('sha256') if type(direct_value)is dict else None
    if type(direct_value)is not dict or direct_value.get('url')!='file://'+str(wheel) or recorded_hash not in (None,wsha): raise SystemExit('installed wheel origin differs')
    root=Path(dist.locate_file('')).resolve(strict=True); files=[]
    refuse_bytecode_caches(root)
    weft=importlib.import_module('weft')
    if weft.__version__!=VERSION: raise SystemExit('installed Weft import version differs')
    for item in sorted(dist.files or (),key=str):
        rel=Path(str(item)).as_posix()
        if rel.startswith(('weft/','umf_weft-0.4.56.dist-info/')):
            path=root/rel; d,n=digest(path); files.append({'path':rel,'bytes':n,'sha256':d})
    if not files or Path(weft.__file__).resolve()!=root/'weft/__init__.py': raise SystemExit('installed Weft inventory incomplete')
    schemas={}
    for name in SCHEMAS:
        p=(a.schemas/name).resolve(strict=True); d,_=digest(p); schemas[name]={'path':str(p),'sha256':d}
    index={'profile':'ashlar-weft-056-runtime-index/0.1','sourceCommit':SOURCE,'version':VERSION,
        'wheel':{'path':str(wheel),'readPath':str(wheel.resolve(strict=True)),'sha256':wsha},'root':str(root),
        'module':str(Path(weft.__file__).resolve()),'files':files,'schemas':schemas}
    raw=(json.dumps(index,sort_keys=True,separators=(',',':'))+'\n').encode(); out=a.output.resolve(); out.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        if os.write(fd,raw)!=len(raw): raise SystemExit('short index write')
        os.fsync(fd)
    finally: os.close(fd)
    print(json.dumps({'index':str(out),'sha256':sha(raw),'fileCount':len(files),'schemaHashes':{k:v['sha256'] for k,v in schemas.items()}},sort_keys=True))
if __name__=='__main__': main()
