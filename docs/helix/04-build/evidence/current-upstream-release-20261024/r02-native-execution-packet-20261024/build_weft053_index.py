#!/usr/bin/env python3
"""Effect-free index builder for the selected local Weft 0.4.53 wheel cohort."""
from __future__ import annotations
import argparse, hashlib, importlib.metadata, json, os, stat
from pathlib import Path
import weft

VERSION='0.4.53'
SOURCE='726a840e956e25217d6aeea2674fdfcf3f342a24'
WHEEL_SHA='4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c'
SCHEMAS=('compile-request-v0.4.1.schema.json','compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')
def sha(raw): return hashlib.sha256(raw).hexdigest()
def digest(path):
    path=Path(path); st=path.lstat()
    if not stat.S_ISREG(st.st_mode) or path.is_symlink(): raise SystemExit('index input is not a regular file')
    return sha(path.read_bytes()),st.st_size

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--wheel',type=Path,required=True); ap.add_argument('--schemas',type=Path,required=True); ap.add_argument('--output',type=Path,required=True); a=ap.parse_args()
    dist=importlib.metadata.distribution('weft-sql')
    if dist.version!=VERSION or weft.__version__!=VERSION: raise SystemExit('selected Weft version differs')
    wheel=a.wheel.absolute(); wsha,_=digest(wheel)
    if wsha!=WHEEL_SHA: raise SystemExit('selected Weft wheel digest differs')
    direct=dist.read_text('direct_url.json')
    if not direct or json.loads(direct).get('archive_info',{}).get('hashes',{}).get('sha256')!=WHEEL_SHA or json.loads(direct).get('url')!='file://'+str(wheel): raise SystemExit('installed wheel origin differs')
    root=Path(dist.locate_file('')).resolve(strict=True)
    files=[]
    for item in sorted(dist.files or (),key=str):
        rel=Path(str(item)).as_posix()
        if rel.startswith(('weft/','weft_sql-0.4.53.dist-info/')):
            path=root/rel; d,n=digest(path); files.append({'path':rel,'bytes':n,'sha256':d})
    if not files or Path(weft.__file__).resolve().as_posix()!=Path(root/'weft/__init__.py').as_posix(): raise SystemExit('installed Weft inventory incomplete')
    schemas={}
    for name in SCHEMAS:
        p=(a.schemas/name).resolve(strict=True); d,_=digest(p); schemas[name]={'path':str(p),'sha256':d}
    index={'profile':'ashlar-weft-053-runtime-index/0.1','sourceCommit':SOURCE,'version':VERSION,
        'wheel':{'path':str(wheel),'readPath':str(wheel.resolve(strict=True)),'sha256':WHEEL_SHA},'root':str(root),
        'module':str(Path(weft.__file__).resolve()),'files':files,'schemas':schemas}
    raw=(json.dumps(index,sort_keys=True,separators=(',',':'))+'\n').encode()
    out=a.output.resolve(); out.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        if os.write(fd,raw)!=len(raw): raise SystemExit('short index write')
        os.fsync(fd)
    finally: os.close(fd)
    print(json.dumps({'index':str(out),'sha256':sha(raw),'fileCount':len(files),'schemaHashes':{k:v['sha256']for k,v in schemas.items()}},sort_keys=True))
if __name__=='__main__': main()
