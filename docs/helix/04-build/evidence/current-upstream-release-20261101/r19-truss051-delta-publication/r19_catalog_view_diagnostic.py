#!/usr/bin/env python3
"""One bounded public catalog-view Outcome capture on a fresh disposable copy of stopped R19 clone."""
from __future__ import annotations
import hashlib, importlib.util, json, os, shutil, time
from pathlib import Path
import psycopg
from truss import LocalPostgres, CatalogRuntimeConfiguration, adopt_psycopg_host, create_reference_assembly

HERE=Path(__file__).resolve().parent
SCOPE=json.loads((HERE/'r19-publication-scope.json').read_text())
DIAG=Path('/private/tmp/ashlar-r19-catalog-view-diagnostic-20261101')
PGDATA=DIAG/'pgdata'
R19_CLONE=Path(SCOPE['paths']['migration_clone_pgdata'])
MAX_COPY=SCOPE['limits']['migration_preservation_bytes']
MAX_SECONDS=180

def digest_tree(path: Path):
    size=count=0; h=hashlib.sha256()
    for base,dirs,files in os.walk(path,followlinks=False):
        dirs.sort();files.sort()
        for name in dirs:
            p=Path(base)/name
            if p.is_symlink() or not p.is_dir(): raise RuntimeError(f'unsafe directory {p}')
            h.update(b'D\0'+p.relative_to(path).as_posix().encode()+b'\0')
        for name in files:
            p=Path(base)/name
            if p.is_symlink() or not p.is_file(): raise RuntimeError(f'unsafe file {p}')
            raw=p.read_bytes();size+=len(raw);count+=1
            h.update(b'F\0'+p.relative_to(path).as_posix().encode()+b'\0'+str(len(raw)).encode()+b'\0'+hashlib.sha256(raw).digest())
    return size,h.hexdigest(),count

def main():
    if DIAG.exists() or DIAG.is_symlink(): raise RuntimeError('diagnostic namespace must be absent')
    original=digest_tree(R19_CLONE)
    if original[0]>MAX_COPY: raise RuntimeError('diagnostic clone exceeds migration-preservation cap')
    if shutil.disk_usage(DIAG.parent).free<original[0]+256*1024*1024: raise RuntimeError('insufficient free disk for bounded diagnostic copy')
    DIAG.mkdir(mode=0o700);os.chmod(DIAG,0o700)
    shutil.copytree(R19_CLONE,PGDATA,symlinks=False,copy_function=shutil.copy2);os.chmod(PGDATA,0o700)
    copied=digest_tree(PGDATA)
    if copied!=original: raise RuntimeError('diagnostic copy does not match stopped R19 clone')
    start=time.monotonic();observed={}
    with LocalPostgres(PGDATA) as pg:
        if time.monotonic()-start>MAX_SECONDS: raise TimeoutError('diagnostic startup exceeded time bound')
        params=psycopg.conninfo.conninfo_to_dict(pg.info.connection_uri)
        params['user']=SCOPE['r16b_source']['reader_login_role']
        with psycopg.connect(**params,connect_timeout=5) as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
            conn.execute("SET LOCAL statement_timeout='5000ms'")
            observed['principal']=conn.execute("SELECT current_user, current_setting('transaction_read_only'), pg_catalog.has_function_privilege(current_user,'truss.person_catalog_view_basis(integer,integer,integer)'::pg_catalog.regprocedure,'EXECUTE'), pg_catalog.has_function_privilege(current_user,'truss.catalog_view_basis(integer,integer,integer)'::pg_catalog.regprocedure,'EXECUTE')").fetchone()
            host=adopt_psycopg_host(conn,statement_timeout_ms=5000,operation_timeout_ms=30000)
            assembly=None
            try:
                tx=host.adopt_transaction(access_mode='read_write')
                if hasattr(tx,'value'): tx=tx.value
                assembly=create_reference_assembly(CatalogRuntimeConfiguration(),host)
                if hasattr(assembly,'value'): assembly=assembly.value
                seed=assembly.catalog_view_request(definition_count='65536',encoded_bytes='16777216')
                outcome=assembly.catalog_view_in_transaction(tx,seed)
                observed['outcome_type']=type(outcome).__name__
                observed['outcome_repr']=repr(outcome)[:2000]
                error=getattr(outcome,'error',None)
                if error is not None:
                    observed['error']={name:getattr(error,name,None) for name in ('code','sql_state','catalog_code','message')}
                    nested=getattr(error,'error',None)
                    if nested is not None: observed['nested_error']={name:getattr(nested,name,None) for name in ('code','sql_state','catalog_code','message')}
            finally:
                if assembly is not None:
                    try: assembly.dispose()
                    except BaseException as error: observed['dispose_error']=f'{type(error).__name__}: {error}'[:500]
                try:
                    if not host.detach(): observed['detach_refused']=True
                except BaseException as error: observed['detach_error']=f'{type(error).__name__}: {error}'[:500]
            conn.rollback()
        if time.monotonic()-start>MAX_SECONDS: raise TimeoutError('diagnostic exceeded time bound')
    observed.update({'status':'captured_public_catalog_view_outcome','transaction_access_mode':'read_write','source_role':SCOPE['r16b_source']['reader_login_role'],
      'source_clone_copy_tree':copied,'r19_clone_unchanged':True,'acknowledgement':False,'cloud':False,'production_authority':False,'elapsed_seconds':round(time.monotonic()-start,3)})
    if (PGDATA/'postmaster.pid').exists(): raise RuntimeError('diagnostic PostgreSQL did not stop')
    receipt=HERE/'r19-catalog-view-diagnostic-receipt.json';receipt.write_text(json.dumps(observed,indent=2,default=list)+'\n');os.chmod(receipt,0o600)
    print(json.dumps(observed,sort_keys=True,default=list))

if __name__=='__main__':main()
