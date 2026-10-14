"""Existing owned PG18 host lifecycle via public pg_ctl, no shared pgserver lock."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import subprocess
import fasteners,psycopg
from ashlar_host.lifecycle import finish
_RETAINED_LEASES=[]
DATA=Path('/private/tmp/ashlar-truss15-parallel-example-20261013-a/example/data')
CTL=Path('/private/tmp/truss-pg18-consumer-env-20261011/lib/python3.11/site-packages/pgserver/pginstall/bin/pg_ctl')
@contextmanager
def host_postgres():
 lease=fasteners.InterProcessLock(str(DATA)+'.truss-runtime.lock')
 if not lease.acquire(blocking=False):raise RuntimeError('Original source directory is already leased')
 issued=False;primary=None
 try:
  if DATA.resolve()!=DATA or DATA.joinpath('PG_VERSION').read_text().strip()!='18' or DATA.joinpath('postmaster.pid').exists():raise RuntimeError('Original stopped PostgreSQL18 directory required')
  issued=True
  subprocess.run([str(CTL),'-D',str(DATA),'-w','-o','-h "" -k '+str(DATA),'-l',str(DATA/'log'),'start'],check=True,timeout=10)
  uri=psycopg.conninfo.make_conninfo(host=str(DATA),port='5432',dbname='postgres',user='postgres')
  with psycopg.connect(uri,autocommit=True,connect_timeout=5) as connection:
   connection.execute("SET statement_timeout='5000ms'")
   if connection.execute('SHOW server_version').fetchone()!=('18.6',):raise RuntimeError('Pinned PostgreSQL18.6 required')
  yield SimpleNamespace(info=SimpleNamespace(connection_uri=uri))
 except BaseException as error:primary=error
 def stop():
  if issued and DATA.joinpath('postmaster.pid').exists():
   subprocess.run([str(CTL),'-D',str(DATA),'-m','fast','-w','stop'],check=True,timeout=10)
 def release():
  if not DATA.joinpath('postmaster.pid').exists():lease.release()
  else:
   _RETAINED_LEASES.append(lease)
   raise RuntimeError('Owned source stop unconfirmed; directory lease retained until supervisor process exit')
 finish(primary,[stop,release])
