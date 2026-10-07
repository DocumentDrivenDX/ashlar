"""Alternating exact reads; owned read-only child capped60s by its parent."""
import json,subprocess,sys,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_key_screen_r147/read-lane'
def worker():
 s=json.loads((O.parent/'summary.json').read_text());assert s['state'].startswith('Both key-screen')
 c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=15')
 assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
 S='client_dev.ashlar_entropy_20261006_r86.schedule_r139_1'
 rows=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30");assert len(rows)==30
 for i,row in enumerate(rows):
  for mode in (('hash','source_id') if i%2==0 else ('source_id','hash')):
   table=s['tables'][mode];A=table['table'];v=table['version']
   assert A=='client_dev.ashlar_entropy_20261006_r86.key_'+mode+'_r147'
   query=f"SELECT {','.join(COLS)} FROM {A} VERSION AS OF {v} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
   assert c.sql(mode+'-read-'+str(i),query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]})==[row]
 c.close();print('Sixty exact alternating key-screen reads passed')
if __name__=='__main__':
 if sys.argv[1:]==['--worker']:worker()
 else:
  assert not (O/'process-bound.json').exists(),'Inspect existing handle before repeating'
  O.mkdir(parents=True,exist_ok=True);start=time.monotonic()
  with (O/'worker-output.txt').open('w') as log:
   p=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--worker'],stdout=log,stderr=log)
   try:code=p.wait(timeout=60)
   except subprocess.TimeoutExpired:
    p.terminate()
    try:code=p.wait(timeout=5)
    except subprocess.TimeoutExpired:p.kill();code=p.wait()
   (O/'process-bound.json').write_text(json.dumps({'pid':p.pid,'exit_code':code,'worker_wall_s':time.monotonic()-start,'max_worker_wall_s':60},indent=2)+'\n')
  print('Read worker exit',code);sys.exit(0 if code==0 else 1)
