"""Matched immutable E9/E10 full-carrier reads for newly changed seventh keys."""
import json,hashlib,time,math
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from seventh_changes_r589 import SeventhChanges
from overlay_sql_r395 import FIELDS
from normalized_apply_sql_r276 import pin
B=Path(__file__).resolve().parent
def main():
 source=B/'out/native/ashlar_maintained_concurrent_r626/audited-summary.json';pub=json.loads(source.read_text());assert pub['state']=='All64 complete maintained carriers/absence pass four synchronized readers';old=new=pub['table'];assert new['version']==12
 O=B/'out/native/ashlar_binding_compare_r630';assert not O.exists();O.mkdir();start=time.monotonic();c=BoundedReads(O,socket_timeout=30);a={'state':'reading newly changed keys','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'old':old,'new':new,'indices':pub['indices'],'reads':[],'bounds':{'read_bytes':25000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':300},'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['binding_compare_r630.py','seventh_changes_r589.py','overlay_sql_r395.py']},'qualification':'Exact32key R626 cohort atE12, parameter/literal full20field queries alternate in two passes. Literals UTF8hex encode strings and canonicalize integer typedkeys; semantic predicates/values unchanged. No SQL resultcache, no cold/sustained/concurrency/freshness/billion admission; fixed text perkey does not establish generic plan reuse.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and time.monotonic()-start<300;save();return h
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false';rs=c.sql('detail','DESCRIBE DETAIL '+new['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rs[0]))['id']==new['id'];assert c.sql('head','DESCRIBE HISTORY '+new['table']+' LIMIT 1')[0][0]=='12'
  columns=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in FIELDS);where='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)';a['queries']={family:f'SELECT {columns} FROM {pin(t["table"],t["version"])} WHERE {where}' for family,t in [('parameter',old),('literal',new)]};changes=SeventhChanges()
  for phase in range(2):
   for j,index in enumerate(a['indices']):
    x=changes.change(index);row=x['parameter'];params={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']}
    for family in (['parameter','literal'] if (phase+j)%2==0 else ['literal','parameter']):
     query=a['queries'][family];bound=params
     if family=='literal':
      lit=lambda v:"decode(unhex('"+str(v).encode().hex()+"'),'UTF-8')"
      query=query.replace(':hash',lit(params['hash'])).replace(':source',lit(params['source'])).replace(':type',str(int(params['type']))).replace(':id',str(int(params['id'])));bound=None
     expected_row=x['after'];expected=[] if expected_row is None else [[expected_row[f].replace('T',' ').removesuffix('Z') if f=='published_at' else expected_row[f] for f in FIELDS]];assert c.sql(f'point-{family}-{phase}-{index}',query,parameters=bound)==expected;a['reads'].append({'family':family,'phase':phase,'index':index,'statement_id':c.records[-1]['statement_id']});save()
   metrics()
  assert c.sql('final-head','DESCRIBE HISTORY '+new['table']+' LIMIT 1')[0][0]=='12';h=metrics();records={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def group(rr):return {'queries':len(rr),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in rr]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rr]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rr]),'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rr)}
  assert len(a['reads'])==128 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads']);a['per_family']={f:group([r for r in a['reads'] if r['family']==f]) for f in ['parameter','literal']};a['per_family_phase']={f+'-'+str(p):group([r for r in a['reads'] if r['family']==f and r['phase']==p]) for f in ['parameter','literal'] for p in range(2)};a.update(state='All128 matched binding-comparison full-carrier direct reads pass',wall_s=time.monotonic()-start);save();print(json.dumps({k:a[k] for k in ['state','costs','wall_s','per_family','per_family_phase']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same driver handles, no read replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
