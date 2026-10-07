"""Tiny native changed-state controls for fail-closed publisher metadata component."""
import json,time,copy,hashlib
from pathlib import Path
from persistent_sql import Client
from publisher_custody_r462 import capture,verify,collect,CustodyMismatch
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_custody_controls_r463';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=90,cancel_after=45);start=time.monotonic();F='client_dev.ashlar_entropy_20261006_r86';t=F+'.custody_control_r463';other=F+'.custody_other_r463';a={'state':'running private native custody controls','controls':[],'bounds':{'read_bytes':10000000,'write_remote_bytes':10000000,'spill_to_disk_bytes':0,'wall_s':180}}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def refuse(label,actual,expected):
 try:verify(actual,expected)
 except CustodyMismatch as e:a['controls'].append({'label':label,'result':'refused','reason':str(e)});save()
 else:raise RuntimeError('Accepted '+label)
save()
try:
 c.sql('create','CREATE TABLE '+t+' (id BIGINT NOT NULL, exact_json STRING) USING DELTA');c.sql('create-other','CREATE TABLE '+other+' (id BIGINT NOT NULL, exact_json STRING) USING DELTA');baseline=capture(c,'fixture',t);verify(baseline,baseline);a['baseline']=baseline;save()
 c.sql('append',"INSERT INTO "+t+" VALUES (1,'{\"902\":1.2300e+04}')");advanced=capture(c,'fixture',t);refuse('unexpected native head',advanced,baseline);verify(advanced,advanced)
 c.sql('change-properties',"ALTER TABLE "+t+" SET TBLPROPERTIES ('delta.targetFileSize'='16777216')");changed=capture(c,'fixture',t);expected=copy.deepcopy(advanced);expected['head']=changed['head'];refuse('native properties changed with permitted new head',changed,expected)
 c.sql('change-schema','ALTER TABLE '+t+' ADD COLUMNS (unexpected STRING)');schema=capture(c,'fixture',t);expected=copy.deepcopy(changed);expected['head']=schema['head'];refuse('native schema changed with permitted new head',schema,expected)
 replacement=capture(c,'fixture',other);replacement['table']=t;replacement['head']=baseline['head'];refuse('other native UUID under expected binding',replacement,baseline)
 malformed=copy.deepcopy(schema);del malformed['detail']['minWriterVersion'];refuse('missing protocol metadata',malformed,schema)
 protocol=copy.deepcopy(schema);protocol['detail']['minWriterVersion']='999';refuse('changed protocol metadata local injection',protocol,schema)
 reordered=copy.deepcopy(schema);props=json.loads(reordered['detail']['properties']);reordered['detail']['properties']=json.dumps(dict(reversed(list(props.items()))));verify(reordered,schema);a['controls'].append({'label':'structured metadata reordered','result':'accepted identical meanings'})
 class Broken:
  def sql(self,*args):raise RuntimeError('Injected worker failure')
 try:collect([Broken()],[schema])
 except RuntimeError as e:a['controls'].append({'label':'worker failure','result':'no accepted cohort','reason':str(e)})
 else:raise RuntimeError('Accepted failed cohort')
 for i in range(20):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if i==19:raise
   time.sleep(1)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Native changed-state and local incomplete-worker custody controls pass',wall_s=time.monotonic()-start,code_sha256={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['publisher_custody_r462.py','custody_controls_r463.py']});assert a['wall_s']<180;save();(O/'live-statement.json').rename(O/'completed-last-statement.json');print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles; no replay',error=str(e));save();raise
