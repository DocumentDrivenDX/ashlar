"""Native-evidence positive and adversarial provenance/membership refusal checks."""
import copy,json
from pathlib import Path
from raw_validation_queries import origin_sql,verified_origin,raw_validation
B=Path(__file__).resolve().parent
records=[json.loads(l) for l in (B/'out/native/ashlar_origin_validation_r151/statements.jsonl').read_text().splitlines()]
r=next(x for x in records if x['label']=='stage-origin');stage='client_dev.ashlar_entropy_20261006_r86.schedule_r139_1'
p=verified_origin(stage,100000,r)
query=raw_validation(stage,'client_dev.ashlar_entropy_20261006_r86.source_record_r89',17,'r139-b1',p)
actual=next(x['sql'] for x in records if x['label']=='bounded-0')
# Decoded hex tokens allow an exact equivalence of literal forms without interpreting arbitrary SQL.
actual=actual.replace("apply_batch_id='r139-b1'","apply_batch_id=decode(unhex('723133392d6231'),'UTF-8')")
assert ' '.join(query.split())==' '.join(actual.split())
cases=[]
for name in ['wrong-query','wrong-version','failed','missing-id','null-feed','empty-epoch','two-origins','wrong-count','noncanonical-count','numeric-feed']:
 x=copy.deepcopy(r)
 if name=='wrong-query':x['sql']=origin_sql(stage)+' WHERE false'
 elif name=='wrong-version':x['sql']=x['sql'].replace('AS OF 0','AS OF 1')
 elif name=='failed':x['response']['status']['state']='UNKNOWN'
 elif name=='missing-id':x['statement_id']=None
 elif name=='null-feed':x['response']['result']['data_array'][0][0]=None
 elif name=='empty-epoch':x['response']['result']['data_array'][0][1]=''
 elif name=='two-origins':x['response']['result']['data_array'].append(['other','epoch','1'])
 elif name=='wrong-count':x['response']['result']['data_array'][0][2]='99999'
 elif name=='noncanonical-count':x['response']['result']['data_array'][0][2]=100000
 elif name=='numeric-feed':x['response']['result']['data_array'][0][0]=42
 try:verified_origin(stage,100000,x)
 except ValueError:cases.append(name)
 else:raise AssertionError(name)
try:raw_validation(stage.replace('_1','_2'),'client_dev.ashlar_entropy_20261006_r86.source_record_r89',17,'r139-b1',p)
except ValueError:cases.append('different-stage')
else:raise AssertionError('different-stage')
(B/'out/raw-validation-guard-check.json').write_text(json.dumps({'native_positive':'Builder matches executed r151 exact bounded SQL after equivalent batch literal spelling','refused':cases,'qualification':'Owned trusted controller provenance guards; not adversarial record authentication, generic SQL interpretation or real producer fencing'},indent=2)+'\n')
print('Native-evidence query matches;11 adversarial provenance controls refused')
