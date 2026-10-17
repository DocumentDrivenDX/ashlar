from pathlib import Path
import json,hashlib
from collections import Counter
r=Path('/private/tmp/ashlar-medical-query-public-cli-20261016-x');q=r/'read-query-output';config=json.loads((r/'query-configuration.json').read_bytes());read=config['read'];receipt=json.loads((q/'receipt.json').read_bytes());terminal=json.loads((r/'outer-attempt.json').read_bytes());assert terminal['exit_code']==0 and terminal['primary_type']is None
assert (r/'cli.stdout').read_bytes()==(q/'receipt.json').read_bytes();assert config['cases']==['all-objects'];cases=receipt['query_batch']['cases'];assert len(cases)==8
for pin in read['inputs'].values():
 raw=Path(pin['path']).read_bytes();assert len(raw)==pin['bytes']and hashlib.sha256(raw).hexdigest()==pin['sha256']
graph=json.loads(Path(read['inputs']['graph']['path']).read_bytes());canon=lambda x:json.dumps(x,sort_keys=True,separators=(',',':'));allrows=[];guards=0;sourceguards=0;nullable_null=nullable_bool=0;counts={}
def contains(x,y):return x==y or (isinstance(x,dict)and any(contains(v,y)for v in x.values()))or(isinstance(x,list)and any(contains(v,y)for v in x))
for case in cases:
 assert case['recompile_equal']is True
 for pin in [case['evidence'],*case['original_protocol'].values()]:
  raw=(q/pin['file']).read_bytes();assert len(raw)==pin['bytes']and hashlib.sha256(raw).hexdigest()==pin['sha256']
 assert case['original_protocol']['response']==case['original_protocol']['recompiled_response']
 artifact=json.loads((q/case['original_protocol']['response']['file']).read_bytes());ev=json.loads((q/case['evidence']['file']).read_bytes());e=ev['evidence']
 assert ev['semantic_rows']==case['semantic_rows'];assert Counter(map(canon,e['oracle']['witnesses']['original_rows']))==Counter(map(canon,case['semantic_rows']))
 assert e['native_result']['rows']==e['oracle']['rows'];assert e['opening_native_files']==e['closing_native_files']
 for guard in e['guards']:
  assert contains(artifact['obligations'],guard['check'])
  if guard['check'].get('publicSourceOnly')is True:
   assert guard['check']['field']['element']=='resource_references.reference_id';sourceguards+=1
   actual=[]
   for row in guard['native']['rows']:
    value=json.loads(row[4])[guard['check']['propertyId']];assert type(value)is int and row[5]==str(value);actual.append(row[5])
   expected=[o['values']['resource_references.reference_id']for o in graph['objects']if o['type']['element']=='resource_references']
   assert len(actual)==17 and Counter(actual)==Counter(expected)
  else:assert guard['native']['rows']==[['0']];guards+=1
 for row in e['native_result']['rows']:
  for value in row:
   if isinstance(value,str)and value.startswith('{"state":'):
    value=json.loads(value);nullable_null+=value.get('state')=='null';nullable_bool+=type(value.get('value'))is bool
 allrows.extend(case['semantic_rows']);counts[case['case']]=len(case['semantic_rows'])
assert len(allrows)==len(graph['objects'])==51 and Counter(map(canon,allrows))==Counter(map(canon,graph['objects']))
before=json.loads((r/'native-files-before.json').read_bytes());after=json.loads((r/'native-files-after.json').read_bytes());assert before==after
closing=receipt['query_batch']['batch_closing']['closing_observation'];opening=receipt['observation'];assert opening['native']==closing['native'];ack=opening['protected_ack'];assert str(ack['position'])==str(closing['protected_ack']['position'])
for k,pin in closing['protected_ack']['original_bytes'].items():
 raw=bytes.fromhex(ack[k]);assert len(raw)==pin['bytes']and hashlib.sha256(raw).hexdigest()==pin['sha256']
revalidation=json.loads((q/'public-revalidation.json').read_bytes())
for k in ('model','graph','baseline'):assert revalidation[k+'Sha256']==read['inputs'][k]['sha256']
result={'exit_code':0,'elapsed_seconds':terminal['elapsed_seconds'],'projections':8,'original_object_counter':51,'counts':counts,'nullable_null_cells':nullable_null,'nullable_bool_cells':nullable_bool,'zero_original_native_violation_guards':guards,'original_integer_source_guards':sourceguards,'integer_source_guard_rows':17,'public_stdout_equals_receipt':True,'full_native_files_equal':True,'full_ack_closing_pins':True,'source_input_pins':True,'exact_protocol_recompile_pins':True}
print(json.dumps(result));Path('/private/tmp/medical-x-retained-verification.json').write_text(json.dumps(result,indent=2)+'\n')
