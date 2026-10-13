import json,subprocess,hashlib
from pathlib import Path
from test_weft_query import WeftQueryTests
from test_publication import FakeBackend,TABLE
from ashlar.pins import PinVector
source=Path(__file__).with_name('original-weft-query.py').read_bytes()
ns={'__package__':'ashlar'};exec(compile(source,'original-weft-query.py','exec'),ns)
t=WeftQueryTests();q,a,p,e,policy=t.setup();events=[];pending=[]
def cold():events.append({'event':'body','held':p.active});yield 'unprotected'
def decode(*args):
 g=cold();pending.append(g);return {'nested':[g]}
policy.decode_result=decode
rows=ns['read_weft'](e,FakeBackend(),p,PinVector('a','manifest','p1','a'*64,{TABLE:('trusted-uuid',6)}),policy,request=q,artifact=a,context='authorized',supported_profiles=['ashlar-delta/0.3'],supported_revisions={'source':['r1']})
assert not p.active and pending[0].gi_frame is not None
assert next(rows[0][0]['nested'][0])=='unprotected'
for g in pending:g.close()
print(json.dumps({'sourceSha256':hashlib.sha256(source).hexdigest(),'returnedAfterClose':not p.active,'deferredBodyEvents':events,'scope':'Original read_weft source with original compiled query/test SQL executor and pin controls; actual Python nested-generator escape after held interval. Not Databricks execution or authentic policy/source admission.'},indent=2))
