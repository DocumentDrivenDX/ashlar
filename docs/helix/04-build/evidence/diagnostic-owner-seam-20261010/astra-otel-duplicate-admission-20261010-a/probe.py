from pathlib import Path
import hashlib,json
from ashlar_host.otel import OtelStartupError
from test_otel_supervision import SupervisionTests,CHILD
case=SupervisionTests()
child=CHILD.replace("write({'dependency_admitted':True})", "write({'dependency_admitted':True})\nwrite({'dependency_admitted':True})")
try:
 case.create(child)
 raise AssertionError('duplicate unexpectedly constructed run')
except OtelStartupError as error:
 receipt={'scope':'Actual finite Python fake child and current facade; no SDK worker/provider/receiver/network/native.',
 'observed':{'errorType':type(error).__name__,'cleanup_complete':error.cleanup_complete,'dependency_admitted':error.dependency_admitted,
 'compositionFallbackPredicate':type(error)is OtelStartupError and error.cleanup_complete is True and error.dependency_admitted is True},
 'childrenReaped':all(p.returncode is not None for p in case.children),'pipesClosed':all(p.stdin.closed and p.stdout.closed for p in case.children)}
 assert receipt['observed']['compositionFallbackPredicate'] is True
 assert receipt['childrenReaped'] and receipt['pipesClosed']
 receipt['sourceSnapshots']=[]
 for name in ('otel.py','diagnostic_composition.py','test_otel_supervision.py'):
  p=Path(__file__).parent/name;b=p.read_bytes();receipt['sourceSnapshots'].append(dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest()))
 p=Path(__file__).parent/'review.json';p.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
