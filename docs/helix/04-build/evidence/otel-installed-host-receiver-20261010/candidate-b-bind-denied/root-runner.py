import hashlib,json,subprocess,time
from pathlib import Path
ROOT=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-b')
opening=json.loads((ROOT/'opening-inputs.json').read_text())
def verify():
 for row in opening['files']:
  raw=Path(row['path']).read_bytes()
  assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256'],row['path']
verify()
command=json.loads((ROOT/'command.json').read_text())
assert command['source']['sha256']=='d6b4eaf033ae4c4df23faaa25be5b2b723287d53195586bf1cc1abb4821f3fbf'
assert hashlib.sha256((ROOT/'command.json').read_bytes()).hexdigest()=='37941ff0cffd879c5b9b07dfad4cec2279c32a79fe17ad5bb53dc4373c3d7f50'
assert not any((ROOT/name).exists() for name in ['capture','result.json','result.stage.json','expected.json','receiver-observations.json'])
assert not list(ROOT.glob('request-*.pb'))
started=time.monotonic()
with (ROOT/'process.stdout').open('xb') as stdout,(ROOT/'process.stderr').open('xb') as stderr:
 completed=subprocess.run(command['argv'],cwd=command['cwd'],env=command['env'],stdout=stdout,stderr=stderr)
elapsed=time.monotonic()-started
closing_error=None
try:verify()
except Exception as error:closing_error=type(error).__name__
receipt={'format':'ashlar-actual-process/0.1','argv':command['argv'],'environment':command['env'],'cwd':command['cwd'],'exitCode':completed.returncode,'durationSeconds':elapsed,'selectedInputCount':len(opening['files']),'closingCustodyPassed':closing_error is None,'closingErrorClass':closing_error,'scope':'Actual installed public adapter and bounded cooperative loopback receiver; no native workflow or full C006 claim.'}
(ROOT/'process.json').write_text(json.dumps(receipt,indent=2)+'\n')
if closing_error is None:(ROOT/'closing-inputs.json').write_bytes((ROOT/'opening-inputs.json').read_bytes())
print(json.dumps(receipt))
