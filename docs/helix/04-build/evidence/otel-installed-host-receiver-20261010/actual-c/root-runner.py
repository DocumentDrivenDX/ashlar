import hashlib,json,subprocess,time
from pathlib import Path
ROOT=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-c')
opening=json.loads((ROOT/'opening-inputs.json').read_text())
def verify():
 for row in opening['files']:
  raw=Path(row['path']).read_bytes()
  assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256'],row['path']
verify()
command=json.loads((ROOT/'command.json').read_text())
assert command['source']['sha256']=='b1da715f60001fb8f650167d074daf66eb469cf16b0bfaccfe6b8f170ee4857c'
assert hashlib.sha256((ROOT/'command.json').read_bytes()).hexdigest()=='63dcbd893077164759ab3a9074d24ca6d01e531c0dcdace78c00ace642cccae8'
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
