import contextlib,hashlib,io,json,runpy,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
manifest_raw=(ROOT/'command.json').read_bytes();assert len(manifest_raw)<=1024*1024
manifest=json.loads(manifest_raw)
def verify():
 for entry in manifest['inputs']:
  path=Path(entry['path']);assert path.is_file() and not path.is_symlink()
  assert path.stat().st_size==entry['bytes']
  raw=path.read_bytes();assert len(raw)==entry['bytes'] and hashlib.sha256(raw).hexdigest()==entry['sha256']
 assert (ROOT/'command.json').read_bytes()==manifest_raw
class BoundedText(io.StringIO):
 def __init__(self):super().__init__();self.total=0
 def write(self,text):
  size=len(text.encode('utf8'))
  if self.total+size>256*1024:raise ValueError('bounded-smoke-stream')
  self.total+=size;return super().write(text)
verify();started=time.monotonic();primary=None;closing=True
stdout=BoundedText();stderr=BoundedText()
try:
 with contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):
  runpy.run_path(str(ROOT/'run.py'),run_name='__main__')
except BaseException as exc:primary=exc
finally:
 try:verify()
 except BaseException as exc:
  closing=False
  if primary is None:primary=exc
  else:
   try:setattr(primary,'closing_custody_failed',True)
   except BaseException:pass
if primary is not None:raise primary
from ashlar import _weft_installation_mechanics as mechanics
from ashlar.weft_paths_keys_package import read_snapshot
result=read_snapshot(ROOT/'provisional-result.json',64*1024)
report={'format':'ashlar-paths-keys-smoke-launch/0.2','elapsedSeconds':time.monotonic()-started,'closingCustody':closing,'sourceCommit':manifest['sourceCommit'],'resultSha256':hashlib.sha256(result).hexdigest(),'scope':'Direct root process; each transport owns its compiler group and30-second bound. No outerforcedkill/globaldeadline or native/source/ACK proof.'}
def refuse():raise ValueError('launch-publication-refused')
for name,stream in [('stdout',stdout),('stderr',stderr)]:
 raw=stream.getvalue().encode('utf8');mechanics.write_owned(ROOT/(name+'.log'),raw,refuse=refuse)
mechanics.write_owned(ROOT/'launch-result.json',(json.dumps(report,indent=2)+'\n').encode(),refuse=refuse)
print(json.dumps(report))
