import io,json,sys,hashlib
from pathlib import Path
from unittest.mock import patch
from ashlar.cli import main
class ShortWriter:
 def __init__(self):self.sent=bytearray();self.buffer=self
 def write(self,raw):self.sent.extend(raw[:1]);return 1
 def flush(self):pass
writer=ShortWriter();err=io.StringIO();code=0
with patch.object(sys,'argv',['ashlar','diagnostics','--run-directory','/synthetic/unused']),patch.object(sys,'stdout',writer),patch.object(sys,'stderr',err),patch('ashlar_host.diagnostics.read_diagnostics',return_value={'synthetic':'bounded-result'}):
 try:main()
 except SystemExit as failure:code=failure.code
result={'scope':'Public source CLI with inert reader and short binary stdout; no SDK/native/receiver/filesystem read.', 'exitCode':code,'actualStdoutHex':bytes(writer.sent).hex(),'stderr':err.getvalue(),'finding':'CLI accepted short write as success' if code==0 else 'short write refused'}
p=Path(__file__).parent/'cli.py';b=p.read_bytes();result['source']={'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
(Path(__file__).parent/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
