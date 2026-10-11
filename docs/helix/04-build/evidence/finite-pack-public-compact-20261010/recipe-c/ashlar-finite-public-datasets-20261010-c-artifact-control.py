"""Inert exact selected probe artifact sink; never imports producer or launches child."""
import ast,sys
from pathlib import Path
sys.path.insert(0,'/private/tmp/ashlar-finite-public-datasets-20261010-c-source/src')
from ashlar_host.lifecycle import owned_context
source=Path('/private/tmp/ashlar-finite-public-datasets-20261010-c-probe.py').read_text()
function=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef)and n.name=='write_artifact')
namespace={'owned_context':owned_context};exec(compile(ast.Module(body=[function],type_ignores=[]),'selected-probe-artifact','exec'),namespace)
for body,closing in ((KeyboardInterrupt('write'),OSError('close')),(ValueError('write'),SystemExit('close')),(KeyboardInterrupt('write'),SystemExit('close'))):
    class Stream:
        closed=False
        def write(self,raw):raise body
        def __enter__(self):return self
        def __exit__(self,*args):self.closed=True;raise closing
    stream=Stream()
    class File:
        def open(self,*args):return stream
    try:namespace['write_artifact'](File(),b'original')
    except BaseException as error:assert error is (closing if isinstance(body,Exception)else body)and stream.closed
    else:raise AssertionError('Primary artifact cancellation lost')
print('Three inert artifact cancellation controls PASS; no public producer or child')
