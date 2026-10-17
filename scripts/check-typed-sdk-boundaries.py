"""Explicit source/import map for the two new typed SDK integration boundaries.

Scope is these modules only; existing Ashlar source architecture is not claimed.
"""
import ast
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
ALLOWED={
 'src/ashlar/async_access.py':{'__future__','asyncio','concurrent.futures','contextlib','contextvars','copy','dataclasses','datetime','threading','time','typing','uuid','hashlib','umf_access.adoption','umf_access.codec_support','umf_access.engine_catalog','umf_access.native_program','umf_access.pages','ashlar.access','ashlar.publication'},
 'src/ashlar/execution_scope.py':{'contextlib'},
 'src/ashlar_host/typed_bulk.py':{'__future__','dataclasses','collections.abc','datetime','decimal','time','typing','hashlib','json','pydantic','umf_access.bulk','umf_access.protocols','umf_access.native_actions','umf_access.native_program','umf_access.adoption','umf_access.commands','umf_access.targets','ashlar.weft_query','ashlar.execution_scope','ashlar_host.delta_atomic','ashlar_host.delta_receipt'},
 'src/ashlar_host/typed_actions.py':{'__future__','collections.abc','datetime','time','typing','hashlib','json','umf_access.native_actions','umf_access.native_program','umf_access.protocols','umf_access.types','ashlar.weft_query','ashlar.execution_scope','ashlar_host.delta_atomic','ashlar_host.delta_receipt'},
 'src/ashlar_host/delta_action_admission.py':{'contextlib','dataclasses','pathlib','time','hashlib','json','threading','umf_access.native_program','ashlar_host.delta_atomic'},
 'src/ashlar/access.py':{'__future__','contextlib','collections.abc','dataclasses','datetime','decimal','time','types','typing','copy','hashlib','json','pydantic','umf_access.adoption','umf_access.engine_catalog','umf_access.reviewed','umf_access.pages','umf_access.codec_support','umf_access.native_program','umf_access.related_keys','ashlar.weft_query','ashlar.execution_scope','ashlar.publication','ashlar.weft_decode','ashlar.weft_related_keys_decode','ashlar.sdk_related_keys'},
 'src/ashlar_host/delta_atomic.py':{'dataclasses','pathlib','hashlib','json','re','time','urllib.parse','py4j.protocol'},
}
def imports(source,package):
 for node in ast.walk(ast.parse(source)):
  if isinstance(node,ast.Import):yield from (name.name for name in node.names)
  elif isinstance(node,ast.ImportFrom):
   if node.level:yield '.'.join(package.split('.')[:len(package.split('.'))-node.level+1]+([node.module]if node.module else []))
   else:yield node.module or ''
def check(sources):
 errors=[]
 for path,allowed in ALLOWED.items():
  for imported in imports(sources[path],'.'.join(Path(path).parts[1:-1])):
   if imported not in allowed:errors.append(path+': unowned import '+imported)
 return errors
if __name__=='__main__':
 sources={path:(ROOT/path).read_text()for path in ALLOWED};errors=check(sources)
 if '--negative-controls'in sys.argv:
  for path in ALLOWED:
   changed=dict(sources);changed[path]+='\nimport subprocess\n'
   if not check(changed):raise SystemExit('unowned execution import escaped')
   changed[path]=sources[path]+'\ndef hidden():\n import socket\n'
   if not check(changed):raise SystemExit('deferred unowned I/O import escaped')
 if errors:print('\n'.join(errors),file=sys.stderr);raise SystemExit(1)
 print('typed SDK boundaries: explicitly mapped modules; negative controls passed'if '--negative-controls'in sys.argv else'typed SDK boundaries: pass')
