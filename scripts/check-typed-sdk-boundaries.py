"""Explicit source/import map for typed SDK integration owners.

Scope is these modules only; existing Ashlar source architecture is not claimed.
"""
import ast
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
ALLOWED={
 'src/ashlar_host/configured_typed_graph_sdk_binding.py':{'hashlib','json','re','ashlar.schema','ashlar_host.configured_typed_graph_weft_binding'},
 'src/ashlar_host/configured_sdk_page_oracle.py':{'hashlib','ashlar.schema','ashlar.typed_graph_source_config','ashlar.typed_graph_source','ashlar.weft_binding','ashlar_host.configured_local_read','ashlar_host.configured_typed_graph_sdk_binding','ashlar_host.delta_custody'},
 'src/ashlar_host/configured_sdk_pages.py':{'contextlib','dataclasses','datetime','hashlib','importlib','importlib.metadata','json','threading','time','uuid','ashlar.native','ashlar.weft_binding','ashlar.access','umf_access.engine_catalog','ashlar_host.configured_local_read','ashlar_host.configured_query_aliases','ashlar_host.configured_typed_graph_sdk_binding','ashlar_host.configured_sdk_page_oracle','ashlar_host.delta_custody','ashlar_host.path_capture','ashlar_host.lifecycle'},
 'src/ashlar/execution_scope.py':{'contextlib'},
 'src/ashlar_host/typed_bulk.py':{'__future__','dataclasses','collections.abc','datetime','decimal','time','typing','hashlib','json','pydantic','umf_access.bulk','umf_access.protocols','umf_access.native_actions','umf_access.native_program','umf_access.adoption','umf_access.commands','umf_access.targets','ashlar.weft_query','ashlar.execution_scope','ashlar_host.delta_atomic'},
 'src/ashlar_host/typed_actions.py':{'__future__','collections.abc','datetime','time','typing','hashlib','json','umf_access.native_actions','umf_access.native_program','umf_access.protocols','umf_access.types','ashlar.weft_query','ashlar.execution_scope','ashlar_host.delta_atomic'},
 'src/ashlar_host/delta_action_admission.py':{'contextlib','dataclasses','pathlib','time','hashlib','json','threading','umf_access.native_program','ashlar_host.delta_atomic'},
 'src/ashlar/access.py':{'__future__','contextlib','collections.abc','datetime','decimal','time','types','typing','copy','json','hashlib','pydantic','umf_access.adoption','umf_access.engine_catalog','umf_access.reviewed','umf_access.pages','umf_access.codec_support','umf_access.native_program','ashlar.weft_query','ashlar.execution_scope','ashlar.publication'},
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
 print('typed SDK boundaries: 9 explicitly mapped modules; negative controls passed'if '--negative-controls'in sys.argv else'typed SDK boundaries: pass')
