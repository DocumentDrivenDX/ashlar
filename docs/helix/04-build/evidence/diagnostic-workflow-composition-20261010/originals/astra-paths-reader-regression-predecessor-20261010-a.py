import ast,inspect,unittest
from pathlib import Path
from unittest.mock import patch
from test_host_paths_diagnostics import DiagnosticWorkflowTests
p=Path('/private/tmp/astra-paths-reader-predecessor-20261010-c/publication_reader.py')
raw=p.read_text();tree=ast.parse(raw);fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='open_commerce_reader')
source=ast.get_source_segment(raw,fn)
suite=unittest.TestSuite([DiagnosticWorkflowTests('test_reader_acquired_region_preserves_primary_with_and_without_facts')])
with patch.object(inspect,'getsource',return_value=source):
 result=unittest.TextTestRunner(verbosity=2).run(suite)
assert len(result.failures)==8 and not result.errors
assert all('is not' in failure and 'synthetic-close' in failure for _,failure in result.failures)
print('EXPECTED_PREDECESSOR_FAILURES=8; guard-reached assertion preceded identity assertion')
