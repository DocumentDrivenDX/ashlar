# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real original-file renewal with injected export port; no UMF/native claim."""
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
import unittest
from unittest.mock import patch
from ashlar_host.evolution_cli import InvocationFile
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.local_graph_export import LocalGraphExportUse
from ashlar_host.puppy_scenario_sources import PuppyScenarioFilePolicy

class Tests(unittest.TestCase):
 def test_exact_file_use_context_and_independent_drift_closing(self):
  with TemporaryDirectory()as directory:
   root=Path(directory).resolve();original=Path('examples/domain-packs/archaeology/upstream')
   files=[]
   for source,name in ((original/'ontology.json','model'),(original/'graph/fixture.json','graph'),(original/'pack.json','pack')):
    p=root/name;p.write_bytes(source.read_bytes());files.append(InvocationFile.read(p,262144))
   export=object.__new__(LocalGraphExportUse);export._context=object()
   with patch.object(LocalGraphExportUse,'renew'):
    policy=PuppyScenarioFilePolicy(FinitePackDefinition('archaeology'),tuple(files),export)
    facts=policy.definition.inputs(*(f.raw for f in files));policy.admit_source(MappingProxyType(facts),export.context)
    with self.assertRaises(PermissionError):policy.admit_source(facts,object())
    files[0].path.write_bytes(b'changed');seen=[];renew=InvocationFile.renew
    def observe(file,maximum):seen.append(file.path.name);return renew(file,maximum)
    with patch.object(InvocationFile,'renew',observe),self.assertRaises(Exception):policy.renew()
    self.assertEqual(seen,['model','graph','pack'])
