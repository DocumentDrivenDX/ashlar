import unittest
from unittest.mock import patch
from pathlib import Path
from ashlar_host.pack_count_star_reader import FinitePackProvider
from ashlar_host.finite_publication import FiniteFileDriver,GRAPH_ROLES
from ashlar_host.finite_dataset import FinitePackSource
from ashlar_host.finite_pack import FinitePackDefinition
ROOT=Path(__file__).resolve().parents[1]
def fixture(name='archaeology'):
 source=object.__new__(FinitePackSource);source.definition=FinitePackDefinition(name)
 base=ROOT/'examples/domain-packs'/name/'upstream';source.model=(base/'ontology.json').read_bytes();source.graph=(base/'graph/fixture.json').read_bytes()
 driver=object.__new__(FiniteFileDriver);driver.context=object();source.context=driver.context;driver.source=source;driver.manifest={};driver.tables={r:'local.test.'+r for r in GRAPH_ROLES};return driver
class ReaderTests(unittest.TestCase):
 def test_requires_actual_typed_driver(self):
  with self.assertRaises(ValueError):FinitePackProvider(object(),{}, {})
 def test_original_context_must_be_shared(self):
  driver=fixture();driver.source.context=object();aliases={t:t for t in driver.tables.values()}
  with self.assertRaises(PermissionError):FinitePackProvider(driver,aliases,{})
 def test_alias_snapshot_and_nonheld_runtime(self):
  driver=fixture();aliases={t:t for t in driver.tables.values()};binding={'original':1}
  with patch.object(FinitePackSource,'metadata',return_value={}):provider=FinitePackProvider(driver,aliases,binding)
  aliases.clear();binding['original']=2
  self.assertEqual(len(provider.aliases),4);self.assertEqual(provider.expected_binding,'{"original":1}')
  with self.assertRaises(PermissionError):provider.runtime(driver.context)
 def test_injective_complete_aliases(self):
  driver=fixture()
  with self.assertRaises(ValueError):FinitePackProvider(driver,{t:'local.same.x'for t in driver.tables.values()},{})
 def test_definition_substitution_refused_before_executable_method(self):
  driver=fixture();called=[]
  class Forged:
   def inputs(self,*args):called.append(1);raise AssertionError('must not run')
  driver.source.definition=Forged()
  with self.assertRaises(ValueError):FinitePackProvider(driver,{t:t for t in driver.tables.values()},{})
  self.assertEqual(called,[])
 def test_interval_cancellation_identity_and_no_closed_success(self):
  from contextlib import contextmanager
  for body,closing,expected in [(KeyboardInterrupt(),ValueError('cleanup'),0),(ValueError('body'),SystemExit(),1)]:
   driver=fixture()
   with patch.object(FinitePackSource,'metadata',return_value={}):provider=FinitePackProvider(driver,{t:t for t in driver.tables.values()},{})
   driver.request={'stream':'fixture'}
   @contextmanager
   def writer(*args):
    try:yield
    finally:raise closing
   with patch.object(FiniteFileDriver,'writer',writer),patch.object(provider,'resolve',return_value='opening'):
    try:
     with provider.interval(driver.context):raise body
    except BaseException as actual:self.assertIs(actual,body if expected==0 else closing)
    else:self.fail('failure lost')
   self.assertFalse(provider.active);self.assertIsNone(provider.closed)
   with self.assertRaises(PermissionError):provider.closed_interval_custody(driver.context)
