import importlib.util,pathlib,tempfile,unittest
from unittest.mock import patch
p=pathlib.Path(__file__).with_name('capture.py');s=importlib.util.spec_from_file_location('capture',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Tests(unittest.TestCase):
 def test_success_persists(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d);self.assertIsNone(m.persist_observation(p,{'status':'candidate'},None));self.assertTrue((p/'observation.json').exists())
 def test_primary_preserved_on_open_failure(self):
  primary=KeyboardInterrupt()
  with patch.object(pathlib.Path,'open',side_effect=OSError('ignored payload')):
   self.assertIs(m.persist_observation(pathlib.Path('/unused'),{},primary),primary)
  self.assertTrue(primary.cleanup_failed)
 def test_primary_preserved_on_write_or_close_failure(self):
  class F:
   def __init__(self,phase):self.phase=phase
   def __enter__(self):return self
   def write(self,_):
    if self.phase=='write':raise OSError('ignored')
   def __exit__(self,*_):
    if self.phase=='close':raise OSError('ignored')
  for phase in ['write','close']:
   primary=KeyboardInterrupt()
   with patch.object(pathlib.Path,'open',return_value=F(phase)):
    self.assertIs(m.persist_observation(pathlib.Path('/unused'),{},primary),primary)
   self.assertTrue(primary.cleanup_failed)
 def test_no_primary_exposes_write_failure(self):
  e=OSError('ignored')
  with patch.object(pathlib.Path,'open',side_effect=e):self.assertIs(m.persist_observation(pathlib.Path('/unused'),{},None),e)
if __name__=='__main__':unittest.main()
