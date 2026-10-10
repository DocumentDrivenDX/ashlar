import importlib.util,pathlib,tempfile,sys,time,unittest,os
p=pathlib.Path(__file__).with_name('launch-reviewed-build.py'); sp=importlib.util.spec_from_file_location('launcher',p); m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
class Tests(unittest.TestCase):
 def run_fake(self,code,timeout=2,maximum=4096):
  with tempfile.TemporaryDirectory() as d:
   return m.capture([sys.executable,'-c',code],d,{'PATH':'/usr/bin:/bin'},d+'/out',d+'/err',timeout,maximum)
 def test_success(self): self.assertEqual(self.run_fake('print("ok")')['exitCode'],0)
 def test_output_bound(self):
  with self.assertRaisesRegex(ValueError,'output bound'): self.run_fake('print("x"*5000)')
 def test_pipe_descendant_deadline(self):
  t=time.monotonic()
  with self.assertRaises(TimeoutError): self.run_fake('import subprocess,sys; subprocess.Popen([sys.executable,"-c","import time;time.sleep(20)"])',.2)
  self.assertLess(time.monotonic()-t,2)
 def test_success_descendant_cleaned(self):
  with tempfile.TemporaryDirectory() as d:
   pid=pathlib.Path(d)/'pid'; code='import subprocess,sys; p=subprocess.Popen([sys.executable,"-c","import time;time.sleep(20)"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);open('+repr(str(pid))+',"w").write(str(p.pid))'
   from unittest.mock import patch
   actual=m.os.killpg
   with patch.object(m.os,'killpg',wraps=actual) as killed:
    result=m.capture([sys.executable,'-c',code],d,{'PATH':'/usr/bin:/bin'},d+'/o',d+'/e',2,4096)
    self.assertEqual(result['exitCode'],0); self.assertEqual(killed.call_count,1)
if __name__=='__main__': unittest.main()
