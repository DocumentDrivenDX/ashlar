import importlib.util,pathlib,unittest,unittest.mock as mock,tempfile,sys,json,hashlib,os,time
ROOT=pathlib.Path('/private/tmp/weft-paths-keys-3a2a79c-build-20261010-a')
P=ROOT/'launch-reviewed-build.py';S=importlib.util.spec_from_file_location('reviewed_build',P);M=importlib.util.module_from_spec(S);S.loader.exec_module(M)
COMMAND=ROOT/'command-b.json';SHA=hashlib.sha256(COMMAND.read_bytes()).hexdigest()
class Controls(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.temp.name);self.addCleanup(self.temp.cleanup)
 def capture(self,code,timeout=2,maximum=4096):return M.capture([sys.executable,'-c',code],str(self.root),{'PATH':'/usr/bin:/bin'},self.root/'out',self.root/'err',timeout,maximum)
 def test_success(self):
  r=self.capture('import sys;print("ok");sys.stderr.write("err")');self.assertEqual(r,{'exitCode':0,'stdoutBytes':3,'stderrBytes':3});self.assertEqual((self.root/'out').read_bytes(),b'ok\n')
 def test_stream_limit(self):
  with self.assertRaisesRegex(ValueError,'Build output bound exceeded'):self.capture('print("x"*10000)',maximum=127)
  self.assertLessEqual((self.root/'out').stat().st_size,127)
 def test_shared_deadline_pipe_descendant(self):
  start=time.monotonic()
  with self.assertRaises(TimeoutError):self.capture('import subprocess,sys;subprocess.Popen([sys.executable,"-c","import time;time.sleep(3)"])',timeout=.15)
  self.assertLess(time.monotonic()-start,2)
 def test_setup_primary_reaps_and_closes(self):
  real=M.subprocess.Popen;seen=[];primary=KeyboardInterrupt('setup')
  def popen(*a,**kw):
   p=real(*a,**kw);seen.append(p);return p
  with mock.patch.object(M.subprocess,'Popen',side_effect=popen),mock.patch.object(M.os,'set_blocking',side_effect=primary):
   with self.assertRaises(KeyboardInterrupt) as caught:self.capture('import time;time.sleep(3)')
  self.assertIs(caught.exception,primary);self.assertIsNotNone(seen[0].poll());self.assertTrue(seen[0].stdout.closed and seen[0].stderr.closed)
 def test_first_cleanup_cancellation_identity(self):
  real=M.os.killpg
  for index,primary in enumerate([KeyboardInterrupt('cleanup'),SystemExit(17)]):
   with self.subTest(type=type(primary).__name__):
    def kill(*a):
     try:real(*a)
     except ProcessLookupError:pass
     raise primary
    out=self.root/str(index);out.mkdir()
    with mock.patch.object(M.os,'killpg',side_effect=kill):
     try:M.capture([sys.executable,'-c','print("ok")'],str(out),{'PATH':'/usr/bin:/bin'},out/'out',out/'err',2,4096)
     except BaseException as e:self.assertIs(e,primary)
     else:self.fail('cancellation swallowed')
 def test_body_preserved_over_cleanup(self):
  real=M.os.killpg;primary=KeyboardInterrupt('body');secondary=SystemExit(19)
  def kill(*a):
   try:real(*a)
   except ProcessLookupError:pass
   raise secondary
  with mock.patch.object(M.os,'set_blocking',side_effect=primary),mock.patch.object(M.os,'killpg',side_effect=kill):
   with self.assertRaises(KeyboardInterrupt) as caught:self.capture('import time;time.sleep(3)')
  self.assertIs(caught.exception,primary);self.assertIs(primary.cleanup_failed,True)
 def main(self,capture,verify):
  argv=[str(P),'--command',str(COMMAND),'--sha256',SHA,'--phase','source-metadata','--receipt',str(self.root/'receipt')]
  with mock.patch.object(sys,'argv',argv),mock.patch.object(M,'capture',side_effect=capture) as c,mock.patch.object(M,'verify',side_effect=verify) as v:
   try:M.main()
   except BaseException as e:return e,c.call_count,v.call_count
   return None,c.call_count,v.call_count
 def test_main_capture_cancel_closing(self):
  primary=KeyboardInterrupt('capture');e,c,v=self.main(primary,[None,None]);self.assertIs(e,primary);self.assertEqual((c,v),(1,2));self.assertFalse((self.root/'receipt').exists())
 def test_main_secondary_closing_preserves_body(self):
  primary=KeyboardInterrupt('capture');e,c,v=self.main(primary,[None,OSError('closing')]);self.assertIs(e,primary);self.assertIs(primary.closing_custody_failed,True);self.assertEqual(v,2)
 def test_main_closing_alone_refuses_success(self):
  primary=SystemExit(7);e,c,v=self.main(lambda *a:{'exitCode':0,'stdoutBytes':0,'stderrBytes':0},[None,primary]);self.assertIs(e,primary);self.assertEqual(v,2);self.assertFalse((self.root/'receipt').exists())
 def test_main_opening_refusal_no_child(self):
  primary=ValueError('opening');e,c,v=self.main(lambda *a:None,primary);self.assertIs(e,primary);self.assertEqual((c,v),(0,1));self.assertFalse((self.root/'receipt').exists())
 def test_main_nonzero_still_closes_retains_failure(self):
  e,c,v=self.main(lambda *a:{'exitCode':3,'stdoutBytes':0,'stderrBytes':2},[None,None]);self.assertIsInstance(e,SystemExit);self.assertEqual(e.code,3);self.assertEqual(v,2);self.assertEqual(json.loads((self.root/'receipt').read_bytes())['exitCode'],3)
if __name__=='__main__':unittest.main()
