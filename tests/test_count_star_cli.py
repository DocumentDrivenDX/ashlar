"""Selected public CLI transport ports; inert callbacks do not qualify install."""
import io,unittest
from unittest.mock import patch
from types import SimpleNamespace
from ashlar.cli import main
from ashlar import weft_count_star_distribution as d
class CountStarCliTests(unittest.TestCase):
    def invoke(self,args,raw=b'{}\n'):
        stdin=io.TextIOWrapper(io.BytesIO(raw));stdout=io.TextIOWrapper(io.BytesIO(),write_through=True);stderr=io.StringIO()
        self.addCleanup(stdin.close);self.addCleanup(stdout.close)
        return stdin,stdout,stderr,patch('sys.argv',['ashlar']+args)
    def test_exact_original_stdin_stdout_no_package_on_reopen(self):
        request=b'{"sql":"SELECT COUNT(*)"}\n';response=b'{"status":"blocked"}\n';stdin,stdout,stderr,argv=self.invoke(['compile-weft-count-star','--index','/index','--installation','/installed'],request)
        with argv,patch('sys.stdin',stdin),patch('sys.stdout',stdout),patch('sys.stderr',stderr),patch.object(d,'compile_count_star_distribution',return_value=response)as compile:
            main()
        self.assertEqual(compile.call_args.args[1],request);self.assertIsNone(compile.call_args.args[0].package);self.assertEqual(stdout.buffer.getvalue(),response);self.assertEqual(stderr.getvalue(),'')
    def test_install_reports_only_on_stderr(self):
        stdin,stdout,stderr,argv=self.invoke(['install-weft-count-star','--index','/index','--package','/package','--output','/installed'])
        with argv,patch('sys.stdout',stdout),patch('sys.stderr',stderr),patch.object(d,'install_count_star_distribution',return_value=SimpleNamespace(cleanup_pending=False)):
            main()
        self.assertEqual(stdout.buffer.getvalue(),b'');self.assertEqual(stderr.getvalue(),'ashlar-weft-count-star: installed\n')
    def test_error_redacts_operator_and_compiler_details(self):
        stdin,stdout,stderr,argv=self.invoke(['compile-weft-count-star','--index','/index','--installation','/installed'])
        with argv,patch('sys.stdin',stdin),patch('sys.stdout',stdout),patch('sys.stderr',stderr),patch.object(d,'compile_count_star_distribution',side_effect=OSError('private SQL/path')):
            with self.assertRaises(SystemExit)as caught:main()
        self.assertEqual(caught.exception.code,2);self.assertEqual(stdout.buffer.getvalue(),b'');self.assertEqual(stderr.getvalue(),'ashlar-weft-count-star: refused\n')
if __name__=='__main__':unittest.main()
