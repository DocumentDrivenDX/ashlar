from contextlib import contextmanager
import unittest
from ashlar.quarantine import CleanupRequest,CleanupQuarantine

class QuarantineTests(unittest.TestCase):
    def test_terminal_verification_refuses_before_database_access(self):
        class Executor:
            @contextmanager
            def transaction(self,context):
                raise AssertionError('Unverified terminal reached database')
                yield
        class Policy:
            def verify_terminal(self,*args):raise PermissionError('Original native outcome unresolved')
        request=CleanupRequest('operation','catalog.schema.table','uuid',b'original')
        with self.assertRaises(PermissionError):
            CleanupQuarantine(Executor(),Policy()).close(request,b'caller-complete',context=object())
    def test_original_binary_intent_is_bound_without_text_conversion(self):
        calls=[]
        class Executor:
            @contextmanager
            def transaction(self,context):yield self
            def query(self,sql,parameters):calls.append((sql,parameters))
        class Policy:
            def admit_cleanup(self,*args):pass
        request=CleanupRequest('operation','catalog.schema.table','uuid',b'\x00\xff\x01')
        CleanupQuarantine(Executor(),Policy()).begin(request,context=object())
        self.assertEqual(calls[0][1]['intent'],'00ff01')
        self.assertNotIn('00ff01',calls[0][0])
