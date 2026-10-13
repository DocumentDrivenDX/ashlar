"""Scoped immutable comparison reuse; parent native gates stay independently live."""
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.finite_publication import FiniteFileDriver
from ashlar_host.supply_chain_publication import FiniteSupplyChainDriver

class Tests(unittest.TestCase):
    def fixture(self):
        driver=object.__new__(FiniteSupplyChainDriver);driver.held=True
        driver.transport=SimpleNamespace(query=lambda *args:None,spark=object(),targets={'table':SimpleNamespace(uuid='original',path=Path('/original/table'),table='table')})
        driver.policy=object();driver.context=object();driver.source=object();driver.current_admission=lambda *args:None
        driver.tables={'objects':'table'};driver.columns={'objects':(('id','STRING'),)}
        return driver
    def test_same_scope_once_changed_inputs_ports_and_new_scope_observe_again(self):
        driver=self.fixture();events=[]
        @contextmanager
        def parent(*args,**kwargs):
            events.append('parent-opening');yield 'actual-selection'
            self.assertIsNone(driver._read_projection_memo);events.append('parent-fresh-closing')
        with patch.object(FiniteFileDriver,'_existing_read_interval',side_effect=parent),patch.object(FiniteFileDriver,'_check')as check:
            with driver._existing_read_interval('original',driver.context,restore=False):
                driver._check({'table':1},{'objects':[]});driver._check({'table':1},{'objects':[]});self.assertEqual(check.call_count,1)
                driver._check({'table':2},{'objects':[]});self.assertEqual(check.call_count,2)
                driver.transport.targets['table'].uuid='changed';driver._check({'table':2},{'objects':[]});self.assertEqual(check.call_count,3)
                driver.transport.query=lambda *args:None;driver._check({'table':2},{'objects':[]});self.assertEqual(check.call_count,4)
            with driver._existing_read_interval('original',driver.context,restore=False):driver._check({'table':2},{'objects':[]})
            self.assertEqual(check.call_count,5)
            driver._check({'table':2},{'objects':[]});self.assertEqual(check.call_count,6)
        self.assertEqual(events,['parent-opening','parent-fresh-closing']*2)
    def test_cancellation_discards_memo_before_failing_parent_closing(self):
        driver=self.fixture();primary=KeyboardInterrupt();events=[]
        @contextmanager
        def parent(*args,**kwargs):
            events.append('opening')
            try:yield
            finally:
                self.assertIsNone(driver._read_projection_memo);events.append('fresh-closing')
                raise RuntimeError('native closing changed')
        with patch.object(FiniteFileDriver,'_existing_read_interval',side_effect=parent),patch.object(FiniteFileDriver,'_check')as check:
            with self.assertRaises(KeyboardInterrupt)as caught:
                with driver._existing_read_interval('original',driver.context,restore=False):driver._check({'table':1},{'objects':[]});raise primary
            self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
            self.assertIsNone(driver._read_projection_memo);driver._check({'table':1},{'objects':[]});self.assertEqual(check.call_count,2)
        self.assertEqual(events,['opening','fresh-closing'])
