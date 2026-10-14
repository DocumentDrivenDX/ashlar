"""Existing-parser configuration routes; no producer, compiler or native execution."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from ashlar import cli
from ashlar_host.pack_configuration import FinitePackBudget,make_finite_pack_configuration
from ashlar_host.pack_native_cli import PROFILE,SOURCE_PROFILE,parse_finite_pack_invocation


class PackConfigurationTests(unittest.TestCase):
    def selectors(self,root):
        return {name:root/name for name in ('publication','output','jars','pack_file','model','graph','producer_source','bun','git')}

    def build(self,root,pack,mode,query_profile=None,profile=None,**changes):
        values={**self.selectors(root),'pack':pack,'mode':mode,'profile':profile or (PROFILE if mode=='publish' else SOURCE_PROFILE),'budget':FinitePackBudget('small-local')}
        if mode=='query':values.update(index=root/'index',installation=root/'installation')
        if query_profile is not None:values['query_profile']=query_profile
        values.update(changes)
        return make_finite_pack_configuration(**values)

    def test_all_existing_routes_and_exact_operator_selectors(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve()
            routes=[('archaeology','publish',None),('ecology','publish',None),('payments','publish',None),
                    ('archaeology','query',None),('ecology','query',None),
                    ('payments','query','payments-count'),('payments','query','payments-scenario')]
            for pack,mode,route in routes:
                with self.subTest(pack=pack,mode=mode,route=route):
                    raw=self.build(root,pack,mode,route);value=json.loads(raw)
                    selected=parse_finite_pack_invocation(raw,mode,query_profile=route)
                    self.assertEqual(selected.native.producer.definition.name,pack)
                    for name,path in self.selectors(root).items():
                        actual=value['producer'][{'producer_source':'source','bun':'bun','git':'git'}[name]] if name in ('producer_source','bun','git') else value[name]
                        self.assertEqual(actual,str(path))
                    self.assertEqual(value['producer']['maximum_receipt_bytes'],33554432)
                    self.assertEqual(value['operation_capacity']['max_intent_bytes'],8388608)
                    if mode=='query':
                        self.assertEqual(value['query']['maximum_public_source_bytes'],2097152)
                        self.assertEqual((value['query']['maximum_rows'],value['query']['maximum_observation_bytes']),(128,16777216))
                    self.assertFalse((root/'output').exists())
                    self.assertLessEqual(len(raw),65536)

    def test_unsupported_routes_and_query_only_inputs_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve()
            for pack,mode,route in [('supply-chain','publish',None),('supply-chain','query',None),('payments','query',None),('ecology','query','payments-count')]:
                with self.subTest(pack=pack,mode=mode,route=route), self.assertRaises(ValueError):self.build(root,pack,mode,route)
            for changes in ({'index':root/'index'},{'query_profile':'payments-count'},{'profile':SOURCE_PROFILE}):
                with self.assertRaises(ValueError):self.build(root,'payments','publish',**changes)
            with self.assertRaises(ValueError):self.build(root,'ecology','query',index=None)
            with self.assertRaises(ValueError):self.build(root,'ecology','query',model='relative')
            with self.assertRaises(ValueError):FinitePackBudget('unbounded')

    def test_public_cli_prints_parser_validated_config_without_sdk_or_effects(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();values=self.selectors(root)
            argv=['ashlar','configure-finite-pack','--pack','ecology','--mode','query','--profile',SOURCE_PROFILE,'--budget-profile','small-local','--index',str(root/'index'),'--installation',str(root/'installation')]
            for name,value in values.items():argv+=['--'+name.replace('_','-'),str(value)]
            output=io.StringIO()
            with patch('sys.argv',argv),contextlib.redirect_stdout(output),patch('ashlar_host.pack_native_cli._publish',side_effect=AssertionError('no publication')),patch('ashlar_host.pack_native_cli._open',side_effect=AssertionError('no reader')):
                cli.main()
            self.assertEqual(parse_finite_pack_invocation(output.getvalue().encode(),'query').native.producer.definition.name,'ecology')
            self.assertFalse((root/'output').exists())
            code="import sys;from ashlar_host.pack_configuration import FinitePackBudget;assert not any(n in sys.modules for n in ('pyspark','duckdb','psycopg','graphframes'));assert FinitePackBudget('small-local')"
            subprocess.run([sys.executable,'-B','-c',code],check=True)
