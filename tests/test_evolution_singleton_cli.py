# SPDX-License-Identifier: MIT OR Apache-2.0
"""Installed shared custody owner; inert SDK boundary, no native runtime."""
import hashlib
import io
import json
import os
import stat
import sys
import unittest
from contextlib import redirect_stdout,redirect_stderr
from unittest.mock import patch
import test_evolution_verification as verification
import ashlar_host.evolution_singleton  # Load before fixture sys.modules snapshot.

class SingletonCommandTests(unittest.TestCase):
    setUp=verification.CommandTests.setUp
    def call(self,**identity):
        from ashlar_host.evolution_verification_cli import run_singleton_command
        return run_singleton_command(str(self.configuration),str(self.provider),hashlib.sha256(self.provider.read_bytes()).hexdigest(),**({'kind':'object','type_id':1,'entity_id':7}|identity))
    def sdk(self,selection,provider):
        from ashlar_host.lifecycle import owned_context
        with owned_context(provider.open_verification(selection.verification)):
            self.events.append('read')
        self.assertFalse((self.root/'receipt.json').exists())
        return json.dumps({'profile':'ashlar-original-two-source-singleton/0.1','original_sha256':'a'*64,'ordinal':0,'kind':selection.kind,'type_id':selection.type_id,'entity_id':selection.entity_id,'records':[{'source_system':'A','row':None},{'source_system':'B','row':None}]}).encode()
    def test_actual_shared_owner_closes_before_fresh_receipt(self):
        with patch('ashlar_host.evolution_singleton.read_evolution_singletons',side_effect=self.sdk):answer=self.call()
        self.assertEqual(self.events,['read','provider-close'])
        path=self.root/'receipt.json';self.assertEqual(json.loads(path.read_bytes()),answer)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o600)
        self.verify_mock.assert_not_called()
    def test_closing_drift_and_cancel_withhold_receipt(self):
        def drift():self.configuration.write_text(self.configuration.read_text()+' ')
        self.closing=drift
        with patch('ashlar_host.evolution_singleton.read_evolution_singletons',side_effect=self.sdk):
            with self.assertRaises(ValueError):self.call()
        self.assertFalse((self.root/'receipt.json').exists())
        primary=KeyboardInterrupt('stop')
        with patch('ashlar_host.evolution_singleton.read_evolution_singletons',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt)as caught:self.call()
        self.assertIs(caught.exception,primary)
    def test_closed_identity_and_result_correspondence(self):
        with patch('ashlar_host.evolution_singleton.read_evolution_singletons')as sdk:
            for selection in ({'kind':'other'},{'type_id':True},{'entity_id':2**63}):
                with self.assertRaises(ValueError):self.call(**selection)
            sdk.assert_not_called()
        good=self.sdk
        for field,value in [('ordinal',True),('entity_id',8),('profile','wrong'),('original_sha256','b'*64)]:
            def wrong(selection,provider):
                answer=json.loads(good(selection,provider));answer[field]=value;return json.dumps(answer).encode()
            with patch('ashlar_host.evolution_singleton.read_evolution_singletons',side_effect=wrong):
                with self.assertRaises(ValueError):self.call()
            self.assertFalse((self.root/'receipt.json').exists())
    def test_installed_dispatch_sanitizes_and_preserves_cancel(self):
        from ashlar.cli import main
        argv=['ashlar','read-commerce-evolution-singletons','--configuration','config','--provider','provider','--provider-sha256','a'*64,'--kind','edge','--type-id','2','--entity-id','-7']
        with patch.object(sys,'argv',argv),patch('ashlar_host.evolution_verification_cli.run_singleton_command',return_value={'ok':True})as command,redirect_stdout(io.StringIO())as out:
            main()
        command.assert_called_once_with('config','provider','a'*64,kind='edge',type_id=2,entity_id=-7);self.assertEqual(json.loads(out.getvalue()),{'ok':True})
        with patch.object(sys,'argv',argv),patch('ashlar_host.evolution_verification_cli.run_singleton_command',side_effect=ValueError('private')),redirect_stderr(io.StringIO())as err:
            with self.assertRaises(SystemExit):main()
        self.assertNotIn('private',err.getvalue())
