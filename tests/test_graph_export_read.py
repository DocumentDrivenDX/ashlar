# SPDX-License-Identifier: MIT OR Apache-2.0
"""Saved-file singleton controls, not native or source authority evidence."""
import json
import unittest
from unittest.mock import patch
from pathlib import Path
import test_graph_export_diff as pair_fixture
from ashlar.graph_release import graph_key
from ashlar_host.graph_export_read import run_read_graph_export_command
from ashlar_host.local_graph_export import LocalGraphExportUse

class Tests(unittest.TestCase):
    def fixture(self):
        return pair_fixture.Tests.fixture(self)
    def test_exact_node_edge_and_missing(self):
        root, configs, original, _ = self.fixture()
        for kind in ('nodes', 'edges'):
            row = original[kind][0]
            result = run_read_graph_export_command(configs[0], row['graph_id'], root/(kind+'.json'))
            self.assertEqual(result['carrier'], row)
            self.assertTrue(result['found'])
            self.assertEqual(result['publication_id'], original['publication']['publication_id'])
        result = run_read_graph_export_command(configs[0], graph_key('node','another-source','1','1'), root/'absent.json')
        self.assertFalse(result['found']); self.assertIsNone(result['carrier'])
    def test_closing_drift_and_cancellation_withhold_report(self):
        root, configs, original, _ = self.fixture();output=root/'refused.json'
        renewal=LocalGraphExportUse.renew;seen=[];cancel=KeyboardInterrupt()
        def changed(owner):
            renewal(owner)
            if owner._active:seen.append(owner.context)
            elif seen:raise cancel
        with patch.object(LocalGraphExportUse,'renew',changed):
            with self.assertRaises(KeyboardInterrupt) as caught:
                run_read_graph_export_command(configs[0],original['nodes'][0]['graph_id'],output)
        self.assertIs(caught.exception,cancel);self.assertFalse(output.exists())
    def test_cli_dispatch_and_safe_refusal(self):
        from ashlar.cli import main
        with patch('ashlar_host.graph_export_read.run_read_graph_export_command') as run, patch('sys.argv',['ashlar','read-graph-export','--configuration','/config','--graph-id','key','--output','/output']):
            main()
        run.assert_called_once_with('/config','key','/output')

    def test_actual_file_drift_and_invalid_key_withhold_report(self):
        root, configs, original, _ = self.fixture(); output = root/'drift.json'
        with self.assertRaises(ValueError):
            run_read_graph_export_command(configs[0], 'not-a-canonical-key', output)
        self.assertFalse(output.exists())
        renew = LocalGraphExportUse.renew; changed = []
        release = Path(json.loads(configs[0].read_bytes())['release_path'])
        def drift(owner):
            renew(owner)
            if owner._active and not changed:
                changed.append(True); release.write_bytes(release.read_bytes()+b' ')
        with patch.object(LocalGraphExportUse,'renew',drift):
            with self.assertRaises((ValueError,PermissionError)):
                run_read_graph_export_command(configs[0],original['nodes'][0]['graph_id'],output)
        self.assertFalse(output.exists())

    def test_pending_replacement_is_not_published_or_removed(self):
        root, configs, original, _ = self.fixture(); output = root/'replaced.json'
        pending = root/'replaced.json.pending'
        def replace(fd):
            pending.unlink(); pending.write_bytes(b'foreign')
        with patch('ashlar_host.graph_export_read.os.fsync',side_effect=replace):
            with self.assertRaises(PermissionError):
                run_read_graph_export_command(configs[0],original['nodes'][0]['graph_id'],output)
        self.assertFalse(output.exists());self.assertEqual(pending.read_bytes(),b'foreign')

    def test_short_write_and_link_failure_remove_only_owned_staging(self):
        import os
        root, configs, original, _ = self.fixture()
        actual_fdopen=os.fdopen
        class ShortWriter:
            def __init__(self,fd,mode):self.stream=actual_fdopen(fd,mode)
            def write(self,raw):return self.stream.write(raw[:-1])
            def close(self):self.stream.close()
        output=root/'short.json'
        with patch('ashlar_host.graph_export_read.os.fdopen',side_effect=ShortWriter):
            with self.assertRaises(OSError):run_read_graph_export_command(configs[0],original['nodes'][0]['graph_id'],output)
        self.assertFalse(output.exists());self.assertFalse(output.with_name(output.name+'.pending').exists())
        output=root/'link.json'
        with patch('ashlar_host.graph_export_read.os.link',side_effect=OSError('inert refusal')):
            with self.assertRaises(OSError):run_read_graph_export_command(configs[0],original['nodes'][0]['graph_id'],output)
        self.assertFalse(output.exists());self.assertFalse(output.with_name(output.name+'.pending').exists())

    def test_link_boundary_replacement_withholds_failed_link(self):
        import os
        root, configs, original, _ = self.fixture(); output = root/'link-swap.json'
        pending = output.with_name(output.name+'.pending'); actual_link=os.link
        def replaced(src,dst,**kwargs):
            pending.unlink();pending.write_bytes(b'foreign at link boundary')
            return actual_link(src,dst,**kwargs)
        with patch('ashlar_host.graph_export_read.os.link',side_effect=replaced):
            with self.assertRaises(PermissionError):
                run_read_graph_export_command(configs[0],original['nodes'][0]['graph_id'],output)
        self.assertFalse(output.exists())
        self.assertEqual(pending.read_bytes(),b'foreign at link boundary')
