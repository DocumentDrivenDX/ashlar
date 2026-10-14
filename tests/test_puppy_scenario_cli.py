"""Small parent controls; fake creation/Docker proves no native graph behavior."""
from contextlib import redirect_stdout
from dataclasses import asdict
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar.cli import main
from ashlar_host.puppy_scenario_cleanup import decode_scenario_creation, cleanup_scenario_container
from ashlar_host.puppy_scenario_configuration import parse_puppy_scenario_configuration
from ashlar_host.puppy_finite_local import PuppyScenarioContainerReceipt
import ashlar_host.puppy_scenario_cli as command
import test_puppy_scenario_configuration as fixture


class Tests(unittest.TestCase):
    def selected(self, root):
        value = fixture.Tests().value()
        value['output'] = str(root/'output')
        return value, parse_puppy_scenario_configuration(json.dumps(value).encode())

    def receipt(self, config, nonce):
        r = config.runtime
        return PuppyScenarioContainerReceipt('d'*64, r.image, r.name, nonce,
            config.output/'prepared/scenarios.duckdb', '/tmp/scenarios.duckdb',
            r.http_port, r.bolt_port, None, 2, 3*1024**3)

    def test_parent_nonce_and_actual_inspection_precede_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            _, config = self.selected(root)
            receipt = self.receipt(config, 'a'*32)
            with patch('ashlar_host.puppy_scenario_cleanup.capture') as capture:
                with self.assertRaises(PermissionError):
                    cleanup_scenario_container(config, receipt, invocation_nonce='b'*32)
                capture.assert_not_called()
            ports = {'8081/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '18081'}],
                     '7687/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '17687'}]}
            row = {'Id': receipt.container_id, 'Image': receipt.image, 'Name': '/'+receipt.name,
                'Config': {'Labels': {'ashlar.scenario.invocation': 'a'*32}},
                'HostConfig': {'NanoCpus': 2000000000, 'Memory': 3*1024**3, 'PortBindings': ports},
                'Mounts': [{'Type': 'bind', 'Source': str(receipt.readonly_mount_source),
                    'Destination': receipt.readonly_mount_target, 'RW': False}],
                'State': {'Running': True}, 'NetworkSettings': {'Ports': ports}}
            calls = []; listed = 0
            def capture(argv, **kwargs):
                nonlocal listed
                calls.append(argv[1:])
                if argv[1:3] == ['container', 'ls']:
                    listed += 1
                    return ((receipt.container_id+'\n').encode() if listed == 1 else b'', b'')
                if argv[1] == 'inspect': return json.dumps([row]).encode(), b''
                return b'', b''
            with patch('ashlar_host.puppy_scenario_cleanup.capture', capture):
                result = cleanup_scenario_container(config, receipt, invocation_nonce='a'*32)
            self.assertTrue(result['cleanup_complete'])
            self.assertEqual([call[0] for call in calls], ['container', 'inspect', 'stop', 'rm', 'container'])
            self.assertTrue(all(receipt.container_id in call or 'id='+receipt.container_id in call for call in calls))

    def test_cli_dispatches_only_explicit_configuration(self):
        with patch('sys.argv', ['ashlar', 'query-original-pack-puppygraph', '--configuration', '/owned/config.json']), \
                patch.object(command, 'run_puppy_scenario_command', return_value={'cases': 17}) as run, \
                redirect_stdout(io.StringIO()) as output:
            main()
        run.assert_called_once_with('/owned/config.json')
        self.assertEqual(json.loads(output.getvalue()), {'cases': 17})

    def test_timeout_reaps_actual_group_then_checks_actual_child_id(self):
        # A fixed inert supervisor emits a fake creation event and pauses. This
        # checks actual OS ownership/timeout; Docker and graph semantics are absent.
        supervisor = '''import json,os,signal,sys
fd=int(sys.argv[2]);args=sys.argv[3:]
signal.signal(signal.SIGTERM,lambda *_:None)
config=json.load(open(args[args.index('--configuration')+1]))
nonce=args[args.index('--invocation-nonce')+1]
r=config['runtime']
receipt={'container_id':'d'*64,'image':r['image'],'name':r['name'],'invocation_nonce':nonce,
'readonly_mount_source':config['output']+'/prepared/scenarios.duckdb',
'readonly_mount_target':'/tmp/scenarios.duckdb','http_port':r['http_port'],
'bolt_port':r['bolt_port'],'gremlin_port':None,'cpus':2,'memory_bytes':3*1024**3}
os.write(fd,(json.dumps({'type':'created','receipt':receipt})+'\\n').encode())
os.close(fd)
while True:signal.pause()
'''
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); root.chmod(0o700)
            value, _ = self.selected(root)
            value['bounds'].update(native_seconds=1, whole_seconds=37)
            config = root/'config.json'; config.write_text(json.dumps(value)); config.chmod(0o600)
            cleaned = []
            def cleanup(config, receipt, **kwargs):
                cleaned.append(receipt.container_id)
                return {'cleanup_complete': True}
            with patch.object(command, '_SUPERVISOR', supervisor), \
                    patch.object(command, 'cleanup_scenario_container', cleanup):
                with self.assertRaises(TimeoutError): command.run_puppy_scenario_command(config)
            self.assertEqual(cleaned, ['d'*64])
            terminal = json.loads(next(root.glob('ashlar-original17-supervision-*/terminal.json')).read_bytes())
            self.assertTrue(terminal['process_cleanup']['session_leader_reaped'])
            self.assertTrue(terminal['process_cleanup']['groups_absent'])
            self.assertFalse((root/'output/result.json').exists())
