# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed operator configuration preserves mode and finite resource selection."""
import json
from pathlib import Path
from unittest import TestCase
from ashlar_host.graphframes_cli import parse_graphframes_invocation
class Tests(TestCase):
 def config(self):return {'profile':'ashlar-graphframes-invocation/0.1','mode':'live-publication','pack':'/operator/pack.json','output':'/operator/output','runtime':{'python':'/operator/python','python_sha256':'a'*64,'java_home':'/operator/java','java_sha256':'b'*64,'worker_revision':'c'*40,'worker_inventory_sha256':'d'*64,'sdk_inventory_sha256':'e'*64,'jars':'/operator/jars','timeout_seconds':300,'maximum_stream_bytes':1048576,'maximum_evidence_bytes':16777216}}
 def test_explicit_isolated_runtime_and_mode(self):
  cfg=parse_graphframes_invocation(json.dumps(self.config()).encode());self.assertEqual(cfg.runtime.work,cfg.output/'engine');self.assertEqual(cfg.runtime.timeout_seconds,300)
 def test_no_retained_fallback_and_unknown_capacity_refuse(self):
  for field,value in [('release','/operator/release'),('unknown',True)]:
   config=self.config();config[field]=value
   with self.assertRaises(ValueError):parse_graphframes_invocation(json.dumps(config).encode())
  for value in [True,301,0,-1,1.2]:
   config=self.config();config['runtime']['timeout_seconds']=value
   with self.assertRaises(ValueError):parse_graphframes_invocation(json.dumps(config).encode())
 def test_duplicate_and_giant_integer_refuse(self):
  raw=json.dumps(self.config()).encode()
  for changed in [raw.replace(b'"mode":',b'"mode":"duplicate","mode":'),raw.replace(b'300',b'9'*60000)]:
   with self.assertRaises(ValueError):parse_graphframes_invocation(changed)
 def test_offline_requires_both_external_original_digests(self):
  config=self.config();config.update(mode='protected-ack-export',release='/operator/release',release_sha256='b'*64,custody='/operator/custody',custody_sha256='c'*64)
  cfg=parse_graphframes_invocation(json.dumps(config).encode());self.assertEqual(cfg.mode,'protected-ack-export')
  del config['custody_sha256']
  with self.assertRaises(ValueError):parse_graphframes_invocation(json.dumps(config).encode())

 def test_original_commerce_profile_explicit_publication_no_offline_fallback(self):
  from ashlar_host.graphframes_cli import CommerceGraphFramesInvocation,COMMERCE_PROFILE
  config=self.config();config.update(profile=COMMERCE_PROFILE,publication='/operator/original-publication')
  selected=parse_graphframes_invocation(json.dumps(config).encode());self.assertIs(type(selected),CommerceGraphFramesInvocation);self.assertEqual(selected.publication,Path('/operator/original-publication'))
  for change in ({'mode':'protected-ack-export'},{'publication':'/operator/output'},{'publication':'relative'}, {'release':'/operator/release'}):
   with self.assertRaises(ValueError):parse_graphframes_invocation(json.dumps(dict(config,**change)).encode())
  del config['publication']
  with self.assertRaises(ValueError):parse_graphframes_invocation(json.dumps(config).encode())

 def test_installed_public_cli_dispatch_and_redacted_refusal(self):
  import contextlib,io
  from unittest import mock
  from ashlar.cli import main
  args=['query-graphframes','--configuration','/operator/config','--provider','/operator/provider.py','--provider-sha256','a'*64]
  with mock.patch('ashlar_host.graphframes_cli.run_graphframes_command',return_value={'complete':True})as run,contextlib.redirect_stdout(io.StringIO()),mock.patch('sys.argv',['ashlar',*args]):
   self.assertIsNone(main())
   run.assert_called_once_with('/operator/config','/operator/provider.py','a'*64)
  stream=io.StringIO()
  with mock.patch('ashlar_host.graphframes_cli.run_graphframes_command',side_effect=ValueError('private provider detail')),contextlib.redirect_stderr(stream),mock.patch('sys.argv',['ashlar',*args]):
   with self.assertRaises(SystemExit)as raised:main()
   self.assertEqual(raised.exception.code,2)
   self.assertNotIn('private provider detail',stream.getvalue())
