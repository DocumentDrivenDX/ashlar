# SPDX-License-Identifier: MIT OR Apache-2.0
"""Neutral owner controls over real owner types and explicit inert native seams."""
import json
import unittest
from dataclasses import FrozenInstanceError
from ashlar_host.commerce_graph_release import hold_original_commerce_graph_release, HeldOriginalCommerceGraphRelease
import test_commerce_graphframes_installed as fixtures

class Tests(unittest.TestCase):
 # Reuse fixture methods only; the existing suite retains its own tests.
 opened=fixtures.ReaderTests.opened
 active_session=fixtures.ReaderTests.active_session
 def test_neutral_complete_release_renewal_and_interval_lifetime(self):
  session,events=self.active_session();opened=session.opened
  with hold_original_commerce_graph_release(opened)as held:
   self.assertIs(type(held),HeldOriginalCommerceGraphRelease)
   self.assertEqual((len(json.loads(held.release.payload)['nodes']),len(json.loads(held.release.payload)['edges'])),(11,10))
   held.renew();held.renew()
   with self.assertRaises(FrozenInstanceError):held.release=held.release
   self.assertTrue(opened.driver.held);self.assertTrue(opened.driver.pin_held)
  with self.assertRaises(PermissionError):held.renew()
  self.assertFalse(opened.driver.held);self.assertFalse(opened.driver.pin_held)
 def test_neutral_wrong_owner_never_opens_interval(self):
  with self.assertRaises(ValueError):
   with hold_original_commerce_graph_release(object()):self.fail('yield reached')
 def test_neutral_body_cancellation_attempts_full_closing(self):
  session,events=self.active_session();opened=session.opened;primary=KeyboardInterrupt('inert neutral cancellation')
  with self.assertRaises(KeyboardInterrupt)as caught:
   with hold_original_commerce_graph_release(opened):raise primary
  self.assertIs(caught.exception,primary)
  reads=[c for c in opened.driver.transport.query.call_args_list if c.args[0].startswith('SELECT ')]
  self.assertEqual(len(reads),4)
  self.assertFalse(opened.driver.pin_held);self.assertFalse(opened.driver.held)


 def test_each_row_uses_held_local_checks_without_full_source_reread(self):
  from types import SimpleNamespace
  from unittest.mock import patch
  session,events=self.active_session();opened=session.opened
  original=type(opened).renew_original_graph_read
  calls=[]
  def renewal(owner,vector,*,context):
   calls.append(vector);return original(owner,vector,context=context)
  with patch.object(type(opened),'renew_original_graph_read',renewal):
   with hold_original_commerce_graph_release(opened)as held:
    policy=held._policy;descriptor=SimpleNamespace(raw=opened.manifest)
    before=len(calls)
    table=opened.driver.tables['object_current'];row=policy.expected['object_current'][0]
    for _ in range(100):policy.authorize_graph_row(descriptor,table,row,opened.context)
    self.assertEqual(len(calls),before)
   self.assertGreater(len(calls),before) # Real full closing renewal still happens.
   with self.assertRaises(PermissionError):policy.authorize_graph_row(descriptor,table,row,opened.context)
