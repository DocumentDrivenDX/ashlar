# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public installed owner ports with inert SDK observations; no remote authority proof."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from ashlar.authority import AuthorityError
from ashlar_host.effective_grants import effective_grants
from ashlar_host.native_owner_lane import NativeOwnerLane

class Tests(unittest.TestCase):
    def test_public_owner_and_legacy_shims_share_one_implementation(self):
        import effective_grants as legacy_grants
        import native_owner_lane as legacy_lane
        self.assertIs(legacy_grants.effective_grants,effective_grants)
        self.assertIs(legacy_lane.NativeOwnerLane,NativeOwnerLane)
        self.assertEqual(effective_grants.__module__,'ashlar_host.effective_grants')
        self.assertEqual(NativeOwnerLane.__module__,'ashlar_host.native_owner_lane')
    def test_current_actor_managed_owner_and_paginated_permission_refusals(self):
        api=SimpleNamespace(do=Mock(side_effect=[{'next_page_token':'second'},{'privilege_assignments':[{'principal':'reader','privileges':[{'privilege':'SELECT'}]}]}]))
        table=SimpleNamespace(owner='owner',table_type=SimpleNamespace(value='MANAGED'))
        workspace=SimpleNamespace(current_user=SimpleNamespace(me=Mock(return_value=SimpleNamespace(user_name='owner'))),tables=SimpleNamespace(get=Mock(return_value=table)),api_client=api)
        retained=[];owner=NativeOwnerLane(workspace,'owner',lambda *args:retained.append(args))
        self.assertIs(owner.admit('table','c.s.t',managed=True),table);self.assertEqual(len(retained),2)
        self.assertEqual(api.do.call_args_list[1].kwargs['query']['page_token'],'second')
        api.do=Mock(return_value={'privilege_assignments':[{'principal':'foreign','privileges':[{'privilege':'MODIFY'}]}]})
        with self.assertRaises(AuthorityError):owner.admit('table','c.s.t',managed=True)
        workspace.current_user.me.return_value=SimpleNamespace(user_name='changed');api.do.reset_mock()
        with self.assertRaises(PermissionError):owner.admit('table','c.s.t',managed=True)
        api.do.assert_not_called()
        workspace.current_user.me.return_value=SimpleNamespace(user_name='owner');table.table_type.value='EXTERNAL'
        with self.assertRaises(AuthorityError):owner.admit('table','c.s.t',managed=True)
        api.do.assert_not_called()

if __name__=='__main__':unittest.main()
