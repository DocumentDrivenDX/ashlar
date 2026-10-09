import unittest
from unittest.mock import patch
from local_outbox_connection import connect
class Tests(unittest.TestCase):
    def test_invalid_roles_refuse_before_container_or_authentication_access(self):
        with patch('local_outbox_connection.subprocess.check_output') as inspect:
            for role in [None,'postgres','ashlar_ack_x;RESET ROLE','ashlar_ack_'+'x'*60]:
                with self.assertRaises(PermissionError):connect(role)
            inspect.assert_not_called()
    def test_wrong_container_and_nonlocal_endpoint_refuse_before_authentication(self):
        import json
        cases=[{'Config':{'Labels':{'ashlar.purpose':'other'}}},
               {'Config':{'Labels':{'ashlar.purpose':'end-to-end-development'}},'NetworkSettings':{'Ports':{'5432/tcp':[{'HostIp':'0.0.0.0','HostPort':'15432'}]}}}]
        for container in cases:
            with patch('local_outbox_connection.subprocess.check_output',return_value=json.dumps([container]).encode()):
                with self.assertRaises(ValueError):connect('ashlar_ack_operator_fixture')
