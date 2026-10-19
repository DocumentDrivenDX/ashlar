# SPDX-License-Identifier: MIT OR Apache-2.0
import sys,types,unittest
from dataclasses import dataclass
from unittest.mock import patch
from ashlar_host.truss_runtime import require_truss_psycopg_runtime

class Tests(unittest.TestCase):
    def test_selected_public_profile_refuses_wrong_client_tuple_before_connection(self):
        @dataclass
        class PublicProfile:
            psycopg:str='3.3.6'
            implementation:str='binary'
            libpq:int=180006
            server:int=180006
        sdk=types.ModuleType('truss');sdk.PsycopgProfile=PublicProfile
        client=types.ModuleType('psycopg');client.__version__='3.2.13';client.pq=types.SimpleNamespace(__impl__='binary',version=lambda:180000)
        with patch.dict(sys.modules,{'truss':sdk,'psycopg':client}):
            with self.assertRaises(ValueError)as caught:require_truss_psycopg_runtime()
            self.assertIn("('3.3.6', 'binary', 180006)",str(caught.exception))
            self.assertIn("('3.2.13', 'binary', 180000)",str(caught.exception))
            client.__version__='3.3.6';client.pq.version=lambda:180006
            self.assertIsNone(require_truss_psycopg_runtime())
