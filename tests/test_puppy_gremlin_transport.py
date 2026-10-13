"""Optional pinned-driver wire admission; no engine or socket required."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from gremlin_python.statics import long as SDKLong
from ashlar_host.puppy_gremlin_transport import RetainedGraphSONSerializer,validate_graphson,passive_rows,BoundedGremlinTransport

class Tests(unittest.TestCase):
    def test_original_wire_before_decode_and_exact_count_boundary(self):
        frame=json.dumps({'requestId':'00000000-0000-0000-0000-000000000001','status':{'code':200,'message':'','attributes':{'@type':'g:Map','@value':[]}},'result':{'data':{'@type':'g:List','@value':[{'@type':'g:Map','@value':['key','R1','paths',{'@type':'g:Int64','@value':4}]}]},'meta':{}}}).encode()
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory)/'1';path.mkdir();serializer=RetainedGraphSONSerializer(path)
            decoded=serializer.deserialize_message(frame)
            rows,census=passive_rows(decoded['result']['data'],count_fields=('paths',))
            self.assertEqual(rows,[{'key':'R1','paths':4}]);self.assertEqual(census['sdk_counts'],1)
            self.assertEqual((path/'wire-1.json').read_bytes(),frame)
            malformed=b'{"result":{"@type":"g:Map","@value":["key","R1","key","R2"]}}'
            with self.assertRaises(ValueError):serializer.deserialize_message(malformed)
            self.assertEqual((path/'wire-2.json').read_bytes(),malformed)
        for raw in (b'{"x":1,"x":2}',b'{"@type":"g:Float","@value":1.0}'):
            with self.assertRaises(ValueError):validate_graphson(raw)
        for value in ([{'key':SDKLong(1)}],[{'paths':1.0}]):
            with self.assertRaises(ValueError):passive_rows(value,count_fields=('paths',))

    def test_transport_timeout_and_session_closure_are_owned(self):
        import asyncio,time
        transport=BoundedGremlinTransport(time.monotonic()+.01)
        async def stall():await asyncio.sleep(1)
        with self.assertRaises(TimeoutError):transport.run(stall())
        calls=[]
        class Socket:
            async def close(self):calls.append('socket');raise ValueError('close refused')
        class Session:
            async def close(self):calls.append('session')
        transport.socket=Socket();transport.session=Session()
        with self.assertRaises(ValueError):transport.close()
        self.assertEqual(calls,['socket','session']);self.assertTrue(transport.loop.is_closed())

    def test_inflight_result_timeout_closes_transport_before_client_pool(self):
        import time
        from types import SimpleNamespace
        from ashlar_host import puppy_gremlin_transport as module
        events=[]
        class Transport:
            def __init__(self,deadline):pass
            def close(self):events.append('transport')
        class Future:
            def result(self,timeout):raise TimeoutError('inflight')
        class Result:
            def all(self):return Future()
        class Submitted:
            def result(self,timeout):return Result()
        class Client:
            def __init__(self,*args,transport_factory,**kwargs):self.transport=transport_factory()
            def submit_async(self,*args,**kwargs):return Submitted()
            def close(self):events.append('client')
        with tempfile.TemporaryDirectory()as directory:
            owner=SimpleNamespace(deadline=time.monotonic()+30,config=SimpleNamespace(prepared=Path(directory)/'prepared',gremlin_port=25482,username='inert',password='inert'),record_observation=lambda *a:events.append('result'))
            with patch.object(module,'Client',Client),patch.object(module,'BoundedGremlinTransport',Transport):
                with self.assertRaises(TimeoutError):module.query_gremlin(owner,'g.V()')
        self.assertEqual(events,['transport','client'])
