"""Pinned public GraphSON transport; original wire precedes exact count decoding."""
import asyncio,json,threading,time
import aiohttp
from gremlin_python.driver.transport import AbstractBaseTransport
from importlib.metadata import version
from pathlib import Path
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV3d0
from gremlin_python.statics import long as SDKLong
from .lifecycle import finish


def require_gremlin_runtime():
    if version('gremlinpython')!='3.7.3':raise ValueError('Explicit GremlinPython3.7.3 GraphSON runtime required')


def passive_rows(value,*,count_fields=(),maximum_bytes=1048576):
    """Convert only exact SDK signed64 counts at selected projected field names."""
    if type(count_fields)is not tuple or any(name not in ('degree','paths','targets')for name in count_fields):raise ValueError('Explicit known count projection required')
    active=set();nodes=0;census={'sdk_counts':0,'plain_integers':0}
    def copy(item,depth,field=None):
        nonlocal nodes
        nodes+=1
        if depth>64 or nodes>250000:raise ValueError('Bounded SDK carrier structure required')
        if type(item)is SDKLong:
            if field not in count_fields:raise ValueError('SDK integer promotion only for declared count fields')
            result=int.__int__(item)
            if not 0<=result<2**63:raise ValueError('Exact nonnegative signed64 count required')
            census['sdk_counts']+=1;return result
        if type(item)is int:
            if field not in count_fields or not 0<=item<2**63:raise ValueError('Exact declared signed64 count required')
            census['plain_integers']+=1;return item
        if item is None or type(item)in (str,bool):return item
        if type(item)not in (dict,list):raise ValueError('Unknown SDK carrier refused')
        if id(item)in active:raise ValueError('Cyclic SDK carrier refused')
        active.add(id(item))
        try:
            if type(item)is list:return[copy(entry,depth+1,field)for entry in item]
            if any(type(key)is not str for key in item):raise ValueError('Exact named SDK projection required')
            return{key:copy(entry,depth+1,key)for key,entry in item.items()}
        finally:active.remove(id(item))
    rows=copy(value,0)
    raw=json.dumps(rows,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    if type(rows)is not list or len(rows)>1000 or len(raw)>maximum_bytes:raise ValueError('Complete bounded Gremlin rows required')
    return rows,census


def validate_graphson(raw):
    """Refuse collapsing objects/maps and unknown scalar families before SDK parse."""
    def pairs(entries):
        result={}
        for key,value in entries:
            if key in result:raise ValueError('Duplicate original JSON object key')
            result[key]=value
        return result
    value=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite JSON refused')))
    nodes=0
    def visit(item,depth=0):
        nonlocal nodes
        nodes+=1
        if depth>64 or nodes>250000:raise ValueError('Bounded original GraphSON structure required')
        if type(item)is list:
            for entry in item:visit(entry,depth+1)
        elif type(item)is dict:
            if '@type'in item:
                if set(item)!= {'@type','@value'}:raise ValueError('Exact GraphSON typed envelope required')
                kind=item['@type'];body=item['@value']
                if kind not in ('g:Map','g:List','g:Int32','g:Int64','g:String','g:UUID'):raise ValueError('Unknown original GraphSON scalar family')
                if kind=='g:Map':
                    if type(body)is not list or len(body)%2:raise ValueError('Exact GraphSON map pairs required')
                    keys=set()
                    for key in body[::2]:
                        if type(key)is dict and key.get('@type')=='g:String' and set(key)=={'@type','@value'}:key=key['@value']
                        if type(key)is not str or key in keys:raise ValueError('Distinct exact GraphSON map keys required')
                        keys.add(key)
                elif kind=='g:List' and type(body)is not list:raise ValueError('Exact GraphSON list required')
                elif kind in ('g:Int32','g:Int64'):
                    bits=32 if kind=='g:Int32' else 64
                    if type(body)is not int or not -(2**(bits-1))<=body<2**(bits-1):raise ValueError('Exact GraphSON integer required')
                elif kind in ('g:String','g:UUID') and type(body)is not str:raise ValueError('Exact GraphSON text required')
            for entry in item.values():visit(entry,depth+1)
        elif item is not None and type(item)not in (str,bool,int):raise ValueError('Unknown original JSON scalar')
    visit(value)


class BoundedGremlinTransport(AbstractBaseTransport):
    """Public transport factory with bounded connection, IO and closure."""
    def __init__(self,deadline):
        self.deadline=deadline;self.loop=asyncio.new_event_loop();self.session=None;self.socket=None;self.lock=threading.RLock()
    def run(self,operation,*,closing=False):
        with self.lock:
            bound=3 if closing else min(15,self.deadline-time.monotonic())
            if bound<=0:
                operation.close();raise TimeoutError('Owned Gremlin deadline elapsed')
            return self.loop.run_until_complete(asyncio.wait_for(operation,bound))
    def connect(self,url,headers=None):
        async def connect():
            self.session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))
            self.socket=await self.session.ws_connect(url,headers=headers,max_msg_size=1048576)
        try:self.run(connect())
        except BaseException as error:
            finish(error,[self.close]);raise
    def write(self,message):self.run(self.socket.send_bytes(message))
    def read(self):
        message=self.run(self.socket.receive())
        if message.type==aiohttp.WSMsgType.TEXT:return message.data.encode('utf-8')
        if message.type==aiohttp.WSMsgType.BINARY:return message.data
        raise ValueError('Original Gremlin websocket closed or failed')
    def close(self):
        with self.lock:
            if self.loop.is_closed():return
            async def close_socket():
                if self.socket is not None:await self.socket.close()
            async def close_session():
                if self.session is not None:await self.session.close()
            try:finish(None,[lambda:self.run(close_socket(),closing=True),lambda:self.run(close_session(),closing=True)])
            finally:self.loop.close()
    @property
    def closed(self):return self.loop.is_closed() or (self.socket is not None and self.socket.closed)


class RetainedGraphSONSerializer(GraphSONSerializersV3d0):
    """Retain bounded exact incoming frames before SDK decoding, never requests."""
    def __init__(self,directory,*,maximum_frame_bytes=1048576,maximum_total_bytes=16777216):
        if (type(maximum_frame_bytes)is not int or not 1<=maximum_frame_bytes<=1048576
                or type(maximum_total_bytes)is not int or not 1<=maximum_total_bytes<=16777216):
            raise ValueError('Explicit bounded GraphSON capture limits required')
        self.maximum_frame_bytes=maximum_frame_bytes;self.maximum_total_bytes=maximum_total_bytes
        super().__init__();self.directory=Path(directory);self.lock=threading.Lock();self.count=0;self.total=0
    def deserialize_message(self,message):
        if type(message)is str:raw=message.encode('utf-8')
        elif type(message)is bytes:raw=message
        else:raise ValueError('Exact raw GraphSON bytes/text required')
        if len(raw)>self.maximum_frame_bytes:raise ValueError('Bounded raw GraphSON frame required')
        with self.lock:
            if self.total+len(raw)>self.maximum_total_bytes:raise ValueError('Selected GraphSON capture limit exceeded')
            files=list(self.directory.parent.glob('*/wire-*.json'))
            if len(files)>=128 or sum(path.stat().st_size for path in files)+len(raw)>16777216:raise ValueError('Bounded whole GraphSON capture required')
            self.count+=1
            with (self.directory/('wire-'+str(self.count)+'.json')).open('xb')as stream:
                if stream.write(raw)!=len(raw):raise ValueError('Complete original wire capture required')
            self.total+=len(raw)
        validate_graphson(raw)
        return super().deserialize_message(message)


def query_gremlin(owner,script,*,count_fields=()):
    require_gremlin_runtime()
    if type(script)is not str or not script or len(script.encode())>65536:raise ValueError('Bounded original Gremlin script required')
    remaining=owner.deadline-time.monotonic()
    if remaining<=0:raise TimeoutError('Owned Gremlin deadline elapsed')
    directory=owner.config.prepared.parent/'gremlin-wire';directory.mkdir(mode=0o700,exist_ok=True)
    # One request has one independent directory; duplicate frames cannot overwrite.
    ordinal=sum(1 for path in directory.iterdir()if path.is_dir())+1
    selected=directory/str(ordinal);selected.mkdir(mode=0o700)
    client=None;primary=None;rows=None;census=None;transports=[]
    def transport_factory():
        transport=BoundedGremlinTransport(owner.deadline);transports.append(transport);return transport
    try:
        client=Client('ws://127.0.0.1:'+str(owner.config.gremlin_port)+'/gremlin','g',
            username=owner.config.username,password=owner.config.password,
            message_serializer=RetainedGraphSONSerializer(selected),pool_size=1,max_workers=2,
            transport_factory=transport_factory)
        result=client.submit_async(script,request_options={'evaluationTimeout':int(min(15,remaining)*1000)}).result(timeout=min(15,max(0,owner.deadline-time.monotonic())))
        native=result.all().result(timeout=min(15,max(0,owner.deadline-time.monotonic())))
        rows,census=passive_rows(native,count_fields=count_fields)
    except BaseException as error:primary=error
    finish(primary,[transport.close for transport in transports]+([client.close]if client is not None else[]))
    owner.record_observation('gremlin-projection',json.dumps({'script':script,'rows':rows,'census':census},ensure_ascii=False,separators=(',',':')).encode())
    return rows
