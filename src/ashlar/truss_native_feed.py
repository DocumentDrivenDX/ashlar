# SPDX-License-Identifier: MIT OR Apache-2.0
"""Scalar entity afterimages from original Truss native feed bytes.

Pure extraction; source authority, durable publication and ACK stay host-owned.
Original transaction bytes include all catalog/source/reservation prerequisites.
"""
import base64,hashlib,json,re
from dataclasses import dataclass
from decimal import Decimal
from .apply import Change,EntityKey,EntityState

MAX_NATIVE_PAGE_BYTES=2*1024*1024

class NativeFeedError(ValueError):pass

@dataclass(frozen=True)
class NativeFeedChanges:
    original:bytes
    event_bytes:tuple
    changes:tuple
    prior:bytes
    next:bytes
    edge_order_keys:tuple=()


def _json(raw,*,native=False):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise NativeFeedError('Duplicate native member')
            result[key]=value
        return result
    def refuse(value):raise NativeFeedError('Scalar STRING profile refuses noninteger numeric values')
    return json.loads(raw,object_pairs_hook=pairs,parse_float=(Decimal if native else refuse),parse_constant=refuse)



def _string_value(value):
    """Decode only the public closed exact-value STRING carrier."""
    if type(value)is not dict or set(value)!={'kind','text'}or value['kind']!='string'or type(value['text'])is not str:raise NativeFeedError('Original closed exact STRING value required')
    return value['text']


@dataclass(frozen=True)
class ExactNativeNumber:
    token:str


def _typed_value(value):
    if type(value)is not dict:raise NativeFeedError('Closed exact scalar carrier required')
    kind=value.get('kind')
    if kind=='string':return _string_value(value)
    if kind=='null'and set(value)=={'kind'}:return None
    if kind=='boolean'and set(value)=={'kind','value'}and type(value['value'])is bool:return value['value']
    if kind in ('integer','decimal')and set(value)=={'kind','text'}and type(value['text'])is str:
        token=value['text']
        pattern=r'-?(?:0|[1-9][0-9]*)'if kind=='integer'else r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?'
        if len(token)<=1048576 and re.fullmatch(pattern,token,flags=re.ASCII):return ExactNativeNumber(token)
    raise NativeFeedError('Supported closed exact scalar carrier required')


def _typed_properties_json(values):
    # Numeric tokens are emitted directly after closed JSON grammar admission.
    def scalar(value):
        if type(value)is ExactNativeNumber:return value.token
        return json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)
    return '{'+','.join(json.dumps(key,ensure_ascii=False)+':'+scalar(values[key])for key in sorted(values))+'}'


def _integer(value):
    text=str(value)
    if type(value)not in (int,str)or not text.isascii()or not text.isdecimal()or str(int(text))!=text or not 1<=int(text)<2**63:raise NativeFeedError('Original positive signed64 value required')
    return int(text)


def _scalar_entity_changes(content,*,sha256,prior,next,feed,allow_edges,typed=False):
    """Extract one complete transaction; never infer a version from a sequence."""
    if type(content)is not bytes or len(content)>8388608 or hashlib.sha256(content).hexdigest()!=sha256 or any(type(v)is not bytes for v in (prior,next))or type(feed)is not str or not feed:raise NativeFeedError('Complete original feed selection required')
    payload=_json(content,native=True);before=_json(prior,native=True);after=_json(next,native=True)
    if payload.get('interfaceVersion')!='truss-native-feed/0.1'or payload.get('prior')!=before or payload.get('next')!=after or before.get('sourceEpoch')!=after.get('sourceEpoch')or before.get('installationId')!=after.get('installationId'):raise NativeFeedError('Original native cursor correspondence required')
    epoch=before['sourceEpoch'];source=before['installationId']
    rows=payload.get('changes')
    if type(rows)is not list or len(rows)>4096:raise NativeFeedError('Bounded complete journal array required')
    if payload.get('writerXid') is None and ('writerXid' not in payload or rows or any(payload.get(k)!=[] for k in ('catalogPrerequisites','revisions','sources','reservations'))):
        raise NativeFeedError('Empty interval cannot carry transaction members')
    originals=[];changes=[];seen=set();property_versions=set();edge_orders=[];last=0
    for row in rows:
        seq=_integer(row['seq'])
        if seq<=last or str(row['xid'])!=payload.get('writerXid')or row['entity_kind']not in (('o','e')if allow_edges else('o',)):raise NativeFeedError('Ordered original scalar entity transaction required')
        kind='object'if row['entity_kind']=='o'else'edge'
        last=seq;wrapper=row['new_value']
        if wrapper.get('interfaceVersion')!='truss-journal-exact-bytes/0.2.0-proposal'or wrapper.get('component')!='event':raise NativeFeedError('Exact native event wrapper required')
        raw=base64.b64decode(wrapper['bytesBase64'],validate=True)
        if hashlib.sha256(raw).hexdigest()!=wrapper['bytesSha256']:raise NativeFeedError('Original event digest differs')
        event=_json(raw,native=True);originals.append(raw)
        if event.get('interfaceVersion')!='truss-history-event/0.2.0-proposal'or event.get('sourceEpoch')!=epoch or event.get('xid')!=payload['writerXid']or _integer(event['seq'])!=seq or _integer(event['eventVersion'])!=_integer(row['ver']):raise NativeFeedError('Original event position/version differs')
        operation=event['operation']
        if operation=='property':
            identity=event['identity'];property_versions.add((EntityKey(source,kind,_integer(identity['typeId']),_integer(identity['id'])),_integer(event['eventVersion'])));continue
        if operation not in ('create','metadata','delete'):raise NativeFeedError('Unsupported native lifecycle event')
        image=event['before']if operation=='delete'else event['after']
        if image.get('interfaceVersion')!='truss-history-record/0.1.0'or image.get('kind')!=kind or (kind=='object'and image.get('ownership')!={'state':'rootless'}):raise NativeFeedError('Full original rootless object required')
        identity=image['identity'];key=EntityKey(source,kind,_integer(identity['typeId']),_integer(identity['id']));version=_integer(event['eventVersion'])
        if key.id!=_integer(row['entity_id'])or key.type_id!=_integer(row['entity_type'])or _integer(image['recordVersion'])!=(version-1 if operation=='delete'else version):raise NativeFeedError('Actual afterimage/version correspondence differs')
        marker=(key,version)
        if marker in seen:raise NativeFeedError('Ambiguous entity-version afterimage')
        seen.add(marker);props={};retained={}
        for prop in image['properties']:
            name=str(_integer(prop['propertyId']))
            if name in props or type(prop.get('definitionPin'))is not str:raise NativeFeedError('Original distinct STRING properties required')
            props[name]=(_typed_value if typed else _string_value)(prop['value'])
        for prop in image['retained']:
            if type(prop['name'])is not str or prop['name']in retained:raise NativeFeedError('Original retained STRING properties required')
            retained[prop['name']]=(_typed_value if typed else _string_value)(prop['value'])
        endpoints=None
        if kind=='edge':
            if image['properties']!=[]:raise NativeFeedError('Selected native edge profile has no declared properties')
            endpoints=tuple(EntityKey(source,'object',_integer(image[role]['typeId']),_integer(image[role]['id']))for role in ('source','target'))
            order=image.get('orderKey')
            if order=={'state':'null'}:order_key=None
            elif type(order)is dict and set(order)=={'state','text'}and order['state']=='text'and type(order['text'])is str:order_key=order['text']
            else:raise NativeFeedError('Original complete native edge order key required')
            edge_orders.append((key,version,order_key))
        encode=_typed_properties_json if typed else lambda value:json.dumps(value,ensure_ascii=False,separators=(',',':'),sort_keys=True)
        state=EntityState(key,version,str(event['eventCatalogRevision']),encode(props),encode(retained),endpoints)
        changes.append(Change(feed,epoch,payload['writerXid']+':'+str(seq),hashlib.sha256(raw).hexdigest(),'delete'if operation=='delete'else('create'if operation=='create'else'replace'),state))
    if not property_versions<=seen:raise NativeFeedError('Property transaction lacks complete afterimage')
    return NativeFeedChanges(content,tuple(originals),tuple(changes),prior,next,tuple(edge_orders))



def scalar_object_changes(content,*,sha256,prior,next,feed):
    """Retain the strict object-only entrypoint; edge transactions refuse."""
    return _scalar_entity_changes(content,sha256=sha256,prior=prior,next=next,feed=feed,allow_edges=False)


def scalar_entity_changes(content,*,sha256,prior,next,feed):
    """Rootless STRING objects and native scalar edges, with typed endpoints."""
    return _scalar_entity_changes(content,sha256=sha256,prior=prior,next=next,feed=feed,allow_edges=True)


def typed_scalar_entity_changes(content,*,sha256,prior,next,feed):
    """Exact scalar tokens on rootless objects/native edges; no nested families."""
    return _scalar_entity_changes(content,sha256=sha256,prior=prior,next=next,feed=feed,allow_edges=True,typed=True)


def native_feed_batch(content,*,sha256,prior,next,feed):
    """Keep a genuine whole-page native carrier; no synthetic JSONL envelope."""
    from .source import SourceBatch,records_digest
    if type(content)is not bytes or not 1<=len(content)<=MAX_NATIVE_PAGE_BYTES or hashlib.sha256(content).hexdigest()!=sha256 or any(type(v)is not bytes or not 1<=len(v)<=16384 for v in (prior,next))or type(feed)is not str or not feed or '\0'in feed:raise NativeFeedError('Bounded original native page and cursors required')
    payload=_json(content,native=True);before=_json(prior,native=True);after=_json(next,native=True)
    if type(payload)is not dict or payload.get('interfaceVersion')!='truss-native-feed/0.1'or payload.get('prior')!=before or payload.get('next')!=after:raise NativeFeedError('Original native page/cursor correspondence required')
    for cursor in (before,after):
        if type(cursor)is not dict or cursor.get('profile')!='truss-caller-asserted-feed/0.1':raise NativeFeedError('Original native cursor profile required')
        if any(type(cursor.get(k))is not str or not cursor[k]or '\0'in cursor[k]for k in ('installationId','sourceEpoch','consumerId','registrationId','nextXid')):raise NativeFeedError('Original native cursor identity required')
        position=cursor['nextXid']
        if not position.isascii()or not position.isdecimal()or str(int(position))!=position or not 0<=int(position)<2**64:raise NativeFeedError('Original unsigned native cursor position required')
    if any(before[k]!=after[k]for k in ('installationId','sourceEpoch','consumerId','registrationId')):raise NativeFeedError('Native registration/installation/epoch changed')
    if any(type(payload.get(k))is not list for k in ('catalogPrerequisites','revisions','changes','sources','reservations'))or type(payload.get('configuration'))is not dict:raise NativeFeedError('Complete original native transaction members required')
    writer=payload.get('writerXid')
    if writer is None:
        if ('writerXid' not in payload or int(after['nextXid'])<int(before['nextXid']) or
                any(payload[k] for k in ('catalogPrerequisites','revisions','changes','sources','reservations'))):
            raise NativeFeedError('Complete empty committed interval required')
        batch_id='empty-interval:'+sha256
    else:
        if type(writer)is not str or not writer.isascii()or not writer.isdecimal()or str(int(writer))!=writer or not 1<=int(writer)<2**64 or not int(before['nextXid'])<=int(writer)<int(after['nextXid']):raise NativeFeedError('Actual committed writer interval required')
        batch_id=writer
    encode=lambda value:base64.b64encode(value).decode('ascii')
    return SourceBatch('truss-native-feed/0.1',feed,before['sourceEpoch'],batch_id,encode(prior),encode(next),(),content,b'',records_digest((content,)))
