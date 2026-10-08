"""Bounded candidate Truss complete-feed/0.1 fragment custody assembly.

No property-feed reconstruction, live source admission or ACK. Host policy must
qualify original registered definitions, native complete committed membership,
prerequisites, clock/retention/worker custody and selected payload interpretation.
"""
from dataclasses import dataclass
import hashlib
from .schema import _json
from .truss_input import exact_artifact,_canonical,_text,_hash

class FeedAssemblyError(ValueError):pass

@dataclass(frozen=True)
class AssembledFeed:
    original_manifest: bytes
    original_fragments: tuple
    ordered_payloads: tuple


def assemble_feed(manifest_artifact,fragments,*,policy,context):
    raw=exact_artifact(manifest_artifact);manifest=_json(raw)
    fields={'interfaceVersion','context','xid','manifestProfile','configurationPrerequisites','members','prerequisites','manifestSha256'}
    if not isinstance(manifest,dict) or set(manifest)!=fields or manifest['interfaceVersion']!='truss-feed-transaction/0.1.0':raise FeedAssemblyError('Selected complete-feed manifest shape required')
    if _canonical(manifest).encode()!=raw:raise FeedAssemblyError('Original manifest archive must retain complete canonical wire bytes')
    _hash(manifest['manifestSha256'])
    preimage=dict(manifest);preimage.pop('manifestSha256')
    digest=hashlib.sha256(b'truss-canonical/0.1.0\ntruss-feed-manifest/0.1.0\n'+_canonical(preimage).encode()).hexdigest()
    if digest!=manifest['manifestSha256']:raise FeedAssemblyError('Framed original manifest digest differs')
    for name in ('configurationPrerequisites','prerequisites'):
        if not isinstance(manifest[name],list):raise FeedAssemblyError('Complete prerequisite arrays required')
    scope=manifest['context']
    if not isinstance(scope,dict) or set(scope)!={'sourceEpoch','feedProfile','scopeIdentity'}:raise FeedAssemblyError('Complete original feed context required')
    for value in scope.values():_text(value)
    _text(manifest['xid'])
    def profile(pin):
        if not isinstance(pin,dict) or set(pin)!={'identity','version','sha256'}:raise FeedAssemblyError('Complete original profile pin required')
        _text(pin['identity']);_text(pin['version']);_hash(pin['sha256'])
    profile(manifest['manifestProfile'])
    for prerequisite in manifest['prerequisites']:
        if not isinstance(prerequisite,dict) or set(prerequisite)!={'revision','artifact'}:raise FeedAssemblyError('Complete revision prerequisite required')
        _text(prerequisite['revision']);exact_artifact(prerequisite['artifact'])
    for prerequisite in manifest['configurationPrerequisites']:
        if not isinstance(prerequisite,dict) or set(prerequisite)!={'configuration','artifact'} or not isinstance(prerequisite['configuration'],dict):raise FeedAssemblyError('Complete original configuration prerequisite required')
        exact_artifact(prerequisite['artifact'])
    members=manifest['members']
    if not isinstance(members,list) or not 1<=len(members)<=1000:raise FeedAssemblyError('Bounded nonempty native membership required')
    inventory={}
    for member in members:
        if not isinstance(member,dict) or set(member)!={'ordinal','key','payloadProfile','payloadSha256'}:raise FeedAssemblyError('Closed complete member shape required')
        profile(member['payloadProfile'])
        ordinal=member['ordinal'];_text(ordinal);_hash(member['payloadSha256'])
        if not ordinal.isascii() or not ordinal.isdecimal() or len(ordinal)>19 or str(int(ordinal))!=ordinal or int(ordinal)>=2**63 or ordinal in inventory:raise FeedAssemblyError('Unique canonical native ordinal required')
        inventory[ordinal]=member
    if policy.admit_manifest(raw,context) is not None:raise FeedAssemblyError('Original native manifest admission incomplete')
    seen={};originals=[];size=len(raw)
    for fragment_raw in fragments:
        if len(originals)>=128 or not isinstance(fragment_raw,bytes) or len(fragment_raw)>1048576:raise FeedAssemblyError('Bounded original fragment bytes required')
        size+=len(fragment_raw)
        if size>16*1048576:raise FeedAssemblyError('Complete feed assembly resource limit')
        fragment=_json(fragment_raw)
        if not isinstance(fragment,dict) or set(fragment)!={'manifest','records'} or fragment['manifest']!=manifest or not isinstance(fragment['records'],list) or not 1<=len(fragment['records'])<=1000:raise FeedAssemblyError('Fragment differs from original transaction')
        originals.append(fragment_raw)
        for record in fragment['records']:
            if not isinstance(record,dict) or set(record)!={'ordinal','payload'} or not isinstance(record['ordinal'],str) or record['ordinal'] not in inventory:raise FeedAssemblyError('Foreign or malformed member ordinal')
            payload=exact_artifact(record['payload']);ordinal=record['ordinal']
            if record['payload']['sha256']!=inventory[ordinal]['payloadSha256']:raise FeedAssemblyError('Member payload differs from original manifest')
            original=(record['payload']['identity'],payload)
            if ordinal in seen and seen[ordinal]!=original:raise FeedAssemblyError('Conflicting original fragment replay')
            seen[ordinal]=original
    if set(seen)!=set(inventory):raise FeedAssemblyError('Incomplete transaction; no complete boundary')
    ordered=tuple(seen[member['ordinal']][1] for member in members)
    if policy.admit_complete(raw,ordered,context) is not None:raise FeedAssemblyError('Complete source/payload admission incomplete')
    return AssembledFeed(raw,tuple(originals),ordered)
