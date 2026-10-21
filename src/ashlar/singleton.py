# SPDX-License-Identifier: MIT OR Apache-2.0
"""Resolver-bound singleton reads under complete active pin custody."""
import hashlib,json,re
from types import MappingProxyType
from .native import _quoted
from .publication import resolve_publication,ResolutionError

_LOOKUP_HASH_PROFILES={
    'ashlar-delta/0.3':'sha256-json-v1',
    'ashlar-delta/0.4-xxhash64':'spark-xxhash64-typed-key/0.1',
}


def _native_lookup_hash(profile, kind):
    """Return the closed Spark expression for the selected native hash profile."""
    if profile=='ashlar-delta/0.3':
        return None
    if profile!='ashlar-delta/0.4-xxhash64':
        raise ResolutionError('Unsupported exact singleton carrier profile')
    return ('xxhash64(CAST(:source AS STRING),CAST(:type AS BIGINT),'
            'CAST(:id AS BIGINT))')


def read_singleton(executor, backend, pins, vector, policy, *, publication_id,
                   table, kind, source, type_id, entity_id, context,
                   supported_profiles, supported_revisions):
    """Resolve and execute one native point lookup before releasing read custody.

    policy.bind_descriptor must independently bind original manifest custody to
    the trusted pin vector. policy.authorize_row authenticates and applies current
    row policy (including absence) before returning anything. Neither has a
    permissive default. The pin transaction encloses resolution and execution.
    """
    if kind not in ('object','edge') or not isinstance(source,str) or not source:
        raise ResolutionError('Explicit typed source identity required')
    if any(type(v) is not int or not -(2**63)<=v<2**63 for v in (type_id,entity_id)):
        raise ResolutionError('Singleton identity outside signed64 profile')
    if table not in vector.targets:
        raise ResolutionError('Singleton target absent from complete pin vector')
    profiles=tuple(supported_profiles)
    if (not profiles or len(set(profiles))!=len(profiles)
            or any(profile not in _LOOKUP_HASH_PROFILES for profile in profiles)):
        raise ResolutionError('Explicit qualified singleton carrier profiles required')
    typed='type_id' if kind=='object' else 'rel_type_id'
    with pins.hold(vector,context=context):
        resolved=resolve_publication(backend,publication_id,
                    {t:pair[0] for t,pair in vector.targets.items()},context=context,
                    supported_profiles=profiles,supported_revisions=supported_revisions)
        profile=resolved.descriptor.profile
        if profile not in _LOOKUP_HASH_PROFILES:
            raise ResolutionError('Unsupported exact singleton carrier profile')
        if profile=='ashlar-delta/0.3':
            identity={'source_system':source,typed:type_id,'id':entity_id}
            hashed=hashlib.sha256(json.dumps(identity,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        if any(resolved.snapshots[t].version!=pair[1] for t,pair in vector.targets.items()):
            raise ResolutionError('Publication does not match held original version vector')
        if policy.bind_descriptor(resolved.descriptor,vector,context) is not None:
            raise ResolutionError('Original descriptor/pin custody incomplete')
        snapshot=resolved.snapshots[table];quoted=_quoted(table)
        def identity_check():
            rows=executor.query('DESCRIBE DETAIL '+quoted,{}).rows
            if len(rows)!=1 or rows[0].get('id')!=snapshot.uuid:
                raise ResolutionError('Singleton target replaced')
        identity_check()
        integers=[typed,'id','entity_version']+(['source_type','source_id','target_type','target_id'] if kind=='edge' else [])
        # Native TIMESTAMP JSON responses can truncate fractional precision.
        # Preserve the instant as signed epoch-microsecond text, independent of
        # session timezone and floating-point/Date conversions.
        hash_expression=_native_lookup_hash(profile,kind)
        if profile=='ashlar-delta/0.3':
            projection='* EXCEPT ('+','.join(integers+['published_at'])+'),'+','.join('cast('+c+' AS STRING) AS '+c for c in integers)+',cast(unix_micros(published_at) AS STRING) AS published_at'
            predicate='lookup_hash=:hash'
            parameters={'hash':hashed,'source':source,'type':str(type_id),'id':str(entity_id)}
        else:
            projection='* EXCEPT ('+','.join(integers+['published_at','lookup_hash'])+'),'+','.join('cast('+c+' AS STRING) AS '+c for c in integers)+',cast(lookup_hash AS STRING) AS lookup_hash,cast('+hash_expression+' AS STRING) AS requested_lookup_hash,cast(unix_micros(published_at) AS STRING) AS published_at'
            predicate='lookup_hash='+hash_expression
            parameters={'source':source,'type':str(type_id),'id':str(entity_id)}
        rows=executor.query('SELECT '+projection+' FROM '+quoted+' VERSION AS OF '+str(snapshot.version)
            +' WHERE '+predicate+' AND source_system=:source AND '+typed+'=cast(:type AS BIGINT) AND id=cast(:id AS BIGINT) LIMIT 2',parameters).rows
        if len(rows)>1:raise ResolutionError('Ambiguous native singleton identity')
        row=dict(rows[0]) if rows else None
        if row is not None:
            expected={'source_system':source,typed:str(type_id),'id':str(entity_id)}
            if profile=='ashlar-delta/0.3':expected['lookup_hash']=hashed
            else:
                requested_hash=row.pop('requested_lookup_hash',None)
                native_hash=row.get('lookup_hash')
                if (not isinstance(native_hash,str) or not re.fullmatch(r'0|-?[1-9][0-9]*',native_hash)
                        or not -(2**63)<=int(native_hash)<2**63 or native_hash!=requested_hash):
                    raise ResolutionError('Native signed64 xxhash64 identity mismatch')
            if any(row.get(k)!=v for k,v in expected.items()):
                raise ResolutionError('Native singleton identity carrier mismatch')
        if row is not None:
            if 'published_at' not in row:raise ResolutionError('Missing exact timestamp carrier')
            stamp=row['published_at']
            if stamp is not None and (not isinstance(stamp,str) or len(stamp)>20 or not re.fullmatch(r'0|-?[1-9][0-9]*',stamp) or not -(2**63)<=int(stamp)<2**63):
                raise ResolutionError('Timestamp requires canonical signed64 epoch-microsecond text')
        identity_check()
        # Revalidate current retention/descriptor policy after execution too:
        # a read crossing expiry or a shortened configuration returns nothing.
        if backend.validate_descriptor(resolved.descriptor,context) is not None:
            raise ResolutionError('Current publication admission expired')
        if backend.authorize(context,publication_id,tuple(vector.targets)) is not None:
            raise ResolutionError('Current read authority incomplete')
        if policy.authorize_row(resolved.descriptor,table,row,context) is not None:
            raise ResolutionError('Current singleton policy incomplete')
        result=MappingProxyType(row) if row is not None else None
    # Return only after the pin context's final custody/authorization checks.
    return result
