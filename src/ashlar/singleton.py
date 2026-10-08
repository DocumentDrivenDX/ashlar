"""Resolver-bound singleton reads under complete active pin custody."""
import hashlib,json
from types import MappingProxyType
from .native import _quoted
from .publication import resolve_publication,ResolutionError


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
    typed='type_id' if kind=='object' else 'rel_type_id'
    identity={'source_system':source,typed:type_id,'id':entity_id}
    hashed=hashlib.sha256(json.dumps(identity,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
    with pins.hold(vector,context=context):
        resolved=resolve_publication(backend,publication_id,
                    {t:pair[0] for t,pair in vector.targets.items()},context=context,
                    supported_profiles=supported_profiles,supported_revisions=supported_revisions)
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
        projection='* EXCEPT ('+','.join(integers)+'),'+','.join('cast('+c+' AS STRING) AS '+c for c in integers)
        rows=executor.query('SELECT '+projection+' FROM '+quoted+' VERSION AS OF '+str(snapshot.version)
            +' WHERE lookup_hash=:hash AND source_system=:source AND '+typed+'=cast(:type AS BIGINT) AND id=cast(:id AS BIGINT) LIMIT 2',
            {'hash':hashed,'source':source,'type':str(type_id),'id':str(entity_id)}).rows
        if len(rows)>1:raise ResolutionError('Ambiguous native singleton identity')
        row=dict(rows[0]) if rows else None
        if row is not None and any(row.get(k)!=v for k,v in {'source_system':source,typed:str(type_id),'id':str(entity_id),'lookup_hash':hashed}.items()):
            raise ResolutionError('Native singleton identity carrier mismatch')
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
