"""Bounded complete-row parity at exact native versions, not retention authority.

Expected schemas/inventories must be independently derived from admitted source
and prior state. This verifier supports STRING/BIGINT/TIMESTAMP carriers only.
It never authorizes publication, certifies protocol or creates a retention pin.
"""
import collections
import re
from .native import _quoted

class EffectValidationError(ValueError):
    pass


def validate_effect_snapshot(executor, table, uuid, version, columns, expected_rows):
    quoted=_quoted(table)
    if not isinstance(uuid,str) or not uuid or type(version) is not int or not 0<=version<2**63:
        raise EffectValidationError('Exact trusted UUID/version required')
    columns=tuple(tuple(c) for c in columns)
    if not columns or len({c[0] for c in columns})!=len(columns):
        raise EffectValidationError('Complete unique expected columns required')
    for name,kind in columns:
        if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',name) or kind not in ('STRING','BIGINT','TIMESTAMP'):
            raise EffectValidationError('Unsupported exact column profile')
    rows=list(expected_rows)
    if len(rows)>1000 or any(set(r)!={c[0] for c in columns} or any(v is not None and not isinstance(v,str) for v in r.values()) for r in rows):
        raise EffectValidationError('Bounded complete string/null expected rows required')
    def identity():
        actual=executor.query('DESCRIBE DETAIL '+quoted,{}).rows
        if len(actual)!=1 or actual[0].get('id')!=uuid:raise EffectValidationError('Effect target identity changed')
    identity()
    pinned=quoted+' VERSION AS OF '+str(version)
    schema=executor.query('SELECT * FROM '+pinned+' LIMIT 0',{})
    if schema.rows or tuple(schema.columns)!=columns:raise EffectValidationError('Exact effect schema mismatch')
    projection=','.join(('cast(unix_micros(`'+name+'`) AS STRING)' if kind=='TIMESTAMP' else 'cast(`'+name+'` AS STRING)')+' AS `'+name+'`' for name,kind in columns)
    actual=executor.query('SELECT '+projection+' FROM '+pinned+' LIMIT 1001',{}).rows
    names=[c[0] for c in columns]
    if len(actual)>1000 or any(set(r)!=set(names) for r in actual):raise EffectValidationError('Incomplete native inventory')
    # Multiset comparison retains duplicate multiplicity and SQL NULL versus
    # text "null"; JSON text values remain exact, never normalized.
    expected=collections.Counter(tuple(r[n] for n in names) for r in rows)
    observed=collections.Counter(tuple(r[n] for n in names) for r in actual)
    if observed!=expected:raise EffectValidationError('Complete native effect parity mismatch')
    identity()
    return {'table':table,'uuid':uuid,'version':version,'rows':len(rows),'columns':len(columns)}
