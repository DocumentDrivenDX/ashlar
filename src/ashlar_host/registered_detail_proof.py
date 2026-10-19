# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh fixed-Boolean native identity proof; no cached authority or raw values."""
from .evolution_verification_types import VerificationBounds

NAMES=('i0','i1','i2','i3')
LEGACY_NAMES=('id','_ashlar_detail_0','_ashlar_detail_1','_ashlar_detail_2')
class RegisteredDetailProofError(ValueError):pass


def capture_registered_detail_proof(frame,expected,*,bounds):
    """Return four exact fresh comparison bits, or None for legacy schema fallback.

    Expected strings are disclosed by the selected installation, not copied from
    native output. Conservative legacy payload bounds are enforced before action.
    """
    from pyspark.sql import functions as F
    if type(bounds)is not VerificationBounds or type(expected)is not tuple or len(expected)!=4:
        raise RegisteredDetailProofError('Exact registered detail selection required')
    bounds.__post_init__()
    fields=frame.schema.fields
    if (type(fields)is not list or len(fields)!=4
            or any(field.name!=name or field.dataType.simpleString()!='string'
                   or type(field.nullable)is not bool or type(field.metadata)is not dict
                   for field,name in zip(fields,LEGACY_NAMES))):
        raise RegisteredDetailProofError('Exact original projected STRING schema required')
    # Command-generated metadata is normally empty. Unusual metadata continues
    # through the existing complete schema/aggregate guard, never a weaker proof.
    if any(field.metadata for field in fields):return None
    schema_budget=64;measurements=[]
    for name,value in zip(LEGACY_NAMES,expected):
        if type(value)is not str or not value:
            raise RegisteredDetailProofError('Bounded original nonnull identity strings required')
        if len(value)>bounds.cell_bytes:return None
        try:name_bytes=len(name.encode('utf-8'));value_bytes=len(value.encode('utf-8'))
        except UnicodeError as error:raise RegisteredDetailProofError('Original UTF8 identity strings required')from error
        if name_bytes>bounds.cell_bytes or value_bytes>bounds.cell_bytes:return None
        # Same schema accounting as the legacy four primitive STRING fields:
        # text32+6n, empty metadata32, primitive datatype32, initial64.
        schema_budget+=96+6*name_bytes
        # A single-field JSON struct's names/values can require six bytes per
        # original UTF8 byte. This intentionally overestimates escape expansion.
        measurements.append(9+6*(name_bytes+value_bytes))
    envelope=512+128*9 # original4measurements+marker+3statistics+status
    legacy_total=256+schema_budget+64+6*sum(measurements)+2*envelope
    # Fixed BOOL output is at most2 rows×4 cells; include conservative Python/
    # wire/schema framing, independent of the expected native string lengths.
    wire_envelope=512+4*128+2*4*64
    if (schema_budget>bounds.input_bytes or envelope>bounds.input_bytes
            or max(measurements)>bounds.cell_bytes or legacy_total>bounds.total_bytes
            or wire_envelope>bounds.input_bytes or wire_envelope>bounds.total_bytes):
        return None
    projected=frame.select(*(F.encode(F.col(name),'UTF-8').eqNullSafe(
        F.encode(F.lit(value),'UTF-8')).alias(alias)
        for name,value,alias in zip(LEGACY_NAMES,expected,NAMES))).limit(2)
    proof_fields=projected.schema.fields
    if (type(proof_fields)is not list or len(proof_fields)!=4
            or any(field.name!=name or field.dataType.simpleString()!='boolean'
                   or field.nullable is not False or type(field.metadata)is not dict or field.metadata
                   for field,name in zip(proof_fields,NAMES))):
        raise RegisteredDetailProofError('Exact fixed native Boolean proof schema required')
    rows=projected.collect()
    if type(rows)is not list or len(rows)!=1:
        raise RegisteredDetailProofError('Exact singleton native identity proof required')
    row=rows[0].asDict()
    if type(row)is not dict or set(row)!=set(NAMES)or any(type(row[name])is not bool for name in NAMES):
        raise RegisteredDetailProofError('Exact closed native Boolean proof cells required')
    return tuple(row[name]for name in NAMES)
