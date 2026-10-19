# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded one-row metadata observation; no source or native authority.

The JVM may inspect two large rows. This bounds transferred driver payload,
not executor heap, and preserves original native values without JSON decoding.
"""
from decimal import Decimal
from .evolution_verification_types import VerificationBounds
from .lifecycle import finish


class SingletonCaptureError(ValueError):
    pass


def capture_native_singleton(frame,names,measurements,*,bounds,base,row_constant):
    """Return provisional original rows and metrics from one closed aggregate."""
    from pyspark.sql import functions as F
    if (type(bounds)is not VerificationBounds or type(names)is not list or not names
            or type(measurements)is not list or len(names)!=len(measurements)
            or type(base)is not int or base<0 or type(row_constant)is not int or row_constant<0):
        raise SingletonCaptureError('Exact bounded singleton capture required')
    bounds.__post_init__()
    aliases=['_ashlar_verification_cell_'+str(i)for i in range(len(names))]
    marker='_ashlar_verification_real'
    stats_names=['_ashlar_verification_count','_ashlar_verification_max','_ashlar_verification_total']
    status='_ashlar_verification_valid';payload='_ashlar_verification_payload'
    reserved=aliases+[marker]+stats_names+[status]
    if set(names)&set(reserved):raise SingletonCaptureError('Reserved native guard alias collision')
    # Preserve the general capture's conservative two-envelope inequality. The
    # single summary plus at most one packed row replaces its row+sentinel wire.
    envelope=512+128*len(reserved)
    if envelope>bounds.input_bytes or envelope>bounds.total_bytes:
        raise SingletonCaptureError('Bounded native metric envelope required')
    quote=lambda name:F.col('`'+name.replace('`','``')+'`')
    schema=frame.schema.json()
    real=frame.limit(2).select(*[quote(name)for name in names],
        *[value.alias(alias)for value,alias in zip(measurements,aliases)])
    lengths=[quote(alias)for alias in aliases];total=lengths[0]
    for value in lengths[1:]:total=total+value
    summary=real.agg(
        F.count(F.lit(1)).cast('decimal(38,0)').alias(stats_names[0]),
        F.max(F.greatest(*lengths)if len(lengths)>1 else lengths[0]).cast('decimal(38,0)').alias(stats_names[1]),
        F.sum(total).cast('decimal(38,0)').alias(stats_names[2]),
        F.collect_list(F.struct(*[quote(name)for name in names+aliases])).alias(payload))
    summary=summary.select(quote(stats_names[0]),
        *[F.when(quote(stats_names[0])==F.lit(0),F.lit(0).cast('decimal(38,0)'))
            .otherwise(quote(name)).alias(name)for name in stats_names[1:]],quote(payload))
    count,cell,size=[quote(name)for name in stats_names]
    valid=(count.isNotNull()&cell.isNotNull()&size.isNotNull()
        &(count>=F.lit(0))&(count<=F.lit(1))
        &(cell>=F.lit(0))&(cell<=F.lit(bounds.cell_bytes))&(size>=F.lit(0))
        &(F.lit(base)+count*F.lit(row_constant)+size*F.lit(6)+(count+F.lit(1))*F.lit(envelope)<=F.lit(bounds.total_bytes)))
    admitted=summary.select(*[quote(name)for name in stats_names],
        F.coalesce(valid,F.lit(False)).alias(status),
        F.when(F.coalesce(valid,F.lit(False)),quote(payload)).otherwise(F.lit(None)).alias(payload))
    iterator=None;primary=None;rows=[];observed=None;seen=False
    def integer(value):
        if (type(value)not in (int,Decimal)or type(value)is Decimal and not value.is_finite()
                or value<0 or int(value)!=value):
            raise SingletonCaptureError('Exact finite native guard statistic required')
        return int(value)
    try:
        iterator=admitted.toLocalIterator()
        if not callable(getattr(iterator,'close',None)):
            raise SingletonCaptureError('Closable native guard iterator required')
        for native in iterator:
            if seen:raise SingletonCaptureError('Exactly one singleton native summary required')
            seen=True;row=native.asDict()
            if type(row)is not dict or set(row)!=set(stats_names+[status,payload]):
                raise SingletonCaptureError('Closed singleton native summary required')
            if type(row[status])is not bool:raise SingletonCaptureError('Exact native guard status required')
            observed=tuple(integer(row[name])for name in stats_names)
            count,cell,size=observed
            if (not row[status]or count>1 or cell>bounds.cell_bytes
                    or base+count*row_constant+6*size+(count+1)*envelope>bounds.total_bytes):
                raise SingletonCaptureError('Native bounds refused before payload transfer')
            packed=row[payload]
            if type(packed)is not list or len(packed)!=count:
                raise SingletonCaptureError('Complete singleton packed count required')
            actual_cell=actual_total=0
            for item in packed:
                original=item.asDict()
                if type(original)is not dict or set(original)!=set(names+aliases):
                    raise SingletonCaptureError('Closed original singleton payload required')
                values=[integer(original[name])for name in aliases]
                actual_cell=max(actual_cell,max(values));actual_total+=sum(values)
                rows.append({name:original[name]for name in names})
            if (len(rows),actual_cell,actual_total)!=observed:
                raise SingletonCaptureError('Complete singleton count/byte correspondence required')
        if not seen or frame.schema.json()!=schema:
            raise SingletonCaptureError('Complete stable singleton schema/summary required')
    except BaseException as error:primary=error
    finally:finish(primary,[iterator.close]if iterator is not None and callable(getattr(iterator,'close',None))else [])
    return rows,observed
