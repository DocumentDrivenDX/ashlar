"""Bounded positional String capture for a held path-query host.

The caller owns publication custody, native execution and cleanup. Limits cover
retained rows and UTF-8 cell payloads, not Spark partition or executor memory.
An exceeded limit refuses the whole result; it never publishes a prefix.
"""
from dataclasses import dataclass


class PathCaptureError(ValueError):
    def __init__(self, message, *, cleanup_failed=False):
        super().__init__(message)
        self.cleanup_failed = cleanup_failed


@dataclass(frozen=True)
class PathCaptureConfig:
    maximum_rows: int
    maximum_cell_bytes: int
    maximum_total_cell_bytes: int

    def __post_init__(self):
        for value in (self.maximum_rows, self.maximum_cell_bytes,
                      self.maximum_total_cell_bytes):
            if type(value) is not int or value < 1:
                raise PathCaptureError('Explicit positive capture limits required')
        if self.maximum_cell_bytes > self.maximum_total_cell_bytes:
            raise PathCaptureError('Cell limit exceeds total payload limit')


def _capture_string_frame(frame, *, config: PathCaptureConfig):
    """Capture exact columns/positions without changing the admitted statement.

    Duplicate output names remain distinct positions. Native NULL is retained
    for subsequent descriptor-specific admission, never coerced to text.
    """
    if type(config) is not PathCaptureConfig:
        raise PathCaptureError('Explicit capture configuration required')
    fields = frame.schema.fields
    if not fields or any(type(field.name) is not str or not field.name
                         or field.dataType.simpleString().upper() != 'STRING'
                         or type(field.nullable) is not bool for field in fields):
        raise PathCaptureError('Complete native String output schema required')
    schema = [[field.name, 'STRING'] for field in fields]
    iterator = frame.toLocalIterator(prefetchPartitions=False)
    rows = []
    total = 0
    failure = None
    try:
        for row in iterator:
            if len(rows) >= config.maximum_rows:
                raise PathCaptureError('Result row capacity exceeded')
            values = list(row)
            if len(values) != len(fields):
                raise PathCaptureError('Native output arity differs')
            for field, value in zip(fields, values):
                if value is None:
                    if not field.nullable:
                        raise PathCaptureError('Unexpected native NULL')
                    continue
                if type(value) is not str:
                    raise PathCaptureError('Exact native String cell required')
                # Check before encoding a potentially large cell. UTF-8 uses at
                # least one byte per scalar; encoding then enforces exact bytes.
                if len(value) > config.maximum_cell_bytes:
                    raise PathCaptureError('Result cell capacity exceeded')
                try:
                    size = len(value.encode('utf-8'))
                except UnicodeEncodeError:
                    raise PathCaptureError('Exact UTF-8 cell required') from None
                if size > config.maximum_cell_bytes:
                    raise PathCaptureError('Result cell capacity exceeded')
                total += size
                if total > config.maximum_total_cell_bytes:
                    raise PathCaptureError('Result payload capacity exceeded')
            rows.append(values)
    except Exception as error:
        failure = error if isinstance(error, PathCaptureError) else PathCaptureError('Native result capture failed')
        raise failure from None
    except BaseException as error:
        failure = error
        raise
    finally:
        close = getattr(iterator, 'close', None)
        if close is not None:
            try:
                close()
            except Exception:
                if failure is None:
                    raise PathCaptureError('Result iterator cleanup failed', cleanup_failed=True) from None
                if isinstance(failure, PathCaptureError):
                    failure.cleanup_failed = True
    return {'schema': schema, 'native_schema': [
        {'name': field.name, 'native_type': 'STRING', 'nullable': field.nullable}
        for field in fields], 'rows': rows}


def capture_string_frame(frame, *, config: PathCaptureConfig):
    """Safe host boundary, including native schema and iterator acquisition."""
    try:
        return _capture_string_frame(frame, config=config)
    except PathCaptureError:
        raise
    except Exception:
        raise PathCaptureError('Native result capture failed') from None
