# SPDX-License-Identifier: MIT OR Apache-2.0
"""Interval-local reuse of successful pure request semantics, never authority.

Exact original builtin strings form each key. Source, native, lease, policy and
ACK observations remain independent; no value survives its explicit interval.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import sys

_CURRENT = ContextVar('ashlar_request_validation_memo', default=None)
_MAX_ENTRIES = 16
_MAX_BYTES = 8 * 1024 * 1024


class _Memo:
    def __init__(self):
        self.active = True
        self.values = {}
        self.bytes = 0

    def validate(self, request, validator):
        if (not self.active or type(request) is not dict or len(request) not in (7, 8)
                or any(type(k) is not str or type(v) is not str for k, v in request.items())):
            return validator(request)
        items = tuple(sorted(request.items()))
        key = (validator, items)
        if key in self.values:
            return self.values[key]
        result = validator(request)
        if type(result) is not str:
            return result
        # Count retained strings conservatively, including repeated references.
        capacity = (sys.getsizeof(key) + sys.getsizeof(items) + sum(sys.getsizeof(pair) +
            sum(sys.getsizeof(value) for value in pair) for pair in items) +
            sys.getsizeof(result) + 256)
        if len(self.values) < _MAX_ENTRIES and capacity <= _MAX_BYTES - self.bytes:
            self.values[key] = result
            self.bytes += capacity
        return result


def validate_request_semantics(request, validator):
    """Reuse only successful exact-byte correspondence within the current interval."""
    memo = _CURRENT.get()
    return validator(request) if memo is None else memo.validate(request, validator)


@contextmanager
def request_validation_memo():
    """Own one bounded semantic parse interval; clear on success or cancellation."""
    memo = _Memo()
    token = _CURRENT.set(memo)
    try:
        yield
    finally:
        memo.active = False
        _CURRENT.reset(token)
        memo.values.clear()
        memo.bytes = 0
