# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact portable contributor bags; neither decoding nor comparison grants authority."""
from __future__ import annotations
from collections import Counter
from dataclasses import dataclass
import json

VERSION = 'weft.stage-contributor-bag/0.1-candidate'


@dataclass(frozen=True)
class CurrentContributors:
    original_bytes: bytes
    expected_scans: tuple[str, ...]
    tuples: tuple[tuple[tuple[str, str], ...], ...]


def _text(value, maximum=None):
    if type(value) is not str or not value:
        raise ValueError('Nonempty exact contributor string required')
    try: size = len(value.encode('utf-8', errors='strict'))
    except UnicodeError as error: raise ValueError('Valid Unicode contributor required') from error
    if maximum is not None and size > maximum:
        raise ValueError('Contributor key UTF-8 capacity exceeded')
    return value


def decode_current_contributors(original_bytes, *, expected_scans,
                                maximum_bytes=1_048_576, maximum_tuples=10_000):
    """Decode original UTF-8 JSON without sorting, deduplication or identity inference."""
    if type(maximum_bytes) is not int or not 1 <= maximum_bytes <= 16_777_216:
        raise ValueError('Exact positive contributor byte limit required')
    if type(maximum_tuples) is not int or not 1 <= maximum_tuples <= 1_000_000:
        raise ValueError('Exact positive contributor tuple limit required')
    if type(expected_scans) is not tuple or not 1 <= len(expected_scans) <= 64:
        raise ValueError('Explicit bounded scan tuple required')
    for scan in expected_scans: _text(scan)
    if len(set(expected_scans)) != len(expected_scans):
        raise ValueError('Distinct expected scan identities required')
    if type(original_bytes) is not bytes or len(original_bytes) > maximum_bytes:
        raise ValueError('Bounded original contributor bytes required')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate contributor JSON member')
            result[key] = value
        return result
    def reject(_): raise ValueError('Contributor JSON numeric atoms refused')
    try:
        value = json.loads(original_bytes.decode('utf-8', errors='strict'),
            object_pairs_hook=pairs, parse_int=reject, parse_float=reject, parse_constant=reject)
    except (UnicodeError, RecursionError) as error:
        raise ValueError('Valid bounded contributor JSON required') from error
    if type(value) is not dict or set(value) != {'version', 'tuples'} or value['version'] != VERSION:
        raise ValueError('Exact contributor bag profile required')
    rows = value['tuples']
    if type(rows) is not list or len(rows) > maximum_tuples:
        raise ValueError('Bounded complete contributor tuple bag required')
    decoded = []
    for row in rows:
        if type(row) is not list or len(row) != len(expected_scans):
            raise ValueError('Exact contributor tuple arity required')
        cells = []
        for scan, cell in zip(expected_scans, row):
            if (type(cell) is not dict or set(cell) != {'scan', 'key'}
                    or type(cell['scan']) is not str or cell['scan'] != scan):
                raise ValueError('Original ordered scan identity required')
            cells.append((scan, _text(cell['key'], 256)))
        decoded.append(tuple(cells))
    return CurrentContributors(original_bytes, expected_scans, tuple(decoded))


def require_matching_contributors(actual, independent_expected):
    """Compare independently supplied complete bags, preserving occurrence multiplicity.

    Callers own how the expected source bag was established; this pure equality
    check supplies no publication, authorization or native execution evidence.
    """
    if type(actual) is not CurrentContributors or type(independent_expected) is not CurrentContributors:
        raise TypeError('Decoded actual and independent expected contributors required')
    if (actual.expected_scans != independent_expected.expected_scans
            or Counter(actual.tuples) != Counter(independent_expected.tuples)):
        raise ValueError('Complete contributor occurrence bags differ')
