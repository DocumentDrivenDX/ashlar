"""Pure required-root String-key decoding; held bag truth is host-owned.

The caller first admits bounded original metadata, source and publication pins.
This module checks carrier/descriptor correspondence only: equal tuples retain
edge multiplicity, but cannot attest native edge order or full-bag truncation.
"""
from dataclasses import dataclass
import json
import re
from typing import Union


class RelatedKeysDecodeError(ValueError):
    """Payload-free rejection of an unadmitted descriptor or carrier."""


@dataclass(frozen=True)
class RelatedKeysDecodeConfig:
    maximum_cell_bytes: int

    def __post_init__(self) -> None:
        if (type(self.maximum_cell_bytes) is not int
                or not 1 <= self.maximum_cell_bytes <= 16 * 1024 * 1024):
            raise RelatedKeysDecodeError('Explicit finite cell byte limit required')


@dataclass(frozen=True)
class DecodedRelatedKeys:
    original: bytes
    items: tuple
    truncated: bool


def _require(condition, message):
    if not condition:
        raise RelatedKeysDecodeError(message)


def _closed(value, keys):
    _require(type(value) is dict and set(value) == set(keys), 'Closed metadata required')


def _text(value, *, name=False):
    _require(type(value) is str and '\0' not in value and (not name or bool(value)),
             'Exact Unicode String required')
    try:
        value.encode('utf8')
    except UnicodeError:
        raise RelatedKeysDecodeError('Unicode scalar String required') from None


def _descriptor(representation, model_pins):
    _require(type(model_pins) in (list, tuple) and bool(model_pins),
             'Explicit admitted model pins required')
    pins = set()
    for pin in model_pins:
        _closed(pin, ('documentId', 'revision', 'sha256', 'umfVersion'))
        for value in pin.values():
            _text(value, name=True)
        _require(re.fullmatch('[0-9a-f]{64}', pin['sha256']) is not None
                 and pin['umfVersion'] in ('0.7.0', '0.8.0'), 'Exact supported model pin required')
        pair = (pin['documentId'], pin['revision'])
        _require(pair not in pins, 'Duplicate model pin')
        pins.add(pair)
    _closed(representation, ('kind', 'relationship', 'key', 'bound'))
    _require(type(representation['kind']) is str and representation['kind'] == 'relatedKeys',
             'Required-root relatedKeys representation required')
    bound = representation['bound']
    _require(type(bound) is int and 1 <= bound <= 1000, 'Exact collection bound required')
    relationship = representation['relationship']
    _closed(relationship, ('documentId', 'revision', 'module', 'relationship'))
    for value in relationship.values():
        _text(value, name=True)
    pair = (relationship['documentId'], relationship['revision'])
    _require(pair in pins, 'Relationship outside admitted pins')
    key = representation['key']
    _closed(key, ('id', 'fields', 'types'))
    _text(key['id'], name=True)
    fields, types = key['fields'], key['types']
    _require(type(fields) is list and type(types) is list and bool(fields)
             and len(fields) == len(types), 'Complete authored key arity required')
    seen = set()
    for field, logical in zip(fields, types):
        _closed(field, ('documentId', 'revision', 'module', 'element'))
        for value in field.values():
            _text(value, name=True)
        _require((field['documentId'], field['revision']) == pair,
                 'Key Field outside original relationship revision')
        identity = tuple(field[k] for k in ('documentId', 'revision', 'module', 'element'))
        _require(identity not in seen, 'Distinct authored key Fields required')
        seen.add(identity)
        _closed(logical, ('family', 'facets', 'nullable'))
        _require(type(logical['family']) is str and logical['family'] == 'string'
                 and type(logical['facets']) is dict and not logical['facets']
                 and logical['nullable'] is False, 'Required exact String key type required')
    # Own primitive metadata before parsing; retain no mutable descriptor aliases.
    return bound, len(fields)


def _number(_):
    raise RelatedKeysDecodeError('Native JSON numeric atoms refused')


def _pairs(entries):
    value = {}
    for key, item in entries:
        _require(key not in value, 'Duplicate JSON member')
        value[key] = item
    return value


def decode_related_keys(representation: dict[str, object], raw: Union[str, bytes], *,
                        model_pins: Union[list[dict[str, str]], tuple[dict[str, str], ...]],
                        config: RelatedKeysDecodeConfig) -> DecodedRelatedKeys:
    """Retain exact bytes and ordered String occurrences, without bag repair.

    Pins/descriptors require prior host admission. This pure API cannot prove
    source validity, edge tie-breaking, complete bags or truncation truth.
    """
    _require(type(config) is RelatedKeysDecodeConfig, 'Explicit frozen decoder configuration required')
    bound, arity = _descriptor(representation, model_pins)
    if type(raw) is str:
        size = 0
        for char in raw:
            code = ord(char)
            _require(not 0xD800 <= code <= 0xDFFF, 'Invalid UTF8 carrier')
            size += 1 if code < 128 else 2 if code < 2048 else 3 if code < 65536 else 4
            _require(size <= config.maximum_cell_bytes, 'Carrier byte limit exceeded')
        original = raw.encode('utf8')
    else:
        _require(type(raw) is bytes and len(raw) <= config.maximum_cell_bytes,
                 'Bounded original UTF8 bytes required')
        original = raw
    try:
        value = json.loads(original.decode('utf8'), object_pairs_hook=_pairs,
                           parse_int=_number, parse_float=_number, parse_constant=_number)
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise RelatedKeysDecodeError('Invalid bounded UTF8 JSON carrier') from None
    _closed(value, ('items', 'truncated'))
    items, truncated = value['items'], value['truncated']
    _require(type(items) is list and len(items) <= bound and type(truncated) is bool,
             'Closed bounded collection required')
    _require(not truncated or len(items) == bound, 'True marker requires a full prefix')
    result, previous = [], None
    for item in items:
        _require(type(item) is list and len(item) == arity, 'Complete String key tuple required')
        for atom in item:
            _text(atom)
        ordered = tuple(item)
        _require(previous is None or previous <= ordered, 'Exact String tuple order required')
        previous = ordered
        result.append(ordered)
    return DecodedRelatedKeys(original, tuple(result), truncated)
