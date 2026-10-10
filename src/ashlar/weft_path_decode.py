"""Pure selected path carrier decoding; publication/source truth is caller-owned."""
from dataclasses import dataclass
from copy import deepcopy
import json
import re
from .weft_decode import decode_exact_scalar


class PathDecodeError(ValueError):
    """Payload-free rejection of an unadmitted carrier or descriptor."""


@dataclass(frozen=True)
class PathDecodeConfig:
    maximum_cell_bytes: int

    def __post_init__(self):
        if type(self.maximum_cell_bytes) is not int or not 1 <= self.maximum_cell_bytes <= 16 * 1024 * 1024:
            raise PathDecodeError('Positive explicit cell byte limit required')


@dataclass(frozen=True)
class PathItem:
    intermediate: tuple
    terminal: tuple
    edges: tuple


@dataclass(frozen=True)
class DecodedPaths:
    original: bytes
    state: str
    items: tuple
    truncated: object


def _require(condition, message):
    if not condition:
        raise PathDecodeError(message)


def _closed(value, keys):
    _require(type(value) is dict and set(value) == set(keys), 'Closed descriptor required')


def _name(value):
    _require(type(value) is str and bool(value) and '\0' not in value, 'Exact identity name required')
    try:
        value.encode('utf8')
    except UnicodeError:
        raise PathDecodeError('Unicode identity required') from None


def _identity(value, pins, relationship=False):
    _closed(value, ('documentId', 'revision', 'module', 'relationship' if relationship else 'element'))
    for item in value.values():
        _name(item)
    _require((value['documentId'], value['revision']) in pins, 'Identity outside admitted pin context')


def _scalar(logical, value):
    decoder = {'string': 'text', 'boolean': 'boolean', 'integer': 'exact-integer', 'decimal': 'exact-decimal'}
    _require(type(logical) is dict and logical.get('family') in decoder, 'Unsupported key family')
    try:
        return decode_exact_scalar({'kind': 'scalar', 'logicalType': logical,
                                    'carrier': 'text', 'decoder': decoder[logical['family']]}, value).value
    except ValueError:
        raise PathDecodeError('Key outside exact declared decoder subset') from None


def _key(value, record, pins):
    _closed(value, ('id', 'fields', 'types'))
    _name(value['id'])
    fields, types = value['fields'], value['types']
    _require(type(fields) is list and type(types) is list and len(fields) == len(types) and bool(fields), 'Exact key arity required')
    seen_fields = set()
    for field, logical in zip(fields, types):
        _identity(field, pins)
        component = tuple(field[k] for k in ('documentId', 'revision', 'module', 'element'))
        _require(component not in seen_fields, 'Distinct authored key Fields required')
        seen_fields.add(component)
        _require((field['documentId'], field['revision']) == (record['documentId'], record['revision']), 'Key source revision differs')
        _closed(logical, ('family', 'facets', 'nullable'))
        _require(logical['nullable'] is False and type(logical['facets']) is dict, 'Non-null exact key type required')
        family, facets = logical['family'], logical['facets']
        if family in ('string', 'boolean'):
            _require(not facets, 'Unknown key facets')
        elif family == 'integer':
            _closed(facets, ('integerWidth',))
            width = facets['integerWidth']
            _closed(width, ('bits', 'signed'))
            _require(type(width['bits']) is int and 1 <= width['bits'] <= 64 and type(width['signed']) is bool, 'Unsupported integer width')
        elif family == 'decimal':
            _closed(facets, ('precision', 'scale'))
            _require(type(facets['precision']) is int and type(facets['scale']) is int and 1 <= facets['precision'] <= 28 and 0 <= facets['scale'] <= facets['precision'], 'Unsupported decimal facets')
        else:
            raise PathDecodeError('Unsupported key family')
    return types


def _span(value):
    _closed(value, ('start', 'end'))
    _require(type(value['start']) is int and type(value['end']) is int and 0 <= value['start'] <= value['end'], 'Ordered original span required')


def _descriptor(value, model_pins):
    _require(type(model_pins) in (list, tuple) and bool(model_pins), 'Explicit admitted model pin context required')
    pins = set()
    for pin in model_pins:
        _closed(pin, ('documentId', 'revision', 'sha256', 'umfVersion'))
        for item in pin.values():
            _name(item)
        _require(re.fullmatch('[0-9a-f]{64}', pin['sha256']) is not None, 'Exact source digest required')
        _require(pin['umfVersion'] in ('0.7.0', '0.8.0'), 'Unsupported model wire version')
        pair = (pin['documentId'], pin['revision'])
        _require(pair not in pins, 'Duplicate model pin')
        pins.add(pair)
    _require(type(value) is dict and set(value) in ({'kind', 'path', 'startRecord', 'bound', 'edgeEncoding'}, {'kind', 'path', 'startRecord', 'bound', 'edgeEncoding', 'outerJoin'}), 'Closed relatedPaths representation required')
    _require(value['kind'] == 'relatedPaths' and value['edgeEncoding'] == 'signed64-decimal/0.1', 'Exact path carrier profile required')
    _require(type(value['bound']) is int and 1 <= value['bound'] <= 1000, 'Path bound required')
    path = value['path']
    _closed(path, ('startScan', 'hops', 'span', 'hopSpans'))
    _name(path['startScan'])
    _span(path['span'])
    _require(type(path['hops']) is list and len(path['hops']) == 2 and type(path['hopSpans']) is list and len(path['hopSpans']) == 2, 'Exactly two authored hops required')
    previous = path['span']['start']
    for span in path['hopSpans']:
        _span(span)
        _require(previous <= span['start'] <= span['end'] <= path['span']['end'], 'Original hop spans out of order')
        previous = span['end']
    target_types = []
    for hop in path['hops']:
        _closed(hop, ('identity', 'inverse', 'from', 'to', 'sourceKey', 'targetKey', 'sourceMultiplicity', 'targetMultiplicity', 'targetLifecycle'))
        _identity(hop['identity'], pins, True)
        _identity(hop['from'], pins)
        _identity(hop['to'], pins)
        revision = (hop['identity']['documentId'], hop['identity']['revision'])
        _require(all((hop[role]['documentId'], hop[role]['revision']) == revision for role in ('from', 'to')), 'Relationship endpoint source revision differs')
        _require(type(hop['inverse']) is bool and hop['targetLifecycle'] in ('owned', 'independent'), 'Exact authored relationship metadata required')
        for multiplicity in (hop['sourceMultiplicity'], hop['targetMultiplicity']):
            _closed(multiplicity, ('min', 'max'))
            lo, hi = multiplicity['min'], multiplicity['max']
            _require(type(lo) is int and lo >= 0 and (hi == '*' or type(hi) is int and hi >= lo), 'Exact multiplicity required')
        _key(hop['sourceKey'], hop['from'], pins)
        target_types.append(_key(hop['targetKey'], hop['to'], pins))
    _identity(value['startRecord'], pins)
    _require(value['startRecord'] == path['hops'][0]['from'] and path['hops'][0]['to'] == path['hops'][1]['from'], 'Complete Record path continuity required')
    if 'outerJoin' in value:
        _closed(value['outerJoin'], ('scan', 'record'))
        _require(value['outerJoin'] == {'scan': path['startScan'], 'record': value['startRecord']}, 'Exact LEFT root required')
    return target_types


def _number_atom(_):
    raise PathDecodeError('Native JSON numeric atoms refused')


def _pairs(entries):
    result = {}
    for key, value in entries:
        _require(key not in result, 'Duplicate JSON member')
        result[key] = value
    return result


def decode_related_paths(representation: dict, raw, *, model_pins, config: PathDecodeConfig) -> DecodedPaths:
    """Validate selected carrier structure/order, never full-bag/truncation truth.

    Pins and representation must first be admitted by the publication-bound host.
    Numeric key families retain the existing exact scalar decoder's finite subset.
    Duplicate path occurrences and every lexical atom remain unchanged.
    """
    _require(type(config) is PathDecodeConfig, 'Explicit frozen decoder configuration required')
    # Caller admits bounded compiler metadata; own its snapshot before decoding.
    representation, model_pins = deepcopy(representation), deepcopy(model_pins)
    types = _descriptor(representation, model_pins)
    if type(raw) is str:
        size = 0
        for char in raw:
            n = ord(char)
            _require(not 0xD800 <= n <= 0xDFFF, 'Invalid UTF8 carrier')
            size += 1 if n < 128 else 2 if n < 2048 else 3 if n < 65536 else 4
            _require(size <= config.maximum_cell_bytes, 'Carrier byte limit exceeded')
        original = raw.encode('utf8')
    else:
        _require(type(raw) is bytes and len(raw) <= config.maximum_cell_bytes, 'Bounded UTF8 bytes required')
        original = raw
    try:
        value = json.loads(original.decode('utf8'), object_pairs_hook=_pairs,
                           parse_int=_number_atom, parse_float=_number_atom, parse_constant=_number_atom)
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise PathDecodeError('Invalid bounded UTF8 JSON carrier') from None
    if 'outerJoin' in representation:
        if value == {'state': 'absent'}:
            return DecodedPaths(original, 'absent', (), None)
        _closed(value, ('state', 'value'))
        _require(value['state'] == 'value', 'Exact LEFT presence required')
        value = value['value']
    _closed(value, ('items', 'truncated'))
    _require(type(value['items']) is list and len(value['items']) <= representation['bound'] and type(value['truncated']) is bool, 'Bounded collection required')
    _require(not value['truncated'] or len(value['items']) == representation['bound'], 'Truncated collection must fill bound')
    items, previous = [], None
    for item in value['items']:
        _closed(item, ('intermediate', 'terminal', 'edges'))
        keys = []
        for role, logicals in zip(('intermediate', 'terminal'), types):
            atoms = item[role]
            _require(type(atoms) is list and len(atoms) == len(logicals) and all(type(a) is str for a in atoms), 'Exact String key tuple required')
            keys.append(tuple(_scalar(t, a) for t, a in zip(logicals, atoms)))
        edges = item['edges']
        _require(type(edges) is list and len(edges) == 2, 'Two original edge tokens required')
        numbers = []
        for edge in edges:
            _require(type(edge) is str and len(edge) <= 20 and re.fullmatch(r'0|-?[1-9][0-9]*', edge) is not None, 'Canonical signed64 edge token required')
            number = int(edge)
            _require(-(2**63) <= number < 2**63, 'Edge token outside signed64')
            numbers.append(number)
        order = (keys[0], keys[1], *numbers)
        _require(previous is None or previous <= order, 'Typed path order required')
        previous = order
        items.append(PathItem(tuple(item['intermediate']), tuple(item['terminal']), tuple(edges)))
    return DecodedPaths(original, 'value', tuple(items), value['truncated'])
