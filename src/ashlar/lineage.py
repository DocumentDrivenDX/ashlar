"""Exact candidate Truss lineage carriers; no source admission or ID authority."""
MAX_BYTES = 1048576
CANONICAL = 'truss-canonical/0.1.0'
TYPE = 'truss-type-lineage/0.1.0'
RELATIONSHIP = 'truss-relationship-lineage-bytes/0.1.0'

class LineageError(ValueError):
    pass


def _string(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise LineageError('Nonempty PostgreSQL-compatible identity required')
    try:
        value.encode('utf-8')
    except UnicodeError as exc:
        raise LineageError('Identity must contain Unicode scalar values') from exc
    return value


def _quote(value):
    # CONTRACT-009 requires long lowercase escapes, never JSON short escapes.
    return '"' + ''.join('\\"' if c == '"' else '\\\\' if c == '\\'
        else '\\u%04x' % ord(c) if ord(c) < 32 else c for c in value) + '"'


def _canonical(value):
    if isinstance(value, str):
        return _quote(value)
    if isinstance(value, list):
        return '[' + ','.join(_canonical(v) for v in value) + ']'
    if isinstance(value, dict):
        return '{' + ','.join(_quote(k) + ':' + _canonical(value[k])
            for k in sorted(value, key=lambda k: k.encode('utf-8'))) + '}'
    raise LineageError('Unsupported lineage tree')


def _triple(value):
    if type(value) is not dict or set(value) != {'document', 'module', 'element'}:
        raise LineageError('Exact qualified identity triple required')
    return {k: _string(v) for k, v in value.items()}


def lineage_bytes(profile, identity):
    """Bounded closed-tree encoding under explicit candidate profiles.

    The host must independently admit original UMF source/ownership and profile
    custody before these bytes can participate in native catalog acceptance.
    """
    if profile == TYPE:
        if type(identity) is not list or len(identity) != 3:
            raise LineageError('Type lineage requires three authored components')
        tree = [_string(v) for v in identity]
    elif profile == RELATIONSHIP:
        if type(identity) is not dict:
            raise LineageError('Tagged relationship lineage required')
        category = identity.get('category')
        if category == 'authored' and set(identity) == {'category', 'relationship'}:
            tree = {'category': category, 'relationship': _triple(identity['relationship'])}
        elif category == 'composition_field' and set(identity) == {'category', 'profile', 'ownerRecord', 'sourceField'} and identity['profile'] == 'truss-composition-field-lineage/0.1.0':
            tree = {'category': category, 'profile': identity['profile'],
                'ownerRecord': _triple(identity['ownerRecord']), 'sourceField': _triple(identity['sourceField'])}
        else:
            raise LineageError('Unknown or incomplete relationship lineage')
    else:
        raise LineageError('Unsupported lineage profile')
    # Bound source components before building the encoded carrier.
    components = tree if isinstance(tree, list) else [v for t in tree.values() if isinstance(t, dict) for v in t.values()]
    if sum(len(v.encode('utf-8')) for v in components) > MAX_BYTES // 6:
        raise LineageError('Lineage resource bound exceeded')
    result = (CANONICAL + '\n' + profile + '\n' + _canonical(tree)).encode('utf-8')
    if len(result) > MAX_BYTES:
        raise LineageError('Lineage resource bound exceeded')
    return result
