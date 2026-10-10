"""Full original VARCHAR projection through a supplied ordinary Gremlin port.

The existing adapter and release owner retain native schema/engine/carrier
custody and compare every original cell and incident endpoint. This callable
owns query spelling and strict GraphSON string decoding only, not authority.
"""
import re
from .puppy_release import PuppyReleaseError
from ashlar.graph_release import release_columns as columns
from .puppy_native import encoded


def native_string(value, *, nullable):
    if value is None and nullable:
        return None
    if (type(value) is dict and all(type(key) is str for key in value)
            and set(value) == {'@type', '@value'}):
        if type(value['@type']) is not str or value['@type'] != 'g:String' or type(value['@value']) is not str:
            raise PuppyReleaseError('Exact GraphSON string carrier required')
        value = value['@value']
    if type(value) is not str:
        raise PuppyReleaseError('Native string carrier required; scalar promotion refused')
    if len(value) > 16 * 1024 * 1024:
        raise PuppyReleaseError('Bounded native string required')
    try: raw = value.encode('utf-8')
    except UnicodeError as error:
        raise PuppyReleaseError('Unicode scalar string required') from error
    if len(raw) > 16 * 1024 * 1024:
        raise PuppyReleaseError('Bounded native string bytes required')
    return value


class GremlinReleaseProjection:
    def __init__(self, submit):
        if not callable(submit):
            raise PuppyReleaseError('Explicit ordinary Gremlin transport required')
        self.submit = submit

    def __call__(self, kind, label, context, retain):
        if (type(kind) is not str or kind not in ('node', 'edge')
                or type(label) is not str
                or re.fullmatch('Ashlar[0-9a-f]{64}' + ('Node' if kind == 'node' else 'Edge'), label) is None):
            raise PuppyReleaseError('Exact immutable release label required')
        names = list(columns(kind))
        identities = ['native_id'] + (['native_source', 'native_target'] if kind == 'edge' else [])
        aliases = ['carrier_id' if name == 'id' else 'carrier_key' if name == 'graph_id' else name for name in names]
        query = "g." + ('V' if kind == 'node' else 'E') + "().hasLabel('" + label + "').project(" + ','.join(repr(name) for name in names + identities) + ')'
        query += ''.join('.by(__.coalesce(__.values(' + repr(name) + '),__.constant(null)))' for name in aliases)
        query += '.by(__.id())'
        if kind == 'edge': query += '.by(__.outV().id()).by(__.inV().id())'
        native = self.submit(query, context)
        if type(native) is not list or len(native) > 10000:
            raise PuppyReleaseError('Bounded complete native Gremlin rows required')
        # Decode-specific transformations cannot replace original observations.
        # Keep the ordinary transport's decoded GraphSON rows before refusing
        # unsupported typed carriers or comparing normalized original cells.
        retain('gremlin-native-query.json', encoded({'query': query, 'rows': native}))
        rows = []
        for row in native:
            if (type(row) is not dict or any(type(key) is not str for key in row)
                    or set(row) != set(names + identities)):
                raise PuppyReleaseError('Exact complete native Gremlin columns required')
            rows.append({name: native_string(row[name], nullable=name not in identities)
                         for name in names + identities})
        return query, rows
