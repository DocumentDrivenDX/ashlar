"""Explicit native SQL-reader protocol admission; separate from pins/files/schema.

Advertised tableFeatures include writer features; this conservative boundary
requires explicit recognition of all advertised features rather than guessing
which unknown ones are irrelevant. The host must qualify the selected SQL reader.
"""
from dataclasses import dataclass
from .native import _quoted
from .schema import _json

class ProtocolError(ValueError):
    pass

@dataclass(frozen=True)
class ReaderProtocolProfile:
    identity: str
    reader_versions: object
    writer_versions: object
    features: object

    def __post_init__(self):
        if not isinstance(self.identity,str) or not self.identity or len(self.identity)>1024:
            raise ProtocolError('Explicit reader profile identity required')
        for key in ('reader_versions','writer_versions'):
            values=getattr(self,key)
            if not isinstance(values,(tuple,list,set,frozenset)) or not 1<=len(values)<=16 or any(type(v) is not int or not 0<v<2**31 for v in values):
                raise ProtocolError('Explicit bounded protocol versions required')
            object.__setattr__(self,key,frozenset(values))
        if not isinstance(self.features,(tuple,list,set,frozenset)) or len(self.features)>64 or any(not isinstance(v,str) or not v or len(v)>128 for v in self.features):
            raise ProtocolError('Explicit bounded feature inventory required')
        object.__setattr__(self,'features',frozenset(self.features))


def validate_protocol_detail(detail, *, uuid, profile):
    if not isinstance(profile,ReaderProtocolProfile) or not isinstance(detail,dict) or detail.get('id')!=uuid or not isinstance(uuid,str) or not uuid:
        raise ProtocolError('Exact target identity and explicit reader profile required')
    if detail.get('format')!='delta':raise ProtocolError('Unsupported native format')
    versions=[]
    for key,allowed in [('minReaderVersion',profile.reader_versions),('minWriterVersion',profile.writer_versions)]:
        token=detail.get(key)
        if not isinstance(token,str) or not token.isascii() or not token.isdecimal() or len(token)>10 or str(int(token))!=token or int(token) not in allowed:
            raise ProtocolError('Unrecognized native protocol version')
        versions.append(int(token))
    text=detail.get('tableFeatures')
    if not isinstance(text,str) or len(text)>16384:raise ProtocolError('Complete native feature inventory required')
    features=_json(text.encode('utf-8'))
    if not isinstance(features,list) or len(features)>64 or any(not isinstance(v,str) or not v or len(v)>128 for v in features) or len(set(features))!=len(features):
        raise ProtocolError('Malformed or duplicate native features')
    if not set(features)<=profile.features:raise ProtocolError('Unrecognized native table feature')
    return {'uuid':uuid,'profile':profile.identity,'reader_version':versions[0],
            'writer_version':versions[1],'features':sorted(features)}


def inspect_protocol(executor,table,uuid,*,profile):
    """Fresh native table-level protocol observation; no pinned file proof."""
    rows=executor.query('DESCRIBE DETAIL '+_quoted(table),{}).rows
    if len(rows)!=1:raise ProtocolError('Ambiguous native protocol observation')
    return validate_protocol_detail(dict(rows[0]),uuid=uuid,profile=profile)
