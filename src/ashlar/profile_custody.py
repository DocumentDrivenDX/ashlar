"""Verify exact native profile-byte custody; never interpret it as registration."""
from dataclasses import dataclass
import hashlib

class ProfileCustodyError(ValueError):
    pass

FIELDS={'identity','version','definition_hex','definition_sha256','bundle_hex','bundle_sha256','original_database_role','original_session_role','original_xid','original_at'}

@dataclass(frozen=True)
class ProfileCustodyReceipt:
    identity: str
    version: str
    definition: bytes
    source_bundle: bytes
    original_database_role: str
    original_session_role: str
    original_xid: str
    original_at: str


def verify_profile_custody(row, *, identity, version, definition, source_bundle):
    if type(row) is not dict or set(row)!=FIELDS:raise ProfileCustodyError('Unknown or missing native custody fields')
    for text in [identity,version,row['original_database_role'],row['original_session_role'],row['original_at']]:
        if type(text) is not str or not text or '\x00' in text:raise ProfileCustodyError('Explicit native scalar text required')
        try:text.encode('utf-8')
        except UnicodeError as exc:raise ProfileCustodyError('Invalid scalar text') from exc
    if row['identity']!=identity or row['version']!=version:raise ProfileCustodyError('Different original profile identity')
    for raw,limit,encoded,digest in [(definition,1048576,row['definition_hex'],row['definition_sha256']),
                                    (source_bundle,4194304,row['bundle_hex'],row['bundle_sha256'])]:
        if type(raw) is not bytes or not 1<=len(raw)<=limit or type(encoded) is not str or encoded!=raw.hex() or digest!=hashlib.sha256(raw).hexdigest():
            raise ProfileCustodyError('Original profile/source byte custody mismatch')
    xid=row['original_xid']
    if type(xid) is not str or not 1<=len(xid)<=20 or not xid.isascii() or not xid.isdecimal() or str(int(xid))!=xid or not 0<int(xid)<2**64:
        raise ProfileCustodyError('Exact native xid8 text required')
    return ProfileCustodyReceipt(identity,version,definition,source_bundle,row['original_database_role'],row['original_session_role'],xid,row['original_at'])
