"""Admission checks for fresh authenticated Unity Catalog permission inventories.

Complete inherited grants and explicit owner are mandatory inputs. Observations
must be renewed under the real source/writer lane; this is not an authority token.
Platform/metastore administrators remain a separately trusted deployment boundary.
"""
class AuthorityError(ValueError):
    pass

READ_OR_CREATE={'SELECT','BROWSE','USE CATALOG','USE SCHEMA','CREATE CATALOG',
                'CREATE SCHEMA','CREATE TABLE','CREATE MATERIALIZED VIEW','CREATE FUNCTION',
                'CREATE VOLUME','READ VOLUME','WRITE VOLUME','EXECUTE','APPLY TAG'}
WRITE_OR_DELEGATE={'MODIFY','ALL PRIVILEGES','MANAGE','OWN','OWNERSHIP'}


def validate_writer_inventory(owner, grants, *, trusted_writers):
    writer_kind=type(trusted_writers)
    if writer_kind is not list and writer_kind is not tuple and writer_kind is not set and writer_kind is not frozenset:
        raise AuthorityError('Explicit trusted writer identities required')
    captured=tuple(trusted_writers)
    if not captured or any(type(p) is not str or not p for p in captured):
        raise AuthorityError('Explicit trusted writer identities required')
    trusted=set(captured)
    if type(owner) is not str or not owner or owner not in trusted:
        raise AuthorityError('Native owner is outside admitted writer authority')
    grant_kind=type(grants)
    if grant_kind is not list and grant_kind is not tuple:
        raise AuthorityError('Complete native grant inventory required')
    for original in tuple(grants):
        if type(original) is not dict:
            raise AuthorityError('Incomplete native permission record')
        grant=original.copy()
        if any(type(k) is not str for k in grant) or not {'Principal','ActionType','ObjectType','ObjectKey'}.issubset(grant):
            raise AuthorityError('Incomplete native permission record')
        if any(type(grant[k]) is not str or not grant[k] for k in ['Principal','ActionType','ObjectType','ObjectKey']):
            raise AuthorityError('Invalid native permission carrier')
        action=grant['ActionType']
        if action not in READ_OR_CREATE|WRITE_OR_DELEGATE:
            raise AuthorityError('Unqualified native privilege: '+action)
        if action in WRITE_OR_DELEGATE and grant['Principal'] not in trusted:
            raise AuthorityError('Unadmitted native writer/delegator: '+grant['Principal'])
