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
    trusted=set(trusted_writers)
    if not trusted or any(not isinstance(p,str) or not p for p in trusted):
        raise AuthorityError('Explicit trusted writer identities required')
    if not isinstance(owner,str) or not owner or owner not in trusted:
        raise AuthorityError('Native owner is outside admitted writer authority')
    if not isinstance(grants,(list,tuple)):
        raise AuthorityError('Complete native grant inventory required')
    for grant in grants:
        if not isinstance(grant,dict) or not {'Principal','ActionType','ObjectType','ObjectKey'}.issubset(grant):
            raise AuthorityError('Incomplete native permission record')
        if any(not isinstance(grant[k],str) or not grant[k] for k in ['Principal','ActionType','ObjectType','ObjectKey']):
            raise AuthorityError('Invalid native permission carrier')
        action=grant['ActionType']
        if action not in READ_OR_CREATE|WRITE_OR_DELEGATE:
            raise AuthorityError('Unqualified native privilege: '+action)
        if action in WRITE_OR_DELEGATE and grant['Principal'] not in trusted:
            raise AuthorityError('Unadmitted native writer/delegator: '+grant['Principal'])
