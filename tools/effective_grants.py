"""Bounded original Unity Catalog effective-permission API observations.

Retains raw pages, including unknown content; unknown meaning refuses admission.
This reads permissions, never changes them or infers owner/actor authority.
"""
import json
import re
from ashlar.authority import AuthorityError,READ_OR_CREATE,WRITE_OR_DELEGATE


def effective_grants(api,kind,name,*,record_page):
    kind=kind.lower()
    if kind not in ('catalog','schema','table') or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}',name):
        raise AuthorityError('Explicit supported permission scope required')
    if len(name.split('.'))!={'catalog':1,'schema':2,'table':3}[kind]:raise AuthorityError('Permission scope arity differs')
    pages=[];rows=[];tokens=set();principals=set();token=None;size=0
    actions={action.replace(' ','_'):action for action in READ_OR_CREATE|WRITE_OR_DELEGATE}
    def exact(value,required,optional=()):
        if not isinstance(value,dict) or not set(required)<=set(value) or set(value)-set(required)-set(optional):
            raise AuthorityError('Unknown or incomplete effective-permission content; original page retained by caller')
    for ordinal in range(16):
        query={'max_results':0}
        if token is not None:query['page_token']=token
        raw=api.do('GET','/api/2.1/unity-catalog/effective-permissions/'+kind+'/'+name,query=query,headers={'Accept':'application/json'})
        if record_page(raw,ordinal) is not None:raise AuthorityError('Original permission observation persistence incomplete')
        pages.append(raw);size+=len(json.dumps(raw,ensure_ascii=False).encode())
        if size>2*1024*1024:raise AuthorityError('Effective permission observation budget exceeded')
        exact(raw,[],['privilege_assignments','next_page_token'])
        assignments=raw.get('privilege_assignments',[])
        if not isinstance(assignments,list):raise AuthorityError('Complete privilege assignment array required')
        for assignment in assignments:
            exact(assignment,['principal','privileges'])
            principal=assignment['principal']
            if not isinstance(principal,str) or not principal or principal in principals:raise AuthorityError('Missing/duplicate original principal assignment')
            principals.add(principal)
            if len(principals)>4096:raise AuthorityError('Permission principal budget exceeded')
            privileges=assignment['privileges']
            if not isinstance(privileges,list) or not privileges:raise AuthorityError('Complete effective privilege list required')
            for privilege in privileges:
                exact(privilege,['privilege'],['inherited_from_name','inherited_from_type'])
                action=privilege['privilege']
                if not isinstance(action,str) or action not in actions:raise AuthorityError('Unqualified native API privilege')
                inherited=('inherited_from_name' in privilege,'inherited_from_type' in privilege)
                if inherited[0]!=inherited[1]:raise AuthorityError('Complete native privilege origin required')
                origin_kind=privilege.get('inherited_from_type',kind);origin=privilege.get('inherited_from_name',name)
                if origin_kind not in ('catalog','schema','table') or not isinstance(origin,str) or not origin:
                    raise AuthorityError('Unsupported original privilege origin')
                arity={'catalog':1,'schema':2,'table':3}[origin_kind]
                if origin!='.'.join(name.split('.')[:arity]) or arity>len(name.split('.')):
                    raise AuthorityError('Privilege origin outside the original securable ancestry')
                rows.append({'Principal':principal,'ActionType':actions[action],'ObjectType':origin_kind.upper(),'ObjectKey':origin})
                if len(rows)>8192:raise AuthorityError('Effective privilege budget exceeded')
        if 'next_page_token' not in raw:return {'pages':pages,'rows':rows,'qualification':'Original paginated effective permissions including inherited origins; independent native owner, current actor and held writer/source admission remain required.'}
        token=raw['next_page_token']
        if not isinstance(token,str) or not token or token in tokens or len(token)>16384:raise AuthorityError('Missing/repeated native permission continuation')
        tokens.add(token)
    raise AuthorityError('Effective permission page budget exceeded; incomplete inventory refused')
