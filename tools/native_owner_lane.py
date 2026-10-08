"""Fresh complete native owner-lane observations; no remote fencing promise."""
from ashlar.authority import AuthorityError,validate_writer_inventory
from effective_grants import effective_grants


class NativeOwnerLane:
    def __init__(self,workspace,actor,record_page):
        if not isinstance(actor,str) or not actor:raise AuthorityError('Original authenticated owner required')
        self.workspace=workspace;self.actor=actor;self.record_page=record_page

    def admit(self,kind,name,*,managed=False):
        w=self.workspace
        if w.current_user.me().user_name!=self.actor:raise PermissionError('Current actor differs from original owner lane')
        if kind=='catalog':native=w.catalogs.get(name=name)
        elif kind=='schema':native=w.schemas.get(full_name=name)
        elif kind=='table':native=w.tables.get(full_name=name)
        else:raise AuthorityError('Explicit native securable kind required')
        if managed and (kind!='table' or getattr(native.table_type,'value',None)!='MANAGED'):
            raise AuthorityError('Current Unity Catalog managed table required')
        def retain(page,ordinal):return self.record_page(kind,name,page,ordinal)
        observation=effective_grants(w.api_client,kind,name,record_page=retain)
        validate_writer_inventory(native.owner,observation['rows'],trusted_writers=[self.actor])
        return native
