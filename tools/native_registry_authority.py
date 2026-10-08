"""Fresh owner-lane admission for the development raw UMF registry."""
from native_owner_lane import NativeOwnerLane
from native_csv_configuration import installation_namespace


class NativeRegistryAuthority:
    def __init__(self,workspace,installation,root,record_page):
        self.workspace=workspace
        self.namespace=installation_namespace(installation,root)
        self.actor=installation['authenticated_owner']
        self.lane=NativeOwnerLane(workspace,self.actor,record_page)

    def admit(self,table=None):
        catalog=self.namespace.split('.')[0]
        self.lane.admit('catalog',catalog)
        self.lane.admit('schema',self.namespace)
        if table is not None:
            if table!=self.namespace+'.schema_intake':raise PermissionError('Registry outside original installation namespace')
            self.lane.admit('table',table,managed=True)
