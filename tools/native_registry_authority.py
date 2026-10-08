"""Fresh owner-lane admission for the development raw UMF registry."""
from ashlar.authority import validate_writer_inventory
from ashlar.schema import SchemaIntakeError
from effective_grants import effective_grants
from native_csv_configuration import installation_namespace


class NativeRegistryAuthority:
    def __init__(self,workspace,installation,root,record_page):
        self.workspace=workspace
        self.namespace=installation_namespace(installation,root)
        self.actor=installation['authenticated_owner']
        self.record_page=record_page

    def admit(self,table=None):
        w=self.workspace
        if w.current_user.me().user_name!=self.actor:
            raise PermissionError('Original installation owner differs from current registry actor')
        catalog=self.namespace.split('.')[0]
        resources=[('catalog',catalog,w.catalogs.get(name=catalog).owner),
                   ('schema',self.namespace,w.schemas.get(full_name=self.namespace).owner)]
        if table is not None:
            if table!=self.namespace+'.schema_intake':raise PermissionError('Registry outside original installation namespace')
            native=w.tables.get(full_name=table)
            if getattr(native.table_type,'value',None)!='MANAGED':
                raise SchemaIntakeError('Current Unity Catalog managed registry required')
            resources.append(('table',table,native.owner))
        for kind,name,owner in resources:
            def retain(page,ordinal):return self.record_page(kind,name,page,ordinal)
            observations=effective_grants(w.api_client,kind,name,record_page=retain)
            validate_writer_inventory(owner,observations['rows'],trusted_writers=[self.actor])
