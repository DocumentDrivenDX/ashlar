"""Validate caller-supplied generated installation custody, not native authority."""
import re
from uuid import UUID
from generated_carriers import load_generated_carriers,TABLES
from ashlar.publisher import PublicationError


def installation_namespace(installation,root):
    if not isinstance(installation,dict):raise PublicationError('Original installation receipt object required')
    namespace=installation.get('namespace')
    if not isinstance(namespace,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',namespace):
        raise PublicationError('Explicit safe catalog/schema namespace required')
    generated,digest=load_generated_carriers(root)
    if installation.get('state')!='installed' or installation.get('generated_carriers_sha256')!=digest or installation.get('umf_generator')!=generated['generator'] or installation.get('model_inputs')!=generated['inputs']:
        raise PublicationError('Original current UMF-generated installation proof required')
    if not isinstance(installation.get('authenticated_owner'),str) or not installation['authenticated_owner']:
        raise PublicationError('Original authenticated installation owner required')
    tables=installation.get('tables')
    if not isinstance(tables,dict) or set(tables)!={namespace+'.'+role for role in TABLES}:
        raise PublicationError('Complete same-namespace generated carrier inventory required')
    seen=set();types={'string':'STRING','long':'BIGINT','boolean':'BOOLEAN','timestamp':'TIMESTAMP'}
    for role in TABLES:
        entry=tables[namespace+'.'+role]
        if not isinstance(entry,dict):raise PublicationError('Original native carrier identity required')
        uuid=entry.get('uuid')
        try:canonical=str(UUID(uuid))
        except (ValueError,TypeError,AttributeError):raise PublicationError('Canonical native UUID required')
        if uuid!=canonical or uuid in seen:raise PublicationError('Unique original native carrier UUIDs required')
        seen.add(uuid)
        expected=[[field['name'],types[field['deltaType']]] for field in generated['carriers'][role]['columns']]
        if entry.get('columns')!=expected:raise PublicationError('Original generated carrier columns differ')
    # Fresh native actor/owner/grants/UUID/protocol checks remain mandatory in
    # the runner. A receipt alone never authorizes its namespace or UUIDs.
    return namespace
