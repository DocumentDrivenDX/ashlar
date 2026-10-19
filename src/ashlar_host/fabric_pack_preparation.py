# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original17 local projection preparation; no Fabric/publication authority grant."""
import base64,json,re
from pathlib import Path
from ashlar.graph_release import release_columns
from .fabric_graph_definition import make_fabric_graph_definition
from .fabric_pack_scenarios import gql_plan
from .graphframes_pack_scenarios import INTEGER_FIELDS,prepare_scenarios
from .finite_pack import FinitePackDefinition
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export
from .evolution_cli import InvocationFile
from .lifecycle import owned_context


def scenario_properties(profile):
    """Validate the prepared exact cells and return their ordered native types."""
    if type(profile)is not dict or profile.get('pack')not in INTEGER_FIELDS:
        raise ValueError('Closed original17 scenario projection required')
    properties=[{'name':n,'type':'STRING'}for n in (*release_columns('node'),'original_key','original_type')]
    for field in profile['fields']:
        for key,prefix,kind in [('lexical_column','field_','STRING'),('presence_column','presence_','STRING'),('signed64_column','integer_','INT64')]:
            name=field[key]
            if name is None and key=='signed64_column':continue
            if type(name)is not str or re.fullmatch(prefix+'[0-9]+',name)is None:
                raise ValueError('Closed named scenario column required')
            if kind=='INT64'and(field['record_identity'][2],field['original_field']['name'])not in INTEGER_FIELDS[profile['pack']]:
                raise ValueError('Named admitted signed64 projection required')
            properties.append({'name':name,'type':kind})
    names=[p['name']for p in properties]
    if len(names)!=len(set(names)):raise ValueError('Injective scenario columns required')
    for row in profile['nodes']:
        if set(row)!=set(names):raise ValueError('Complete scenario node columns required')
        for prop in properties:
            value=row[prop['name']]
            if value is None:continue
            if prop['type']=='STRING':valid=type(value)is str
            else:valid=type(value)is int and -(2**63)<=value<2**63
            if not valid:raise ValueError('Exact scenario String/null/signed64 cells required')
    return properties


def scenario_definition(profile,nodes,edges):
    """Map the exact prepared lexical/presence and named INT64 projection."""
    properties=scenario_properties(profile)
    names=[p['name']for p in properties]
    documents=make_fabric_graph_definition(nodes,edges)
    node=documents['graphType.json']['nodeTypes'][0]
    node['labels']=['Scenario'+profile['pack'].title()+'Node']
    # Graph Model JSON uses INT; the exact Delta/readback projection remains INT64.
    node['properties']=[{**p,'type':'INT'if p['type']=='INT64'else p['type']}for p in properties]
    documents['graphDefinition.json']['nodeTables'][0]['propertyMappings']=[{'propertyName':n,'sourceColumn':n}for n in names]
    return documents


def prepare_fabric_pack(*,pack,model,graph,pack_file,selection,nodes,edges,output):
    """Write a fresh source-checked private export projection and GQL inventory.

    File-use custody is closed before output creation. This does not load a graph
    or establish the deployment-owned publication/loaded-release policy.
    """
    definition=FinitePackDefinition(pack)
    if pack not in INTEGER_FIELDS:raise ValueError('Original17 pack required')
    if type(selection)is not LocalGraphExportSelection:raise ValueError('Explicit private export selection required')
    inputs=[InvocationFile.read(Path(p),262144)for p in (model,graph,pack_file)]
    output=Path(output)
    protected=[p.path for p in inputs]+[selection.release_path,selection.custody_path]
    if (not output.is_absolute()or output.parent.resolve()!=output.parent or not output.parent.is_dir()
            or output.exists()or output.is_symlink()
            or any(output==p or output in p.parents or p in output.parents for p in protected)):
        raise ValueError('Fresh separate canonical output required')
    with owned_context(open_local_graph_export(selection))as session:
        with session.writer(session.context):
            session.admit_original_export(session.release,session.custody_bytes,session.source,session.context)
            profile=prepare_scenarios(definition,*(p.raw for p in inputs),json.loads(session.release.payload))
            documents=scenario_definition(profile,nodes,edges)
            queries=[{'case':case['original_scenario']['id'],'original_sql':case['original_scenario']['sql'],
                'gql':gql_plan(profile,case['original_scenario']['id'])[0],
                'columns':gql_plan(profile,case['original_scenario']['id'])[1]}for case in profile['cases']]
            session.admit_original_export(session.release,session.custody_bytes,session.source,session.context)
    for p in inputs:p.renew(262144)
    payloads={n:(json.dumps(v,ensure_ascii=False,indent=2,allow_nan=False)+'\n').encode()for n,v in documents.items()}
    rest={'format':'json','parts':[{'path':n,'payload':base64.b64encode(b).decode(),'payloadType':'InlineBase64'}for n,b in payloads.items()]}
    for n,v in [('definition.json',rest),('scenario-profile.json',profile),('queries.json',queries)]:
        payloads[n]=(json.dumps(v,ensure_ascii=False,indent=2,allow_nan=False)+'\n').encode()
    if any(len(raw)>16777216 for raw in payloads.values()):raise ValueError('Bounded scenario preparation required')
    output.mkdir(mode=0o700)
    for name,raw in payloads.items():
        with (output/name).open('xb')as stream:
            if stream.write(raw)!=len(raw):raise OSError('Complete scenario preparation write required')
    return {'pack':pack,'cases':len(queries),'output':str(output),'files':list(payloads),
        'scope':'Private file-use original projection and GQL preparation only; no materialization, Fabric execution or publication authority.'}
