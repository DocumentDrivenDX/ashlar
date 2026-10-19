# SPDX-License-Identifier: MIT OR Apache-2.0
"""Local minimal Fabric mapping for explicitly selected STRING release tables."""
import base64,json,re
from pathlib import Path
from ashlar.graph_release import release_columns

_SCHEMA='https://developer.microsoft.com/json-schemas/fabric/item/graphIndex/definition/'
_UUID=r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'

def _table_path(value):
    if (type(value)is not str or len(value)>4096 or not re.fullmatch(
            r'abfss://'+_UUID+r'@onelake\.dfs\.fabric\.microsoft\.com/'+_UUID+r'/Tables/[A-Za-z0-9][A-Za-z0-9._/-]*',value)):
        raise ValueError('Explicit GUID-form OneLake Delta table path required')
    segments=value.split('/')[5:]
    if any(not part or part in ('.','..')or part.lower()in ('latest','current')for part in segments):
        raise ValueError('Select a release-specific table path, not a mutable alias')
    return value


def make_fabric_graph_definition(nodes,edges):
    """Return four fresh mapping documents; no materialization or engine admission."""
    nodes=_table_path(nodes);edges=_table_path(edges)
    if nodes==edges:raise ValueError('Distinct node and edge release tables required')
    node_columns=release_columns('node');edge_columns=release_columns('edge')
    def properties(columns):return [{'name':name,'type':'STRING'}for name in columns]
    def mappings(columns):return [{'propertyName':name,'sourceColumn':name}for name in columns]
    def schema(name,version='1.0.0'):return _SCHEMA+name+'/'+version+'/schema.json'
    return {
        'dataSources.json':{'$schema':schema('dataSources','1.0.0'),'dataSources':[
            {'name':'ashlar_nodes','type':'DeltaTable','properties':{'path':nodes}},
            {'name':'ashlar_edges','type':'DeltaTable','properties':{'path':edges}}]},
        'graphDefinition.json':{'$schema':schema('graphDefinition'),
            'nodeTables':[{'id':'ashlar_node_table','nodeTypeAlias':'ashlar_node','dataSourceName':'ashlar_nodes','propertyMappings':mappings(node_columns)}],
            'edgeTables':[{'id':'ashlar_edge_table','edgeTypeAlias':'ashlar_edge','dataSourceName':'ashlar_edges','sourceNodeKeyColumns':['src'],'destinationNodeKeyColumns':['dst'],'propertyMappings':mappings(edge_columns)}]},
        'graphType.json':{'$schema':schema('graphType'),
            'nodeTypes':[{'alias':'ashlar_node','labels':['AshlarNode'],'primaryKeyProperties':['graph_id'],'properties':properties(node_columns)}],
            'edgeTypes':[{'alias':'ashlar_edge','labels':['AshlarEdge'],'sourceNodeType':{'alias':'ashlar_node'},'destinationNodeType':{'alias':'ashlar_node'},'properties':properties(edge_columns)}]},
        'stylingConfiguration.json':{'$schema':schema('stylingConfiguration'),
            'modelLayout':{'positions':{'ashlar_node':{'x':0,'y':0}},'styles':{'ashlar_node':{'size':30},'ashlar_edge':{'size':30}},'pan':{'x':0,'y':0},'zoomLevel':1}}}


def prepare_fabric_graph_definition(nodes,edges,output,*,display_name=None):
    """Write readable JSON parts and a REST definition to a fresh local directory."""
    if display_name is not None and (type(display_name)is not str or not display_name.strip() or len(display_name)>256 or any(ord(char)<32 or ord(char)==127 for char in display_name)):
        raise ValueError('Display name must be nonblank, at most 256 characters, without control characters')
    documents=make_fabric_graph_definition(nodes,edges)
    output=Path(output)
    if not output.is_absolute()or output.parent.resolve()!=output.parent:raise ValueError('Canonical absolute local output directory required')
    payloads={name:(json.dumps(value,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode()for name,value in documents.items()}
    definition={'format':'json','parts':[{'path':name,'payload':base64.b64encode(raw).decode('ascii'),'payloadType':'InlineBase64'}for name,raw in payloads.items()]}
    payloads['definition.json']=(json.dumps(definition,indent=2)+'\n').encode()
    if display_name is not None:
        payloads['create-request.json']=(json.dumps({'displayName':display_name,'definition':definition},indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode()
    output.mkdir(mode=0o700)
    for name,raw in payloads.items():
        with (output/name).open('xb')as stream:
            if stream.write(raw)!=len(raw):raise OSError('Incomplete local definition write')
    return {'output':str(output),'files':list(payloads),'node_properties':len(release_columns('node')),'edge_properties':len(release_columns('edge')),'scope':'Local mapping only; no upload, release immutability, loaded graph or engine authority established.'}


def fabric_carrier_queries():
    """Complete carrier and actual relationship-endpoint projections; no parser."""
    cases = []
    for kind, role, pattern in [('node', 'nodes', '(n:AshlarNode)'),
                                ('edge', 'edges', '()-[n:AshlarEdge]->()')]:
        names = release_columns(kind)
        cases.append((role, 'MATCH ' + pattern + ' RETURN ' + ', '.join(
            'n.' + name + ' AS ' + name for name in names), names))
    cases.append(('endpoints', 'MATCH (s:AshlarNode)-[e:AshlarEdge]->(d:AshlarNode) RETURN s.graph_id AS source, e.graph_id AS relationship_id, d.graph_id AS destination',
                  ('source', 'relationship_id', 'destination')))
    return tuple(cases)
