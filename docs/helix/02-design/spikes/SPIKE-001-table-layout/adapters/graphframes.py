# Proposed mappings. create_v02_release_graph has scoped local runtime evidence.
def create_graph(spark, node_tables, edge_tables, table_versions):
    """Projection tables and versions must come from one release manifest.

    These are prepared adapter tables with node_key/edge_key and selected scalar
    columns, not direct object_current/edge_current tables. No latest reads.
    """
    from functools import reduce
    from graphframes import GraphFrame
    def pinned(table):
        if table not in table_versions:
            raise ValueError('Projection table missing from release vector')
        version = table_versions[table]
        if type(version) is not int or version < 0:
            raise ValueError('Invalid Delta version')
        parts = table.split('.')
        if len(parts) != 3 or any(not p or not all(c.isalnum() or c == '_' for c in p) for p in parts):
            raise ValueError('Expected three-part table identifier')
        quoted = '.'.join('`' + p + '`' for p in parts)
        return spark.sql(f'SELECT * FROM {quoted} VERSION AS OF {version}')
    if not node_tables or not edge_tables:
        raise ValueError('Explicit node and edge projection tables required')
    vertices=reduce(lambda a,b:a.unionByName(b,allowMissingColumns=True),[
        pinned(t).selectExpr('node_key AS id','type_id','group_value',
            'group_present','props_json','retained_json') for t in node_tables])
    edges=reduce(lambda a,b:a.unionByName(b,allowMissingColumns=True),[
        pinned(t).selectExpr('edge_key AS edge_id','src','dst','rel_type_id',
            'score','score_present','props_json','retained_json') for t in edge_tables])
    if vertices.groupBy('id').count().filter('count>1').limit(1).count():
        raise ValueError('Nonunique vertex identities')
    if edges.groupBy('edge_id').count().filter('count>1').limit(1).count():
        raise ValueError('Nonunique edge identities')
    for endpoint in ('src','dst'):
        if edges.selectExpr(endpoint+' AS id').join(vertices.select('id'),'id','left_anti').limit(1).count():
            raise ValueError('Missing graph endpoint')
    return GraphFrame(vertices,edges)


def create_v02_release_graph(spark, node_tables, edge_tables, table_versions):
    """r68-compatible mapping; locally executed with Spark 3.5.3 / GraphFrames 0.12.3.

    Native Unity Catalog reader-feature interoperability remains unqualified."""
    from functools import reduce
    from graphframes import GraphFrame

    def pinned(table):
        parts = table.split('.')
        if len(parts) != 3 or any(not p or not all(c.isalnum() or c == '_' for c in p) for p in parts):
            raise ValueError('Expected three-part table identifier')
        version = table_versions.get(table)
        if type(version) is not int or version < 0:
            raise ValueError('Projection missing a valid release version')
        quoted = '.'.join('`' + p + '`' for p in parts)
        return spark.sql(f'SELECT * FROM {quoted} VERSION AS OF {version}')

    if not node_tables or not edge_tables:
        raise ValueError('Explicit release table lists required')
    vertices = reduce(lambda a, b: a.unionByName(b), [
        pinned(t).selectExpr('node_key AS id', 'source_system', 'type_id',
                             'native_id', 'logical_key_json', 'props_json', 'retained_json')
        for t in node_tables])
    edges = reduce(lambda a, b: a.unionByName(b), [
        pinned(t).selectExpr('edge_key AS edge_id', 'src', 'dst', 'source_system',
                             'rel_type_id', 'native_id', 'source_type', 'source_id',
                             'target_type', 'target_id', 'props_json', 'retained_json')
        for t in edge_tables])
    for frame, identity in ((vertices, 'id'), (edges, 'edge_id')):
        if frame.filter(f'{identity} IS NULL').limit(1).count():
            raise ValueError('Null graph identity')
        if frame.groupBy(identity).count().filter('count > 1').limit(1).count():
            raise ValueError('Duplicate graph identity')
    for endpoint in ('src', 'dst'):
        if edges.filter(f'{endpoint} IS NULL').limit(1).count():
            raise ValueError('Null endpoint')
        if edges.selectExpr(endpoint + ' AS id').join(vertices.select('id'), 'id', 'left_anti').limit(1).count():
            raise ValueError('Missing graph endpoint')
    return GraphFrame(vertices, edges)
