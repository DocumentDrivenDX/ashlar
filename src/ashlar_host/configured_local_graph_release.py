"""Complete selected-ordinal graph capture inside the actual configured read hold.

The returned bytes are provisional until the caller closes that hold and its
source/native/runtime owners. This module creates no publication or permission.
"""
from collections import Counter
import json
from ashlar.graph_release import read_graph_release,validate_release_carrier
from .configured_local_read import ConfiguredLocalRead
from .delta_custody import encoded
from .lifecycle import finish


def read_configured_local_graph_release(scope):
    if type(scope)is not ConfiguredLocalRead:
        raise ValueError('Actual configured publication read scope required')
    scope.renew();expected=scope.source_rows()
    roles={scope.tables[r]:r for r in ('object_current','edge_current')}
    bags={r:Counter(map(encoded,expected[r]))for r in roles.values()}
    class Policy:
        def bind_descriptor(self,descriptor,vector,context):
            return scope.bind_descriptor(descriptor,vector,context)
        def authorize_graph_row(self,descriptor,table,row,context):
            self.bind_descriptor(descriptor,scope.vector,context);scope.renew()
            if table not in roles or encoded(dict(row))not in bags[roles[table]]:
                raise ValueError('Original configured graph carrier differs')
        def authorize_graph_result(self,descriptor,nodes,edges,context):
            self.bind_descriptor(descriptor,scope.vector,context);scope.renew()
            for kind,role,rows in (('node','object_current',nodes),('edge','edge_current',edges)):
                original=[validate_release_carrier(dict(row),kind)for row in expected[role]]
                if Counter(encoded(dict(row))for row in rows)!=Counter(map(encoded,original)):
                    raise ValueError('Complete independent configured graph bag differs')
    release=None;primary=None
    try:
        release=read_graph_release(scope.executor,scope.backend,scope.pins,scope.vector,Policy(),
            publication_id=scope.descriptor.publication_id,
            node_table=scope.tables['object_current'],edge_table=scope.tables['edge_current'],
            context=scope.context,supported_profiles=['ashlar-delta/0.3'],
            supported_revisions={k:[v]for k,v in json.loads(scope.descriptor.raw['schema_revisions_json']).items()},
            max_nodes=len(expected['object_current']),max_edges=len(expected['edge_current']))
    except BaseException as error:primary=error
    finish(primary,[scope.renew])
    return release
