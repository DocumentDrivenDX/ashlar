"""Real held finite-publication reader for canonical immutable graph export."""
import hashlib,json
from collections import Counter
from contextlib import contextmanager
from ashlar.graph_release import read_graph_release
from ashlar.manifest import manifest_pin_vector
from .delta_custody import encoded
from .finite_dataset import FinitePackSource
from .finite_publication import FiniteFileDriver
from .finite_native import finite_pack_query_aliases
from .finite_projection import original_finite_pack_oracle
from .pack_count_star_reader import FinitePackProvider
from .pack_count_star_request import pack_count_star_request
from .lifecycle import owned_context,finish

class FiniteGraphPolicy:
    def __init__(self,driver,provider,vector,expected):
        self.driver,self.provider,self.vector=driver,provider,vector
        self.original_source=driver.source;self.original_transport=driver.transport
        self.manifest=encoded(driver.manifest);self.source=encoded(driver.source.metadata())
        self.roles={driver.tables[role]:role for role in ('object_current','edge_current')}
        self.bags={role:Counter(map(encoded,expected[role]))for role in ('object_current','edge_current')}
    def local(self,context):
        if context is not self.driver.context or not self.provider.active or not self.driver.held or self.driver.source is not self.original_source or self.driver.transport is not self.original_transport:raise PermissionError('Actual held original finite publication interval required')
    def require(self,context):
        self.local(context);self.provider.runtime(context)
        if encoded(self.driver.manifest)!=self.manifest or encoded(self.driver.source.metadata())!=self.source:raise PermissionError('Original held finite source/manifest changed')
    def bind_descriptor(self,descriptor,vector,context):
        self.local(context)
        if vector!=self.vector or encoded(dict(descriptor.raw))!=self.manifest:raise PermissionError('Original complete finite vector differs')
    def authorize_graph_row(self,descriptor,table,row,context):
        self.bind_descriptor(descriptor,self.vector,context);role=self.roles.get(table)
        if role is None or encoded(dict(row))not in self.bags[role]:raise PermissionError('Original full finite row differs')
    def authorize_graph_result(self,descriptor,nodes,edges,context):
        self.bind_descriptor(descriptor,self.vector,context)
        for role,rows in (('object_current',nodes),('edge_current',edges)):
            if Counter(encoded({key:value for key,value in row.items()if key not in ('graph_id','src','dst')})for row in rows)!=self.bags[role]:raise PermissionError('Original finite full occurrence bag differs')

class HeldFinitePins:
    def __init__(self,policy):self.policy=policy
    @contextmanager
    def hold(self,vector,*,context):
        self.policy.require(context)
        if vector!=self.policy.vector:raise PermissionError('Exact already-held finite vector required')
        primary=None
        try:yield
        except BaseException as error:primary=error
        finish(primary,[lambda:self.policy.require(context)])


def read_original_finite_graph_export(driver,pack):
    if type(driver)is not FiniteFileDriver or type(driver.source)is not FinitePackSource:raise ValueError('Actual original finite owner required')
    source=driver.source;facts=source.definition.inputs(source.model,source.graph,pack)
    expected=original_finite_pack_oracle(source.definition,source.model,source.graph,source.bindings,source.batch,driver.columns)
    aliases=finite_pack_query_aliases(driver);cases=source.definition.cases(source.model,source.graph,pack)
    registry=[{'table':target.table,'uuid':target.uuid}for target in driver.transport.targets.values()]
    request=pack_count_star_request(source.definition,cases[0][0],pack,source.model,source.graph,source.bindings,driver.manifest,registry,aliases)
    provider=FinitePackProvider(driver,aliases,json.loads(request['target']['bindingJson']))
    vector=manifest_pin_vector(driver.manifest,{table:driver.transport.targets[table].uuid for table in json.loads(driver.manifest['table_versions_json'])},authority='private-local-process')
    policy=FiniteGraphPolicy(driver,provider,vector,expected)
    def read():
        return read_graph_release(driver.transport,provider,HeldFinitePins(policy),vector,policy,publication_id=driver.publication_id,node_table=driver.tables['object_current'],edge_table=driver.tables['edge_current'],context=driver.context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={key:[value]for key,value in json.loads(driver.manifest['schema_revisions_json']).items()},max_nodes=facts['objects'],max_edges=facts['edges'])
    def capture():
        policy.require(driver.context)
        return {'manifest':json.loads(policy.manifest),'snapshots':{table:{'uuid':pair[0],'version':pair[1]}for table,pair in vector.targets.items()},'source':source.metadata(),'public_dataset_receipt_sha256':hashlib.sha256(source.raw).hexdigest()}
    with owned_context(provider.interval(driver.context)):
        opening=capture();receipt=source.raw;release=read();closing_release=read();closing=capture()
        if release!=closing_release or opening!=closing or receipt!=source.raw:raise ValueError('Original finite opening/closing export differs')
    return release,{'opening':opening,'closing':closing,'producer_receipt':receipt,'provider_interval':provider.closed_interval_custody(driver.context)}
