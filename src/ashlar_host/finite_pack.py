"""Closed original UMF0.8 pack selection; correspondence never grants authority."""
from dataclasses import dataclass
import hashlib,json
from ashlar import supply_chain_source,archaeology_source,ecology_source
from .supply_chain_request import source_snapshot

UMF_REVISION='07357eadbda9cf8299b9d72d004cbb77f8091aee'
_PROFILES=(
 ('supply-chain',supply_chain_source,'baf888ab976749e589df385c76900ebf146959b61eaa4b55ab840e142b54ae22',20,23,2,('split-excursion','excursion','replay','lineage','sensor')),
 ('archaeology',archaeology_source,'401c0743e6aa8eddf541c49bb719c35e7fa67fb31113f494e94d09766d9b124f',39,46,13,('cycle','dating','media','missing-media','specialists','lineage','evidence-links','sample')),
 ('ecology',ecology_source,'353f4fedbcd2f3d1a8ee13ad22b1bf496b430dcd766c5d0b1c6fb94d3e0e7481',40,54,7,('effort-event','connected-measurements','censor','match','effort','zero','network','comparability','fishing')))

@dataclass(frozen=True)
class FinitePackDefinition:
    name:str
    def __post_init__(self):
        if type(self.name)is not str or self.name not in tuple(p[0]for p in _PROFILES):raise ValueError('Exact supported original finite pack required')
    def facts(self):
        self.__post_init__();name,builder,pack,objects,edges,nulls,cases=next(p for p in _PROFILES if p[0]==self.name)
        return {'name':name,'model_sha256':builder.SOURCE_SHA,'graph_sha256':builder.GRAPH_SHA,'pack_sha256':pack,'objects':objects,'edges':edges,'present_nulls':nulls,'case_ids':list(cases),'umf_version':'0.8.0','umf_revision':UMF_REVISION,'source_system':'private-original-'+name+'-fixture'}
    def inputs(self,model,graph,pack=None):
        facts=self.facts()
        for raw,key in ((model,'model_sha256'),(graph,'graph_sha256'))+(()if pack is None else ((pack,'pack_sha256'),)):
            if type(raw)is not bytes or hashlib.sha256(raw).hexdigest()!=facts[key]:raise ValueError('Exact immutable original pack bytes required')
        if json.loads(model)['umf']!='0.8.0':raise ValueError('Original declared UMF0.8 required')
        return facts
    def build(self,model,graph):
        facts=self.inputs(model,graph);builder=next(p[1]for p in _PROFILES if p[0]==self.name)
        return builder.build_transaction(model,graph,source_system=facts['source_system'])
    def cases(self,model,graph,pack):
        facts=self.inputs(model,graph,pack);checks=json.loads(pack)['scenario_checks']
        if [c['id']for c in checks]!=facts['case_ids']:raise ValueError('Complete original scenario inventory required')
        return tuple((c['id'],c['sql'])for c in checks)
    def oracle(self,model,graph):
        self.inputs(model,graph)
        if self.name=='supply-chain':
            from .supply_chain_oracle import original_graph_oracle
            return source_snapshot(original_graph_oracle(model,graph))
        if self.name=='archaeology':
            from .archaeology_oracle import original_oracle
        else:
            from .ecology_oracle import original_oracle
        return source_snapshot(original_oracle(model,graph))
