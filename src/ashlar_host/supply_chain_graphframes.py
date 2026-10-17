"""Seven common GraphFrames operations over the held original supply-chain graph.

All original properties remain opaque exact STRING carriers. No authored supply
scenario, native scalar promotion, export lease or publication authority is minted.
"""
import base64,json
from ashlar.graph_release import GraphRelease,validate_release_carrier
from .delta_custody import encoded
from .graphframes_pack_scenarios import bag,common_oracle
from .supply_chain_common_read import OpenedSupplyChainCommonRead
from .supply_chain_projection import original_supply_chain_oracle
from .supply_chain_request import source_snapshot

PROFILE='original-supply-chain-common/0.1'


def prepare_supply_chain_graphframes_profile(opened,release):
    """Correspondence only, admitted by the actual currently held source owner."""
    if type(opened)is not OpenedSupplyChainCommonRead or type(release)is not GraphRelease:
        raise ValueError('Actual original common owner and complete release required')
    opened.require_held_correspondence(opened.context)
    expected=original_supply_chain_oracle(opened.model,opened.graph,opened.bindings,
        opened.source.batch,{role:list(columns)for role,columns in opened.columns.items()})
    value=source_snapshot(json.loads(release.payload));original=json.loads(opened.graph)
    for kind,role,source in (('node','nodes','object_current'),('edge','edges','edge_current')):
        if bag(value[role])!=bag(validate_release_carrier(row,kind)for row in expected[source]):
            raise PermissionError('Complete independently derived original carrier bag differs')
    # The independent original carrier check above binds every retained original
    # record and its native identity; these names are query correspondence only.
    objects={obj['key']:obj for obj in original['objects']}
    nodes=[]
    for row in value['nodes']:
        obj=json.loads(row['retained_json'])['original']
        if obj!=objects.get(obj['key']):raise PermissionError('Original object correspondence differs')
        nodes.append(dict(row,original_key=obj['key'],original_type=encoded(obj['type'])))
    common_oracle(original)  # Require the existing nonvacuous filtered-limit fixture.
    return {'pack':'supply-chain','profile':PROFILE,'fields':[],
        'nodes':nodes,'edges':value['edges'],'cases':[],
        'source_admission':{'original_model_base64':base64.b64encode(opened.model).decode(),
            'original_graph_base64':base64.b64encode(opened.graph).decode()}}
