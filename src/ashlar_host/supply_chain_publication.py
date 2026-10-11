"""Legacy original supply-chain API over the shared finite publication owner."""
from .finite_publication import FiniteFileDriver,GRAPH_ROLES
from .delta_custody import encoded
from .supply_chain_finite_source import FiniteSupplyChainSource

class FiniteSupplyChainDriver(FiniteFileDriver):
    def __init__(self,transport,policy,context,tables,columns,source,publication_id,*,current_admission):
        if type(source)is not FiniteSupplyChainSource:raise ValueError('Owned finite source and independent current admission required')
        super().__init__(transport,policy,context,tables,columns,source,publication_id,current_admission=current_admission)
