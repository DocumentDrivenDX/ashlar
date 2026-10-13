"""Legacy original supply-chain API over the shared finite publication owner."""
from contextlib import contextmanager
from .lifecycle import owned_context
from .finite_publication import FiniteFileDriver,GRAPH_ROLES
from .delta_custody import encoded
from .supply_chain_finite_source import FiniteSupplyChainSource

class FiniteSupplyChainDriver(FiniteFileDriver):
    def __init__(self,transport,policy,context,tables,columns,source,publication_id,*,current_admission):
        if type(source)is not FiniteSupplyChainSource:raise ValueError('Owned finite source and independent current admission required')
        super().__init__(transport,policy,context,tables,columns,source,publication_id,current_admission=current_admission)


    @contextmanager
    def _existing_read_interval(self,stream,context,*,restore):
        # Parent opening/closing observes actual identities, phases and bags.
        # Reuse starts only after its opening and ends before its closing.
        with owned_context(super()._existing_read_interval(stream,context,restore=restore))as selection:
            if getattr(self,'_read_projection_memo',None)is not None:
                raise PermissionError('Nonnested original supply-chain read required')
            self._read_projection_memo={}
            try:yield selection
            finally:self._read_projection_memo=None

    def _check(self,versions,expected):
        memo=getattr(self,'_read_projection_memo',None)
        if memo is None:return super()._check(versions,expected)
        if not self.held:raise PermissionError('Original held supply-chain read required')
        # Keep owner ports nominal: equal-looking replacement transports or
        # methods cannot borrow a prior immutable projection comparison.
        query=self.transport.query
        ports=(self.transport,self.transport.spark,self.policy,self.context,self.source,self.current_admission,
            getattr(query,'__self__',query),getattr(query,'__func__',query))
        shape=encoded({'versions':versions,'expected':expected,'tables':self.tables,
            'columns':self.columns,'publication_id':getattr(self,'publication_id',None),
            'request':getattr(self,'request',None),'manifest':getattr(self,'manifest',None),'targets':{name:{'uuid':target.uuid,'path':str(target.path),'table':target.table}
                for name,target in self.transport.targets.items()}})
        previous=memo.get('observation')
        if previous is not None and all(a is b for a,b in zip(previous[0],ports))and previous[1]==shape:return None
        super()._check(versions,expected)
        memo['observation']=(ports,shape)
