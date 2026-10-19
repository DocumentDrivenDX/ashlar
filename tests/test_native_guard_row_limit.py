# SPDX-License-Identifier: MIT OR Apache-2.0
"""Row limit precedes native measurements; existing sentinel decoder stays strict."""
import sys,unittest
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.delta_custody import LocalDeltaTransport,LocalDeltaError
from ashlar_host.evolution_verification_types import VerificationBounds

class Expression:
    def __getattr__(self,name):return lambda *args,**kwargs:self
    def __add__(self,other):return self
    __radd__=__add__
    __mul__=__add__
    __and__=__add__
    __or__=__add__
    __eq__=__add__
    __ge__=__add__
    __le__=__add__

class Tests(unittest.TestCase):
    def test_limit_before_projection_preserves_complete_bag_and_refuses_overflow(self):
        for source,refuses in ((['one','two'],False),(['one','two','three','four'],True)):
            with self.subTest(source=source):
                events=[];state={};expression=Expression()
                class Iterator:
                    def __init__(self,rows):self.rows=iter(rows)
                    def __iter__(self):return self
                    def __next__(self):
                        row=next(self.rows);return SimpleNamespace(asDict=lambda:row)
                    def close(self):events.append('closed')
                class Frame:
                    schema=SimpleNamespace(fields=[SimpleNamespace(name='value',dataType='string')])
                    def limit(self,n):events.append(('limit',n));state['values']=source[:n];return self
                    def select(self,*args):events.append('select');return self
                    def unionByName(self,other):return self
                    def filter(self,*args):return self
                    def toLocalIterator(self):
                        count=len(state['values']);valid=count<=2
                        def row(value,marker):return {'value':value,'_ashlar_verification_cell_0':1 if marker else 0,'_ashlar_verification_real':marker,'_ashlar_verification_count':count,'_ashlar_verification_max':1,'_ashlar_verification_total':count,'_ashlar_verification_valid':valid}
                        rows=[row(value,1)for value in state['values']]if valid else []
                        rows.append(row(None,0));return Iterator(rows)
                # Distinct sentinel frame; no original projection may precede limit.
                class Sentinel(Frame):
                    def select(self,*args):events.append('sentinel');return self
                functions=SimpleNamespace(**{name:(lambda *args,**kwargs:expression)for name in ('col','lit','sum','max','greatest','coalesce','when')})
                window=SimpleNamespace(unboundedPreceding=-1,unboundedFollowing=1,partitionBy=lambda:SimpleNamespace(rowsBetween=lambda *args:object()))
                transport=object.__new__(LocalDeltaTransport);transport.observation_bounds=VerificationBounds(262144,2,1048576,16777216,1048576,1048576);transport.spark=SimpleNamespace(range=lambda n:Sentinel())
                with patch.dict(sys.modules,{'pyspark':SimpleNamespace(),'pyspark.sql':SimpleNamespace(functions=functions,Window=window)}):
                    if refuses:
                        with self.assertRaisesRegex(LocalDeltaError,'Native bounds refused'):transport._guarded_native_projection(Frame(),['value'],[expression],limit=2,maximum=16777216,base=256,row_constant=64)
                    else:
                        rows,stats=transport._guarded_native_projection(Frame(),['value'],[expression],limit=2,maximum=16777216,base=256,row_constant=64)
                        self.assertEqual(rows,[{'value':'one'},{'value':'two'}]);self.assertEqual(stats,(2,1,2))
                self.assertEqual(events[0],('limit',3));self.assertEqual(events[1],'select');self.assertEqual(events[-1],'closed')
