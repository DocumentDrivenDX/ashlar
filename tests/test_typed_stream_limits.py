"""Iterator controls only; synthetic pages do not qualify an Ashlar backend."""
import unittest
from types import SimpleNamespace
from pydantic import BaseModel,ConfigDict
from umf_access.pages import ReadPage,ReadContinuation,ReadObligationUnmet,StreamLimitReached
from ashlar.access import AshlarTarget

class Parameters(BaseModel):
    model_config=ConfigDict(strict=True,extra='forbid')
class Row(BaseModel):
    value:int
class Target(AshlarTarget):
    def __init__(self,pages):self.pages=pages;self.dispatched=0
    def page(self,*args,**kwargs):
        self.dispatched+=1
        return self.pages[self.dispatched-1]
def page(*values,more=False):
    return ReadPage(tuple(Row(value=value)for value in values),ReadContinuation((values[-1]if values else 0,),'control')if more else None)
ENTRY=SimpleNamespace(parameters=Parameters)
class StreamLimitTests(unittest.TestCase):
    def test_buffered_and_exact_page_limits_are_explicit(self):
        for rows,expected in [(page(1,2,3,more=True),True),(page(1,2,more=True),None)]:
            target=Target([rows]);stream=target.stream(ENTRY,Parameters(),max_rows=2)
            self.assertEqual([next(stream).value,next(stream).value],[1,2])
            with self.assertRaises(StreamLimitReached)as raised:next(stream)
            self.assertIs(raised.exception.truncated,expected)
            self.assertEqual(raised.exception.emitted,2);self.assertEqual(target.dispatched,1)
    def test_natural_exhaustion_and_close(self):
        target=Target([page(1,2)])
        self.assertEqual([row.value for row in target.stream(ENTRY,Parameters(),max_rows=2)],[1,2])
        target=Target([page(1,2,more=True)]);stream=target.stream(ENTRY,Parameters(),max_rows=4)
        next(stream);stream.close();self.assertEqual(target.dispatched,1)
    def test_nonprogress_refuses_without_retry(self):
        target=Target([page(more=True)])
        with self.assertRaises(ReadObligationUnmet):next(target.stream(ENTRY,Parameters(),max_rows=4))
        self.assertEqual(target.dispatched,1)
    def test_invalid_override_refuses_before_page_dispatch(self):
        for size in [0,True,1001]:
            target=Target([])
            with self.assertRaises(ValueError):next(target.stream(ENTRY,Parameters(),max_rows=4,page_size=size))
            self.assertEqual(target.dispatched,0)
if __name__=='__main__':unittest.main()
