from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from effective_grants import effective_grants
from ashlar.authority import AuthorityError,validate_writer_inventory
class API:
    def __init__(self,pages):self.pages=iter(pages);self.calls=[]
    def do(self,method,path,**kwargs):self.calls.append((method,path,kwargs));return next(self.pages)
def assignment(principal,action='MODIFY',**origin):return {'principal':principal,'privileges':[dict(privilege=action,**origin)]}
class EffectiveGrantTests(unittest.TestCase):
    def test_complete_pagination_preserves_inherited_origins_and_writer_checks(self):
        pages=[{'privilege_assignments':[assignment('owner',inherited_from_type='catalog',inherited_from_name='c')],'next_page_token':'next'},
            {'privilege_assignments':[assignment('reader','SELECT')]}]
        api=API(pages);value=effective_grants(api,'TABLE','c.s.t',record_page=lambda *args:None)
        self.assertEqual(value['pages'],pages);self.assertEqual(api.calls[0][2]['query'],{'max_results':0})
        self.assertEqual(api.calls[1][2]['query'],{'max_results':0,'page_token':'next'})
        self.assertEqual(value['rows'][0],{'Principal':'owner','ActionType':'MODIFY','ObjectType':'CATALOG','ObjectKey':'c'})
        validate_writer_inventory('owner',value['rows'],trusted_writers=['owner'])
        value=effective_grants(API([{'privilege_assignments':[assignment('other')]}]),'table','c.s.t',record_page=lambda *args:None)
        with self.assertRaises(AuthorityError):validate_writer_inventory('owner',value['rows'],trusted_writers=['owner'])
    def test_unknown_partial_and_repeated_content_refuse(self):
        for pages in [[{'future':True}],[{'privilege_assignments':[assignment('owner','FUTURE')]}],
            [{'privilege_assignments':[assignment('owner',inherited_from_name='c')]}],
            [{'privilege_assignments':[assignment('owner',inherited_from_type='catalog',inherited_from_name='other')]}],
            [{'next_page_token':''}],[{'next_page_token':'loop'},{'next_page_token':'loop'}],
            [{'privilege_assignments':[assignment('owner')],'next_page_token':'x'},{'privilege_assignments':[assignment('owner')]}]]:
            with self.assertRaises(AuthorityError):effective_grants(API(pages),'table','c.s.t',record_page=lambda *args:None)
    def test_empty_page_continues_and_raw_unknown_page_is_retained_before_refusal(self):
        retained=[]
        pages=[{'next_page_token':'next'},{'privilege_assignments':[assignment('owner')]}]
        result=effective_grants(API(pages),'table','c.s.t',record_page=lambda page,ordinal:retained.append((ordinal,page)))
        self.assertEqual(result['pages'],pages);self.assertEqual(len(result['rows']),1)
        unknown={'future':{'opaque':'18446744073709551615'}}
        with self.assertRaises(AuthorityError):
            effective_grants(API([unknown]),'table','c.s.t',record_page=lambda page,ordinal:retained.append((ordinal,page)))
        self.assertEqual(retained[-1],(0,unknown))
if __name__=='__main__':unittest.main()
