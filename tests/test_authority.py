import unittest
from ashlar.authority import AuthorityError,validate_writer_inventory
class AuthorityTests(unittest.TestCase):
    def grant(self,principal,action):return {'Principal':principal,'ActionType':action,'ObjectType':'CATALOG','ObjectKey':'c'}
    def test_private_owner_and_explicit_readers(self):
        validate_writer_inventory('owner',[],trusted_writers=['owner'])
        validate_writer_inventory('owner',[self.grant('reader','SELECT')],trusted_writers=['owner'])
    def test_inherited_modify_and_management_refuse(self):
        for action in ['MODIFY','MANAGE','ALL PRIVILEGES']:
            with self.assertRaises(AuthorityError):validate_writer_inventory('owner',[self.grant('shared',action)],trusted_writers=['owner'])
    def test_unknown_missing_and_owner_refuse(self):
        for owner,grants in [('other',[]),('owner',None),('owner',[self.grant('reader','FUTURE PRIVILEGE')]),('owner',[{}])]:
            with self.assertRaises(AuthorityError):validate_writer_inventory(owner,grants,trusted_writers=['owner'])
if __name__=='__main__':unittest.main()
