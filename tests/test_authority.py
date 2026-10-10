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
    def test_scalar_mapping_and_invalid_writer_carriers_refuse(self):
        for writers in ['alice',b'alice',{'owner':True},None,1,[],['owner',None],['owner',[]],['']]:
            with self.subTest(writers=writers):
                with self.assertRaises(AuthorityError):
                    validate_writer_inventory('a' if writers=='alice' else 'owner',[],trusted_writers=writers)
    def test_explicit_writer_collections_preserve_whole_identity(self):
        for writers in [['alice'],('alice',),{'alice'},frozenset(['alice'])]:
            with self.subTest(writers=writers):
                validate_writer_inventory('alice',[],trusted_writers=writers)
                with self.assertRaises(AuthorityError):validate_writer_inventory('a',[],trusted_writers=writers)
    def test_executable_container_and_identity_subclasses_refuse(self):
        class ChangingList(list):
            calls=0
            def __iter__(self):
                self.calls+=1
                return iter(['owner'] if self.calls==1 else ['attacker'])
        class UnhashableIdentity(str):
            __hash__=None
        writers=ChangingList(['owner'])
        with self.assertRaises(AuthorityError):validate_writer_inventory('attacker',[],trusted_writers=writers)
        self.assertEqual(writers.calls,0)
        with self.assertRaises(AuthorityError):validate_writer_inventory('owner',[],trusted_writers=[UnhashableIdentity('owner')])
    def test_owner_and_write_principal_cannot_spoof_trusted_identity(self):
        class Spoof(str):
            def __hash__(self):return hash('owner')
            def __eq__(self,other):return True
        with self.assertRaises(AuthorityError):validate_writer_inventory(Spoof('attacker'),[],trusted_writers=['owner'])
        with self.assertRaises(AuthorityError):validate_writer_inventory('owner',[self.grant(Spoof('attacker'),'MODIFY')],trusted_writers=['owner'])
    def test_executable_grant_containers_records_and_fields_refuse(self):
        class ExecutableList(list):
            def __iter__(self):raise AssertionError('Grant iterator called')
        class ExecutableDict(dict):
            def __getitem__(self,key):raise AssertionError('Grant lookup called')
        class UnhashableText(str):
            __hash__=None
        for grants in [ExecutableList(),[ExecutableDict(self.grant('owner','MODIFY'))]]:
            with self.assertRaises(AuthorityError):validate_writer_inventory('owner',grants,trusted_writers=['owner'])
        for field in ['Principal','ActionType','ObjectType','ObjectKey']:
            grant=self.grant('owner','MODIFY');grant[field]=UnhashableText(grant[field])
            with self.subTest(field=field):
                with self.assertRaises(AuthorityError):validate_writer_inventory('owner',[grant],trusted_writers=['owner'])
        grant=self.grant('owner','MODIFY');grant[1]='unused'
        with self.assertRaises(AuthorityError):validate_writer_inventory('owner',[grant],trusted_writers=['owner'])
    def test_metaclass_equality_cannot_spoof_container_types(self):
        callbacks=[]
        class SpoofType(type):
            def __eq__(self,other):
                callbacks.append('equality');return True
        class ForeignWriters(metaclass=SpoofType):
            def __iter__(self):
                callbacks.append('writers');return iter(['owner'])
        class ForeignGrants(metaclass=SpoofType):
            def __iter__(self):
                callbacks.append('grants');return iter([])
        with self.assertRaises(AuthorityError):validate_writer_inventory('owner',[],trusted_writers=ForeignWriters())
        with self.assertRaises(AuthorityError):validate_writer_inventory('owner',ForeignGrants(),trusted_writers=['owner'])
        self.assertEqual(callbacks,[])
if __name__=='__main__':unittest.main()
