import copy
import unittest
import test_truss_graph_admission as graph_fixtures
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_compatible_revisions import compatible_record_revisions


class Tests(unittest.TestCase):
    def fixture(self):
        batch, catalogs = graph_fixtures.Tests().original()
        admission = TrussGraphAdmission(batch, catalogs)
        # Classification fixture derived from genuine admitted r1 identities.
        revision = copy.deepcopy(admission.revisions['1'])
        revision['types']['2']['fields']['99'] = 'optional-definition-pin'
        admission.revisions['2'] = revision
        return admission

    def selected(self, admission):
        return compatible_record_revisions(admission, revision='2', document_id='cohort', type_id='2')

    def test_optional_addition_preserves_original_owner_fields_keys_and_required(self):
        admission = self.fixture()
        self.assertEqual(self.selected(admission), ('1', '2'))
        self.assertEqual(compatible_record_revisions(admission, revision='1',
                         document_id='cohort', type_id='2'), ('1',))

    def test_incompatible_catalogs_excluded_and_noncurrent_selection_refused(self):
        for change in ('pin', 'required', 'owner', 'keys'):
            admission = self.fixture(); old = admission.revisions['1']['types']['2']
            if change == 'pin': old['fields'][next(iter(old['fields']))] = 'changed-pin'
            elif change == 'required': admission.revisions['2']['types']['2']['required'].add('99')
            elif change == 'owner': old['owner'] = ('other-document', 'm', 'B')
            else: old['keys'] = {}
            with self.subTest(change=change): self.assertEqual(self.selected(admission), ('2',))
        for arguments in ({'revision':'3','document_id':'cohort','type_id':'2'},
                          {'revision':'2','document_id':'foreign','type_id':'2'}):
            with self.assertRaises(ValueError): compatible_record_revisions(self.fixture(), **arguments)
        with self.assertRaises(ValueError): compatible_record_revisions(object(), revision='2',
                                               document_id='cohort', type_id='2')
