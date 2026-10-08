import base64
import copy
import hashlib
import json
from pathlib import Path
import unittest
from ashlar.schema import SchemaIntake
from ashlar.truss_input import verify_acceptance_input_custody
from ashlar.report_parts import initial_candidate_report_parts, ReportPartsError

ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/truss_catalog_candidate_20261008_report_parts'
PIN='16c35e8d943769ccfa7bb57d16785aa7159abe65'

class ReportPartsTests(unittest.TestCase):
    def setUp(self):
        self.intake=SchemaIntake.read((B/'intake.json').read_bytes(),'3',trusted_validator_revision=PIN)
        self.interpretation=(B/'interpretation.json').read_bytes()
        archives=json.loads((B/'profile-archives.json').read_text())
        profiles={(name,'0.1'):base64.b64decode(value['bytesBase64']) for value in archives.values() for name in [value['identity']]}
        self.request=verify_acceptance_input_custody((B/'acceptance-input-candidate.json').read_bytes(),profile_archives=profiles,intakes=[self.intake],trusted_validator_revision=PIN,document_order=(self.intake.document_id,))
        self.native=json.loads((B/'summary.json').read_text())['observed'][0]
    def test_original_diagnostics_partial_meaning_and_observed_counts(self):
        result=initial_candidate_report_parts(self.intake,self.interpretation,self.request,self.native)
        self.assertEqual(result['counts'],{'typesAdded':'1','propertiesAdded':'2','keysAdded':'0','relationshipsAdded':'0','endpointsAdded':'0','elementsRetired':'0'})
        self.assertEqual(result['documentInterpretations'][0]['completeness'],'partial')
        self.assertEqual(base64.b64decode(result['diagnostics'][0]['diagnostic']['bytesBase64']),self.intake.artifact)
        self.assertEqual(base64.b64decode(result['documentInterpretations'][0]['evidence']['bytesBase64']),self.interpretation)
        self.assertNotIn('acceptedRevision',result)
        self.assertNotIn('interfaceVersion',result)
        self.assertTrue(all(r['enforcement']=='none' for r in result['assertionInventory']['entries']))
    def test_rejects_incomplete_effects_and_native_meaning_drift(self):
        mutations=[lambda n:n.update(unaccounted_effect='extra'),lambda n:n['properties'].pop(),lambda n:n['other_counts'].pop('journal'),
            lambda n:n['other_counts'].update(journal=1),lambda n:n['properties'][0].update(nullability='nullable'),
            lambda n:n['types'][0].update(lineage_hex='00'),lambda n:n['types'][0].update(definition_rev=True),
            lambda n:n['properties'][0].update(type_id=True),lambda n:n.update(report_count=False),
            lambda n:n['document']['validation'].update(complete=0),
            lambda n:n['types'][0].update(unknown_column='unaccounted')]
        for mutate in mutations:
            n=copy.deepcopy(self.native);mutate(n)
            with self.subTest(native=str(n)[:80]):
                with self.assertRaises(ReportPartsError):initial_candidate_report_parts(self.intake,self.interpretation,self.request,n)
    def test_rejects_original_request_drift(self):
        n=copy.deepcopy(self.native);n['original_request']['acceptanceInputSha256']='0'*64
        with self.assertRaises(ReportPartsError):initial_candidate_report_parts(self.intake,self.interpretation,self.request,n)

if __name__=='__main__':unittest.main()
