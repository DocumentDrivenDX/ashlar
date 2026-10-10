"""Immutable reviewed 5dd metadata and independent owning guard controls.

Claim-only schema callbacks test local metadata selection only. Actual offline
validation, compiler installation and native execution need separate evidence.
"""
import copy,hashlib,json,unittest,zlib
from pathlib import Path
from ashlar_host.count_star_admission import CountStarAdmissionConfig,CountStarSchemaValidation,CountStarPlanError,admit_count_star_artifact
raw=(Path(__file__).parent/'fixtures/weft_count_star_admission.json.zlib').read_bytes()
if len(raw)>262144:raise ValueError('compressed-bound')
expander=zlib.decompressobj();decoded=expander.decompress(raw,2097153)
if len(decoded)>2097152 or not expander.eof or expander.unused_data or expander.unconsumed_tail:raise ValueError('decoded-bound')
if hashlib.sha256(decoded).hexdigest()!='68e315615e8c7c590739c2e2370d64af69c349fb8147a12941c7527b03f756bc':raise ValueError('fixture-pin')
fixture=json.loads(decoded);SCHEMAS={k:v.encode()for k,v in fixture['schemas'].items()};PAIRS={r['id']:(json.loads(r['requestText']),json.loads(r['responseText']))for r in fixture['pairs']}
class CountStarAdmissionTests(unittest.TestCase):
    def config(self):return CountStarAdmissionConfig(16777216,CountStarSchemaValidation(SCHEMAS,lambda *args:None))
    def pair(self,name='count-star:original-replay'):return copy.deepcopy(PAIRS[name])
    def admit(self,q,r):return admit_count_star_artifact(q,r,copy.deepcopy(r),config=self.config())
    def refuses(self,q,r):
        with self.assertRaises(CountStarPlanError):self.admit(q,r)
    def test_exact_selected_metadata_accepts_three_cases_and_refuses_eight_controls(self):
        compiled=0
        for name in PAIRS:
            q,r=self.pair(name)
            if r['status']=='compiled':
                admitted=self.admit(q,r);compiled+=1
                with self.assertRaises(TypeError):admitted.artifact['sql']='changed'
            else:self.refuses(q,r)
        self.assertEqual(compiled,3)
    def test_unknown_capability_and_unknown_owning_obligation_refuse(self):
        q,r=self.pair();r['logicalPlan']['requiredCapabilities'].append('invented');self.refuses(q,r)
        q,r=self.pair();r['obligations'].append(dict(id='invented',owner='host',failureCode='WFT-OBLIGATION',parameters={}));self.refuses(q,r)
    def test_aggregate_capacity_cannot_be_hidden_by_having(self):
        q,r=self.pair();o=next(o for o in r['obligations']if o['id']=='ashlar.arithmetic.exact');o['parameters']['checks'][0]['phase']='projection-survivors';self.refuses(q,r)
    def test_expansion_capacity_and_occurrence_integrity_are_both_required(self):
        for removed in ('ashlar.path.countCapacity','ashlar.path.occurrenceIntegrity'):
            q,r=self.pair('count-star:expansion-count-owner');r['obligations']=[o for o in r['obligations']if o['id']!=removed];self.refuses(q,r)
    def test_old_profile_and_changed_original_binding_refuse(self):
        q,r=self.pair();r['backend']['backendVersion']='0.4.0-paths-keys-candidate';self.refuses(q,r)
        q,r=self.pair();q['target']['bindingJson']+=' ';self.refuses(q,r)
    def test_recompilation_difference_precedes_native_callbacks(self):
        q,r=self.pair();other=copy.deepcopy(r);other['sql']+=' '
        with self.assertRaises(CountStarPlanError):admit_count_star_artifact(q,r,other,config=self.config())
if __name__=='__main__':unittest.main()
