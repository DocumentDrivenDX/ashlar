"""Small structural finite-file controls; no native source or engine qualification."""
import copy,hashlib,json,unittest
from pathlib import Path
from ashlar.graph_release import GraphRelease,validate_release_carrier
from ashlar_host.delta_custody import encoded
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.finite_projection import original_finite_pack_oracle
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.finite_graph_custody import PROFILE,build_finite_graph_custody
from ashlar_host.private_graph_custody import load_private_graph_release

class Tests(unittest.TestCase):
    def pair(self):
        definition=FinitePackDefinition('archaeology');facts=definition.facts();root=Path('examples/domain-packs/archaeology/upstream');model=(root/'ontology.json').read_bytes();graph=(root/'graph/fixture.json').read_bytes();batch,bindings=definition.build(model,graph)
        oracle=original_finite_pack_oracle(definition,model,graph,bindings,batch,fixture_columns(RESOURCE_ROOT));receipt=b'{"fixture":"structural receipt only"}'
        source={**facts,'public_dataset_receipt_sha256':hashlib.sha256(receipt).hexdigest(),'source_transaction_sha256':hashlib.sha256(batch.begin+b''.join(row.raw for row in batch.records)+batch.commit).hexdigest(),'qualification':'Controlled structural fixture only'}
        snapshots={'local.finite.'+role:{'uuid':'00000000-0000-0000-0000-'+str(index).zfill(12),'version':0}for index,role in enumerate(('object_current','edge_current','tombstone','whole_source_history'),1)}
        manifest={'publication_id':'fixture','profile_version':'ashlar-delta/0.3','recorded_at':'1','table_versions_json':encoded({table:0 for table in snapshots}),'schema_revisions_json':encoded({facts['source_system']:facts['model_sha256']}),'source_progress_json':'{}','validation_report_json':encoded({'complete':True,'source_admission':source})}
        value={'format':'ashlar-graph-release/0.1','publication':manifest,'snapshots':snapshots,'roles':{'nodes':'local.finite.object_current','edges':'local.finite.edge_current'},'nodes':[validate_release_carrier(row,'node')for row in oracle['object_current']],'edges':[validate_release_carrier(row,'edge')for row in oracle['edge_current']],'mapping':{'identity':'ashlar-key/1','properties':'Original exact canonical text','residuals':['Original history remains pinned'],'capabilities':{'reversibleIdentity':True,'independentEdges':True,'isolatedNodes':True,'exactCanonicalText':True,'selectedScalarPromotion':False,'nativeReleaseMaterialization':False,'engineExecution':False},'losses':[],'engineSupport':[]}}
        payload=(encoded(value)+'\n').encode();release=GraphRelease(payload,hashlib.sha256(payload).hexdigest())
        opening={'manifest':manifest,'snapshots':snapshots,'source':source,'public_dataset_receipt_sha256':hashlib.sha256(receipt).hexdigest()}
        captured={'opening':opening,'closing':copy.deepcopy(opening),'producer_receipt':receipt,'provider_interval':{'profile':'ashlar-immutable-file-publication-interval/0.1','publication_id':'fixture','manifest_sha256':hashlib.sha256(encoded(manifest).encode()).hexdigest(),'source':source,'qualification':'Controlled fixture only'}}
        custody=build_finite_graph_custody(release,captured);return release,custody
    def test_distinct_trusted_finite_file_pair_loads_without_protected_ack(self):
        release,custody=self.pair();value=load_private_graph_release(release.payload,release.sha256,custody,hashlib.sha256(custody).hexdigest())
        self.assertEqual((len(value['nodes']),len(value['edges'])),(39,46));self.assertEqual(json.loads(custody)['format'],PROFILE);self.assertNotIn('ack',json.loads(custody))
        with self.assertRaises(ValueError):load_private_graph_release(release.payload,release.sha256,custody,'0'*64)
    def test_changed_closing_source_or_incomplete_counts_refuse(self):
        release,custody=self.pair();changed=json.loads(custody);changed['interval']['closing']['source']['umf_version']='changed';raw=(encoded(changed)+'\n').encode()
        with self.assertRaises(ValueError):load_private_graph_release(release.payload,release.sha256,raw,hashlib.sha256(raw).hexdigest())
        value=json.loads(release.payload);value['nodes'].pop();payload=(encoded(value)+'\n').encode();changed=json.loads(custody);changed['releaseSha256']=hashlib.sha256(payload).hexdigest();raw=(encoded(changed)+'\n').encode()
        with self.assertRaises(ValueError):load_private_graph_release(payload,changed['releaseSha256'],raw,hashlib.sha256(raw).hexdigest())
