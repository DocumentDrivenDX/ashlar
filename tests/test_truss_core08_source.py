# SPDX-License-Identifier: MIT OR Apache-2.0
"""Version selection/correspondence only; no native SDK acceptance claim."""
import base64,hashlib,json,tempfile,unittest
from pathlib import Path
from ashlar_host.truss_local_setup import selections
from ashlar_host.truss_record_mutation import preflight
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar.truss_native_feed import native_feed_batch
import test_truss_typed_admission as inert

MODEL=Path(__file__).resolve().parents[1]/'examples/truss-native/core08-asset.umf.json'

class Tests(unittest.TestCase):
    def test_original08_setup_create_preflight_preserve_bytes_and_future_version_refused(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();model=root/'asset.json';model.write_bytes(MODEL.read_bytes())
            bun=root/'bun';bun.write_text('inert');origin=root/'origin.json';origin.write_text('{}')
            config={'data_directory':str(root/'data'),'bun':str(bun),'document_path':str(model),
                'document_id':'ashlar-core08-asset','document_revision':'r1','origin_path':str(origin),'request_key':'setup'}
            path=root/'setup.json';path.write_text(json.dumps(config))
            selected=selections(path,root/'setup-report.json')
            self.assertEqual(selected[3],MODEL.read_bytes());self.assertEqual(selected[5],('assets',))
            config.pop('data_directory');config.update(connection_uri='inert',module_id='assets',record_id='Asset',values={
                'asset-key':{'kind':'string','text':'A1'},'units':{'kind':'integer','text':'9007199254740993'},
                'valuation':{'kind':'decimal','text':'1.2300'},'enabled':{'kind':'boolean','value':True}})
            path=root/'create.json';path.write_text(json.dumps(config))
            self.assertEqual(preflight(path,root/'create-report.json')[3],MODEL.read_bytes())
            bad=json.loads(MODEL.read_bytes());bad['umf']='0.9.0';model.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):preflight(path,root/'create-report.json')
            self.assertFalse((root/'data').exists());self.assertFalse((root/'create-report.json').exists())

    def test_inert08_accepted_report_keeps_declared_version_and_source_bytes(self):
        batch,catalogs=inert.Tests().fixture('integer',facets={'integerWidth':{'bits':64,'signed':True}})
        payload=json.loads(batch.begin);p=payload['catalogPrerequisites'][0]
        report=json.loads(base64.b64decode(p['reportBytesBase64']));artifact=report['acceptedInput']['documents'][0]['artifact']
        model=json.loads(base64.b64decode(artifact['bytesBase64']));model['umf']='0.8.0'
        raw=json.dumps(model,separators=(',',':')).encode();artifact['bytesBase64']=base64.b64encode(raw).decode();artifact['sha256']=hashlib.sha256(raw).hexdigest()
        r=json.dumps(report,separators=(',',':')).encode();p['reportBytesBase64']=base64.b64encode(r).decode();p['reportSha256']=hashlib.sha256(r).hexdigest()
        body=json.dumps(payload,separators=(',',':')).encode()
        source=native_feed_batch(body,sha256=hashlib.sha256(body).hexdigest(),prior=base64.b64decode(batch.cursor_before),next=base64.b64decode(batch.cursor_after),feed=batch.feed)
        admission=TrussGraphAdmission(source,catalogs)
        self.assertEqual(admission.revisions['1']['models'][model['id']][1],raw)
        self.assertEqual(admission.metadata()['declared_umf']['1'][model['id']],'0.8.0')

if __name__=='__main__':unittest.main()
