import hashlib,json,tempfile,unittest
from pathlib import Path
from ashlar_host.truss_continuation_setup import run_configure_truss_continuation_command

class Tests(unittest.TestCase):
    def fixture(self,root):
        publication=root/'publication';publication.mkdir()
        jars=[]
        for name in ('delta.jar','storage.jar'):
            p=root/name;p.write_text('original');jars.append(str(p))
        oldpage=publication/'oldpage.json';oldpage.write_text('original-old-page')
        nextpage=publication/'nextpage.json';nextpage.write_text('original-next-page')
        intent={'feed':'feed','source_page_path':str(nextpage),'source_page_sha256':hashlib.sha256(nextpage.read_bytes()).hexdigest(),'request_digest':'b'*64,'publication_id':'next-publication'}
        selected=publication/'intent.json';selected.write_text(json.dumps(intent));selection={'path':str(selected),'sha256':hashlib.sha256(selected.read_bytes()).hexdigest()}
        config={'connection_uri':'actual-ordinary-uri','publication_directory':str(publication),'jars':jars,'feed':'feed','source_page_path':str(oldpage),'source_page_sha256':hashlib.sha256(oldpage.read_bytes()).hexdigest(),'request_digest':'a'*64}
        configuration=root/'configuration.json';configuration.write_text(json.dumps(config))
        report={'predecessor':'old-publication','scope':'caller-asserted-original','publications':[{'manifest':{'publication_id':'next-publication'},'source_page':{'path':str(nextpage),'sha256':intent['source_page_sha256']},'publication_intent':selection}]}
        result=root/'continued.json';result.write_text(json.dumps(report));return configuration,result,config,report
    def test_advance_and_empty_preserve_operator_selections_private(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();configuration,result,config,report=self.fixture(root);output=root/'next.json'
            run_configure_truss_continuation_command(configuration,result,output)
            self.assertEqual(json.loads(output.read_text()),{k:config[k]for k in ('connection_uri','publication_directory','jars')}|{'publication_intent':report['publications'][0]['publication_intent']})
            self.assertEqual(output.stat().st_mode&0o777,0o600)
            report['publications']=[];result.write_text(json.dumps(report));empty=root/'empty.json'
            run_configure_truss_continuation_command(configuration,result,empty)
            self.assertEqual(json.loads(empty.read_text()),config)
    def test_wrong_intent_hash_and_existing_output_refuse(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();configuration,result,config,report=self.fixture(root);output=root/'next.json'
            original=result.read_text();report['publications'][0]['publication_intent']['sha256']='f'*64;result.write_text(json.dumps(report))
            with self.assertRaises(ValueError):run_configure_truss_continuation_command(configuration,result,output)
            self.assertFalse(output.exists());result.write_text(original);output.write_text('preserve')
            with self.assertRaises(ValueError):run_configure_truss_continuation_command(configuration,result,output)
            self.assertEqual(output.read_text(),'preserve')
