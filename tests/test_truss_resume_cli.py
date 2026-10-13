import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from ashlar_host.truss_resume_cli import parse_resume_truss_configuration,run_resume_truss_graph_command


class Tests(unittest.TestCase):
    def test_saved_intent_selection_and_digest_refusal(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();publication=root/'original';publication.mkdir()
            page=publication/'source-page-2.json';page.write_bytes(b'original archive')
            jars=[root/'delta.jar',root/'storage.jar']
            for path in jars:path.write_bytes(b'existing')
            intent={'feed':'graph','source_page_path':str(page),
                'source_page_sha256':hashlib.sha256(page.read_bytes()).hexdigest(),
                'request_digest':'a'*64,'publication_id':'truss-graph-2'}
            saved=publication/'publication-intent-2.json';saved.write_text(json.dumps(intent))
            selection={'path':str(saved),'sha256':hashlib.sha256(saved.read_bytes()).hexdigest()}
            config={'connection_uri':'postgresql://ordinary','publication_directory':str(publication),
                'jars':[str(path) for path in jars],'publication_intent':selection}
            selected=root/'configuration.json';selected.write_text(json.dumps(config))
            parsed=parse_resume_truss_configuration(selected,root/'resumed.json')
            for key in ('feed','source_page_path','source_page_sha256','request_digest'):
                self.assertEqual(parsed[0][key],intent[key])
            self.assertNotIn('publication_intent',parsed[0])
            saved.write_text(json.dumps(intent)+' ')
            with self.assertRaisesRegex(ValueError,'intent digest differs'):
                parse_resume_truss_configuration(selected,root/'resumed.json')
            saved.write_text(json.dumps(intent))
            config['request_digest']='b'*64;selected.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError,'Exact original Truss resume configuration'):
                parse_resume_truss_configuration(selected,root/'resumed.json')
            self.assertFalse((root/'resumed.json').exists())

    def test_closed_configuration_preflight_without_native_imports(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();publication=root/'original';publication.mkdir()
            page=root/'source-page.json';page.write_bytes(b'original archive')
            jars=[root/'delta.jar',root/'storage.jar']
            for path in jars:path.write_bytes(b'existing')
            config={'connection_uri':'postgresql://ordinary','feed':'graph','source_page_path':str(page),
                'source_page_sha256':hashlib.sha256(page.read_bytes()).hexdigest(),
                'publication_directory':str(publication),'request_digest':'a'*64,'jars':[str(path) for path in jars]}
            selected=root/'configuration.json';selected.write_text(json.dumps(config))
            output=root/'resumed'
            parsed=parse_resume_truss_configuration(selected,output)
            self.assertEqual(parsed[1],publication);self.assertEqual(parsed[3],tuple(jars));self.assertFalse(output.exists())
            config['reset']=True;selected.write_text(json.dumps(config))
            with patch('ashlar_host.truss_resume_cli.LocalDeltaTransport.require_existing_journal') as preflight:
                with self.assertRaisesRegex(ValueError,'Exact original Truss resume configuration'):
                    run_resume_truss_graph_command(selected,output)
                preflight.assert_not_called()
            self.assertFalse(output.exists())
