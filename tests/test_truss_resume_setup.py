# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import io
from contextlib import redirect_stdout
from ashlar.cli import main
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from ashlar_host.truss_resume_setup import run_configure_truss_resume_command


class Tests(unittest.TestCase):
    def test_prepare_exact_intent_selection_and_refuse_overwrite(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();publication=root/'publication';publication.mkdir()
            page=publication/'source-page-1.json';page.write_bytes(b'original archive')
            intent=publication/'publication-intent-1.json'
            intent.write_text(json.dumps({'feed':'graph','request_digest':'a'*64,'publication_id':'original',
                'source_page_path':str(page),'source_page_sha256':hashlib.sha256(page.read_bytes()).hexdigest()}))
            jars=[root/'delta.jar',root/'storage.jar']
            for path in jars:path.write_bytes(b'existing')
            original={'connection_uri':'postgresql://reader:private@localhost/db','administrator_uri':'private-admin',
                'reader_role':'reader','consumer_id':'original','feed':'graph','jars':[str(path)for path in jars],'maximum_pages':1}
            config=root/'publish.json';config.write_text(json.dumps(original))
            output=root/'resume.json'
            stdout=io.StringIO()
            with redirect_stdout(stdout),patch('sys.argv',['ashlar','configure-truss-resume','--configuration',str(config),'--publication-intent',str(intent),'--output',str(output)]):
                main()
            self.assertEqual(json.loads(stdout.getvalue()),{'output':str(output)})
            prepared=json.loads(output.read_bytes())
            self.assertEqual(prepared['publication_directory'],str(publication))
            self.assertEqual(prepared['publication_intent']['sha256'],hashlib.sha256(intent.read_bytes()).hexdigest())
            self.assertEqual(set(prepared),{'connection_uri','jars','publication_directory','publication_intent'})
            self.assertNotIn(original['connection_uri'],stdout.getvalue());self.assertEqual(output.stat().st_mode&0o777,0o600)
            saved=output.read_bytes()
            with self.assertRaises(ValueError):run_configure_truss_resume_command(config,intent,output)
            self.assertEqual(output.read_bytes(),saved)
            intent.write_text('{}')
            refused=root/'refused.json'
            with self.assertRaises(ValueError):run_configure_truss_resume_command(config,intent,refused)
            self.assertFalse(refused.exists())
