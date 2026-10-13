"""Missing original custody must refuse before source or engine effects."""
import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.truss_resume_cli import run_resume_truss_graph_command

class Tests(unittest.TestCase):
    def test_absent_or_symlink_journal_never_opens_or_creates_database(self):
        for symlink in (False,True):
            with self.subTest(symlink=symlink),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();publication=root/'publication';publication.mkdir()
                page=root/'source-page.json';page.write_bytes(b'original selected archive')
                jars=[root/'delta.jar',root/'storage.jar']
                for jar in jars:jar.write_bytes(b'original executable selection')
                journal=publication/'operations.sqlite'
                if symlink:
                    other=root/'other.sqlite';other.write_bytes(b'foreign custody');journal.symlink_to(other)
                configuration=root/'resume.json';configuration.write_text(json.dumps({'connection_uri':'private-selected-connection','publication_directory':str(publication),'jars':[str(jar)for jar in jars],'feed':'original','source_page_path':str(page),'source_page_sha256':hashlib.sha256(page.read_bytes()).hexdigest(),'request_digest':'a'*64}))
                before=configuration.read_bytes();output=root/'result.json'
                with patch('ashlar_host.truss_resume_cli.sqlite3.connect',side_effect=AssertionError('No database connection permitted')):
                    with self.assertRaisesRegex(ValueError,'journal unavailable.*retain source checkpoint'):
                        run_resume_truss_graph_command(configuration,output)
                self.assertEqual(configuration.read_bytes(),before)
                self.assertEqual(page.read_bytes(),b'original selected archive')
                self.assertFalse(output.exists());self.assertFalse((root/'result.json-warehouse').exists())
                if symlink:self.assertEqual(other.read_bytes(),b'foreign custody')
                else:self.assertFalse(journal.exists())
