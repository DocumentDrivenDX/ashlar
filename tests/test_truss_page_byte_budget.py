# SPDX-License-Identifier: MIT OR Apache-2.0
import json,sys,tempfile,unittest
import ashlar_host.truss_runtime
import ashlar_host.commerce_export_cli
from pathlib import Path
from types import SimpleNamespace
from contextlib import nullcontext
from unittest.mock import patch
from ashlar_host.truss_note_cli import run_publish_truss_graph_command
from ashlar_host.truss_note_publication import publish_graph_pages,publish_note_pages
from ashlar_host.truss_resume_setup import run_configure_truss_resume_command

class Tests(unittest.TestCase):
    def config(self,root):
        jars=[root/'delta.jar',root/'storage.jar']
        for p in jars:p.write_bytes(b'existing')
        value={'connection_uri':'ordinary','administrator_uri':'administrator','reader_role':'reader','consumer_id':'consumer','feed':'feed','jars':list(map(str,jars)),'maximum_pages':2}
        path=root/'publisher';path.write_text(json.dumps(value));return path,value
    def test_default_and_explicit_budget_forwarded_and_resume_shape_preserved(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();config,value=self.config(root)
            for budget in (None,2097152):
                selected=dict(value)
                if budget is not None:selected['maximum_page_bytes']=budget
                config.write_text(json.dumps(selected))
                with patch.dict(sys.modules,{'psycopg':SimpleNamespace(connect=lambda *a,**k:None)}),patch('ashlar_host.truss_runtime.require_truss_psycopg_runtime'),patch('ashlar_host.commerce_export_cli.open_commerce_export_spark',return_value=nullcontext('spark')),patch('ashlar_host.truss_note_publication.register_note_consumer'),patch('ashlar_host.truss_note_publication.publish_graph_pages',return_value={})as publish:
                    run_publish_truss_graph_command(config,root/('output-'+str(budget)))
                self.assertEqual(publish.call_args.kwargs['maximum_page_bytes'],1048576 if budget is None else budget)
            with patch('ashlar_host.truss_note_publication.publish_note_pages')as note:publish_graph_pages(None,root/'graph',connection_factory=lambda:None,consumer_id='consumer',feed='feed',maximum_page_bytes=2097152)
            self.assertEqual(note.call_args.kwargs['maximum_page_bytes'],2097152)
            with patch('ashlar_host.truss_note_publication.read_page',side_effect=RuntimeError('observed'))as read:
                with self.assertRaisesRegex(RuntimeError,'observed'):publish_note_pages(None,root/'native',connection_factory=lambda:None,consumer_id='consumer',feed='feed',maximum_page_bytes=2097152)
            self.assertEqual(read.call_args.kwargs['maximum_bytes'],2097152);self.assertFalse((root/'native').exists())
            intent=root/'intent';intent.write_text('{"feed":"feed"}')
            def parse(candidate,destination):
                raw=json.loads(candidate.read_text());self.assertNotIn('maximum_page_bytes',raw);self.assertEqual(raw['connection_uri'],'ordinary')
            with patch('ashlar_host.truss_resume_setup.parse_resume_truss_configuration',side_effect=parse):run_configure_truss_resume_command(config,intent,root/'reader')
            self.assertNotIn('maximum_page_bytes',json.loads((root/'reader').read_text()))
    def test_invalid_budget_refuses_before_source_or_registration(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d).resolve();config,value=self.config(root);intent=root/'intent';intent.write_text('{"feed":"feed"}')
            for budget in (True,0,2097153,'2097152'):
                config.write_text(json.dumps({**value,'maximum_page_bytes':budget}))
                with patch('ashlar_host.truss_note_publication.register_note_consumer')as register,patch('ashlar_host.truss_note_publication.read_page')as read:
                    with self.assertRaisesRegex(ValueError,'Page byte budget'):run_publish_truss_graph_command(config,root/'output')
                    with self.assertRaisesRegex(ValueError,'Page byte budget'):publish_note_pages(None,root/'output',connection_factory=lambda:None,consumer_id='consumer',feed='feed',maximum_page_bytes=budget)
                    register.assert_not_called();read.assert_not_called()
                with patch('ashlar_host.truss_resume_setup.parse_resume_truss_configuration')as parser:
                    with self.assertRaisesRegex(ValueError,'Page byte budget'):run_configure_truss_resume_command(config,intent,root/'reader')
                    parser.assert_not_called()
                self.assertFalse((root/'output').exists());self.assertFalse((root/'reader').exists())
