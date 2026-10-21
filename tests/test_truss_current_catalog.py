import json
import base64
import hashlib
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host.truss_current_catalog import interpret_current_catalog


EVIDENCE = (Path(__file__).parents[1] / 'docs/helix/04-build/evidence'
            / 'truss-current-catalog-0.10-20261021')


def receipt_inputs():
    report_raw = (EVIDENCE / 'accepted-report.json').read_bytes()
    view_raw = (EVIDENCE / 'catalog-view.json').read_bytes()
    source_raw = (EVIDENCE / 'accepted-source.umf.json').read_bytes()
    report = json.loads(report_raw)
    accepted = report['acceptedInput']['documents'][0]
    source = SimpleNamespace(
        document_id=accepted['documentId'],
        document_revision=accepted['documentRevision'],
        artifact_identity=accepted['artifact']['identity'],
        registration_id=report['registrationId'],
        sha256=accepted['artifact']['sha256'],
        content=source_raw,
        catalog_revision=1,
    )
    sources = {('1', accepted['documentId']): source}
    return report_raw, view_raw, sources, source


class TrussCurrentCatalogTests(unittest.TestCase):
    def test_actual_010_acceptance_receipt_interprets_assigned_catalog(self):
        report, view, sources, _ = receipt_inputs()
        admitted = interpret_current_catalog('1', report, view, sources)
        self.assertEqual(set(admitted['types']), {'1', '2'})
        self.assertEqual(set(admitted['edges']), {'1', '2'})
        self.assertEqual(admitted['models']['typed-graph-example'][1],
                         (EVIDENCE / 'accepted-source.umf.json').read_bytes())
        self.assertEqual(len(json.loads(view)['definitions']), 11)

    def test_actual_receipt_refuses_tampered_source_digest_and_revision(self):
        report_raw, view, sources, source = receipt_inputs()
        report = json.loads(report_raw)
        report['acceptedInput']['documents'][0]['artifact']['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'differs from report'):
            interpret_current_catalog('1', json.dumps(report).encode(), view, sources)

        _, view, sources, source = receipt_inputs()
        sources[('1', source.document_id)] = SimpleNamespace(
            document_id=source.document_id,
            document_revision='tampered-revision',
            artifact_identity=source.artifact_identity,
            registration_id=source.registration_id,
            sha256=source.sha256,
            content=source.content,
            catalog_revision=1,
        )
        with self.assertRaisesRegex(ValueError, 'identity, revision or digest'):
            interpret_current_catalog('1', report_raw, view, sources)

    def test_artifact_identity_is_distinct_from_document_id_and_registration_is_bound(self):
        report_raw, view, sources, source = receipt_inputs()
        report = json.loads(report_raw)
        report['acceptedInput']['documents'][0]['artifact']['identity'] = 'accepted-artifact-alias'
        source.artifact_identity = 'accepted-artifact-alias'
        aliased_report = json.dumps(report).encode()
        admitted = interpret_current_catalog('1', aliased_report, view, sources)
        self.assertEqual(set(admitted['types']), {'1', '2'})

        source.artifact_identity = 'typed-graph-example'
        with self.assertRaisesRegex(ValueError, 'identity, revision or digest'):
            interpret_current_catalog('1', aliased_report, view, sources)
        source.artifact_identity = 'accepted-artifact-alias'
        source.registration_id = 'different-registration'
        with self.assertRaisesRegex(ValueError, 'identity, revision or digest'):
            interpret_current_catalog('1', aliased_report, view, sources)

    def test_catalog_reader_selects_live_current_custody_only_for_report_06(self):
        from ashlar.truss_native_feed import native_feed_batch
        from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
        from tests.test_truss_graph_sql import Tests as GraphTests
        original,_=GraphTests().batch()
        payload=json.loads(original.begin)
        report=(EVIDENCE/'accepted-report.json').read_bytes()
        payload['catalogPrerequisites']=[{'revision':'1','reportBytesBase64':base64.b64encode(report).decode(),
            'reportSha256':hashlib.sha256(report).hexdigest()}]
        raw=json.dumps(payload).encode()
        current=native_feed_batch(raw,sha256=hashlib.sha256(raw).hexdigest(),
            prior=base64.b64decode(original.cursor_before),next=base64.b64decode(original.cursor_after),feed=original.feed)
        with patch('ashlar_host.truss_catalog_reader.read_current_catalog_evidence',return_value='current') as live, \
             patch('ashlar_host.truss_catalog_reader.read_historical_catalog_views',return_value='historical') as archived:
            self.assertEqual(read_graph_catalog_evidence(lambda:None,current),'current')
            live.assert_called_once()
            archived.assert_not_called()

        with patch('ashlar_host.truss_catalog_reader.read_current_catalog_evidence',return_value='current') as live, \
             patch('ashlar_host.truss_catalog_reader.read_historical_catalog_views',return_value='historical') as archived:
            self.assertEqual(read_graph_catalog_evidence(lambda:None,original),'historical')
            live.assert_not_called()
            archived.assert_called_once()

if __name__ == '__main__':
    unittest.main()
