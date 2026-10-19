# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest
from unittest.mock import patch
import test_truss_graph_sql as graph_cases
from ashlar.attempt_store import verify_request_digest
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.truss_publication import publish_truss_transaction
from ashlar_host.publication_planning import truss_request_for


class Tests(unittest.TestCase):
    def test_native_request_and_coordinator_keep_original_page_and_checkpoint(self):
        batch,_=graph_cases.Tests().batch()
        request=truss_request_for('notes',batch,'origin',{'notes':'1'})
        self.assertEqual(verify_request_digest(request),request['request_digest'])
        backend=object();context=object();descriptor=object()
        with patch('ashlar.publisher.publish_batch',return_value=descriptor)as publish:
            actual=publish_truss_transaction(backend,'notes',batch,predecessor='origin',
                schema_revisions_json=request['schema_revisions_json'],context=context)
        self.assertIs(actual,descriptor)
        publish.assert_called_once_with(backend,'notes',batch,predecessor='origin',
            schema_revisions_json=request['schema_revisions_json'],context=context,
            source_checkpoint_json=truss_checkpoint(batch))
