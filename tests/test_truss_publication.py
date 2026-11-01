# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest
import json
from unittest.mock import patch
import test_truss_graph_sql as graph_cases
from ashlar.attempt_store import verify_request_digest
from ashlar.publication import Descriptor
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.truss_publication import publish_truss_transaction
from ashlar_host.publication_planning import truss_request_for
from ashlar_host.truss_note_publication import validate_deferred_ack_pair


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

    def test_explicit_deferred_ack_flag_flows_to_coordinator(self):
        batch,_=graph_cases.Tests().batch()
        backend=object();context=object();descriptor=object()
        with patch('ashlar.publisher.publish_batch',return_value=descriptor)as publish:
            actual=publish_truss_transaction(backend,'notes',batch,predecessor='origin',
                schema_revisions_json='{"notes":"1"}',context=context,acknowledge_source=False)
        self.assertIs(actual,descriptor)
        self.assertIs(publish.call_args.kwargs['acknowledge_source'],False)

    def test_deferred_ack_pair_binds_valid_original_request_and_committed_descriptor(self):
        batch,_=graph_cases.Tests().batch()
        request=truss_request_for('notes',batch,'origin',{'notes':'1'})
        checkpoint=json.loads(request['source_checkpoint_json'])
        expected_id='truss-graph-'+request['request_digest']
        descriptor=Descriptor(expected_id,'local/1',{}, {'notes':'1'},
            {checkpoint['feed']:checkpoint},{'complete':True,'request_digest':request['request_digest']}, {})
        self.assertEqual(validate_deferred_ack_pair(request,descriptor,expected_publication_id=expected_id),
            request['request_digest'])
        with self.assertRaises(PermissionError):
            validate_deferred_ack_pair(request,descriptor,expected_publication_id='truss-graph-wrong')
        with self.assertRaises(ValueError):
            validate_deferred_ack_pair({**request,'request_digest':'0'*64},descriptor,expected_publication_id=expected_id)
