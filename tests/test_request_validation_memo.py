# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure semantic reuse; real request parser and inert original source owners."""
import hashlib,json,time,unittest
from unittest.mock import patch
from ashlar.attempt_store import verify_request_digest,AttemptStoreError
from ashlar.request_validation_memo import request_validation_memo
from ashlar.source_checkpoint import validate_checkpoint_request
from ashlar_host.driver import request_for
import test_registered_outbox_sources as source_fixture
from test_registered_outbox_sources import transaction


def request():
    return request_for('stream',transaction()[0],'original-predecessor',{'A':'original-schema'})


class Tests(unittest.TestCase):
    def test_real_parser_reuse_changed_bytes_and_interval_cancel(self):
        original=request()
        with patch('ashlar.source_checkpoint.validate_checkpoint_request',wraps=validate_checkpoint_request) as parse:
            with request_validation_memo():
                for _ in range(12):self.assertEqual(verify_request_digest(dict(original)),original['request_digest'])
                self.assertEqual(parse.call_count,1)
                changed=dict(original);changed['predecessor']='changed'
                with self.assertRaises(AttemptStoreError):verify_request_digest(changed)
                changed=dict(original);checkpoint=json.loads(changed['source_checkpoint_json']);checkpoint['feed']='foreign'
                changed['source_checkpoint_json']=json.dumps(checkpoint)
                changed['request_digest']=hashlib.sha256(json.dumps({k:v for k,v in changed.items()if k!='request_digest'},sort_keys=True,separators=(',',':')).encode()).hexdigest()
                for _ in range(2):
                    with self.assertRaises(AttemptStoreError):verify_request_digest(changed)
                self.assertEqual(parse.call_count,3) # Failed validation is never cached.
            verify_request_digest(original);self.assertEqual(parse.call_count,4)
            primary=KeyboardInterrupt()
            with self.assertRaises(KeyboardInterrupt)as observed:
                with request_validation_memo():verify_request_digest(original);raise primary
            self.assertIs(observed.exception,primary)
            verify_request_digest(original);self.assertEqual(parse.call_count,6)

    def test_capacity_type_and_validator_identity_preserve_fallback(self):
        from ashlar.request_validation_memo import validate_request_semantics
        original=request()
        with patch('ashlar.source_checkpoint.validate_checkpoint_request',wraps=validate_checkpoint_request)as parse:
            with request_validation_memo():
                for i in range(17):
                    value=dict(original);value['predecessor']=str(i);value['request_digest']=hashlib.sha256(json.dumps({k:v for k,v in value.items()if k!='request_digest'},sort_keys=True,separators=(',',':')).encode()).hexdigest();verify_request_digest(value)
                verify_request_digest(value);self.assertEqual(parse.call_count,18)
        with request_validation_memo(),patch('ashlar.request_validation_memo._MAX_BYTES',1),patch('ashlar.source_checkpoint.validate_checkpoint_request',wraps=validate_checkpoint_request)as parse:
            verify_request_digest(original);verify_request_digest(original);self.assertEqual(parse.call_count,2)
        class Text(str):pass
        with request_validation_memo(),patch('ashlar.source_checkpoint.validate_checkpoint_request',wraps=validate_checkpoint_request)as parse:
            changed=dict(original);changed['stream']=Text(original['stream'])
            verify_request_digest(changed);verify_request_digest(changed);self.assertEqual(parse.call_count,2)
            self.assertEqual(validate_request_semantics(original,lambda value:'foreign'),'foreign')
            self.assertEqual(verify_request_digest(original),original['request_digest'])

    def test_source_authority_sessions_and_closing_are_not_cached(self):
        fixture=source_fixture.Tests();fixture.setUp()
        with request_validation_memo():
            fixture.owner.admit(fixture.request,context=fixture.context)
            fixture.owner.admit(fixture.request,context=fixture.context)
        self.assertEqual(len(fixture.factory_calls),2)
        self.assertEqual(len(fixture.policy.calls),6)
        self.assertEqual(fixture.connection.reads,4)
        self.assertEqual(fixture.connection.calls,['rollback','close']*2)

    def test_copied_context_cannot_repopulate_after_close_or_cancel(self):
        from contextvars import copy_context
        from ashlar.request_validation_memo import validate_request_semantics
        original=request()
        for cancel in (False, True):
            with patch('ashlar.source_checkpoint.validate_checkpoint_request',wraps=validate_checkpoint_request)as parse:
                try:
                    with request_validation_memo():
                        verify_request_digest(original);copied=copy_context()
                        if cancel:raise KeyboardInterrupt()
                except KeyboardInterrupt:pass
                copied.run(verify_request_digest,original);copied.run(verify_request_digest,original)
                self.assertEqual(parse.call_count,3)
        result=[];calls=[]
        def container(value):calls.append(1);return result
        with request_validation_memo():
            self.assertIs(validate_request_semantics(original,container),result)
            self.assertIs(validate_request_semantics(original,container),result)
        self.assertEqual(len(calls),2)

    def test_actual_semantic_workload_parse_counts(self):
        original=request();counts={};elapsed={}
        for mode in ('uncached','memo'):
            from contextlib import nullcontext
            with patch('ashlar.source_checkpoint.validate_checkpoint_request',wraps=validate_checkpoint_request)as parse:
                started=time.perf_counter()
                with request_validation_memo()if mode=='memo'else nullcontext():
                    for _ in range(100):verify_request_digest(dict(original))
                elapsed[mode]=time.perf_counter()-started;counts[mode]=parse.call_count
        self.assertEqual(counts,{'uncached':100,'memo':1})
        print(json.dumps({'pure_request_validation_workload':counts,'elapsed_seconds':elapsed},sort_keys=True))
