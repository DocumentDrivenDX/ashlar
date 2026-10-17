"""Explicit installed source validation dispatch; no producer or service effects."""
import contextlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

class Tests(unittest.TestCase):
    def arguments(self):
        return ['ashlar','validate-configured-source','--configuration','/operator/source.json',
            '--producer-source','/operator/umf','--bun','/operator/bun','--git','/operator/git',
            '--output','/operator/new-output','--producer-timeout-seconds','30',
            '--producer-maximum-output-bytes','1048576','--producer-maximum-receipt-bytes','1048576',
            '--maximum-receipt-bytes','1048576']
    def test_explicit_values_reach_owner_and_clean_json(self):
        from ashlar.cli import main
        answer={'status':'validated'};out=io.StringIO()
        with patch.object(sys,'argv',self.arguments()),patch('ashlar_host.configured_source_admission.validate_configured_source',return_value=answer)as run,contextlib.redirect_stdout(out):
            main()
        config=run.call_args.args[0]
        self.assertEqual(config.configuration,Path('/operator/source.json'))
        self.assertEqual(config.producer.source,Path('/operator/umf'))
        self.assertEqual(config.producer.timeout_seconds,30)
        self.assertEqual(config.maximum_receipt_bytes,1048576)
        self.assertEqual(json.loads(out.getvalue()),answer)
    def test_failure_has_no_success_output_or_exception_detail(self):
        from ashlar.cli import main
        out=io.StringIO();err=io.StringIO()
        with patch.object(sys,'argv',self.arguments()),patch('ashlar_host.configured_source_admission.validate_configured_source',side_effect=ValueError('private source content')),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err),self.assertRaises(SystemExit)as failure:
            main()
        self.assertEqual(failure.exception.code,2);self.assertEqual(out.getvalue(),'')
        self.assertEqual(err.getvalue(),'ashlar configured source validation refused\n')
