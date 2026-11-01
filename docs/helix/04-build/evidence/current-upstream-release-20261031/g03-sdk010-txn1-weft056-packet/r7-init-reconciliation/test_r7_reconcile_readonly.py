import unittest
from r7_reconcile_readonly import VerificationError, verify_policy_bindings

class ReconciliationTests(unittest.TestCase):
    def fixture(self):
        policy = "umf-sdk-0.10.0/pinned-source-policy"
        carrier = {"profile": "installation/0.3", "physical_profile": "delta/0.4",
            "source_policy_revision": "ashlar-local-current-delta4-sdk010-source-policy/0.1",
            "umf_revision": "0.10.0", "umf_archive_sha256": "archive",
            "umf_realization_profile": "ashlar-local-current-delta4-umf-source-sdk010/0.1"}
        manifest = {"sourcePolicyRevision": policy, "installationProfile": "installation/0.3",
            "carrierProfile": "delta/0.4", "sdk010": {"archiveSha256": "archive"}}
        registry = {"physical_profile": "delta/0.4", "carrier_profile_sha256": "carrier-hash",
            "scope": {"typed_graph_policy_revision": policy}}
        runtime_report = {"profile": "ashlar-configured-local-initialized/0.3", "operation": "initialize",
            "published": False, "protected_ack": False,
            "installation": {"scope": {"typed_graph_policy_revision": policy}}}
        return manifest, registry, carrier, runtime_report

    def test_distinct_adapter_and_umf_policy_fields_both_validate(self):
        verify_policy_bindings(*self.fixture(), "carrier-hash")

    def test_runtime_policy_revision_mismatch_refuses(self):
        manifest, registry, carrier, report = self.fixture()
        report["installation"]["scope"]["typed_graph_policy_revision"] = "wrong"
        with self.assertRaisesRegex(VerificationError, "UMF policy revision"):
            verify_policy_bindings(manifest, registry, carrier, report, "carrier-hash")

    def test_adapter_policy_revision_mismatch_refuses(self):
        manifest, registry, carrier, report = self.fixture()
        carrier["source_policy_revision"] = "wrong"
        with self.assertRaisesRegex(VerificationError, "adapter source-policy"):
            verify_policy_bindings(manifest, registry, carrier, report, "carrier-hash")

if __name__ == "__main__":
    unittest.main()
