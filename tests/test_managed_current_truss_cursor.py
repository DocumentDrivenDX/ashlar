"""Authored offline parser fixtures; these are not evidence of Truss acceptance."""
import hashlib
import json
import unittest

from ashlar_host.managed_current_truss_cursor import (
    MAX_CURSOR_BYTES,
    TrussCursorCorrespondenceError,
    validate_selected_truss_cursor,
)


IDENTITY = {
    "feed": "feed-A",
    "epoch": "epoch-1",
    "consumer_id": "consumer-A",
    "registration_id": "feed-registration-A",
    "installation_id": "installation-A",
    "ack_scope": "scope-A",
    "ack_registration_id": "ack-registration-A",
}


def cursor_bytes(**changes):
    value = {
        "profile": "truss-caller-asserted-feed/0.1",
        "installationId": "installation-A",
        "sourceEpoch": "epoch-1",
        "consumerId": "consumer-A",
        "registrationId": "feed-registration-A",
        "nextXid": "4",
        "futureExtension": {"opaque": ["preserve", 7]},
    }
    value.update(changes)
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


class ManagedCurrentTrussCursorTests(unittest.TestCase):
    def check(self, raw, identity=IDENTITY):
        return validate_selected_truss_cursor(
            raw, cursor_sha256=hashlib.sha256(raw).hexdigest(), selected_identity=identity
        )

    def test_preserves_original_bytes_extensions_and_non_genesis_position(self):
        raw = cursor_bytes()
        result = self.check(raw)
        self.assertEqual(result.original, raw)
        self.assertEqual(result.next_xid, "4")
        self.assertEqual(result.extensions["futureExtension"], {"opaque": ["preserve", 7]})
        self.assertNotIn("feed", result.identity)
        self.assertNotIn("ack_scope", result.identity)

    def test_rejects_digest_mismatch_and_unbounded_bytes(self):
        raw = cursor_bytes()
        with self.assertRaises(TrussCursorCorrespondenceError):
            validate_selected_truss_cursor(raw, cursor_sha256="0" * 64, selected_identity=IDENTITY)
        with self.assertRaises(TrussCursorCorrespondenceError):
            self.check(b"x" * (MAX_CURSOR_BYTES + 1))

    def test_rejects_duplicate_members_and_invalid_json(self):
        raw = b'{"profile":"truss-caller-asserted-feed/0.1","profile":"other"}'
        with self.assertRaises(TrussCursorCorrespondenceError):
            self.check(raw)
        with self.assertRaises(TrussCursorCorrespondenceError):
            self.check(b"\xff")
        with self.assertRaises(TrussCursorCorrespondenceError):
            self.check(b"[]")

    def test_rejects_python_integer_digit_limit_in_cursor_json(self):
        huge_extension_number = b'{"profile":"truss-caller-asserted-feed/0.1","extension":' + b"9" * 5000 + b"}"
        with self.assertRaises(TrussCursorCorrespondenceError):
            self.check(huge_extension_number)

    def test_rejects_bad_profile_or_selected_identity(self):
        with self.assertRaises(TrussCursorCorrespondenceError):
            self.check(cursor_bytes(profile="other"))
        for key, value in (("consumer_id", "consumer-B"),
                           ("registration_id", "source-registration-A"),
                           ("installation_id", "installation-B"),
                           ("epoch", "epoch-B")):
            selected = dict(IDENTITY)
            selected[key] = value
            with self.assertRaises(TrussCursorCorrespondenceError):
                self.check(cursor_bytes(), selected)

    def test_rejects_missing_or_malformed_identity_fields(self):
        for field in ("installationId", "sourceEpoch", "consumerId", "registrationId"):
            raw = cursor_bytes()
            value = json.loads(raw)
            del value[field]
            with self.assertRaises(TrussCursorCorrespondenceError):
                self.check(json.dumps(value).encode())
        for selected_key in ("installation_id", "epoch", "consumer_id", "registration_id"):
            selected = dict(IDENTITY)
            selected[selected_key] = True
            with self.assertRaises(TrussCursorCorrespondenceError):
                self.check(cursor_bytes(), selected)

    def test_rejects_noncanonical_or_out_of_range_position(self):
        for token in ("00", "+1", "-1", "١", str(2**64), "1.0", "1" * 21):
            with self.subTest(token=token), self.assertRaises(TrussCursorCorrespondenceError):
                self.check(cursor_bytes(nextXid=token))
        with self.assertRaises(TrussCursorCorrespondenceError):
            self.check(cursor_bytes(nextXid=4))


if __name__ == "__main__":
    unittest.main()
