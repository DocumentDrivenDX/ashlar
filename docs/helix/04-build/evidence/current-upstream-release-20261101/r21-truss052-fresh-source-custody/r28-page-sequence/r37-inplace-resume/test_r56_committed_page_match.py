import base64, importlib.util, json, pathlib, unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("r56match", HERE / "r56_committed_page_match.py")
matcher = importlib.util.module_from_spec(spec); spec.loader.exec_module(matcher)

class MatchTests(unittest.TestCase):
    def test_committed_page_identity_and_r49_property_bindings(self):
        delta = pathlib.Path("/private/tmp/ashlar-r36-truss052-page-sequence-20261101/delta")
        current = json.loads((delta / "source-page-678967ac3deeafa90b514ae8a5a1b05f78b0b32e5f89135f4c67205fede001ea.json").read_bytes())
        seed = json.loads((delta / "source-page-8d6db42c4d167a36468e261cf2b495fdaefedf1a4a963dbd09cae91f39a3c9ea.json").read_bytes())
        model = json.loads((HERE.parent / "r28-model.umf.json").read_bytes())
        rows = [
            {"typeId":"1","moduleId":"m","elementId":"person_key","propertyId":"1"},
            {"typeId":"1","moduleId":"m","elementId":"person_name","propertyId":"2"},
            {"typeId":"1","moduleId":"m","elementId":"quantity","propertyId":"3"},
            {"typeId":"2","moduleId":"m","elementId":"place_key","propertyId":"4"},
            {"typeId":"2","moduleId":"m","elementId":"place_name","propertyId":"5"},
        ]
        content = base64.b64decode(current["content"])
        page = matcher.validate_public_page(content, expected_content_sha256=current["sha256"],
            expected_consumer_id=current["consumer_id"], expected_registration_id=current["registration_id"],
            expected_prior_sha256="506d67a8dc1f8af88beb73bcd2b899afda8e8bebc62604561b4246a934e88c94",
            expected_next=base64.b64decode(current["next"]))
        self.assertEqual(str(page["writerXid"]), "847")
        crosscheck = matcher.validate_seed_property_bindings(base64.b64decode(seed["content"]), model, rows)
        self.assertEqual(crosscheck["accepted_property_count"], 5)
        self.assertEqual(crosscheck["property_ids"]["1.m.person_key"], "1")
        rows[0]["propertyId"] = "999"
        with self.assertRaises(matcher.MatchError):
            matcher.validate_seed_property_bindings(base64.b64decode(seed["content"]), model, rows)

if __name__ == "__main__": unittest.main()
