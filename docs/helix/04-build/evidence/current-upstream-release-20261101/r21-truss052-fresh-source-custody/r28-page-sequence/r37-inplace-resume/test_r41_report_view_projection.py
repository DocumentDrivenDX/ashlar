import base64
import importlib.util
import json
from pathlib import Path
import unittest


HERE = Path(__file__).resolve().parent
PACKET = HERE.parent
WORKER = HERE / "r41-local-worker.py"


def load_worker():
    spec = importlib.util.spec_from_file_location("r41_local_worker_test", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReportViewProjectionTests(unittest.TestCase):
    def test_actual_report_06_shape_uses_exact_complete_view_refs(self):
        worker = load_worker()
        page = json.loads((HERE / "r39-public-feed-page.json").read_bytes())
        report_raw = base64.b64decode(page["catalogPrerequisites"][1]["reportBytesBase64"], validate=True)
        report = json.loads(report_raw)
        self.assertEqual(report["interfaceVersion"], "truss-catalog-registration-report/0.6")
        self.assertNotIn("properties", report)
        model = json.loads((PACKET / "r28-model.umf.json").read_bytes())
        doc = model["id"]
        type_records = {}
        definitions = []
        counter = 0
        for module in model["modules"]:
            elements = {element["id"]: element for element in module["elements"]}
            for entity in module["elements"]:
                if entity.get("kind") != "record":
                    continue
                type_id = f"type-{entity['id']}"
                owner = (doc, module["id"], entity["id"])
                fields = {}
                for member in entity["members"]:
                    counter += 1
                    field = elements[member["element"]]
                    property_id = f"property-{counter}"
                    pin = f"pin-{counter}"
                    fields[property_id] = pin
                    definitions.append({"reference": {
                        "kind": "property", "owner": {"documentId": doc, "moduleId": member["module"]},
                        "typeId": type_id, "authoredIdentity": field["id"],
                        "propertyId": property_id, "definitionPin": pin}})
                type_records[type_id] = {"owner": owner, "fields": fields}
        selected = {"report": report_raw.decode(), "view": json.dumps({
            "interfaceVersion": "truss-catalog-view/0.2.0", "scope": {"kind": "complete"},
            "definitions": definitions}).encode(),
            "models": {doc: (model, b"")}, "types": type_records}
        projection = json.loads(worker.project_current_view_property_allocations(selected, doc))
        self.assertEqual(len(projection["properties"]), counter)
        self.assertEqual(projection["acceptedInput"], report["acceptedInput"])
        self.assertNotIn("properties", report)

    def test_ref_pin_mismatch_and_duplicate_ref_fail_closed(self):
        worker = load_worker()
        report_raw = json.dumps({"interfaceVersion": "truss-catalog-registration-report/0.6"})
        model = {"modules": [{"id": "m", "elements": [
            {"id": "T", "kind": "record", "members": [{"module": "m", "element": "f"}]},
            {"id": "f", "kind": "field", "name": "f"}]}]}
        doc = "doc"
        ref = {"reference": {"kind": "property", "owner": {"documentId": doc, "moduleId": "m"},
            "typeId": "t", "authoredIdentity": "f", "propertyId": "p", "definitionPin": "pin"}}
        selected = {"report": report_raw, "view": json.dumps({"interfaceVersion": "truss-catalog-view/0.2.0",
            "scope": {"kind": "complete"}, "definitions": [ref]}).encode(),
            "models": {doc: (model, b"")}, "types": {"t": {"owner": (doc, "m", "T"), "fields": {"p": "wrong"}}}}
        with self.assertRaisesRegex(worker.Refusal, "identity or definition pin"):
            worker.project_current_view_property_allocations(selected, doc)
        selected["types"]["t"]["fields"]["p"] = "pin"
        view = json.loads(selected["view"]); view["definitions"].append(ref)
        selected["view"] = json.dumps(view).encode()
        with self.assertRaisesRegex(worker.Refusal, "duplicate complete-view"):
            worker.project_current_view_property_allocations(selected, doc)


if __name__ == "__main__":
    unittest.main()
