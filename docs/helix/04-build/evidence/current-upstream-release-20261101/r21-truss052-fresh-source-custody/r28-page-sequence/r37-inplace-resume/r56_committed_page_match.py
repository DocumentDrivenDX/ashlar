"""Custody checks for reuse of the already-committed R55 page."""
from __future__ import annotations
import base64, hashlib, json

class MatchError(ValueError):
    pass

def verify_publication_selection(publication_id: str, request: dict, *,
                                 expected_publication_id: str, expected_request_digest: str) -> bytes:
    if type(publication_id) is not str or publication_id != expected_publication_id:
        raise MatchError("selected publication ID differs from committed R55 intent")
    if type(request) is not dict or request.get("request_digest") != expected_request_digest:
        raise MatchError("request's declared digest differs from committed R55 intent")
    digest_input = dict(request); digest_input.pop("request_digest")
    digest_raw = json.dumps(digest_input, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=False, allow_nan=False).encode()
    if hashlib.sha256(digest_raw).hexdigest() != expected_request_digest:
        raise MatchError("canonical publication request digest differs from committed R55 intent")
    return json.dumps(request, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()

def _pairs(items):
    out = {}
    for key, value in items:
        if key in out: raise MatchError(f"duplicate JSON key: {key}")
        out[key] = value
    return out

def validate_public_page(page_content: bytes, *, expected_content_sha256: str,
                         expected_consumer_id: str, expected_registration_id: str,
                         expected_prior_sha256: str, expected_next: bytes) -> dict:
    if hashlib.sha256(page_content).hexdigest() != expected_content_sha256:
        raise MatchError("fresh public page content differs from committed R55 page")
    page = json.loads(page_content, object_pairs_hook=_pairs)
    if (page.get("prior", {}).get("consumerId") != expected_consumer_id or
            page.get("next", {}).get("consumerId") != expected_consumer_id or
            page.get("prior", {}).get("registrationId") != expected_registration_id or
            page.get("next", {}).get("registrationId") != expected_registration_id):
        raise MatchError("committed page consumer/registration identity differs")
    if type(expected_prior_sha256) is not str or len(expected_prior_sha256) != 64 or type(expected_next) is not bytes:
        raise MatchError("opaque cursor custody inputs are malformed")
    # Opaque cursor bytes are compared against the public FeedPage object by the caller.
    if page.get("writerXid") is None:
        raise MatchError("committed page lacks writer XID")
    return page

def validate_seed_property_bindings(seed_page_content: bytes, model: dict, property_rows: list,
                                    type_field_pins: dict) -> dict:
    """Cross-check R41 complete-view allocations against retained R49 history.

    R41's accepted-view projection supplies property IDs. R49 event afterimages
    supply their durable definition pins. No field-order or value-shape inference
    occurs here; R36's existing matcher validates the continuation values.
    """
    page = json.loads(seed_page_content, object_pairs_hook=_pairs)
    refs = {}
    for row in property_rows:
        key = (str(row["typeId"]), row["moduleId"], row["elementId"])
        if key in refs: raise MatchError("duplicate accepted-view property allocation")
        refs[key] = str(row["propertyId"])
    elements = {(m["id"], e["id"]): e for m in model["modules"] for e in m["elements"]}
    type_members = {}
    for row in property_rows:
        matching = [e for e in elements.values() if e.get("kind") == "record" and
                    any(m["module"] == row["moduleId"] and m["element"] == row["elementId"] for m in e.get("members", []))]
        if len(matching) != 1: raise MatchError("accepted member does not have one unique record owner")
        type_members[str(row["typeId"])] = matching[0]["name"]
    observed = {}
    for change in page.get("changes", []):
        raw = base64.b64decode(change["new_value"]["bytesBase64"], validate=True)
        event = json.loads(raw, object_pairs_hook=_pairs)
        after = event.get("after", {})
        if after.get("kind") != "object": continue
        type_id = str(after.get("identity", {}).get("typeId"))
        for prop in after.get("properties", []):
            candidates = [k for k in refs if k[0] == type_id and refs[k] == str(prop.get("propertyId"))]
            if len(candidates) != 1: raise MatchError("R49 property ID does not uniquely bind an accepted model member")
            key = candidates[0]
            binding = (str(prop["propertyId"]), str(prop["definitionPin"]))
            selected_pin = type_field_pins.get(type_id, {}).get(binding[0])
            if type(selected_pin) is not str or selected_pin != binding[1]:
                raise MatchError("R49 definition pin differs from selected Truss type fields")
            if key in observed and observed[key] != binding: raise MatchError("R49 property definition pin changed")
            observed[key] = binding
    if set(observed) != set(refs): raise MatchError("R49 page does not bind every accepted property allocation")
    return {"property_ids": {".".join(k): v for k, v in refs.items()},
            "definition_pins": {".".join(k): observed[k][1] for k in refs},
            "accepted_property_count": len(refs), "retained_r49_crosscheck": True}
