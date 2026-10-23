# SPDX-License-Identifier: MIT OR Apache-2.0
"""Unverified, offline readiness checklist for the fresh Fabric R1/R2 packet.

The report consumes caller-selected artifact anchors and typed observations. It
does not read files, contact services, resume intents, or grant launch authority.
"""
from datetime import datetime
from hashlib import sha256
from pathlib import PurePosixPath
import re
from typing import Mapping


_CANDIDATE_ROW = re.compile(
    r"^\| (R[12]) \| `([0-9a-f-]{36})` \| `([^`]+)`; `([^`]+)` \|$",
    re.MULTILINE,
)
_ROLES = ("r1", "r2")
_TABLE_ROLES = ("nodes", "edges")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_OBSERVATION_FIELDS = {
    "value", "artifact_identity", "artifact_sha256", "observed_at", "source_provenance",
}
_ANCHOR_FIELDS = {
    "packet_sha256", "runner_revision", "runner_sha256", "selection_record",
}
_ANCHOR_RECORD_FIELDS = {"artifact_identity", "artifact_sha256", "observed_at", "source_provenance"}
_OBSERVATION_REQUIREMENTS = {
    "r1_fresh_source_readmission": ("true", "fresh-r1-source-readmission-missing"),
    "r2_accepted_source_transaction": ("true", "r2-accepted-source-transaction-missing"),
    "r2_complete_publication_vector": ("true", "r2-complete-publication-vector-missing"),
    "r2_protected_ack": ("true", "r2-protected-ack-missing"),
    "resources_and_capacity_selected": ("true", "current-workspace-capacity-lakehouse-selection-missing"),
    "principals_and_authorities_verified": ("true", "current-principal-and-owner-authority-missing"),
    "free_capacity_proves_zero_cost": ("true", "free-capacity-zero-cost-proof-missing"),
    "candidate_table_paths_absent": ("true", "read-only-candidate-table-path-absence-missing"),
    "candidate_display_names_absent": ("true", "read-only-candidate-display-name-absence-missing"),
    "runtime_and_numeric_limits_match": ("true", "current-runtime-and-finite-limits-unverified"),
    "cost_bound_usd": ("zero-integer", "fabric-total-cost-bound-is-not-proven-zero"),
}


def _valid_digest(value) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _valid_record(record, fields) -> bool:
    if not isinstance(record, Mapping) or set(record) != fields:
        return False
    if any(type(record.get(key)) is not str or not record[key].strip()
           for key in ("artifact_identity", "observed_at", "source_provenance")):
        return False
    if not _valid_digest(record.get("artifact_sha256")):
        return False
    try:
        observed_at = datetime.fromisoformat(record["observed_at"].replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False
    return observed_at.tzinfo is not None and observed_at.utcoffset() is not None


def _plan_candidates(packet_text: str) -> dict:
    if type(packet_text) is not str or "Fresh original-commerce Fabric functional attempt plan" not in packet_text:
        raise ValueError("Retained fresh Fabric plan text required")
    matches = list(_CANDIDATE_ROW.finditer(packet_text))
    roles = [match.group(1).lower() for match in matches]
    if len(matches) != 2 or sorted(roles) != sorted(_ROLES):
        raise ValueError("Exactly one retained R1 and one retained R2 candidate row required")
    rows = {match.group(1).lower(): {
        "candidate_id": match.group(2),
        "paths": {"nodes": match.group(3), "edges": match.group(4)},
    } for match in matches}
    if "not an accepted source transaction, UMF admission, publication, or ACK" not in packet_text:
        raise ValueError("R2 preparation-only boundary missing from retained plan")
    return rows


def _overlap(left: str, right: str) -> bool:
    a, b = PurePosixPath(left), PurePosixPath(right)
    return a == b or a in b.parents or b in a.parents


def _selected_anchors(packet_text: str, runner_inputs: Mapping, anchors: Mapping, blockers: list[str]) -> None:
    if not isinstance(anchors, Mapping) or set(anchors) != _ANCHOR_FIELDS:
        blockers.append("independent-packet-and-runner-anchors-missing")
        return
    selection_record = anchors.get("selection_record")
    if not _valid_record(selection_record, _ANCHOR_RECORD_FIELDS):
        blockers.append("anchor-selection-provenance-record-invalid-or-missing")
    if not _valid_digest(anchors.get("packet_sha256")) or sha256(packet_text.encode("utf-8")).hexdigest() != anchors.get("packet_sha256"):
        blockers.append("retained-plan-sha256-does-not-match-independent-anchor")
    if (type(anchors.get("runner_revision")) is not str
            or _REVISION.fullmatch(anchors["runner_revision"]) is None
            or runner_inputs.get("runner_revision") != anchors.get("runner_revision")):
        blockers.append("runner-revision-does-not-match-independent-anchor")
    if (not _valid_digest(anchors.get("runner_sha256"))
            or runner_inputs.get("runner_sha256") != anchors.get("runner_sha256")):
        blockers.append("runner-sha256-does-not-match-independent-anchor")


def _observation_value(observations, name, expected_kind, blocker, blockers):
    record = observations.get(name) if isinstance(observations, Mapping) else None
    if not _valid_record(record, _OBSERVATION_FIELDS):
        blockers.append(blocker)
        return None
    value = record["value"]
    if expected_kind == "true":
        if type(value) is not bool or value is not True:
            blockers.append(blocker)
            return None
    elif expected_kind == "zero-integer":
        # bool is an int subclass; a truth value must never satisfy a cost cap.
        if type(value) is not int or value != 0:
            blockers.append(blocker)
            return None
    return value


def build_fabric_launch_readiness_report(
    *, packet_text: str, runner_inputs: Mapping, current_observations: Mapping,
    independent_anchors: Mapping,
) -> dict:
    """Return a fail-closed checklist from already supplied values only.

    Anchor selections and observation records are caller-supplied and remain
    unauthenticated assertions. Matching their digests checks correspondence,
    not provenance, authority, or service state. This function is always blocked
    from launch and never resumes an R2 candidate.
    """
    if not isinstance(runner_inputs, Mapping) or not isinstance(current_observations, Mapping):
        raise ValueError("Runner selection and current observations must be mappings")
    candidates = _plan_candidates(packet_text)
    blockers: list[str] = []
    _selected_anchors(packet_text, runner_inputs, independent_anchors, blockers)

    selected_ids = runner_inputs.get("candidate_ids")
    selected_paths = runner_inputs.get("candidate_paths")
    if not isinstance(selected_ids, Mapping) or not isinstance(selected_paths, Mapping):
        blockers.append("runner-candidate-selection-missing")
        selected_ids, selected_paths = {}, {}

    all_paths: list[str] = []
    for role in _ROLES:
        expected = candidates[role]
        if selected_ids.get(role) != expected["candidate_id"]:
            blockers.append(f"{role}-runner-candidate-does-not-match-retained-plan")
        role_paths = selected_paths.get(role)
        if not isinstance(role_paths, Mapping) or any(type(role_paths.get(key)) is not str for key in _TABLE_ROLES):
            blockers.append(f"{role}-runner-paths-do-not-match-retained-plan")
            continue
        if any(role_paths[key] != expected["paths"][key] for key in _TABLE_ROLES):
            blockers.append(f"{role}-runner-paths-do-not-match-retained-plan")
        all_paths.extend(role_paths[key] for key in _TABLE_ROLES)
    if len(all_paths) == 4 and any(
        _overlap(all_paths[i], all_paths[j])
        for i in range(4) for j in range(i + 1, 4)
    ):
        blockers.append("candidate-path-collision")

    known_intent_states = {"absent", "recorded", "unknown", "creating", "created"}
    intent_state = runner_inputs.get("r2_intent_state")
    state_known = type(intent_state) is str and intent_state in known_intent_states
    if not state_known:
        blockers.append("r2-intent-state-missing-or-unknown-preserve-and-reconcile")
    ambiguous = state_known and intent_state in {"unknown", "creating"}
    if ambiguous:
        blockers.append("r2-intent-ambiguous-preserve-and-reconcile-original-intent")

    for name, (kind, blocker) in _OBSERVATION_REQUIREMENTS.items():
        _observation_value(current_observations, name, kind, blocker, blockers)

    if runner_inputs.get("r2_source_state", "preparation") != "preparation":
        blockers.append("r2-authored-candidate-must-remain-preparation-only")
    # Even well-formed records supplied by the caller are not authenticated by
    # this offline report and cannot satisfy the native authority/launch gate.
    blockers.append("caller-supplied-preflight-records-are-unauthenticated")
    blockers = list(dict.fromkeys(blockers))
    return {
        "format": "ashlar-fabric-launch-readiness/0.2",
        "readiness": "blocked-unverified-checklist",
        "launch_authorized": False,
        "automatic_approval": False,
        "anchor_status": "caller-supplied-match-check-only; unauthenticated",
        "observation_status": "typed-claims-only; unauthenticated; no live service reads performed",
        "candidates": candidates,
        "r1_status": "requires-fresh-live-source-readmission",
        "r2_status": "preparation-only; accepted source/publication/ACK are separate prerequisites",
        "r2_resume": ("refused-ambiguous-intent" if ambiguous else
                      "refused-unknown-intent-state" if not state_known else
                      "not-performed-by-readiness-report"),
        "preserve_existing_intents": ambiguous or not state_known,
        "blockers": blockers,
    }
