import hashlib
import json
import subprocess
from pathlib import Path

ROOT = Path('/Users/erik/Projects/ashlar')
PATHS = [
    'docs/helix/02-design/contracts/CONTRACT-001-publication-boundary.md',
    'docs/helix/02-design/contracts/CONTRACT-003-delta-graph-tables.md',
    'docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md',
    'docs/helix/02-design/technical-designs/TD-001-publication-recovery.md',
    'docs/helix/03-test/consumer-conformance-plan.md',
]

def pin(path):
    path = Path(path)
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

opening = [pin(ROOT / rel) for rel in PATHS]
head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
for rel in PATHS:
    before = subprocess.check_output(['git', 'show', 'HEAD:' + rel], cwd=ROOT)
    after = (ROOT / rel).read_bytes()
    assert before.split(b'---', 2)[:2] == after.split(b'---', 2)[:2]
    lines = iter(after.splitlines())
    for old_line in before.splitlines():
        assert any(line == old_line for line in lines), rel
subprocess.run(['git', 'diff', '--check', '--', *PATHS], cwd=ROOT, check=True, capture_output=True)
closing = [pin(ROOT / rel) for rel in PATHS]
assert opening == closing

receipt = {
    'status': 'approved-five-file-desired-state-evolution-slice',
    'scope': 'Governing boundary, finite source profile, architecture ownership, precise recovery correspondence and test requirements only. No source implementation, installed workflow, native execution or mechanical formal assurance approval.',
    'repository_head': head,
    'files': closing,
    'opening_closing_equal': True,
    'checks': {
        'all_prior_document_lines_preserved_in_order': True,
        'frontmatter_and_deliberate_traceability_unchanged': True,
        'diff_check_exit': 0,
        'source_or_native_tests_executed_by_reviewer': False,
    },
    'resolved_findings': [
        'Installed API has no source provisioning, role/grant creation, Docker credential discovery or privilege escalation port.',
        'Fresh configuration supplies planned layout and independent authority; actual native UUIDs are established and retained during admitted initialization. Resume binds the original actual registry.',
        'Resume never replaces submitted/uncertain intent. An originally unsubmitted ordinal in the complete retained plan may receive its first submission under renewed admission.',
        'A report failure does not imply rollback of an already committed publication or ACK.',
        'CONTRACT003 names source-epoch-qualified/0.1, eight distinct exact tuple batch identities, unchanged original event/delivery/digest bytes and recomputed inner byte cursors.',
        'The finite profile explicitly retains R2 price12.75, R3 fulfillment/incident-edge deletion and R4 Field35 absent/present augmentation with the new independent supplier edge.',
        'Outer PostgreSQL source positions and independently zero-based inner JSONL byte cursors remain separate; neither is inferred from or substituted for the other.',
    ],
    'owning_contract_map': {
        'CONTRACT-001': 'Fresh/resume APIs, eight configuration ownership groups, authority/session separation, retained original plan/journal and success-release rules.',
        'CONTRACT-003': 'Exact named original0.8 source/materialization profile, source-qualified envelopes and independent complete prefix oracle.',
        'ADR-001': 'Portable preparation and host admission/orchestration ownership, package resources, typed config/construction and actual dependency-checker adoption.',
        'TD-001': 'Retained request/ordered effects/prior anchors/clock/registry process-resume design and PUB-F1-F4/L1 correspondence.',
        'TP-001': 'All eight original prefix comparisons, ordinary source ACK readback, installed fresh-process resume, uncertain/missing custody and cancellation/cleanup controls.',
    },
    'finite_profile': {
        'order': ['A1', 'B1', 'A2', 'B2', 'A3', 'B3', 'A4', 'B4'],
        'prefixes': [[1,0], [1,1], [2,1], [2,2], [3,2], [3,3], [4,3], [4,4]],
        'final_counts_inventory_only': {'object_current':22, 'edge_current':20, 'tombstone':4, 'whole_source_history':90},
        'final_source_heads': [4,4],
        'required': 'Full per-prefix original current/history/tombstone cells, exact source bytes, revisions, global ancestry, UUID/version vectors, source-qualified IDs and progress, all8 protected receipts, replay identity and historicalR1 correspondence.',
        'producer': 'Separate exact public UMF e44cd15f336dfb33db35acf20eee13dd120a1a28 profile and expected-source verification; no latest-version equivalence or receipt-only admission.',
    },
    'helix_0154_disposition': {
        'modularity': 'Pure standard-library core; host-owned external integration and explicit authority ports; installed resources replace checkout discovery, and actual checker changes/negative controls are required.',
        'configuration': 'Distinct immutable fresh/resume types, one declared owner/source per group, no operator secret defaults or ambient runtime selection, portable3.9 and selectedhost3.11 retained.',
        'observability': 'CONTRACT006 public sanitized composition, independently observed phase/outcome/loss and primary-cancellation preservation; telemetry does not confer authority or commit truth.',
        'formal': 'Precise PUB safety/liveness correspondence and explicit retained-input/current-authority/termination assumptions; source tests or old same-process evidence cannot establish fresh-process recovery or mechanical proof.',
    },
    'review_inputs': [pin('/private/tmp/astra-installed-commerce-evolution-design-review-20261010-a.json'), pin('/private/tmp/astra-installed-commerce-evolution-design-addendum-20261010-a.json'), pin('/private/tmp/astra-installed-commerce-evolution-recovery-design-20261010-a.json')],
    'remaining_findings': [],
    'not_modified': 'No production source or governing document edited by reviewer. Foreign end-to-end-plan preserved.',
    'reviewer_script': pin(__file__),
}
output = Path('/private/tmp/astra-installed-commerce-evolution-governance-review-20261010-a.json')
with output.open('x') as stream:
    json.dump(receipt, stream, indent=2)
    stream.write('\n')
print(json.dumps(dict(status=receipt['status'], receipt=pin(output), files=closing)))
