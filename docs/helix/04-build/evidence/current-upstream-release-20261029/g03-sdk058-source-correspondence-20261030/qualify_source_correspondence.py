#!/usr/bin/env python3
"""Run the SDK 0.5.8 typed-graph correspondence addendum into a fresh output dir."""
import argparse
import hashlib
import json
from pathlib import Path

from ashlar_host.typed_graph_sdk058_admission import (
    SDK058_ADMISSION_PROFILE,
    SDK058_ARCHIVE_SHA256,
    SDK058_ENTRY_SHA256,
    SDK058_PRODUCER_REVISION,
    TypedGraphSdk058AdmissionConfig,
    admit_typed_graph_sdk058,
)
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[5]
BASE = REPO / 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def pin(path, label, base):
    if path.is_symlink() or not path.is_file():
        raise SystemExit(f'not a regular file: {label}')
    raw = path.read_bytes()
    return {'base': base, 'path': label, 'sizeBytes': len(raw), 'sha256': sha(raw)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime-root', type=Path, required=True)
    parser.add_argument('--node', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    runtime = args.runtime_root.resolve(strict=True)
    node = args.node.resolve(strict=True)
    output = args.output_dir.resolve()
    if output.exists() or output.is_symlink():
        raise SystemExit('output directory must be new and private')
    output.mkdir(mode=0o700, parents=True)

    typed = BASE / 'inputs/typed-graph'
    config = load_typed_graph_jsonl_configuration(typed / 'configured-typed-graph-source.json')
    model = (typed / 'typed-graph-model.umf.json').read_bytes()
    admission = HERE / 'inputs/typed-graph/sdk058-source-admission.json'
    runtime_config = TypedGraphSdk058AdmissionConfig(
        node=node, sdk_package_root=runtime / 'sdk-install/node_modules/@umf/core',
        sdk_archive=runtime / 'sdk-distribution/umf-sdk.tgz',
        producer_closure=runtime / 'sdk-producer-closure.json',
        producer_receipt=runtime / 'sdk-producer-receipt.json',
        install_custody=runtime / 'sdk-install/g03-sdk-install-custody.json',
        source_admission_manifest=admission,
    )
    receipt = admit_typed_graph_sdk058(runtime_config, config, model)
    old = json.loads((BASE / 'outputs/old-2ede-receipt.json').read_bytes())
    request_raw = (BASE / 'outputs/typed-graph-request.json').read_bytes()
    request = json.loads(request_raw)
    release = json.loads(receipt)
    equal = release['groups'] == old['groups']
    if not equal or old['producerRevision'] != config.producer_revision:
        raise SystemExit('release correspondence differs from retained historical receipt')

    carrier = json.loads((BASE / 'outputs/carrier-proposals.json').read_bytes())
    expected_roles = ['object_current', 'edge_current', 'tombstone', 'whole_source_history',
                      'publication_manifest', 'publication_attempt_phase']
    bridge_path = Path(carrier['existingBridge']['path']).resolve(strict=True)
    bridge_sha = sha(bridge_path.read_bytes())
    if (carrier.get('sdk', {}).get('distribution') != 'UMF SDK 0.5.8'
            or carrier.get('sdk', {}).get('entrySha256') != SDK058_ENTRY_SHA256
            or bridge_sha != '34deb5c79b5dcb1ace8093e66a4b1213a98cbcc1fb50bfd24fdc4a21721c76b5'
            or carrier.get('existingBridge', {}).get('sha256') != bridge_sha
            or [item.get('role') for item in carrier.get('roles', [])] != expected_roles
            or not all(item.get('equal') is True for item in carrier['roles'])):
        raise SystemExit('selected SDK `.4` bridge comparison is not exactly pinned')
    implementation_files = [
        REPO / 'src/ashlar_host/typed_graph_sdk058_admission.py',
        REPO / 'src/ashlar_host/resources/typed_graph_values_sdk058.mjs',
        REPO / 'src/ashlar/typed_graph_source.py',
    ]
    qualification = {
        'profile': 'ashlar.g03-sdk058-source-correspondence-qualification/1',
        'status': 'passed',
        'additiveTo': str(BASE.relative_to(REPO)),
        'admissionProfile': SDK058_ADMISSION_PROFILE,
        'selectedSdk': {
            'distribution': '@umf/core 0.5.8', 'semanticCore': '0.10.0',
            'tagCommit': 'efb4042719866888ad80c01cb2da694972eb2738',
            'archiveProducerCommit': '855f6c48649496fc60b3303798790d0ddb55d38f',
            'archiveProducerTree': '53f2bfffbc0c5780e79d76d8fd3d202239d088b2',
            'archiveProducerCommitDistinctFromTag': True,
            'archiveSha256': SDK058_ARCHIVE_SHA256, 'installedEntrySha256': SDK058_ENTRY_SHA256,
        },
        'runtime': {'nodeVersion': 'v26.12.1', 'nodeSha256': '9af7e24478e538455cf4af53a2607f1b79304bd5e25458b041fb855c3c167991'},
        'typedGraph': {
            'sourceProducerRevision': old['producerRevision'],
            'releaseReceiptProducerRevision': SDK058_PRODUCER_REVISION,
            'groups': len(release['groups']),
            'events': sum(len(g['events']) for g in request['groups']),
            'allFullGroupResultsExactlyEqualToHistoricalReceipt': equal,
            'checks': 'Raw event bytes and digests, source digest, batch cursors, record/entity and endpoint structure, request identity, SDK Core record results, complete compact dataset results, keys and relationships.',
            'receiptSha256': sha(receipt), 'receiptBytes': len(receipt),
        },
        'carrier': {
            'roles': len(carrier['roles']),
            'allSixExistingBridgeProposalObjectsEqualReleaseSdk': all(item['equal'] for item in carrier['roles']),
            'existingBridgeSha256': bridge_sha,
        },
        'implementation': {str(path.relative_to(REPO)): sha(path.read_bytes()) for path in implementation_files},
        'authorityBoundary': 'Effect-free local release qualification only. It does not authenticate the producer, establish native Spark/Delta execution, production source authority, catalog/publication authority, or ACK custody.',
    }
    receipt_path = output / 'typed-graph-release-receipt.json'
    qualification_path = output / 'source-correspondence-qualification.json'
    receipt_path.write_bytes(receipt)
    qualification_path.write_bytes(json.dumps(qualification, indent=2, sort_keys=True).encode() + b'\n')

    baseline_refs = [
        'inputs/typed-graph/configured-typed-graph-source.json',
        'inputs/typed-graph/typed-graph-source.jsonl',
        'inputs/typed-graph/typed-graph-binding.json',
        'inputs/typed-graph/typed-graph-model.umf.json',
        'outputs/typed-graph-request.json', 'outputs/old-2ede-receipt.json',
        'outputs/carrier-proposals.json',
    ]
    refs = [pin(BASE / rel, str(BASE.relative_to(REPO) / rel), 'repo') for rel in baseline_refs]
    refs.append(pin(bridge_path, str(bridge_path.relative_to(REPO)), 'repo'))
    pins = [
        pin(admission, str(admission.relative_to(REPO)), 'repo'),
        pin(HERE / 'README.md', str((HERE / 'README.md').relative_to(REPO)), 'repo'),
        pin(HERE / 'qualify_source_correspondence.py', str((HERE / 'qualify_source_correspondence.py').relative_to(REPO)), 'repo'),
        *[pin(path, str(path.relative_to(REPO)), 'repo') for path in implementation_files],
        pin(receipt_path, str(receipt_path.relative_to(output)), 'output'),
        pin(qualification_path, str(qualification_path.relative_to(output)), 'output'),
    ]
    closure = {'profile': 'ashlar.g03-sdk058-source-correspondence-closure/1', 'status': 'passed',
               'addendumFilePins': pins, 'priorPacketReferences': refs, 'selfPin': 'omitted by design'}
    (output / 'source-correspondence-closure.json').write_bytes(json.dumps(closure, indent=2, sort_keys=True).encode() + b'\n')
    print(json.dumps({'status': 'passed', 'groups': qualification['typedGraph']['groups'],
                      'events': qualification['typedGraph']['events'], 'receiptSha256': sha(receipt),
                      'carrierRoles': qualification['carrier']['roles'],
                      'allCarrierProposalsEqual': qualification['carrier']['allSixExistingBridgeProposalObjectsEqualReleaseSdk']}))


if __name__ == '__main__':
    main()
