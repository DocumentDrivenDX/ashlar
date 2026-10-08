"""Original raw intake proof inventory; no native admission or inferred compatibility."""
from ashlar.publisher import PublicationError


def bind_intake_proofs(intakes, proofs, namespace):
    if not intakes or len(intakes) != len(proofs):
        raise PublicationError('Complete original intake proof inventory required')
    selected = {}; registry = None
    for intake, proof in zip(intakes, proofs):
        if (intake.source_sha256, intake.artifact_sha256, intake.document_revision,
            intake.validator_revision) != (proof['source_sha256'], proof['artifact_sha256'],
                                          proof['revision'], proof['validator_revision']):
            raise PublicationError('Exact original native UMF intake proof required')
        target = (proof['table'], proof['table_uuid'])
        if len(intakes)>1 and (target[0] != namespace + '.schema_intake' or registry is not None and registry != target):
            raise PublicationError('Schema proofs require the same installed native registry')
        registry = target
        key = (intake.document_id, intake.document_revision)
        if key in selected:
            raise PublicationError('Duplicate original schema revision')
        selected[key] = (intake, proof)
    return tuple(selected.values())
