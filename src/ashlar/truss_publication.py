"""Publish a complete native Truss page through the durable coordinator."""
from .source_checkpoint import truss_checkpoint


def publish_truss_transaction(backend,stream,batch,*,predecessor,schema_revisions_json,context):
    """Keep opaque Truss checkpoints in every durable publication phase.

    The backend owns native source admission, serialized Delta effects, immutable
    publication and public Truss ACK. This entrypoint supplies no default policy.
    """
    from .publisher import publish_batch
    checkpoint=truss_checkpoint(batch)
    return publish_batch(backend,stream,batch,predecessor=predecessor,
        schema_revisions_json=schema_revisions_json,context=context,
        source_checkpoint_json=checkpoint)
