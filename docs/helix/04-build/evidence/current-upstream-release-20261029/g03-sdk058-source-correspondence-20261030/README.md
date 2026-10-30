---
ddx:
  id: ashlar.evidence.g03-sdk058-source-correspondence-20261030
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk058-delta4-realization-candidate-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# UMF SDK 0.5.8 typed-graph source correspondence addendum

This addendum leaves the reviewed [`g03-sdk058-delta4-realization-candidate-20261030`](../g03-sdk058-delta4-realization-candidate-20261030/) packet unchanged. It references that packet's frozen G03 model, configuration, binding, source stream, request, historical 2ede receipt, and six-role carrier comparison rather than copying them.

The focused runner executes the Ashlar-owned full typed-graph correspondence checker using the exact retained UMF SDK 0.5.8 distribution. It binds the historical source configuration to the SDK using a separate versioned admission manifest. The release receipt preserves all three full result groups from the 2ede receipt while carrying the explicit SDK entry-hash producer token. It checks raw event bytes and digests, source digest, batch cursors, object/edge structure, endpoints, public Core record results, complete compact dataset results, keys, and relationships.

This is effect-free local release qualification. It does not authenticate the producer, establish native Spark/Delta execution, production source authority, catalog/publication authority, or ACK custody. The archive producer commit `855f6c...` is an ancestor of selected tag commit `efb404...`, and remains a distinct commit identity.

Run from the repository root with `PYTHONPATH=src`, a retained SDK runtime root, the pinned Node binary, and a new private output directory:

```sh
PYTHONPATH=src .venv/bin/python docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-source-correspondence-20261030/qualify_source_correspondence.py \
  --runtime-root /path/to/g03-sdk058-local-producer-actual-002 \
  --node /opt/homebrew/Cellar/node/26.12.1/bin/node \
  --output-dir /private/tmp/ashlar-g03-sdk058-source-correspondence-run
```

The runner refuses a pre-existing output directory. The captured receipt, comparison result, and additive closure are retained under `outputs/`.
