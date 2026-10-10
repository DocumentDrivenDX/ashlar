"""Compatibility preparation only; no source/native authority is established.

Installed operators must supply their independent policy to prepare_carrier or
use prepare-puppy-release. This legacy tools interface checks exact original
canonical correspondence only, preserving its historical qualification.
"""
import json
from ashlar_host.puppy_carrier import prepare_carrier
from ashlar_host.puppy_release_document import load_graph_release
from ashlar_host.puppy_release_cli import directory_identity, retain


class OriginalCorrespondenceOnly:
    def admit_original_release(self, release, context):
        load_graph_release(release.payload, release.sha256)


def prepare(payload, trusted_sha256, output):
    receipt=prepare_carrier(payload,trusted_sha256,output,
        policy=OriginalCorrespondenceOnly(),context=None)
    from pathlib import Path
    output=Path(output)
    retain(output,'receipt.json',(json.dumps(receipt,indent=2)+'\n').encode(),directory_identity(output))
    return receipt
