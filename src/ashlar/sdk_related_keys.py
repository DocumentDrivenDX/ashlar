"""Typed transport of original admitted Ashlar related-Key occurrences.

This conversion neither establishes publication authority nor reinterprets full
bag/truncation truth. Those remain with the original held Ashlar policy.
"""

from umf_access.codec_support import validate_graph
from umf_access.dsl import RelatedKeys
from umf_access.related_keys import validate_decoder

from .publication import ResolutionError
from .weft_related_keys_decode import (
    DecodedRelatedKeys,
    RelatedKeysDecodeConfig,
    decode_related_keys,
)


def typed_related_keys(value, decoder, key_model):
    """Preserve original String occurrences, including repeated equal Keys."""
    metadata = validate_decoder(decoder, key_model)
    if (
        metadata["profile"] != "ashlar-related-keys-string/0.1"
        or type(value) is not DecodedRelatedKeys
    ):
        raise ResolutionError("Original Ashlar related-Key carrier required")
    checked = decode_related_keys(
        metadata["representation"],
        value.original,
        model_pins=metadata["modelPins"],
        config=RelatedKeysDecodeConfig(8 * 1024 * 1024),
    )
    if checked != value:
        raise ResolutionError("Original Ashlar decoded related-Key bytes differ")
    fields = key_model.UMF_FIELDS
    items = tuple(
        validate_graph(
            key_model,
            {
                name: component
                for (_, _, name), component in zip(fields, row, strict=True)
            },
        )
        for row in value.items
    )
    return RelatedKeys(items, value.truncated)
