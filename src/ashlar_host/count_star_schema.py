"""Offline public-schema validation for the explicit path admission boundary.

The caller supplies the exact three schema byte strings. The owning admission
port verifies their pins; this adapter validates with Draft 2020-12 using only
in-memory resources. It performs no default insertion, value coercion or format
assertion. Artifact resource limits and isolated input snapshots belong to the
calling admission boundary, not to this schema adapter.
"""
import json
from typing import Mapping

from .count_star_admission import CountStarPlanError, CountStarSchemaValidation


_SCHEMA_BASE = 'https://github.com/DocumentDrivenDX/weft/raw/main/docs/helix/02-design/contracts/'
_DRAFT = 'https://json-schema.org/draft/2020-12/schema'
_REFUSAL = 'Pinned public schema validation refused'


def make_offline_count_star_schema_validation(schemas: Mapping[str, bytes]) -> CountStarSchemaValidation:
    """Construct the actual validator port from held, pinned schema bytes.

    The returned callback accepts the owning port's three-argument contract:
    complete immutable bundle, exact root filename and an owned candidate.
    Unknown roots or changed bundles refuse. No path or URI is ever retrieved.
    Missing validator dependencies refuse construction without a fallback.
    """
    validators = {}

    def validate(bundle, root_filename, candidate):
        try:
            if (type(root_filename) is not str or root_filename not in validators
                    or set(bundle) != set(port.schemas)
                    or any(type(bundle[name]) is not bytes or bundle[name] != raw
                           for name, raw in port.schemas.items())):
                raise CountStarPlanError(_REFUSAL)
            validators[root_filename].validate(candidate)
        except Exception:
            raise CountStarPlanError(_REFUSAL) from None

    try:
        # Construct the owning port before parsing or using any schema bytes.
        port = CountStarSchemaValidation(schemas, validate)
        from jsonschema import Draft202012Validator
        from referencing import Registry
        from referencing.exceptions import NoSuchResource
        from referencing.jsonschema import DRAFT202012

        def refuse_retrieval(uri):
            raise NoSuchResource(ref=uri)

        documents = {}
        resources = []
        for name, raw in port.schemas.items():
            document = json.loads(raw)
            if document.get('$id') != _SCHEMA_BASE + name or document.get('$schema') != _DRAFT:
                raise CountStarPlanError(_REFUSAL)
            documents[name] = document
            resources.append((document['$id'], DRAFT202012.create_resource(document)))
        registry = Registry(retrieve=refuse_retrieval).with_resources(resources)
        validators.update({name: Draft202012Validator(document, registry=registry,
                                                      format_checker=None)
                           for name, document in documents.items()})
    except Exception:
        raise CountStarPlanError(_REFUSAL) from None
    return port
