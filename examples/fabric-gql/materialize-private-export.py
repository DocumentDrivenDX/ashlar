# SPDX-License-Identifier: MIT OR Apache-2.0
"""Notebook template for explicit LOCAL DEVELOPMENT use of a CLI export pair.

Use existing Spark with normal Delta support; do not create a second Spark
session in a Fabric notebook. Replace paths and independently selected digests.
No continued PostgreSQL/native publication availability is asserted.
"""
from pathlib import Path
from ashlar_host.local_graph_export import LocalGraphExportSelection,open_local_graph_export
from ashlar_host.graph_release_delta import materialize_private_graph_release_delta
from ashlar_host.fabric_graph_definition import prepare_fabric_graph_definition

# Copy these SHA256 selections from the admitted export-original-commerce summary,
# not from an untrusted file's freshly computed hash.
selection = LocalGraphExportSelection(
    Path('/absolute/path/to/export/release.graph.json'),
    Path('/absolute/path/to/export/custody.json'),
    'REPLACE_WITH_SELECTED_RELEASE_SHA256',
    'REPLACE_WITH_SELECTED_CUSTODY_SHA256')
NODES = 'abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/release_1_nodes'
EDGES = 'abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/release_1_edges'
DEFINITION_OUTPUT = '/absolute/path/to/fresh-local-definition'

with open_local_graph_export(selection) as session:
    receipt = materialize_private_graph_release_delta(
        spark, session.release.payload, session.release.sha256,
        custody_payload=session.custody_bytes,
        trusted_custody_sha256=session.custody_sha256,
        source=session.source, node_path=NODES, edge_path=EDGES,
        policy=session.policy, context=session.context)

# Only after both carrier bags and the ordinary local file-use owner close:
definition = prepare_fabric_graph_definition(NODES, EDGES, DEFINITION_OUTPUT)
print(receipt)
print(definition)
# Loading/querying a Fabric graph still needs its actual engine/authentication owner.
