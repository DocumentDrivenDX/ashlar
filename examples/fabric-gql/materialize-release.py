"""Notebook template: supply real release admission and fresh OneLake paths.

This file does not acquire credentials or create a graph model. The notebook
owns its Spark session. `my_operator` is your actual deployment integration.
"""
from ashlar_host.graph_release_delta import materialize_graph_release_delta
from ashlar_host.fabric_graph_definition import prepare_fabric_graph_definition
from my_operator import open_admitted_public_graph_release

# Replace with two distinct, nonoverlapping immutable release destinations.
NODES = 'abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/release_1_nodes'
EDGES = 'abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/release_1_edges'
DEFINITION_OUTPUT = '/absolute/path/to/fresh-local-definition'

with open_admitted_public_graph_release() as session:
    # session.release is the original public canonical GraphRelease from your
    # current read owner. session.policy.admit_original_release(release,context)
    # independently admits its actual source/publication, not just its hash.
    release = session.release
    receipt = materialize_graph_release_delta(
        spark, release.payload, release.sha256,  # existing notebook Spark session
        node_path=NODES, edge_path=EDGES,
        policy=session.policy, context=session.context)

# Only after both full STRING Delta bags and original admission have succeeded:
definition = prepare_fabric_graph_definition(NODES, EDGES, DEFINITION_OUTPUT)
print(receipt)
print(definition)
# Review/deploy the definition through your actual Fabric operator separately.
