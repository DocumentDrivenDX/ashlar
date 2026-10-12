# Original commerce export

The functional export ran successfully against the existing original commerce publication using main `10e140e880bdfe21527f9d05be460c223998fd7c`, local Spark[1], 512 MiB, and the retained SDK/runtime selections. The process exited 0 within the selected 120-second limit after its ordinary source/ACK reader and Spark interval closed. This is source-mode workflow evidence, not a new installed-package qualification. All five automatic CI workflows passed for that commit.

The unchanged original files are retained locally, contain 11 nodes and 10 edges, and are excluded from the repository:

- `release.graph.json`: SHA256 `d1b058741d7875a5e0b6173d9cf473ecf9979aeaf57f46062ff0a327674268e4`.
- `custody.json`: SHA256 `958048f374f3ad729017a5d88ef2524d3280475cfd71477b1c543417024d7ef6`.

## Use the Fabric notebook

Generate a private notebook with `examples/fabric-gql/prepare-commerce-notebook.py`, selecting the export paths, the hashes above, explicit node/edge OneLake paths, and a fresh output. The ready local notebook is `/private/tmp/ashlar-commerce-fabric-export-20261012-a/delivery/commerce-to-fabric.ipynb`. Import that notebook into the existing **Ashlar Graph Integration** workspace and attach **Ashlar_Graph_Releases**. Run its two code cells when trial capacity is available. The first installs Ashlar from the exact tested source commit; the second embeds the original export files and independently selected hashes, uses the notebook's existing Spark session, and writes fresh OneLake tables `dbo.commerce_d1b05874_nodes` and `dbo.commerce_d1b05874_edges`. Existing destinations are refused. It then creates local graph-definition files and a create-model request named `Ashlar_Commerce_d1b05874`; the output prints their paths.

The notebook Python cell compiles locally. It has not executed in Fabric: the lakehouse returned “Capacity limit exceeded.” No graph model has been created or loaded for this export. The file-use owner supports this local development export only; it does not grant continued live source availability, canonical publication queries, or loaded Fabric graph custody. The next cloud step is to execute the notebook, create/load the generated model, and query its actual data.
