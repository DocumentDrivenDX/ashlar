# SPDX-License-Identifier: MIT OR Apache-2.0
"""Compatibility wrapper; use installed ashlar prepare-fabric-notebook."""
import argparse
from pathlib import Path
from ashlar_host.local_graph_export import LocalGraphExportSelection
from ashlar_host.fabric_notebook import prepare_fabric_notebook as prepare

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('release', 'custody', 'release-sha256', 'custody-sha256', 'nodes', 'edges', 'output', 'display-name'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    print(prepare(LocalGraphExportSelection(Path(args.release),Path(args.custody),args.release_sha256,args.custody_sha256),
                  args.nodes,args.edges,args.output,args.display_name))
