# SPDX-License-Identifier: MIT OR Apache-2.0
"""Prepare a local Fabric notebook from an explicitly selected export pair.

The generated notebook contains the selected data. Keep it private and upload
only to the intended workspace. This command does not contact any service.
"""
import argparse
import base64
import json
import os
from pathlib import Path
from ashlar_host.local_graph_export import LocalGraphExportSelection, open_local_graph_export
from ashlar_host.fabric_graph_definition import prepare_fabric_graph_definition
import tempfile
from .lifecycle import finish

SOURCE_COMMIT = '12f26f03638b4d603d5aaea4444e26cbeb036e92'

def prepare_fabric_notebook(selection, nodes, edges, output, display_name):
    output = Path(output)
    if not output.is_absolute() or output.resolve() != output:
        raise ValueError('Canonical absolute notebook output required')
    if output.exists():
        raise FileExistsError('Fresh notebook output required')
    if output in (selection.release_path, selection.custody_path):
        raise ValueError('Notebook must be separate from export files')
    # Validate the actual mapping arguments locally, without opening Spark.
    with tempfile.TemporaryDirectory() as work:
        prepare_fabric_graph_definition(nodes, edges, Path(work).resolve() / 'definition', display_name=display_name)
    with open_local_graph_export(selection) as session:
        release = session.release.payload
        custody = session.custody_bytes
    code = '''import base64, tempfile
from pathlib import Path
from ashlar_host.local_graph_export import LocalGraphExportSelection, open_local_graph_export
from ashlar_host.graph_release_delta import materialize_private_graph_release_delta
from ashlar_host.fabric_graph_definition import prepare_fabric_graph_definition
root = Path(tempfile.mkdtemp(prefix='ashlar-graph-')).resolve()
'''
    for name, raw in [('release.graph.json', release), ('custody.json', custody)]:
        code += f'(root / {name!r}).write_bytes(base64.b64decode({base64.b64encode(raw).decode()!r}))\n'
    code += f'''selection = LocalGraphExportSelection(root / 'release.graph.json', root / 'custody.json',
 {selection.release_sha256!r}, {selection.custody_sha256!r})
NODES = {nodes!r}
EDGES = {edges!r}
with open_local_graph_export(selection) as session:
 receipt = materialize_private_graph_release_delta(spark,
  session.release.payload, session.release.sha256,
  custody_payload=session.custody_bytes, trusted_custody_sha256=session.custody_sha256,
  source=session.source, node_path=NODES, edge_path=EDGES,
  policy=session.policy, context=session.context)
definition = prepare_fabric_graph_definition(NODES, EDGES, root / 'definition', display_name={display_name!r})
print(receipt)
print(definition)
'''
    compile(code, 'fabric-notebook-cell', 'exec')
    def cell(kind, source):
        value = {'cell_type': kind, 'metadata': {}, 'source': source.splitlines(keepends=True)}
        if kind == 'code': value.update(execution_count=None, outputs=[])
        return value
    notebook = {'nbformat': 4, 'nbformat_minor': 5,
        'metadata': {'ashlar': {'source_commit': SOURCE_COMMIT, 'release_sha256': selection.release_sha256, 'custody_sha256': selection.custody_sha256, 'custody_profile': json.loads(custody)['format']}, 'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'}},
        'cells': [cell('markdown', '# Graph export to Fabric\nAttach the intended lakehouse. Fresh destinations only. This notebook contains selected export data; keep it private. It materializes tables and prepares a model definition, but does not create/load a model or establish continued source availability.\n'),
            cell('code', f'%pip install https://github.com/DocumentDrivenDX/ashlar/archive/{SOURCE_COMMIT}.zip\n'),
            cell('code', code)]}
    # Publish the complete private notebook atomically, without replacing a path.
    temporary=None;primary=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=output.parent,prefix='.ashlar-notebook-',delete=False)as stream:
            temporary=Path(stream.name)
            json.dump(notebook,stream,ensure_ascii=False)
        os.link(temporary,output)
    except BaseException as error:primary=error
    finish(primary,([temporary.unlink]if temporary is not None else []))
    return output

