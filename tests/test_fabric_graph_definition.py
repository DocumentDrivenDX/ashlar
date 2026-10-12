"""Basic local Fabric definition mapping and command tests; no engine I/O."""
import base64,contextlib,io,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar import cli
from ashlar.graph_release import release_columns
from ashlar_host.fabric_graph_definition import make_fabric_graph_definition,prepare_fabric_graph_definition

ROOT='abfss://11111111-1111-1111-1111-111111111111@onelake.dfs.fabric.microsoft.com/22222222-2222-2222-2222-222222222222/Tables/'
class Tests(unittest.TestCase):
 def test_full_canonical_properties_keys_and_endpoints(self):
  docs=make_fabric_graph_definition(ROOT+'release_1_nodes',ROOT+'release_1_edges')
  graph=docs['graphType.json'];node=graph['nodeTypes'][0];edge=graph['edgeTypes'][0]
  self.assertEqual(node['primaryKeyProperties'],['graph_id'])
  for kind,definition in (('node',node),('edge',edge)):
   self.assertEqual([p['name']for p in definition['properties']],list(release_columns(kind)))
   self.assertTrue(all(p['type']=='STRING'for p in definition['properties']))
  self.assertEqual((len(node['properties']),len(edge['properties'])),(18,23))
  mapping=docs['graphDefinition.json']['edgeTables'][0]
  self.assertEqual(mapping['sourceNodeKeyColumns'],['src']);self.assertEqual(mapping['destinationNodeKeyColumns'],['dst'])
  self.assertIn({'propertyName':'graph_id','sourceColumn':'graph_id'},mapping['propertyMappings'])
 def test_malformed_alias_and_colliding_paths_refuse(self):
  for nodes,edges in (('https://example.com/nodes',ROOT+'release_edges'),(ROOT+'latest',ROOT+'release_edges'),(ROOT+'release_nodes',ROOT+'release_nodes')):
   with self.assertRaises(ValueError):make_fabric_graph_definition(nodes,edges)
 def test_public_command_readable_files_and_rest_parts_no_overwrite(self):
  with tempfile.TemporaryDirectory()as tmp:
   output=Path(tmp).resolve()/'definition';summary=io.StringIO()
   argv=['ashlar','prepare-fabric-graph-definition','--nodes',ROOT+'release_nodes','--edges',ROOT+'release_edges','--output',str(output)]
   with patch('sys.argv',argv),contextlib.redirect_stdout(summary):cli.main()
   self.assertEqual(json.loads(summary.getvalue())['node_properties'],18)
   definition=json.loads((output/'definition.json').read_bytes());self.assertEqual(definition['format'],'json')
   self.assertEqual({part['path']for part in definition['parts']},{'dataSources.json','graphDefinition.json','graphType.json','stylingConfiguration.json'})
   for part in definition['parts']:
    self.assertEqual(part['payloadType'],'InlineBase64');self.assertEqual(base64.b64decode(part['payload']), (output/part['path']).read_bytes())
   self.assertEqual(len(list(output.iterdir())),5)
   before=(output/'definition.json').read_bytes()
   with self.assertRaises(FileExistsError):prepare_fabric_graph_definition(ROOT+'release_nodes',ROOT+'release_edges',output)
   self.assertEqual((output/'definition.json').read_bytes(),before)

 def test_display_name_command_emits_complete_create_request(self):
  with tempfile.TemporaryDirectory()as tmp:
   output=Path(tmp).resolve()/'definition'
   argv=['ashlar','prepare-fabric-graph-definition','--nodes',ROOT+'release_nodes','--edges',ROOT+'release_edges','--output',str(output),'--display-name','Ashlar release 1']
   with patch('sys.argv',argv),contextlib.redirect_stdout(io.StringIO()):cli.main()
   request=json.loads((output/'create-request.json').read_bytes())
   self.assertEqual(set(request),{'displayName','definition'})
   self.assertEqual(request['displayName'],'Ashlar release 1')
   self.assertEqual(request['definition'],json.loads((output/'definition.json').read_bytes()))
 def test_invalid_display_name_refuses_before_output_creation(self):
  with tempfile.TemporaryDirectory()as tmp:
   output=Path(tmp).resolve()/'definition'
   for name in ('','   ','bad\nname'):
    with self.assertRaises(ValueError):prepare_fabric_graph_definition(ROOT+'release_nodes',ROOT+'release_edges',output,display_name=name)
    self.assertFalse(output.exists())
