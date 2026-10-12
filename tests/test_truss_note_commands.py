import io,json,unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from ashlar.cli import main

class Tests(unittest.TestCase):
    def dispatch(self,command,module,function):
        output=io.StringIO()
        with patch('sys.argv',['ashlar',command,'--configuration','/private/config.json','--output','/private/result.json']),patch('ashlar_host.'+module+'.'+function,return_value={'private_native_carrier':'not for stdout'})as run,redirect_stdout(output):
            main()
        run.assert_called_once_with('/private/config.json','/private/result.json')
        self.assertEqual(json.loads(output.getvalue()),{'output':'/private/result.json'})
        self.assertNotIn('private_native_carrier',output.getvalue())
    def test_mutation_command_dispatches_selected_public_host(self):
        self.dispatch('mutate-truss-note','truss_note_mutation','run_mutate_truss_note_command')
    def test_evolution_command_dispatches_selected_public_host(self):
        self.dispatch('evolve-truss-note','truss_note_evolution','run_evolve_truss_note_command')

    def test_graph_creation_command_dispatches_selected_public_host(self):
        self.dispatch('mutate-truss-graph','truss_graph_mutation','run_mutate_truss_graph_command')
    def test_graph_change_command_dispatches_selected_public_host(self):
        self.dispatch('change-truss-graph','truss_graph_change','run_change_truss_graph_command')

    def test_schema_acceptance_command_dispatches_selected_public_host(self):
        self.dispatch('accept-truss-schema','truss_schema_acceptance','run_accept_truss_schema_command')
