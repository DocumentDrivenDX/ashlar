from pathlib import Path
import sys
import tempfile
import unittest

TOOLS = Path(__file__).resolve().parents[1] / 'tools'
sys.path.insert(0, str(TOOLS))
from durable_sql import DurableSQL, SQLCustodyError, SQLPending
from materialization_clock import materialization_clock


class API:
    def __init__(self):
        self.calls = []
        self.state = 'SUCCEEDED'
        self.value = '1791478800123456'
        self.workload = 'a' * 64
        self.lost = False

    def do(self, method, path, **kwargs):
        self.calls.append(method)
        if self.lost:
            raise RuntimeError('Original response lost')
        return {'statement_id': 'clock-original', 'status': {'state': self.state},
            'manifest': {'schema': {'columns': [
                {'position': 0, 'name': 'now_us', 'type_text': 'STRING'},
                {'position': 1, 'name': 'workload', 'type_text': 'STRING'}]},
                'total_row_count': 1},
            'result': {'data_array': [[self.value, self.workload]]}}


class ClockTests(unittest.TestCase):
    def test_fresh_process_replays_original_microseconds_without_network(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'original.sqlite')
            api = API()
            journal = DurableSQL(path, api, '2439e1f2e37ac563', 'actor')
            self.assertEqual(materialization_clock(journal, api.workload), '2026-10-08T17:00:00.123456+00:00')
            journal.close()
            api.value = '1'
            journal = DurableSQL(path, api, '2439e1f2e37ac563', 'actor')
            self.assertEqual(materialization_clock(journal, api.workload), '2026-10-08T17:00:00.123456+00:00')
            with self.assertRaises(SQLCustodyError):materialization_clock(journal, 'b' * 64)
            journal.close()
            self.assertEqual(api.calls, ['POST'])

    def test_pending_recovers_same_handle_and_lost_submission_never_reposts(self):
        with tempfile.TemporaryDirectory() as directory:
            api = API()
            journal = DurableSQL(str(Path(directory) / 'pending.sqlite'), api, '2439e1f2e37ac563', 'actor')
            api.state = 'RUNNING'
            with self.assertRaises(SQLPending):materialization_clock(journal, api.workload)
            api.state = 'SUCCEEDED'
            materialization_clock(journal, api.workload)
            self.assertEqual(api.calls, ['POST', 'GET'])
            journal.close()
            api = API();api.lost = True
            journal = DurableSQL(str(Path(directory) / 'lost.sqlite'), api, '2439e1f2e37ac563', 'actor')
            with self.assertRaises(RuntimeError):materialization_clock(journal, api.workload)
            with self.assertRaises(SQLCustodyError):materialization_clock(journal, api.workload)
            self.assertEqual(api.calls, ['POST'])
            journal.close()

    def test_noncanonical_clock_and_mismatched_workload_refuse(self):
        for value, workload in [('01', 'a'*64), ('9223372036854775808', 'a'*64), ('1', 'b'*64)]:
            with tempfile.TemporaryDirectory() as directory:
                api = API();api.value = value;api.workload = workload
                journal = DurableSQL(str(Path(directory) / 'original.sqlite'), api, '2439e1f2e37ac563', 'actor')
                with self.assertRaises(SQLCustodyError):materialization_clock(journal, 'a'*64)
                journal.close()
