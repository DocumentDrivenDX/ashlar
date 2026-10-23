"""Offline-only guard for the inert R05 codec pair. Does not import Spark."""
from __future__ import annotations

import ast
import hashlib
import importlib.util
import signal
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUNNER = HERE.parent / 'edge-write-attribution-20261023' / 'run.py'
PAIR = HERE / 'supervise_codec_pair.py'
CANDIDATE = HERE / 'supervise_codec_candidate_only.py'

for path in (RUNNER, PAIR, CANDIDATE):
    ast.parse(path.read_text())

spec = importlib.util.spec_from_file_location('ashlar_codec_pair_runner_static', RUNNER)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert 'pyspark' not in sys.modules

pair_spec = importlib.util.spec_from_file_location('ashlar_codec_pair_supervisor_static', PAIR)
pair_module = importlib.util.module_from_spec(pair_spec)
pair_spec.loader.exec_module(pair_module)
assert 'pyspark' not in sys.modules

source = RUNNER.read_text()
assert "p.add_argument('--codec',choices=('zstd','snappy'),default='zstd')" in source
assert "p.add_argument('--minimum-free-bytes',type=int,default=5_000_000_000)" in source
assert ".config('spark.sql.parquet.compression.codec',a.codec)" in source
assert "codec_property" not in source
assert "codec='zstd'" not in source
assert "if free<a.minimum_free_bytes" in source
assert "'--minimum-free-bytes', str(MIN_FREE)" in PAIR.read_text()
assert "schema_json=spark.table(table).schema.json()" in source
assert "detail['schemaString']" not in source

pair_source = PAIR.read_text()
assert "RUNNER_SHA256 = 'b132dcf5d4d80fdaec1c47cdaee9980b3ba4a1ee3291ecf95f8d73b201f74003'" in pair_source
assert "INSERT_SQL_SHA256 = '088d365593b311cd9c75f14b9129cf4beaab24657e126a0d538de3fa99f2ff5e'" in pair_source
assert "ROOT = Path('/private/tmp/ashlar-r05-codec-pair-20261023-02')" in pair_source
assert "ashlar-r05-codec-pair-20261023')" not in pair_source
assert hashlib.sha256(RUNNER.read_bytes()).hexdigest() == 'b132dcf5d4d80fdaec1c47cdaee9980b3ba4a1ee3291ecf95f8d73b201f74003'
attempt_sql = HERE.parent / 'edge-write-attribution-20261023' / 'attempt-04' / 'insert.sql'
assert hashlib.sha256(attempt_sql.read_bytes()).hexdigest() == '088d365593b311cd9c75f14b9129cf4beaab24657e126a0d538de3fa99f2ff5e'
assert "ensure_pair_deadline(pair_started, f'{codec} case')" in pair_source
assert "ensure_pair_deadline(pair_started, 'SNAPPY case')" in pair_source
assert "ensure_pair_deadline(pair_started, 'final acceptance')" in pair_source
assert "signal.signal(signal.SIGTERM, _interrupt_handler)" in pair_source
assert "signal.signal(signal.SIGINT, _interrupt_handler)" in pair_source
assert 'def monitor_child(' in pair_source and 'finally:' in pair_source
assert "'process_group_id': pgid" in pair_source
assert "except PermissionError as exc" in pair_source
assert "receipt['permission_fallback_inventory_used'] = True" in pair_source

class FakeChild:
    pid = 43210
    returncode = None
    reaped = False

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.returncode = -signal.SIGTERM
        self.reaped = True
        return self.returncode

child = FakeChild()
group_state = {'alive': True, 'signals': []}

def fake_killpg(_pgid, sig):
    group_state['signals'].append(sig)
    if sig == signal.SIGTERM:
        group_state['alive'] = False
    elif sig == 0 and not group_state['alive']:
        raise ProcessLookupError

def fake_group_alive(_pgid, *, killpg=fake_killpg):
    try:
        killpg(_pgid, 0)
        return True
    except ProcessLookupError:
        return False

def fake_terminate(proc, pgid, cause):
    return pair_module.terminate_and_reap(
        proc, pgid, cause, killpg=fake_killpg, group_alive=fake_group_alive,
        sleep=lambda _seconds: None, monotonic=iter(range(0, 100)).__next__, inventory=lambda _pgid: [],
    )

def fake_popen(*_args, **_kwargs):
    return child

monitor_receipt = pair_module.launch_monitored_child(
    [], {}, None, lambda: (_ for _ in ()).throw(RuntimeError('injected monitor fault')),
    popen=fake_popen, terminate=fake_terminate, group_alive=fake_group_alive,
)
assert monitor_receipt['monitor_exception'].startswith('RuntimeError: injected monitor fault')
assert monitor_receipt['cleanup']['sigterm_sent'] is True
assert monitor_receipt['cleanup']['leader_reaped'] is True and child.reaped
assert monitor_receipt['remaining_process_group_uncertain'] is False
assert monitor_receipt['cleanup_required'] is False
assert monitor_receipt['stop_cause'].startswith('RuntimeError: injected monitor fault')
assert group_state['signals'][0] == signal.SIGTERM

def inventory_text(stdout):
    class Result:
        returncode = 0
    Result.stdout = stdout
    return Result

assert pair_module.process_group_members(42, runner=lambda *_a, **_k: inventory_text('12 42 java child\n13 43 python other\n')) == [
    {'pid': 12, 'pgid': 42, 'command': 'java child'}]
for malformed in ('unknown row\n', '12 x java child\n', '12 42 java\n12 42 duplicate\n'):
    try:
        pair_module.process_group_members(42, runner=lambda *_a, value=malformed, **_k: inventory_text(value))
    except RuntimeError:
        pass
    else:
        raise AssertionError('ambiguous process-group inventory was accepted')

class ExitedChild:
    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.returncode = 0
        return 0

def denied_killpg(_pgid, _sig):
    raise PermissionError('injected sandbox signal denial')

permission_receipt = pair_module.terminate_and_reap(
    ExitedChild(), 24680, 'injected permission fallback', killpg=denied_killpg,
    group_alive=lambda _pgid, **_kwargs: False, sleep=lambda _seconds: None,
    monotonic=iter(range(100)).__next__, inventory=lambda pgid: [],
)
assert permission_receipt['process_group_id'] == 24680
assert permission_receipt['permission_fallback_inventory_used'] is True
assert permission_receipt['post_sigterm_process_inventory'] == []
assert permission_receipt['leader_reaped'] is True
assert permission_receipt['remaining_process_group_uncertain'] is False

baseline_ddl = "CREATE TABLE edge_current USING DELTA TBLPROPERTIES ('delta.dataSkippingStatsColumns'='lookup_hash,id','delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd');"
candidate_ddl = module.ddl_for_codec(baseline_ddl, 'snappy')
assert candidate_ddl.count("'delta.parquet.compression.codec'='snappy'") == 1
assert candidate_ddl.replace("'delta.parquet.compression.codec'='snappy'", "'delta.parquet.compression.codec'='zstd'") == baseline_ddl
assert module.ddl_for_codec(baseline_ddl, 'zstd') == baseline_ddl
for bad_ddl in (baseline_ddl.replace("'delta.parquet.compression.codec'='zstd'", ''), baseline_ddl * 2):
    try:
        module.ddl_for_codec(bad_ddl, 'snappy')
    except ValueError:
        pass
    else:
        raise AssertionError('ambiguous/missing codec property was accepted')

assert "--execute-reviewed-pair" in pair_source
assert "if not args.execute_reviewed_pair" in pair_source
assert "candidate_below_22_72s" in pair_source
assert "release_or_scale_qualification': False" in pair_source
assert 'pyspark' not in sys.modules

candidate_source = CANDIDATE.read_text()
assert "BASE = Path('/private/tmp/ashlar-r05-codec-pair-20261023-02/zstd')" in candidate_source
assert "ROOT = Path('/private/tmp/ashlar-r05-codec-pair-20261023-03')" in candidate_source
assert "pair.run_case('snappy', continuation_started)" in candidate_source
assert "pair.run_case('zstd'" not in candidate_source
assert "verify_baseline_pins()" in candidate_source
assert "hash_and_remove_baseline_warehouse(base_progress)" in candidate_source
assert "baseline_hash_pins_rechecked_after_candidate" in candidate_source
assert "--execute-reviewed-candidate" in candidate_source
assert 'pyspark' not in sys.modules
print('codec isolation, candidate-only continuation, baseline pins, and bounded process cleanup checks passed; no Spark import or session started')
