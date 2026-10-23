"""Inert checks for the prepared pair harness; no Spark import or launch."""
from __future__ import annotations
import ast
import importlib.util
import json
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent

for path in (HERE / "run_case.py", HERE / "run_pair.py"):
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

spec = importlib.util.spec_from_file_location("prepared_pair", HERE / "run_pair.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
case_spec = importlib.util.spec_from_file_location("prepared_case", HERE / "run_case.py")
case_module = importlib.util.module_from_spec(case_spec)
case_spec.loader.exec_module(case_module)


def case(name: str) -> dict:
    selected = {"lookup_hash": -123, "source_system": "layout-probe", "rel_type_id": 1, "id": 123}
    delta = [{
        "numRecords": 12_500_000,
        "min_keys": sorted(selected),
        "max_keys": sorted(selected),
        "null_count_keys": sorted(selected),
        "selected_min": selected,
        "selected_max": selected,
        "selected_null_count": {key: 0 for key in selected},
    } for _ in range(18)]
    types = {"lookup_hash": "INT64", "source_system": "BINARY", "rel_type_id": "INT64", "id": "INT64"}
    chunk = {key: {"physical_type": types[key], "has_non_null_value": True, "min": str(value), "max": str(value), "num_nulls": 0} for key, value in selected.items()}
    return {
        "insert_sql_sha256": "pinned-sql",
        "rows": 225_000_000,
        "files": 18,
        "codecs": ["ZSTD"],
        "lookup": case_module.lookup_summary({"id": 123, "source_delivery_id": "123"}, 18, 42, "FileScan parquet"),
        "footer_statistics_summary": {
            "source_delivery_id": {"column_chunks": 18, "chunks_with_non_null_value": 18 if name == "baseline" else 0},
            "id": {"column_chunks": 18, "chunks_with_non_null_value": 18},
        },
        "delta_selected_stats": delta,
        "footer_row_group_count": 18,
        "footer_selected_stats_by_chunk": [{"rows": 12_500_000, "columns": chunk} for _ in range(18)],
        "settings": {"selected_key_statistics": {"id": True}},
        "insert_seconds": 25.0 if name == "baseline" else 24.0,
        "parquet_bytes": 2_500_000_000,
        "write_stage": {"task_metric_totals": {"output_records": 225_000_000}},
    }

with tempfile.TemporaryDirectory(prefix="ashlar-pair-static-") as temp:
    root = Path(temp)
    for name in ("baseline", "candidate"):
        out = root / name / "out"
        out.mkdir(parents=True)
        (out / "case-summary.json").write_text(json.dumps(case(name)), encoding="utf-8")
    result = module.compare_cases(root)
    assert result["candidate_admissible_for_review"] is True, result
    assert result["lookup_equal"] is True
    assert result["footer_statistics_differences"] == {"source_delivery_id": {
        "baseline": {"column_chunks": 18, "chunks_with_non_null_value": 18},
        "candidate": {"column_chunks": 18, "chunks_with_non_null_value": 0},
    }}
    candidate_path = root / "candidate" / "out" / "case-summary.json"
    changed = json.loads(candidate_path.read_text())
    changed["lookup"]["row"]["source_delivery_id"] = "wrong"
    candidate_path.write_text(json.dumps(changed), encoding="utf-8")
    refused = module.compare_cases(root)
    assert refused["candidate_admissible_for_review"] is False
    assert "exact-lookup-row" in refused["mismatches"]
    for name in ("baseline", "candidate"):
        path = root / name / "out" / "case-summary.json"
        broken = json.loads(path.read_text())
        for row in broken["delta_selected_stats"]:
            row["selected_min"] = {}
            row["min_keys"] = []
        path.write_text(json.dumps(broken), encoding="utf-8")
    missing = module.compare_cases(root)
    assert missing["candidate_admissible_for_review"] is False
    assert "baseline-selected-statistics-evidence" in missing["mismatches"]
print("static pair checks passed; no Spark process was imported or started")
