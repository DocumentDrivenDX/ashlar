"""Reproducible queue arithmetic from recorded runs; no native execution/admission."""
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
SOURCES = {
    "publisher": "out/native/ashlar_isolation_r133/audited-summary.json",
    "maintenance": "out/native/ashlar_singleton_optimize_r140/audited-summary.json",
    "warm_reads": "out/native/ashlar_singleton_warm_r142/audited-summary.json",
}

def calculate():
    evidence = {k: json.loads((BASE / v).read_text()) for k, v in SOURCES.items()}
    batch = evidence["publisher"]["batches"][0]
    rows = batch["rows"]
    service = batch["processing_s"]
    maintenance = evidence["maintenance"]["costs"]["optimize"]["caller_ms"] / 1000
    scenarios = []
    for rate in (10000, 100000):
        fill = rows / rate
        # FIFO, one writer; deterministic service held at the observed single sample.
        finish = 0.0
        batches = []
        for i in range(12):
            oldest = i * fill
            ready = (i + 1) * fill
            start = max(ready, finish)
            finish = start + service
            batches.append({"batch": i + 1, "queue_wait_s": start - ready,
                            "oldest_age_s": finish - oldest,
                            "uniform_record_age_p95_s": finish - oldest - 0.05 * fill})
        scenarios.append({"arrival_rate": rate, "batch_fill_s": fill,
                          "service_utilization": service / fill,
                          "queue_growth_per_batch_s": max(0, service - fill),
                          "batches": batches})
    return {
        "kind": "single-sample deterministic sensitivity model, not sustained measurement",
        "source_sha256": {v: hashlib.sha256((BASE / v).read_bytes()).hexdigest()
                          for v in SOURCES.values()},
        "rows_per_batch": rows, "observed_service_s": service,
        "observed_complete_input_to_manifest_s": batch["complete_input_to_manifest_s"],
        "single_writer_implied_rows_per_s": rows / service,
        "maintenance_sample_s": maintenance,
        "hypothetical_same_cost_maintenance_each_batch_service_s": service + maintenance,
        "hypothetical_same_cost_maintenance_every_10_batches_mean_service_s": service + maintenance / 10,
        "required_service_s_at_10k_s_for_no_queue_growth": rows / 10000,
        "required_service_s_at_100k_s_for_no_queue_growth": rows / 100000,
        "scenarios": scenarios,
        "limitations": ["No measured sustained source, burst or p95 service distribution",
                        "Fixed service costs are assumed, not extrapolated capacity evidence",
                        "Parallel-writer capacity and contention are unmeasured",
                        "Maintenance costs from a separate E23 clone are not additive measured publication",
                        "Uniform arrivals are modeled; batching cannot split a producer transaction"],
    }

if __name__ == "__main__":
    result = calculate()
    output = BASE / "out/publication-capacity-r148.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ("observed_service_s", "single_writer_implied_rows_per_s",
                                           "maintenance_sample_s")}, indent=2))
