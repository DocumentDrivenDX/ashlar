"""Offline R05 proposal arithmetic; imports no Spark and performs no I/O."""

NODE_ROWS = 1_000_000_000
EDGE_ROWS = 5_000_000_000
NODE_SAMPLE_ROWS, NODE_SAMPLE_SECONDS = 45_000_000, 9.67956
EDGE_SAMPLE_ROWS, EDGE_SAMPLE_SECONDS = 225_000_000, 25.962255
THROUGHPUT_FACTOR = 0.80


def endpoints(edge_ordinal: int) -> tuple[int, int]:
    """Independent endpoint oracle for the proposed deterministic edge pattern."""
    if not 0 <= edge_ordinal < EDGE_ROWS:
        raise ValueError("edge ordinal outside fixture domain")
    source = edge_ordinal // 5
    target = (source + (edge_ordinal % 5) // 2) % NODE_ROWS
    return source, target


def seconds_at_discounted_rate(rows: int, sample_rows: int, sample_seconds: float) -> float:
    return rows * sample_seconds / sample_rows / THROUGHPUT_FACTOR


def main() -> None:
    node_seconds = seconds_at_discounted_rate(
        NODE_ROWS, NODE_SAMPLE_ROWS, NODE_SAMPLE_SECONDS
    )
    edge_seconds = seconds_at_discounted_rate(
        EDGE_ROWS, EDGE_SAMPLE_ROWS, EDGE_SAMPLE_SECONDS
    )
    total = node_seconds + edge_seconds
    assert endpoints(0) == (0, 0)
    assert endpoints(1) == (0, 0)
    assert endpoints(2) == (0, 1)
    assert endpoints(EDGE_ROWS - 1) == (NODE_ROWS - 1, 1)
    assert total > 15 * 60
    print("profile=ashlar-r05-full-delta4-proposal/0.1")
    print(f"rows=nodes:{NODE_ROWS},edges:{EDGE_ROWS}")
    print(f"writes_only_seconds=nodes:{node_seconds:.3f},edges:{edge_seconds:.3f},total:{total:.3f}")
    print(f"writes_only_minutes={total / 60:.3f}")
    print("endpoint_oracle=boundary_cases_pass; full persisted scan not performed")
    print("execution=none; Spark, filesystem, cloud, and cleanup untouched")


if __name__ == "__main__":
    main()
