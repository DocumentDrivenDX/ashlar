# Statement-shape comparison disposition: r306

Governing ADR-001/CONTRACT-003 and exact publication/read boundaries remain.
Sixteen uncached full20-field singleton queries alternate stable versus changing
query comments on one persistent connection, identical SQL/parameters in each
pair. Four unchanged keys repeat twice at the qualified maintained version4.
All16exact assertions and18native terminal receipts pass; no table writes.

Stable-comment caller/engine/compile p95 are489.791/89/284ms; changing-comment
values are354.644/95/153ms. With only8queries each, p95 is the maximum. The first
stable query contributes284ms compile; subsequent stable compilations are
145–155ms, versus142–153ms for changing comments. This is startup/ordered
cohort evidence, not a causal regression or service-tail result.

There is no demonstrated compilation reuse advantage from keeping the comment
constant. Keep durable actual statement IDs and parameterized full-identity
queries; don't alter production instrumentation for an unsupported optimization.
The separate native total/caller residual remains roughly110–116ms at sample
p95. Preserve the250ms caller target as unproved, not passed by a small engine-only
sample. Next pursue physical read/planning reduction and concurrency evidence;
comment changes alone are not justified as the next performance workstream.
