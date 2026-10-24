# R02 selected Weft 0.4.53 continuation preparation

This packet pins the public Weft 0.4.53 release separately from the historical
0.4.48 verifier. It also pins the selected Spark 4.2.0 / Delta 4.4.1 cohort and
the three schema files read from Weft source commit
`726a840e956e25217d6aeea2674fdfcf3f342a24`. The schema bytes happen to match
the prior candidate; the .53 distribution and admission profile remain
independent and fail closed.

The .53 verifier checks the selected wheel, installed distribution files, API,
schemas, and the index hash around every API call. The effect-free compiler
probe reproduced the retained Backend03 and Backend02 first/continuation
responses byte-for-byte. `query_configured_projection_precompiled` adds a
narrow execution seam for an admitted .53 artifact under an already-live
configured alias owner. It validates the separately versioned admission
profile and request identity before delegating to the existing held execution
path; it does not compile or change SQL.

`r02_live_alias_gate.py` describes the two hash-bound review holds: one before
the singleton, Backend03 original and positive grouped requests, and Backend02
page 1; another after the page-1 continuation token is observed and before page
2. Each stage consumes its approval before invoking SQL, permits only its exact
ordered operations, and binds the next stage to the completed first-stage
manifest, review, live alias map, vector, and runtime. Its inert tests prove
repeated approval, callback failure, missing page-1 result, or a page-2 call in
stage 1 cannot proceed.

This is preparation only. The gate is not yet wired into the configured CLI or
SDK page runner. In particular, `configured_sdk_pages` currently rechecks its
.48 `CurrentWeftDistribution` and `target.page()` performs both page
compilation and native execution within one call. A .53 live-context page
runner must pause after page-1 token creation, freeze and review the exact
continuation request/response, then resume the same held process before page-2
SQL. The singleton entrypoint also needs to be wired through the same gate.
Until those seams are reviewed and integrated, this packet authorizes no
native SQL, does not qualify .53 native execution, and makes no change to the
R02 `.3` publication result.

The only checks run for this slice are inert unit tests and public compiler
calls against frozen requests. No Spark startup, Delta action, native SQL,
cloud action, source mutation, or ACK occurred.
