# R05 45M/225M GraphFrames action descriptor candidate

This is a draft input candidate for the phase pilot. It freezes the existing
45M-node/225M-edge generator bytes, a source descriptor and the independently
computed seed occurrence bag. The oracle predicts five distinct edge IDs for
seed `(r05-full-scale-source, 1, 0)`: IDs 0–4, with target IDs 0, 0, 1, 1, 2.

The manifest is intentionally marked `draft-unreviewed` and has no external
review anchor. `ReviewedCanonicalActionDescriptor.from_reviewed_bytes` rejects
it until an independent reviewer records review status and a digest-bound
anchor. A future phase worker must require that reviewed descriptor and an R05
holder before DDL or writes; the existing TD-008 holder is not bypassed. This
artifact does not authorize a pilot or claim that the action was run.

The source descriptor and seed oracle are synthetic generator evidence. They do
not establish source/ACK custody, managed Unity Catalog, production authority,
full-scale completion time, memory demand, or support for other graphs.
