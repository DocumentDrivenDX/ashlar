# Full CTAS cancellation disposition: r309/r312

Governing ADR-001/CONTRACT-003 and original targets remain unchanged.
The full39.99M-edge range32 CTAS hit its180-second cancel guard. Same native
statement GET and final query history report CANCELED; Unity Catalog target GET
returns NotFound. No candidate version was qualified, no ZORDER was run and no
singleton test was launched. Original liquid-clustered snapshots remain pinned.

Final native counters:32,507,872,254read bytes,19,382,521,882attempted write bytes,
zero reported spill. Attempted output does not prove committed table bytes or
physical orphan inventory; cleanup/retention was not performed. The original
summary's last pre-mutation counters were zero; cancel-audit.json is the terminal
cost source. Future reserve models must charge this attempt explicitly.

This is a bounded build failure, not evidence that partition32orUC Delta cannot
meet requirements. Do not rerun the identical full CTAS or enlarge compute.
Instead create a separately named owned empty candidate and append20disjoint
2M-identity source ranges, binding each actual commit and verifying its exact
full20-field source groups before proceeding. Parent39.99M rows after10k deletes
must remain the final scope: staged intermediate subsets do not qualify layout,
performance or billion admission. No duplicate range or blind unknown-outcome
retry is allowed. A complete all400group digest comparison, all32partition
coverage and whole maintenance/read/ingest tests are still required.

The first append must demonstrate a measured2M-stage read/write/time envelope
before charging the remaining19stages. Include canceled19.383GB attempted writes
in aggregate work/storage sensitivity; previous80GB successful-build budget must
not silently hide this cost. The extra physical overlap and failed output remain
unknown until an actual inventory; no automatic VACUUM or sourceACK is authorized
by this evidence. Pending range32 singleton/concurrent scripts are gated on a
missing full-candidate audited summary and must remain unexecuted.
