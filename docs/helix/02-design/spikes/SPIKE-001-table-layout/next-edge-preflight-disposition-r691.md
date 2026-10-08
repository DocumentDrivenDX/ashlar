# Observed-head growth preflight: R687–R691

The current8M edge extent passes complete-field/membership parity at observed
heads edge13/raw13/journal14/adjacency12. Seven intervening OPTIMIZE commits
(two carrier/two raw/three journal) are explicitly reconciled by independent
live-row oracle comparison. Schemas/UUIDs and closing head observations match.
R689 audits all30 exact final statements and320 complete-field groups.

The phase took163.151 seconds and read23,469,698,703 bytes with zero writes/spill.
All changed-head digest blocks were uncached. Two unchanged adjacency-pin12
blocks were cached and matched exact previously qualified uncached R682 results.
Strict R688 stopped on that distinction; its local failure is preserved. No fresh
adjacency I/O or enforcement of statement-API session cache settings is claimed.

R690 updates conditional read sensitivity to80,409,096,109 bytes from current
maintained file geometry; combined preflight-plus-growth bounds stay85GB read,
35GB write and zero spill. The next controller must charge the completed R687
preflight costs, recheck actual heads before each owned append, and preserve the
16M independent local oracle and complete endpoint checks. Prior setup-cancellation
metrics remain unfinalized/unknown in their separate ledger. This is not a complete
billing cap, source fence, retention proof, publication or1B/5B admission.

No next-stage writes occurred. Next execute the bounded16M-new-edge growth
controller against these qualified starting heads, with durable partial receipts
and same-handle recovery if interrupted. All provisional performance gates remain
unproved or failed in their recorded scopes; UC Delta remains selected.
