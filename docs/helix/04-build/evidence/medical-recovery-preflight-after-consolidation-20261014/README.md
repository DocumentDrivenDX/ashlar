# Medical recovery preflight after consolidation

One public installed preflight from source `d19b2f90ec33db4d429bd9a2d3ce2081b6323c4f` ended with outer CLI exit 2 after 67.872074667 seconds. Its captured diagnostic reports native-deadline, native phase begun and unavailable inner worker status (null). It selected 60-second native/150-second overall bounds; this is not a benchmark or a successful recovery.

The 27 original stage records show ordinal-state completed (sequence 15), then five native-target captures completed. The final record is native-target begin for manifest (sequence 26), without an end. This identifies the last observed stage, not the expensive internal call or timeout cause. No successful native closing, publication or ACK submission is claimed.

The separate read-only comparison reports retained journal rows and source observations unchanged from the prior C observation. It is not native closing proof. No publication artifact or advanced ACK is claimed.

Safe original terminal, stdout/stderr, stage and comparison bytes are copied here. The manifest pins the actual executed wheel/configuration, installed origin and runtime custody files at private paths without copying medical inputs, requests, public revalidation, patient data, wheel binaries or large closure inventories. This execution used the stated d19 source, not the later documentation HEAD. No rerun was performed for packaging.
