# Original combined-source ordinal 2 timeout

The authorized installed command from Ashlar `70cd945aae3f3359ca0ec6b7b91a26b5991dbd93` exited 1 after 185.39758075005375 seconds. Its 180-second capture deadline raised `TimeoutError`; the inner worker exit status is unavailable (`null`). Owned process cleanup was confirmed, with no cleanup failure. No ordinal-2 result was released.

The four fixed records show provider entered, native session entered, native session held and provider held. They do not identify which subsequent ledger/driver phase consumed the interval. They establish neither publication completion nor unchanged source/ACK state. Source operations may have progressed; the ACK outcome remains unknown until an independently held owner observes the original installation. No reset or retry follows from this evidence.

[summary.json](summary.json) retains safe terminal and stage metadata and exact private capture hashes. Original configuration, operator credentials, transaction bytes and source payloads remain private. This is not native closing, recovery success, or a performance claim.
