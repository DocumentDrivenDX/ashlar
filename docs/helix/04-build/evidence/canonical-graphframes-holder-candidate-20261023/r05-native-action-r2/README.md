# R05 canonical GraphFrames one-attempt packet — candidate r2

This candidate supersedes r1 for review of R05-owned-table cleanup. Materialization records a bounded fsynced receipt with root/lock/table device and inode identities, Delta UUIDs, and the frozen source digest. After the action fixture closes, the worker reacquires the stable lock, verifies identities and UUIDs through the same Spark session, refuses unexpected entries or links, removes only the two verified table trees, and records the final receipt digest. The parent then independently observes that `attempt-root/tmp/fixture` contains exactly the stable lock and bounded ownership receipt, checks that both table paths are absent, and verifies the receipt hash and root/lock identity linkage against the worker result.

Cleanup implementation and parent observation are present; independent code and wrapper review remains pending. Exact Python, JDK, Spark, and JAR paths are unset, the native slot requires fresh clearance, and separate one-attempt authorization has not been issued. No Spark/native execution is claimed. Any absent receipt, partial materialization, identity mismatch, unexpected entry, symlink, or uncertain deletion preserves evidence and fails closed.

R1 remains unchanged as historical preparation evidence. This r2 candidate does not authorize a run.
