# R05 canonical GraphFrames runtime packet — r3

R3 binds the reviewed action implementation at main `926f2be5` and completes
local runtime selection for a future single bounded action. Sol independently
reviewed the R3 worker, supervisor and accounting changes against R2, checked
the selected artifact hashes and one live metadata-only inventory, and reran
the eight focused inert tests. It remains preparation only:
`reviewStatus=independently-reviewed`, `executionStatus=blocked`,
`executionAuthorization=not-issued`, and `slotStatus` has no fresh clearance.
No Spark/native process was launched and no one-attempt authorization exists.
R2 and r1 remain unchanged.

## Runtime selection and custody

- Action module: `src/ashlar_host/pinned_delta_graphframes.py`, SHA-256
  `6c70b56dd8e2d4d2e4c55c530f3d7c2aa3227a56660b4a9fc7921ac0f62e0d7c`.
- Python: canonical base executable
  `/Users/erik/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3.12`,
  Python 3.12, SHA-256
  `811e2679c20fd2c7413f7c5ca28ce903065eea8dadb8660f1b9379a28959a480`.
  The child receives only the Ashlar source root and the selected
  `/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages`
  through `PYTHONPATH`; it checks `sys.executable`, `sys.path`, import origins,
  Python version, PySpark version, and Delta distribution version before Spark
  setup.
- JDK: canonical Homebrew 21.0.13.0.2 home
  `/opt/homebrew/Cellar/openjdk@21/21.0.13.0.2/libexec/openjdk.jdk/Contents/Home`.
  The Java executable SHA-256 is
  `382e51393d712cf7e228a606142d3c079f5fe12e4b599c62fdd1470e40fe7916`; the
  home `release` file is checked for the exact build and the action verifier
  checks the live JVM build.
- Spark: PySpark 4.2.0 at the selected venv's `site-packages/pyspark`, used as
  `SPARK_HOME`. `spark-submit` SHA-256 is
  `98e6f3b89b9092938a0b163a656c2b9051099821966fc7ab5ef9888fa9f62c6a`.
- Delta 4.4.1 JARs: `delta-spark_4.2_2.13-4.4.1.jar` SHA-256
  `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be` and
  `delta-storage-4.4.1.jar` SHA-256
  `f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f`.
- GraphFrames 0.12.3 JAR SHA-256
  `a78958f7ea3ac89ed1ea9dbbd1546f403d94151e969bb0de96e33697a5f49f50`.

The retained runtime roots are the complete Python base runtime, selected venv
site-packages (including the Spark distribution), JDK home, Delta JAR root and
GraphFrames JAR root. The inventory records all 25,101 directory, regular-file
and symlink entries, counts unique regular-file bytes by device/inode, and
rechecks device, inode, size, mtime and ctime after traversal. It hashes the
selected executable, JAR, Python import-origin and distribution metadata files.
The measured unique retained runtime-tree size is **1,560,274,427 bytes**
(about 1.45 GiB). Including the current retained evidence files and invocation
packet, the measured baseline is **1,567,274,503 bytes**, before a future
authorization file or fresh attempt tree. That leaves 580,209,145 bytes under
the 2 GiB combined limit; the 512 MiB attempt cap leaves about 41.3 MiB for
authorization and additional bounded inputs.

This is metadata custody for full distribution trees, not a SHA-256 of every
file in those trees. Selected files receive content hashes; other files are
bound by path, type, byte size, device/inode, mtime and ctime and are rejected
if those observations drift. Three Python distribution symlinks point to
currently absent temporary pkg-config/manpage targets. They are the only
out-of-root links admitted: the inventory records each exact lexical path and
target and requires the target to remain absent. All other symlinks must resolve
inside a selected root and match the exact recorded target. Symlinks are never
traversed; any changed or undeclared link refuses admission.

`runtime-tree-inventory.json` SHA-256:
`72244f1dca10d58c96b71bb16aa2e17a29c091e477a725b778e8a30d8bfcf411`.
The invocation packet binds that digest, the action/runtime pins, retained
input hashes and fixed one-attempt bounds. It carries no authorization, and
the native slot still needs fresh external clearance. Publication, Unity
Catalog, source progress, protected ACK, production authority and scale
qualification remain absent.

The supervisor now repeats the complete runtime-tree metadata inventory after
the child exits and immediately before writing a success receipt. It checks the
same path/type, byte, identity, timestamp and symlink observations under the
shared 300-second deadline; selected content hashes remain in the retained-input
hash pass. A live metadata-only pass over these 25,101 entries took 7.894
seconds on this host. The inert drift test confirms a changed retained file
refuses the post-child snapshot.

## Focused inert verification

`python3 -m unittest` over `test_runtime_accounting.py` exercises overlapping
root byte de-duplication, selected-file hashing, exact symlink custody without
cycle traversal, exact Python/JDK pin checks, and post-child metadata drift
refusal. It does not import Spark or start a native process.
