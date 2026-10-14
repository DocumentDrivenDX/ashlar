# Medical public-owner renewal observation

One finite local observation used the installed A Python/runtime and the C configuration with its clean pinned UMF producer. The actual `OriginalMedicalRuntimeCustody`, `HeldMedicalDataset` and `MedicalAdmission` owners completed, including their closing checks. No outbox source, ACK, Spark, Delta transport or native driver was acquired.

The process exited zero in 9.969344583 seconds. The individual recorded calls were runtime renewal 0.112070125 seconds, dataset acquisition 6.894690792 seconds, dataset renewal 0.121720416 seconds, admission acquisition 0.287809083 seconds, admission metadata 0.264260459 seconds, dataset closing renewal 0.128840875 seconds and runtime closing 0.116634750 seconds. Admission reported 51 records, 62 edges, 113 history entries and three Boolean receipts.

These are single-call observations, not a benchmark or an explanation of the earlier native preflight deadline. Runtime opening was not timed separately. No publication plan was constructed or measured: its public original transaction acquisition requires the actual bound native driver/transport, which this diagnostic deliberately did not acquire. The 30-second Python alarm was a soft body deadline; required cleanup could extend elapsed time.

`run.py`, `report.json` and `terminal.json` preserve the executed script and original safe outputs. `SHA256.json` records exact hashes, original paths and invocation selections. The private producer revalidation payload remains at its original location and is referenced by byte count/hash only; it is not copied here. No source or publication authority is claimed.
