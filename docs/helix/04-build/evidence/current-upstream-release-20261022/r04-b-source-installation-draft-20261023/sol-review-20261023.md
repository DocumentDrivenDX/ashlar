# Independent review of the inert B installation executor

Sol reviewed the Luna-authored one-attempt B supervisor and worker against the
R04 source, publication, and ACK boundaries. Reviewed SHA-256 inputs:

- `b_install_supervisor.py`: `132ae7ac27df054e31081a7bf9de02bba967f87f1f03e95bf459bcbae5630c78`
- `b_install_worker.py`: `0e8018e27f718cd2e6269b0821bf44817aa50b87c6bd144545b590b46a6db8bb`
- `b-install-config.json`: `e2c9a898deed00ec78b2d540f64a40ed86d398fb9b3509eb734b940ee1deb622`

The worker uses the selected public Truss 0.0.49 preparation, installation,
acceptance/readback, and genesis-consumer interfaces. It marks each settlement
stage before the effect and retains PGDATA on an uncertain result. It performs no
feed read or ACK. The supervisor binds the source, origin, preparation, runtime,
configuration, worker, supervisor, and process/socket admission to a separate
exact approval sidecar. It enforces one attempt, a 600-second worker deadline, a
1 GiB B-root cap, 16 MiB per log, process-group termination, and owned-server
cleanup. The inventory gates are not an OS lock; a later native attempt still
requires an exclusively released local slot and a fresh gate check.

Review found and corrected a precreated-log/empty-attempt mismatch, an
overstated serialization claim, and the missing exceptional supervisor cleanup
path. The final focused Python 3.12 suite passed 17/17 tests and `git diff
--check` passed. These were fake-host and public API-shape checks, with no B
PostgreSQL start, source acceptance, or Spark execution. The current config
retains null B paths/roles/approval and false origin/slot admission, so the
executor remains inert. Actual B authority and release qualification require a
separately reviewed, hash-bound native attempt and its retained readbacks.
