# R13 current-source preparation packet for the R05 GraphFrames tiny action

R13 binds the effect-free preparation packet to Ashlar `21d4941c84c2a2c3528eb1b01dc34078f71b41b2`, the current main revision at preparation freeze. The bound source custody covers 475 entries total: root `pyproject.toml` and every non-bytecode file under `src/ashlar_host`, including every packaged resource; the selected `truss-local` dependency is the Truss Toolkit 0.0.50 wheel with SHA-256 `7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00`.

The selected native action runtime and resources are retained from R12: GraphFrames Python/JAR 0.12.3 with its GraphX companion, Spark 4.2.0, Delta 4.4.1 and JDK 21. The runtime-tree inventory is byte-for-byte reused. R12 remains historical and is not rewritten. R13 hashes its own wrapper, worker, accounting module, source custody manifest, runtime inventory, action resources and all selected runtime pins.

Sol independently reviewed R13 after the stable main-path repair. The wrapper, worker and accounting module are byte-identical to the independently reviewed R12 versions; review also checked the selected-source change (pyproject pins Truss 0.0.50), stable self-path bindings and the static-admission receipt. This review covers packet integrity and preparation only; it does not provide execution authorization or qualify native execution, Truss lifecycle acceptance, feed, publication or ACK.

One focused static admission passed on 2026-10-24. It rechecked the invocation packet, every retained digest and the live 22,629-file / 1,560,856,259-byte runtime inventory. This is static input admission only: no authorization was supplied, the packet-authorization gate was not run, and no process inventory, Spark start, attempt root, source/ACK, fixture or table effect occurred. The authorization proposal remains unsigned and proposal-only; execution authorization is not issued.

This packet does not qualify Truss lifecycle acceptance, feed, publication or ACK; native execution; managed Unity Catalog; production authority; or scale support. No cloud or native execution was performed.

See [invocation packet](invocation-packet.json), [source custody](ashlar-source-custody.json), [static admission](static-admission-20261024.json), [authorization proposal](authorization-proposal.json), and [historical R12 packet](../r05-native-action-r12/README.md).
