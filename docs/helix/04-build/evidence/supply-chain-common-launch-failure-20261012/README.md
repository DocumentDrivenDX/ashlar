# Existing-publication query launch: input-bound refusal

The installed `d10d9adcbc554e3926e0d1ce6966a357cb3508e6` worker exited one before opening Spark or compiling queries. Its requested 1 MiB verification input bound exceeded the constructor’s 256 KiB ceiling. This is a launcher configuration error; it does not qualify native queries.

One actual public source validation produced a 4,795,730-byte receipt identical to the original. Outer closing succeeded. Independent read-only review verified original publication, runtime and recipe correspondence and no matching worker or JVM. No table ingestion, publication or ACK occurred. The original installation, source receipts and failed attempt remain in place.

The archive retains eighteen exact original recipe, resource, source-receipt, stream and review records. The inventory binds each member to its original bytes. SDK binaries, the wheel and native table files are referenced by their retained descriptors. A corrected launcher must use the supported input bound and prove construction before execution.
