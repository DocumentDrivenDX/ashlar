# Serialized async bridge source checkpoint

The separate candidate host bridge passes 19 focused source lifecycle controls.
The page-size continuation control invokes the actual immutable R22 compiler for
both pages at size 1 under an original authored bound of 3. Native row fetching
and cancellation owners in these tests are explicit fake owners; this packet
does not qualify native async execution or driver cancellation.

Controls cover queue expiry without dispatch, remaining deadline after target
entry, caller and worker context, per-buffered-row caller checks, parameter
snapshot custody, invalid inputs before host entry, zero timeout, external and
repeated cancellation settlement, cancellation before async result delivery,
event-loop progress, denied read/page closing and no stream prefetch. Closing the target also withholds still-buffered stream rows. Both
independently found source defects (smaller-page continuation identity and zero
timeout classification) are corrected in the current source; earlier logs remain
in private recovery custody.

Required next native controls are real in-flight native cancellation, settlement
and cleanup under repeated cancellation, original scope/actor/pin refusal, denied
closing, and positive plain/Related/WithKeys smaller pages at the exact new source
and package pins. Temporal remains HELD. No release or publication is claimed.
