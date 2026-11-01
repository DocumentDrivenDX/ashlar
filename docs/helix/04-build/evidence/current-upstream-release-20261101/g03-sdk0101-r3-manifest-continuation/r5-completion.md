# R5 retained manifest completion

The R5 continuation reopened the original R3 installation and finalized its
retained transaction-one manifest. The bounded supervisor reports
`completed-verified` after 419046 ms, with a maximum process-group RSS of
1172209664 bytes and an empty terminal process group. The postcheck confirms
zero graph operations replayed and zero metadata operations added. The
transaction-one output receipt remains `eef82376bff8eafc281442aa8f7da2e424d0439ab942b5ccb0c5464c07f37b2c`.

The final Delta head vector is object v2, edge v2, tombstone v1, whole-source
history v2, attempts v3, and manifest v2. Only the manifest advanced from R3's
retained v1 head. The resulting manifest row binds the four-role data vector
`edge=2`, `object=2`, `tombstone=1`, `history=2`; the exact manifest-row digest
is `2cc2d176240cbc3bfb3f0900d39b83424103fd5420a23c676ac6785d98bc380e`, and the
realized-vector digest is
`3119f0089fd9045e7620f39905ee5efe79fde344091b6af1ad58841e7a49e657`.

The read-only SQLite check returned `quick_check=ok`. Its journal contains one
source plan, one graph plan, three committed graph operations, one committed
manifest operation, two original committed metadata operations, one unchanged
output receipt, and zero publication artifacts. The database SHA-256 is
`c5738c3cf902ebde890d772db3b1656deb6dc7c7a67443cb2844e78053f1a024`. At the
read-only inspection, the WAL sidecar was empty (0 bytes); the attempt receipts
record an empty worker group. The result records no source ACK, protected ACK,
or cloud effect.

Small durable attempt receipts are copied byte-for-byte in
[`r5-completion-receipts/`](r5-completion-receipts/). The receipt index binds
those copies and the retained invocation, logs, SDK admission outputs, runtime
result, SQLite journal, and all six native Delta heads by digest:
[`r5-completion-receipt-index.json`](r5-completion-receipt-index.json). The
86,463-byte SDK graph receipt is hash-bound there rather than duplicated. The
full attempt remains at the original R5 path in the index.
