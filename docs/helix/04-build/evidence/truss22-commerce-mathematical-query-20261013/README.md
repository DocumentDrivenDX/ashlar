# Original commerce mathematical integer query: capability gap

The installed Ashlar wheel built from main `64ac2f8d98b93d1fe61f05b9dfb6fc35cea7e39a`
(SHA256 `b45040bcffcadba8104c210428fe3fc9f29f51efde17dcb9e6148afebe4eb0f4`)
attempted this read-only query against the existing full commerce publication:

```sql
SELECT l.quantity AS quantity FROM order_lines l
```

The bounded local Truss 0.0.22 / Spark local[1], 512 MiB attempt exited 1 in
13.109 seconds before source guards or result SQL executed. Its original error
is `Original public Weft Truss compiler correspondence differs`. There is no
successful query report or after-checkpoint capture. No mutation, publication or
ACK was requested. This is a failed native attempt, not a mathematical-integer pass.

A separate 0.218-second compiler-only diagnostic reused the successful price
query's original historical binding bytes, changing only authored SQL. Those
historical ephemeral aliases are no longer live: this diagnostic establishes
compiler behavior, not source authority or native execution. The original response
is blocked with `WFT-CAPABILITY`: the selected profile does not admit mathematical
integer source domains. Request/response hashes and exact original captures are
retained. The compatible compiler owner-reported source revision is
`cc17246e8b7e05ff61262baa95d373ea0ca640a1`, binary SHA256
`23cd1b3c713faca6119ad189341f03ca035b482c34e507f19dd60adfdf9b1e82`,
backend `0.1.1-compatible-revisions-candidate`.

Inspection of that compiler's capability gate establishes that selected plan
capabilities trigger refusal; unused integer fields do not by themselves trigger
it. Its separate mathematical-integer profile has distinct source-validity,
representability and exact-result obligations. Silently enabling a manifest flag
on the existing compatible profile would change its immutable semantics. A new
explicit compatible mathematical-integer profile and matching Ashlar obligation
admission are needed before another native quantity attempt. String and relationship
work can continue independently, subject to their existing admission contracts.

The host source-guard slice has scoped unit evidence; this capture does not extend
that evidence to a native pass. The required original-commerce quantity outcome
remains outstanding. No cloud run or scale test was performed. Original harnesses,
failed log, supervisor, before-checkpoint and diagnostic protocol are archived;
private source databases, warehouse and binaries remain outside Git.
