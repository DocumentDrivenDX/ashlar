# Original17 carrier-order failure

The installed `query-original-pack-puppygraph` Cypher attempt from
`20ddbc32ddc887214ae61ed4127b1ff87cb121da` exited 2 after 51.019 seconds
(original elapsed value `51.01852549996693`). Model submission was accepted.
The first opening node-carrier request was refused by the unchanged native
query whitelist at `query_original`, line 573. Zero authored scenarios ran.
Five original requests were retained: the opening request and four closing
carrier attempts. This does not establish that all five were refused or that
no edge-carrier SQL executed; closing result bags were not retained as a
completed report.

The offline comparison uses the exact selected original model/graph/pack and
saved release bytes. Bounded JSON detachment with sorted object keys reordered
the node-column dictionaries alphabetically. The attempted opening script
matches that reordered projection exactly, with SHA256
`84d710420c1e199832670ae9718ee24690620b89149569be27490ce834eaf6e9`.
The fresh original projection used by the whitelist has SHA256
`549a03ea461d7a8e86e71b0d673f081c229f3a4915acdc3adf3a50afcb44f6e5`.
Both contain the same columns. All three node attempts differ from their fresh
whitelist scripts; both edge attempts match. Insertion-order-preserving
detachment reproduces every original whitelist script without changing model,
source, query translations or whitelist policy.

Correction `d2486eea50e69ea60cd18e0f1080203ef6e766b9` preserves insertion
order during bounded detachment. This failure record does not qualify a native
pass for that correction or the seventeen authored scenarios.

`terminal.json` selects safe original outcome fields and records completed
owned container/process cleanup. `queryordercomparison.json` retains each
script comparison, original input hashes and private artifact references.
Configuration credentials, raw logs, protocols and runtime inventories are not
copied. No new native attempt, source mutation, publication or ACK occurred
during diagnosis or evidence retention.
