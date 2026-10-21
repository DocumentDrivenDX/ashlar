# Managed current `.4` compiler fixture

`backend03-artifact.json` is the compiled output of Weft's `weft-paths-keys`
CLI from source commit `9137ca2c3d6f8982ed0d4770de050ba4be0277ee` (Weft
0.4.50). The CLI reads one JSON request from standard input and writes the
compiled artifact to standard output. The retained artifact SHA-256 is
`29b7000ec83fdc2b57fc6185e96b6b61d62152107aa8b202075ac9de3ed5199f`.

The request used synthetic four-role Unity Catalog references and a synthetic
typed graph model. The Ashlar test uses the artifact to check request binding,
target identity, and the closed native-profile obligation. It does not establish
Unity Catalog, Databricks runtime, source-authority, or native-query qualification.
