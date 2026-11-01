# UMF SDK 0.10.1 adoption evidence

This bounded candidate advances Ashlar's UMF SDK pin to 0.10.1 and Python Core
wheel to 0.10.5 while preserving semantic Core 0.10.0. Weft 0.4.56 and Truss
0.0.51 remain unchanged. The historical SDK 0.10.0 profile and receipts remain
available under their existing `sdk010` names.

The versioned `sdk0101` Delta bridge resource profile is bound to SDK archive
SHA-256 `3d95fff822f4f44b3d1a8e56185c3697e94ee66f5315cda7389f5301f3d16d26`,
public entry SHA-256 `7577c4efe5269d9f98187265b31be71f2be69b67cab357ce9bf07dabcd758f27`,
source commit `90cb61eb63aec3f5a77b5991a56e3dfc3d10bd27`, and source tree
`24d2531cbe9025fdc07fb1321a82a81ae31ff12a5`. The Delta bridge bundle is
byte-identical to the SDK 0.10.0-built bundle because the imported Delta DDL
modules and entry are unchanged. Public SDK 0.10.1 `readDocument` and
`realizeSparkDelta4DDL` were compared with the six frozen `.4-xxhash64` carrier
proposals and the existing bridge; all six roles passed exact proposal equality
([qualification result](sdk-delta-parity.json)). This is local proposal parity,
not native Delta execution evidence.

A versioned Bun checker and admission module are wired to the explicit local
SDK0101 configured-source/runtime path. It executes against the retained model
and request before local apply, verifies the returned receipt, and renews source
custody when closing. A bounded current-source admission check passed against
clean UMF source `90cb61eb63aec3f5a77b5991a56e3dfc3d10bd27`, returning an
86,463-byte receipt for the frozen request. Producer timeout is 60 seconds per
process, with a 1 MiB output cap. The generic producer advances because its
imports include Core `src/index.ts`, model interpretation and generated
resources changed between 2ede4c8 and 90cb61e. SDK 0.10.0 historical source
admission retains its explicit 2ede4c8 pin. This work does not qualify managed
publication, cloud execution, ACK custody, or production source authority. No
native or scale runs were made.

Core 0.10.5 artifacts are pinned in `pyproject.toml`: macOS arm64 SHA-256
`9213ff4b87649f551abcfdff40cdb6b0d75d619260988fa8ea4fe26cac7f6788` and Linux
x86_64 SHA-256 `cf33b833ff4eaa080f459cf3c93baa7e3b4e436dab5cd7853dd748e3c220dd16`.
