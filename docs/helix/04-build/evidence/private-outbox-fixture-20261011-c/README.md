# Isolated PostgreSQL fixture setup

The source tool at Ashlar `0abf222c4ae1a9661fc7a5c0f4998b8122273afc` created one fresh local PostgreSQL 17.9 fixture using the selected cached linux/arm64 image, explicit local Docker socket, 1 CPU and 512 MiB. Two original source scopes/signatures and six ordinary LOGIN role sessions were observed. The corrected C attempt exited zero with closing resource checks complete.

Authentication was explicitly isolated loopback trust: the observations establish local session identity, not remotely authenticated production role authority. This evidence qualifies setup only; it does not qualify Delta publication, ACK progress, native queries or Truss acceptance. The owned container was intentionally retained for the next separately reviewed workflow; cleanup was not performed or claimed.

The original B attempt failed before fixture directory creation because invoking OrbStack's resolved multicall executable name `docker-tools` was unsupported. Its raw refusal and receipts remain unchanged. A separate read-only inspection through the lexical `docker` symlink confirmed the exact cached image before C. The archive retains raw outputs, exact selected recipes, source/SDK hash descriptors and independent reviews, without SDK binaries or a full source dump.
