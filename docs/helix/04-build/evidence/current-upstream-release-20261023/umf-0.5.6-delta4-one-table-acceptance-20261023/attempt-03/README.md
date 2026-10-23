# Attempt-03: offline Spark startup, stopped before DDL

This was the single escalated attempt using the two SHA-pinned Delta 4.4.1
JARs via `spark.jars`, with no Maven package request. The authorized escalation
allowed local loopback: Spark 4.2.0 created a session under JDK 21.0.13.0.2.
Before DDL, the runner called `java.lang.Class.forName` for the Delta extension.
That probe used the JVM application classloader and raised
`ClassNotFoundException`; it does not determine whether the configured JAR is
visible through Spark's user classloader. The run stopped at that check. DDL,
insert, table-property observation and writer-setting observation did not
occur. There was no retry.

The child completed in 9.88 seconds. The supervisor observed 9,639,963 owned
bytes against the 268,435,456-byte cap and the 180-second deadline. The exact
JARs, their hashes, the runner, supervisor and complete log are retained beside
this receipt. The attempt's owned temporary directory was removed after
copying the evidence. Earlier attempts remain unchanged; no R5 lifecycle or ACK
was run.
