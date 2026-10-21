---
ddx:
  id: ashlar.truss-graph-local-runtime
  type: runbook
  activity: deploy
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-005
    kind: informed_by
---

# Truss graph resume runtime

Current Truss 0.0.44 / UMF 0.10 graph resume and continuation configurations
must include this top-level `runtime_profile` object:

```json
{
  "runtime_profile": {
    "profile": "ashlar-configured-local-runtime/0.2",
    "java_home": "/absolute/canonical/path/to/jdk-21",
    "jar_inventory": {
      "delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
      "delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f"
    }
  }
}
```

Keep the existing top-level `jars` list, with exactly those two JAR files in
one canonical directory. `java_home` must identify the JDK 21 installation and
match `JAVA_HOME`. The installed runtime must be Python 3.12, PySpark 4.2.0 and
Delta 4.4.1. Ashlar verifies the JAR bytes, Spark/JDK versions, loaded Delta
class and required `spark.sql.legacy.parameterSubstitution.constantsOnly=true`
session setting before use and during close. The setting is selected by Ashlar;
it is not an operator override.

This profile is required only when the saved page is admitted through the
current Truss graph profile. Historical UMF 0.7/0.8 configurations retain their
original declared source profile and continue using the historical commerce
runtime path; omit `runtime_profile` for those configurations. A runtime profile
does not change source, publication, journal, or ACK custody and does not grant
runtime or Truss authority.
