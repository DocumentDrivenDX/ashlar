# SPDX-License-Identifier: MIT OR Apache-2.0
"""Current configured-local runtime identity, independent of historical commerce."""
import hashlib
import importlib.metadata
import sys
import stat
from pathlib import Path
from collections.abc import Mapping

PROFILE = "ashlar-configured-local-runtime/0.2"
SPARK_VERSION = "4.2.0"
DELTA_VERSION = "4.4.1"
JAR_NAMES = ("delta-spark_4.2_2.13-4.4.1.jar", "delta-storage-4.4.1.jar")
JAR_SHA256 = {
    "delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
    "delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
}
PARAMETER_SUBSTITUTION_SETTING = "spark.sql.legacy.parameterSubstitution.constantsOnly"


def attest(config):
    """Return a closed identity after checking the selected Python, JDK and JAR bytes."""
    require = lambda ok: None if ok else (_ for _ in ()).throw(ValueError("Configured-local runtime identity refused"))
    require(sys.version_info[:2] == (3, 12))
    require(importlib.metadata.version("pyspark") == SPARK_VERSION)
    require(importlib.metadata.version("delta-spark") == DELTA_VERSION)
    release = config.java_home / "release"
    require((config.java_home / "bin/java").is_file() and release.is_file())
    release_text = release.read_text(encoding="utf-8")
    require('JAVA_VERSION="21' in release_text)
    inventory = config.jar_inventory
    require(isinstance(inventory, Mapping) and set(inventory.keys()) == set(JAR_NAMES))
    require(all(type(name) is str and type(digest) is str and len(digest) == 64 and
                all(c in "0123456789abcdef" for c in digest)
                for name, digest in inventory.items()))
    require(dict(inventory) == JAR_SHA256)
    require(config.jars.is_dir())
    entries = list(config.jars.iterdir())
    names = {p.name for p in entries}
    require(names in (set(JAR_NAMES), set(JAR_NAMES) | {"inventory.json"}))
    if "inventory.json" in names:
        info = (config.jars / "inventory.json").lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_size <= 65536)
    digests = {}
    jars = []
    for name in JAR_NAMES:
        digest = inventory[name]
        require(type(digest) is str and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest))
        path = config.jars / name
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_size <= 32 * 1024 * 1024)
        h = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(chunk)
        require(h.hexdigest() == digest)
        digests[name] = digest
        jars.append(path)
    return PROFILE, {"python": "3.12", "pyspark": SPARK_VERSION, "delta_spark": DELTA_VERSION,
                     "jdk": "21", "jars": digests,
                     "sessionSettings": {PARAMETER_SUBSTITUTION_SETTING: "true"}}, jars
