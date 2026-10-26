# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed, immutable identity for the selected current local runtime."""
import hashlib
import tempfile
import sys
import types
import unittest
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import patch

from ashlar_host import configured_local_runtime_profile as profile
from ashlar_host.configured_local_runtime import open_finite_spark,verify_driver_memory


class Tests(unittest.TestCase):
    def test_parameter_binding_setting_is_selected_before_session_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            selected = [root / profile.JAR_NAMES[0], root / profile.JAR_NAMES[1]]
            calls = []

            class Builder:
                def master(self, value):
                    calls.append(("master", value)); return self
                def appName(self, value):
                    calls.append(("appName", value)); return self
                def config(self, key, value):
                    calls.append((key, value)); return self
                def getOrCreate(self):
                    calls.append(("getOrCreate", None)); return object()

            session = types.SimpleNamespace(builder=Builder(), getActiveSession=lambda: None,
                                           _instantiatedSession=None)
            sql_module = types.ModuleType("pyspark.sql")
            sql_module.SparkSession = session
            pyspark_module = types.ModuleType("pyspark")
            pyspark_module.__path__ = []
            pyspark_module.sql = sql_module
            pyspark_module.SparkContext = types.SimpleNamespace(_active_spark_context=None)
            config = SimpleNamespace(output=root)
            with patch.dict(sys.modules, {"pyspark": pyspark_module, "pyspark.sql": sql_module}), \
                    patch("ashlar_host.configured_local_runtime._runtime", return_value=selected):
                opened = open_finite_spark(config, selected)
            self.assertIsNotNone(opened)
            key_index = calls.index((profile.PARAMETER_SUBSTITUTION_SETTING, "true"))
            creation_index = calls.index(("getOrCreate", None))
            self.assertLess(key_index, creation_index)
            self.assertIn(("spark.driver.memory", "512m"), calls)

    def test_private_r02_session_selects_and_checks_two_gib_heap_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();selected=[root/profile.JAR_NAMES[0],root/profile.JAR_NAMES[1]];calls=[]
            class Builder:
                def master(self,value):calls.append(("master",value));return self
                def appName(self,value):calls.append(("appName",value));return self
                def config(self,key,value):calls.append((key,value));return self
                def getOrCreate(self):return object()
            session=types.SimpleNamespace(builder=Builder(),getActiveSession=lambda:None,_instantiatedSession=None)
            sql_module=types.ModuleType("pyspark.sql");sql_module.SparkSession=session
            pyspark_module=types.ModuleType("pyspark");pyspark_module.__path__=[];pyspark_module.sql=sql_module
            pyspark_module.SparkContext=types.SimpleNamespace(_active_spark_context=None)
            with patch.dict(sys.modules,{"pyspark":pyspark_module,"pyspark.sql":sql_module}), \
                    patch("ashlar_host.configured_local_runtime._runtime",return_value=selected):
                opened=open_finite_spark(SimpleNamespace(output=root),selected,r02=True)
            self.assertIsNotNone(opened)
            self.assertIn(("spark.driver.memory","2g"),calls)
            self.assertIn(("appName","Ashlar R02 Weft 0.53 continuation"),calls)
            def spark(memory,heap):
                runtime=types.SimpleNamespace(maxMemory=lambda:heap)
                jvm=types.SimpleNamespace(java=types.SimpleNamespace(lang=types.SimpleNamespace(Runtime=types.SimpleNamespace(getRuntime=lambda:runtime))))
                return types.SimpleNamespace(conf=types.SimpleNamespace(get=lambda key:memory),sparkContext=types.SimpleNamespace(_jvm=jvm))
            verify_driver_memory(spark("2g",2*1024**3),r02=True)
            with self.assertRaises(ValueError):verify_driver_memory(spark("512m",512*1024**2),r02=True)
            verify_driver_memory(spark("512m",512*1024**2),r02=False)

    def test_open_refuses_existing_default_session_or_context_before_builder(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            selected = [root / profile.JAR_NAMES[0], root / profile.JAR_NAMES[1]]

            class Builder:
                def master(self, _value): return self
                def appName(self, _value): return self
                def config(self, _key, _value): return self
                def getOrCreate(self): raise AssertionError("foreign Spark ownership was reused")

            session = types.SimpleNamespace(builder=Builder(), getActiveSession=lambda: None,
                                            _instantiatedSession=None)
            spark_context = types.SimpleNamespace(_active_spark_context=None)
            sql_module = types.ModuleType("pyspark.sql")
            sql_module.SparkSession = session
            pyspark_module = types.ModuleType("pyspark")
            pyspark_module.__path__ = []
            pyspark_module.sql = sql_module
            pyspark_module.SparkContext = spark_context
            config = SimpleNamespace(output=root)

            for foreign_session, foreign_context in ((object(), None), (None, object())):
                session._instantiatedSession = foreign_session
                spark_context._active_spark_context = foreign_context
                with patch.dict(sys.modules, {"pyspark": pyspark_module, "pyspark.sql": sql_module}), \
                        patch("ashlar_host.configured_local_runtime._runtime", return_value=selected):
                    with self.assertRaises(ValueError):
                        open_finite_spark(config, selected)

    def test_loaded_delta_api_must_come_from_selected_delta_jar(self):
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory).resolve() / profile.JAR_NAMES[0]
            jar.write_bytes(b"pinned")

            def context_loader(source):
                location = SimpleNamespace(toURI=lambda: SimpleNamespace(getPath=lambda: source))
                code = SimpleNamespace(getLocation=lambda: location)
                protection = SimpleNamespace(getCodeSource=lambda: code)
                clazz = SimpleNamespace(getProtectionDomain=lambda: protection)
                return SimpleNamespace(loadClass=lambda name: clazz)

            def spark(source):
                thread = SimpleNamespace(getContextClassLoader=lambda: context_loader(source))
                return SimpleNamespace(sparkContext=SimpleNamespace(_jvm=SimpleNamespace(
                    java=SimpleNamespace(lang=SimpleNamespace(Thread=SimpleNamespace(
                        currentThread=lambda: thread))))))

            profile.verify_loaded_delta_class(spark(jar.as_posix()), [jar])
            with self.assertRaises(ValueError):
                profile.verify_loaded_delta_class(spark(Path(directory, "other.jar").as_posix()), [jar])

    def test_mappingproxy_inventory_and_upstream_inventory_file_are_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            java = root / "java"
            (java / "bin").mkdir(parents=True)
            (java / "bin" / "java").write_bytes(b"java")
            (java / "release").write_text('JAVA_VERSION="21.0.1"\n', encoding="utf-8")
            jars = root / "jars"
            jars.mkdir()
            digests = {}
            for name in profile.JAR_NAMES:
                raw = ("jar:" + name).encode()
                (jars / name).write_bytes(raw)
                digests[name] = hashlib.sha256(raw).hexdigest()
            (jars / "inventory.json").write_text("{}", encoding="utf-8")
            config = SimpleNamespace(java_home=java, jars=jars,
                                     jar_inventory=MappingProxyType(digests))
            versions = {"pyspark": profile.SPARK_VERSION,
                        "delta-spark": profile.DELTA_VERSION}
            with patch.object(profile, "JAR_SHA256", digests), \
                    patch.object(profile.sys, "version_info", (3, 12, 0)), \
                    patch.object(profile.importlib.metadata, "version", side_effect=versions.__getitem__):
                name, identity, selected = profile.attest(config)
            self.assertEqual(name, profile.PROFILE)
            self.assertEqual(identity["jars"], digests)
            self.assertEqual(identity["sessionSettings"], {
                profile.PARAMETER_SUBSTITUTION_SETTING: "true"})
            self.assertEqual([p.name for p in selected], list(profile.JAR_NAMES))

    def test_extra_jar_or_unrecognized_directory_file_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            java = root / "java"
            (java / "bin").mkdir(parents=True)
            (java / "bin" / "java").write_bytes(b"java")
            (java / "release").write_text('JAVA_VERSION="21.0.1"\n', encoding="utf-8")
            jars = root / "jars"
            jars.mkdir()
            digests = {}
            for name in profile.JAR_NAMES:
                raw = ("jar:" + name).encode()
                (jars / name).write_bytes(raw)
                digests[name] = hashlib.sha256(raw).hexdigest()
            (jars / "unexpected.txt").write_text("untracked", encoding="utf-8")
            config = SimpleNamespace(java_home=java, jars=jars,
                                     jar_inventory=MappingProxyType(digests))
            versions = {"pyspark": profile.SPARK_VERSION,
                        "delta-spark": profile.DELTA_VERSION}
            with patch.object(profile, "JAR_SHA256", digests), \
                    patch.object(profile.sys, "version_info", (3, 12, 0)), \
                    patch.object(profile.importlib.metadata, "version", side_effect=versions.__getitem__):
                with self.assertRaises(ValueError):
                    profile.attest(config)


if __name__ == "__main__":
    unittest.main()
