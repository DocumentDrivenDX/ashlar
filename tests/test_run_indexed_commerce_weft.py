from pathlib import Path
import tempfile
import unittest
import sys
from types import SimpleNamespace
from unittest.mock import patch
import run_indexed_commerce_weft as tool

class LifecycleTests(unittest.TestCase):
    def test_stop_failure_withholds_report(self):
        with tempfile.TemporaryDirectory() as directory:
            class Spark:
                def stop(self):raise OSError('stop failed')
            with self.assertRaises(OSError):tool.finalize(Spark(),{'success':True},Path(directory))
            self.assertFalse((Path(directory)/'report.json').exists())

    def test_report_only_after_stop_and_no_failure_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)
            class Spark:
                def stop(self):self.stopped=True
            spark=Spark();tool.finalize(spark,{'success':True},output)
            self.assertTrue(spark.stopped);self.assertTrue((output/'report.json').exists())
            (output/'report.json').unlink();tool.finalize(spark,None,output)
            self.assertFalse((output/'report.json').exists())

    def test_actual_run_reader_close_failure_withholds_report_and_stops(self):
        self._run_failure(False)

    def test_report_drift_after_queries_withholds_report_and_stops(self):
        self._run_failure(True)

    def _run_failure(self, drift):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();publication=root/'publication';publication.mkdir()
            (publication/'public-dataset.json').write_bytes(b'original');(publication/'report.json').write_bytes(b'{}')
            output=root/'output';jar=root/'jar';jar.write_bytes(b'jar')
            class Builder:
                def master(self,*a):return self
                def appName(self,*a):return self
                def config(self,*a):return self
                def getOrCreate(self):return spark
            class Spark:
                stopped=False
                def stop(self):self.stopped=True
            spark=Spark()
            class Interval:
                def __enter__(self):return self
                def __exit__(self,*a):return False
            provider=SimpleNamespace(interval=lambda context:Interval(),closed_interval_custody=lambda context:{'closed':True})
            opened=SimpleNamespace(provider=provider,context=object(),aliases={},original_native_files={'file':'digest'},native_files=lambda:{'file':'digest'},manifest={'publication_id':'original'},original_report={})
            class Reader:
                def __enter__(self):return opened
                def __exit__(self,*args):
                    if drift:return False
                    raise OSError('reader close failed')
            def public_call(*args,**kwargs):
                (output/'fresh-public-dataset.json').write_bytes(b'original')
                return SimpleNamespace(returncode=0)
            def queries(*args):
                if drift:(publication/'report.json').write_bytes(b'{"changed":true}')
                return {'queries':[]}
            with patch.object(tool,'preflight',return_value=[jar]),patch.object(tool.subprocess,'run',side_effect=public_call),patch.dict(sys.modules,{'pyspark':SimpleNamespace(),'pyspark.sql':SimpleNamespace(SparkSession=SimpleNamespace(builder=Builder()))}),patch.object(tool,'open_commerce_reader',return_value=Reader()),patch.object(tool,'run_indexed_queries',side_effect=queries):
                with self.assertRaisesRegex(ValueError if drift else OSError,'Closing publication report differs' if drift else 'reader close failed'):
                    tool.run(index=root/'index',installation=root/'install',publication=publication,source=root/'source',jars=root,output=output,ack=tool.ACK_PROFILE)
            self.assertTrue(spark.stopped);self.assertFalse((output/'report.json').exists())

    def test_explicit_wrong_operator_profile_refuses_before_public_calls(self):
        with patch.object(tool.subprocess,'check_output',side_effect=AssertionError('public call')):
            with self.assertRaises(ValueError):tool.preflight(Path('source'),Path('publication'),Path('jars'),('other','host',123,'db'))

if __name__=='__main__':unittest.main()
