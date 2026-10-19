# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh local Spark context inside an externally supervised isolated worker."""
from contextlib import contextmanager
import os
from .original_medical_runtime import OriginalMedicalRuntimeCustody, NATIVE_PYTHON_SETTINGS
from .lifecycle import finish
from .medical_process_supervision import OriginalMedicalNativePhase


@contextmanager
def original_medical_native_session(custody, *, phase=None):
    """Acquire a fresh context; the calling worker owns process-group supervision."""
    if type(custody) is not OriginalMedicalRuntimeCustody:
        raise ValueError('Exact medical runtime custody required')
    if phase is not None and type(phase) is not OriginalMedicalNativePhase:
        raise ValueError('Exact original medical native phase required')
    custody.renew()
    if dict(os.environ) != custody.native_environment():
        raise PermissionError('Exact closed selected native environment required')
    if phase is not None:
        phase.begin(custody)
    custody.verify_imports()
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    settings = {
        'spark.master': custody.bounds.master,
        'spark.app.name': 'Ashlar original medical local worker',
        'spark.driver.memory': custody.bounds.driver_memory,
        'spark.ui.enabled': 'false',
        'spark.ui.showConsoleProgress': 'false',
        'spark.sql.shuffle.partitions': '1',
        'spark.databricks.delta.snapshotPartitions': '1',
        'spark.sql.session.timeZone': 'UTC',
        'spark.sql.ansi.enabled': 'true',
        'spark.jars': ','.join(str(path) for path in custody.jar_paths),
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        'spark.executorEnv.PYTHONDONTWRITEBYTECODE': '1',
    }
    settings.update({key:value for key,value,_,_ in NATIVE_PYTHON_SETTINGS})
    context = None; session = None; primary = None

    def observe():
        if dict(os.environ) != custody.native_environment():
            raise PermissionError('Selected native environment changed')
        if context is not None:
            effective = context.getConf()
            if (context.master != custody.bounds.master
                    or any(effective.get(key) != value for key, value in settings.items())):
                raise PermissionError('Effective selected Spark context differs')
        if session is not None:
            if session.sparkContext is not context or session.version != custody.runtime.spark_version:
                raise PermissionError('Fresh selected Spark session identity/version differs')
            sql_keys = ('spark.sql.shuffle.partitions', 'spark.databricks.delta.snapshotPartitions',
                        'spark.sql.session.timeZone', 'spark.sql.ansi.enabled',
                        'spark.sql.extensions', 'spark.sql.catalog.spark_catalog')
            if any(session.conf.get(key) != settings[key] for key in sql_keys):
                raise PermissionError('Effective selected Spark SQL configuration differs')

    try:
        # Public construction refuses a concurrently active context. getOrCreate
        # can reuse a default session even when no thread-active session exists.
        configuration = SparkConf(loadDefaults=False).setAll(list(settings.items()))
        context = SparkContext(conf=configuration)
        session = SparkSession(context)
        observe(); custody.renew()
        yield session
    except BaseException as error:
        primary = error
    finish(primary, [custody.renew, custody.verify_imports, observe]
           + ([session.stop] if session is not None else [])
           + ([context.stop] if context is not None else []))
    if phase is not None:
        phase.end(custody)
