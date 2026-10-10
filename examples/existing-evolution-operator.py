"""Trusted CLI provider for an explicitly selected existing local installation.

Supply every named ASHLAR_* input yourself. DSNs must authenticate ordinary
roles directly. This file performs no credential discovery or installation.
Use the selected installed Spark/Delta/psycopg runtime in a fresh CLI process.
"""
from contextlib import contextmanager
import os
from pathlib import Path
from ashlar_host.ack import AckScope
from ashlar_host.delta_custody import DeltaTarget, LOCAL_OPERATION_CAPACITY_8M
from ashlar_host.evolution_operator import ExistingEvolutionOperator
from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
from ashlar_host.source_sessions import OutboxSourceRegistration


def supplied(name):
    value = os.environ['ASHLAR_' + name]
    if not value:
        raise ValueError('Missing explicit operator input')
    return value


def connection(name):
    def ordinary(context):
        import psycopg
        return psycopg.connect(supplied(name), autocommit=False)
    return ordinary


def registration(label):
    scope = AckScope(supplied(label + '_SERVICE_SCHEMA'), supplied(label + '_SCOPE_ID'),
        supplied(label + '_ACK_INSTALLATION_ID'), supplied(label + '_CONSUMER'),
        supplied(label + '_FEED'), supplied(label + '_EPOCH'))
    return OutboxAuthorityRegistration(OutboxSourceRegistration(scope,
        supplied(label + '_SOURCE_SCHEMA'), supplied(label + '_SOURCE_SIGNATURE_SHA256'),
        connection(label + '_SOURCE_DSN')), supplied(label + '_SOURCE_ROLE'),
        supplied(label + '_ACK_ROLE'), supplied(label + '_DATABASE'), connection(label + '_ACK_DSN'))


@contextmanager
def native_session():
    from pyspark.sql import SparkSession
    if SparkSession.getActiveSession() is not None:
        raise RuntimeError('A fresh dedicated CLI process is required')
    # Explicit existing local jars, never spark.jars.packages/network resolution.
    spark = (SparkSession.builder.master('local[1]').appName('ashlar-existing-evolution')
        .config('spark.jars', supplied('DELTA_JARS'))
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true')
        .getOrCreate())
    try:
        yield spark
    finally:
        spark.stop()


@contextmanager
def open_evolution(invocation):
    capacity_name = supplied('EXISTING_OPERATION_CAPACITY')
    if capacity_name not in ('uncapped', '8MiB'):
        raise ValueError('Select the actual existing operation capacity')
    targets = tuple(DeltaTarget(supplied(role.upper() + '_TABLE'),
        Path(supplied(role.upper() + '_PATH')), supplied(role.upper() + '_UUID'))
        for role in ('attempts', 'manifest', 'object_current', 'edge_current',
                     'tombstone', 'whole_source_history'))
    operator = ExistingEvolutionOperator(supplied('INSTALLATION_ID'),
        Path(supplied('NATIVE_JOURNAL')), Path(supplied('ORIGINAL_RUN_RESERVATION')),
        targets, (registration('A'), registration('B')),
        None if capacity_name == 'uncapped' else LOCAL_OPERATION_CAPACITY_8M, native_session)
    with operator.open_evolution(invocation) as config:
        yield config
