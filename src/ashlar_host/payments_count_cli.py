"""Dedicated explicit base COUNT on an original payments publication."""
from dataclasses import replace
from .pack_native_cli import run_owned_finite_pack_query_command
from .payments_count_query import query_payments_count

def run_query_payments_count_command(configuration,*,record):
    if type(record)is not str or not record:
        raise ValueError('Explicit original payments record required')
    def operation(driver,pack,aliases,compile_config,execution,compiler,native):
        driver.source.definition.inputs(driver.source.model,driver.source.graph,pack)
        execution=replace(execution,native_observer=lambda captured:native(record,captured))
        return query_payments_count(driver,record,aliases,compile_config,execution,observe=compiler)
    return run_owned_finite_pack_query_command(configuration,operation,query_profile='payments-count')
