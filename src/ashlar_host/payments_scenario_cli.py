# SPDX-License-Identifier: MIT OR Apache-2.0
"""Dedicated unchanged original payments currency case."""
from dataclasses import replace
from .pack_native_cli import run_owned_finite_pack_query_command
from .payments_scenario_query import query_payments_scenario

def run_query_payments_scenario_command(configuration,*,case):
    if type(case)is not str or case!='currency':raise ValueError('Exact admitted original currency case required')
    def operation(driver,pack,aliases,compile_config,execution,compiler,native):
        execution=replace(execution,native_observer=lambda captured:native(case,captured))
        return query_payments_scenario(driver,case,pack,aliases,compile_config,execution,observe=compiler)
    return run_owned_finite_pack_query_command(configuration,operation,query_profile='payments-scenario')
