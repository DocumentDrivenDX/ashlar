"""Pure receipt-combination rule for R14 supervisor custody reporting."""

from typing import Optional, Dict, Any


def combine_custody(*, process_status: str, process_effects_may_remain: bool,
                    worker_failure: Optional[Dict[str, Any]], child_started: bool) -> dict:
    """Keep process and database effects separate; missing worker evidence fails closed."""
    allowed = {
        "no_owned_processes_remain",
        "unresolved_processes_remain",
        "process_inventory_unavailable",
    }
    if process_status not in allowed or type(process_effects_may_remain) is not bool:
        raise ValueError("exact process custody evidence required")
    worker_effects = None if worker_failure is None else worker_failure.get("effects_may_remain")
    if worker_effects is not None and type(worker_effects) is not bool:
        raise ValueError("worker effects_may_remain must be a JSON boolean")
    if worker_effects is None:
        # A started child without a trustworthy terminal worker receipt could
        # have committed before crashing or losing its receipt.
        database_effects = bool(child_started)
    else:
        database_effects = worker_effects
    unresolved_process = process_effects_may_remain or process_status != "no_owned_processes_remain"
    return {
        "process_custody_status": process_status,
        "database_effects_may_remain": database_effects,
        "effects_may_remain": bool(unresolved_process or database_effects),
        "custody_status": (
            "unresolved_processes_remain_do_not_retry" if unresolved_process
            else "processes_clear_database_effects_may_remain" if database_effects
            else "no_owned_processes_or_database_effects_reported"
        ),
    }
