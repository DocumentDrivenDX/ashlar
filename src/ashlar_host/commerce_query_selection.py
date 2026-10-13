"""Explicit authored case selection; grants no query or publication authority."""
from .commerce_path_oracle import commerce_path_cases
from .count_star_oracle import commerce_count_star_cases
from .commerce_original_scenarios import CASES


def select_commerce_query_cases(profile,selected_case_ids=()):
    if profile=='count-star':original=commerce_path_cases()+commerce_count_star_cases()
    elif profile=='implicit-arithmetic':original=CASES
    else:raise ValueError('Explicit supported commerce query profile required')
    if (type(selected_case_ids)is not tuple or any(type(value)is not str or not value for value in selected_case_ids)
            or len(set(selected_case_ids))!=len(selected_case_ids)
            or not set(selected_case_ids)<={name for name,sql in original}):
        raise ValueError('Unique known authored commerce case IDs required')
    return original if not selected_case_ids else tuple(case for case in original if case[0]in selected_case_ids)
