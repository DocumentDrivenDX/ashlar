# SPDX-License-Identifier: MIT OR Apache-2.0
"""Unchanged authored GraphFrames case selection; no engine or source authority."""
import hashlib
import json
from .finite_pack import FinitePackDefinition
from .commerce_original_scenarios import PACK_SHA, commerce_original_cases, CASES
from .pack_count_star_compile import select_pack_cases


def original_graphframes_case_ids(pack_bytes):
    digest=hashlib.sha256(pack_bytes).hexdigest()
    if digest==PACK_SHA:return tuple(name for name,_ in commerce_original_cases(pack_bytes))
    for name in ('archaeology','ecology'):
        facts=FinitePackDefinition(name).facts()
        if digest==facts['pack_sha256']:
            cases=json.loads(pack_bytes)['scenario_checks']
            ids=tuple(case['id'] for case in cases)
            if ids!=tuple(facts['case_ids']):raise ValueError('Exact original authored GraphFrames cases required')
            return ids
    raise ValueError('Exact supported original GraphFrames pack required')


def select_graphframes_case_ids(original,selected=()):
    if type(selected)is not tuple:raise ValueError('Immutable original case selection required')
    return tuple(name for name,_ in select_pack_cases(tuple((name,None)for name in original),selected or None))


def select_graphframes_profile(profile,selected=()):
    original=tuple(case['original_scenario']['id']for case in profile['cases'])
    ids=select_graphframes_case_ids(original,selected)
    if ids==original:return profile
    return {**profile,'cases':[case for case in profile['cases']if case['original_scenario']['id']in ids],
        'original_case_ids':list(original),'selected_case_ids':list(ids)}


def validate_graphframes_result_selection(result):
    pack=result.get('pack')
    original=tuple(name for name,_ in CASES) if pack=='commerce' else tuple(FinitePackDefinition(pack).facts()['case_ids'])
    actual=tuple(case['original_scenario']['id']for case in result['cases'])
    if result['format']=='ashlar-selected-pack-graphframes/0.1':
        selected=tuple(result['selected_case_ids'])
        if (result['original_case_ids']!=list(original) or select_graphframes_case_ids(original,selected)!=selected
                or selected==original or actual!=selected or type(result['selected_cases_passed'])is not int
                or result['selected_cases_passed']!=len(selected)):
            raise ValueError('Exact selected original GraphFrames evidence required')
    elif actual!=original or type(result['all_original_cases_passed'])is not int or result['all_original_cases_passed']!=len(original):
        raise ValueError('Complete original authored GraphFrames evidence required')
