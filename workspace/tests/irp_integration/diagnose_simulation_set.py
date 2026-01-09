"""
Diagnostic script to understand regionPerilSimulationSet behavior.

Compares scenarios where UI sends populated array vs empty array.
"""

import json
from pathlib import Path
from helpers.irp_integration import IRPClient


def diagnose_analysis(analysis_manager, app_analysis_id: int):
    """Get detailed info about an analysis to understand simulation set behavior."""
    print(f"\n  appAnalysisId: {app_analysis_id}")

    # Get analysis info from search
    results = analysis_manager.search_analyses(filter=f"appAnalysisId={app_analysis_id}")
    if not results:
        print(f"    NOT FOUND")
        return None

    analysis = results[0]
    analysis_id = analysis.get('analysisId')
    is_group = analysis.get('isGroup', False)
    peril_code = analysis.get('perilCode', '')
    region_code = analysis.get('regionCode', '')
    sub_peril = analysis.get('subPeril', '')
    has_compound = '+' in str(sub_peril)
    engine_version = analysis.get('engineVersion', '')
    framework = analysis.get('analysisFramework', '')

    print(f"    isGroup: {is_group}, perilCode: {peril_code}, regionCode: {region_code}")
    print(f"    subPeril: {sub_peril}, hasCompound: {has_compound}")
    print(f"    engineVersion: {engine_version}, framework: {framework}")

    # Get eventRateSchemeId from additionalProperties
    event_rate_scheme_id = None
    try:
        full_analysis = analysis_manager.get_analysis_by_id(analysis_id)
        additional_props = full_analysis.get('additionalProperties', [])

        for prop in additional_props:
            key = prop.get('key')
            if is_group and key == 'eventRateSchemes':
                properties = prop.get('properties', [])
                if properties:
                    value = properties[0].get('value', {})
                    if isinstance(value, dict):
                        event_rate_scheme_id = value.get('eventRateSchemeId')
                        print(f"    eventRateSchemeId (from eventRateSchemes): {event_rate_scheme_id}")
                break
            elif not is_group and key == 'eventRateSchemeId':
                properties = prop.get('properties', [])
                if properties:
                    event_rate_scheme_id = properties[0].get('id')
                    print(f"    eventRateSchemeId (from additionalProperties): {event_rate_scheme_id}")
                break
    except Exception as e:
        print(f"    Error getting additionalProperties: {e}")

    # Get region count
    try:
        regions = analysis_manager.get_regions(analysis_id)
        elt_count = sum(1 for r in regions if r.get('framework') == 'ELT')
        plt_count = sum(1 for r in regions if r.get('framework') == 'PLT')
        print(f"    regions: {len(regions)} total (ELT: {elt_count}, PLT: {plt_count})")
    except Exception as e:
        print(f"    Error getting regions: {e}")

    return {
        'appAnalysisId': app_analysis_id,
        'analysisId': analysis_id,
        'isGroup': is_group,
        'perilCode': peril_code,
        'regionCode': region_code,
        'subPeril': sub_peril,
        'hasCompoundPeril': has_compound,
        'engineVersion': engine_version,
        'framework': framework,
        'eventRateSchemeId': event_rate_scheme_id,
    }


def load_manual_requests():
    """Load all manual request JSON files."""
    # Try multiple paths to find the files
    possible_paths = [
        Path('workspace/helpers/irp_integration'),
        Path('helpers/irp_integration'),
        Path(__file__).parent.parent.parent / 'helpers' / 'irp_integration',
    ]

    manual_dir = None
    for p in possible_paths:
        if p.exists():
            manual_dir = p
            break

    if manual_dir is None:
        print(f"Warning: Could not find manual request directory. Tried: {possible_paths}")
        return {}

    print(f"Loading manual requests from: {manual_dir.absolute()}")
    pattern = '*_manual_request.json'

    requests = {}
    for f in sorted(manual_dir.glob(pattern)):
        with open(f, 'r') as file:
            data = json.load(file)

        name = f.stem.replace('_manual_request', '').upper()
        sim_set = data.get('regionPerilSimulationSet', [])
        requests[name] = {
            'file': f.name,
            'analysisIds': data.get('analysisIds', []),
            'regionPerilSimulationSet': sim_set,
            'hasSimSet': len(sim_set) > 0,
            'simSetCount': len(sim_set),
        }

    return requests


def main():
    print("Initializing IRP client...")
    irp_client = IRPClient()
    analysis_manager = irp_client.analysis

    # Load all manual requests
    manual_requests = load_manual_requests()

    print("\n" + "="*70)
    print("MANUAL REQUEST FILES SUMMARY")
    print("="*70)
    print(f"\n{'Scenario':<20} {'Has SimSet':<12} {'Count':<8} {'Analysis IDs'}")
    print("-"*70)

    for name, req in manual_requests.items():
        has_sim = "YES" if req['hasSimSet'] else "NO"
        ids_str = str(req['analysisIds'][:5])
        if len(req['analysisIds']) > 5:
            ids_str += f"... ({len(req['analysisIds'])} total)"
        print(f"{name:<20} {has_sim:<12} {req['simSetCount']:<8} {ids_str}")

    # Diagnose each scenario
    all_results = {}

    for name, req in manual_requests.items():
        print(f"\n\n{'#'*70}")
        print(f"# {name} - {'POPULATED' if req['hasSimSet'] else 'EMPTY'} regionPerilSimulationSet")
        print(f"# File: {req['file']}")
        print(f"{'#'*70}")

        scenario_results = []
        for app_id in req['analysisIds']:
            result = diagnose_analysis(analysis_manager, app_id)
            if result:
                scenario_results.append(result)

        all_results[name] = {
            'hasSimSet': req['hasSimSet'],
            'simSetCount': req['simSetCount'],
            'analyses': scenario_results,
        }

    # Print detailed summary comparing populated vs empty
    print(f"\n\n{'#'*70}")
    print("# PATTERN ANALYSIS")
    print(f"{'#'*70}")

    populated = {k: v for k, v in all_results.items() if v['hasSimSet']}
    empty = {k: v for k, v in all_results.items() if not v['hasSimSet']}

    print(f"\n--- POPULATED regionPerilSimulationSet ({len(populated)} scenarios) ---")
    for name, data in populated.items():
        analyses = data['analyses']
        all_groups = all(a.get('isGroup') for a in analyses)
        any_groups = any(a.get('isGroup') for a in analyses)
        all_compound = all(a.get('hasCompoundPeril') for a in analyses)
        any_compound = any(a.get('hasCompoundPeril') for a in analyses)
        peril_codes = set(a.get('perilCode') for a in analyses)
        region_codes = set(a.get('regionCode') for a in analyses)
        event_rate_scheme_ids = set(a.get('eventRateSchemeId') for a in analyses)

        print(f"\n  {name}:")
        print(f"    Count: {len(analyses)}, SimSet entries: {data['simSetCount']}")
        print(f"    All groups: {all_groups}, Any groups: {any_groups}")
        print(f"    All compound: {all_compound}, Any compound: {any_compound}")
        print(f"    Peril codes: {peril_codes}")
        print(f"    Region codes: {region_codes}")
        print(f"    Event rate scheme IDs: {event_rate_scheme_ids}")

    print(f"\n--- EMPTY regionPerilSimulationSet ({len(empty)} scenarios) ---")
    for name, data in empty.items():
        analyses = data['analyses']
        if not analyses:
            print(f"\n  {name}: No analyses found")
            continue

        all_groups = all(a.get('isGroup') for a in analyses)
        any_groups = any(a.get('isGroup') for a in analyses)
        all_compound = all(a.get('hasCompoundPeril') for a in analyses)
        any_compound = any(a.get('hasCompoundPeril') for a in analyses)
        peril_codes = set(a.get('perilCode') for a in analyses)
        region_codes = set(a.get('regionCode') for a in analyses)
        event_rate_scheme_ids = set(a.get('eventRateSchemeId') for a in analyses)

        print(f"\n  {name}:")
        print(f"    Count: {len(analyses)}")
        print(f"    All groups: {all_groups}, Any groups: {any_groups}")
        print(f"    All compound: {all_compound}, Any compound: {any_compound}")
        print(f"    Peril codes: {peril_codes}")
        print(f"    Region codes: {region_codes}")
        print(f"    Event rate scheme IDs: {event_rate_scheme_ids}")


if __name__ == '__main__':
    main()
