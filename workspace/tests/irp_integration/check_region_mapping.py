"""
Check what modelRegionCode we're building vs what exists in simulation sets.
"""

import json
from helpers.irp_integration import IRPClient


def main():
    print("Initializing IRP client...")
    irp_client = IRPClient()
    analysis_mgr = irp_client.analysis
    ref_data = irp_client.reference_data

    # Test cases with their expected behavior
    test_cases = [
        # (app_analysis_id, scenario_name, expected_behavior)
        (5042, "CBHU - Wind Only", "populated"),
        (4940, "USHU - Surge + Wind", "empty"),
        (4985, "USOW - Tornado + Hail + Wind", "empty"),
    ]

    # Get all simulation sets for reference
    sim_sets = ref_data.get_all_simulation_sets()
    sim_set_regions = set(s.get('modelRegionCode') for s in sim_sets)
    print(f"\nAvailable modelRegionCodes in SimulationSet: {sorted(sim_set_regions)}")

    for app_id, scenario, expected in test_cases:
        print(f"\n{'='*60}")
        print(f"Scenario: {scenario} (appAnalysisId={app_id})")
        print(f"Expected behavior: {expected}")
        print('='*60)

        # Get analysis info
        results = analysis_mgr.search_analyses(filter=f"appAnalysisId={app_id}")
        if not results:
            print("  NOT FOUND")
            continue

        analysis = results[0]
        analysis_id = analysis.get('analysisId')
        region_code = analysis.get('regionCode')
        peril_code = analysis.get('perilCode')
        sub_peril = analysis.get('subPeril')

        print(f"  analysisId: {analysis_id}")
        print(f"  regionCode: {region_code}")
        print(f"  perilCode: {peril_code}")
        print(f"  subPeril: {sub_peril}")

        # Get regions
        regions = analysis_mgr.get_regions(analysis_id)
        if regions:
            sample = regions[0]
            sub_region = sample.get('subRegion')

            # What modelRegionCode would we build?
            built_model_region = sub_region + peril_code
            broader_model_region = region_code + peril_code

            print(f"\n  Sample region:")
            print(f"    subRegion: {sub_region}")
            print(f"    Built modelRegionCode (subRegion+perilCode): {built_model_region}")
            print(f"    Broader modelRegionCode (regionCode+perilCode): {broader_model_region}")

            # Check if these exist in simulation sets
            print(f"\n  Exists in SimulationSet?")
            print(f"    {built_model_region}: {'YES' if built_model_region in sim_set_regions else 'NO'}")
            print(f"    {broader_model_region}: {'YES' if broader_model_region in sim_set_regions else 'NO'}")

            # Find matching simulation sets
            matching = [s for s in sim_sets
                       if s.get('modelRegionCode') == broader_model_region
                       and s.get('perilCode') == peril_code]
            print(f"\n  Matching simulation sets for {broader_model_region}:")
            if matching:
                for m in matching[:3]:
                    print(f"    id={m.get('id')}, eventRateSchemeId={m.get('eventRateSchemeId')}, "
                          f"name={m.get('name')[:50]}...")
            else:
                print("    NONE FOUND")


if __name__ == '__main__':
    main()
