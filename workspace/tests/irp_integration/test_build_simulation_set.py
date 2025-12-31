"""
Test script to validate build_region_peril_simulation_set against manual requests.

This script supports multiple test scenarios by loading JSON files from
workspace/helpers/irp_integration/ that contain manual grouping requests
captured from Moody's UI.

Test file naming convention:
    - Files ending with '_manual_request.json' are used as test cases
    - Each file should contain: analysisIds, regionPerilSimulationSet

Run directly (recommended):
    cd workspace
    python -m tests.irp_integration.test_build_simulation_set

Run with pytest:
    python -m pytest workspace/tests/irp_integration/test_build_simulation_set.py -v -s

Add new test scenarios:
    1. Capture a grouping request from Moody's UI browser inspector
    2. Save it as `<scenario_name>_manual_request.json` in workspace/helpers/irp_integration/
    3. The test will automatically pick it up
"""

import json
import sys
from pathlib import Path

from helpers.irp_integration import IRPClient


# Path to manual request files
MANUAL_REQUESTS_DIR = Path(__file__).parent.parent.parent / 'helpers' / 'irp_integration'


def normalize_simulation_set(simulation_set: list) -> set:
    """Convert simulation set to a set of tuples for comparison."""
    return set(
        (
            entry['engineVersion'],
            entry['eventRateSchemeId'],
            entry['modelRegionCode'],
            entry['modelVersion'],
            entry['perilCode'],
            entry['regionCode'],
            entry['simulationPeriods'],
            entry['simulationSetId']
        )
        for entry in simulation_set
    )


def get_manual_request_files() -> list:
    """Find all manual request JSON files for test scenarios."""
    pattern = '*_manual_request.json'
    files = list(MANUAL_REQUESTS_DIR.glob(pattern))
    return files


def load_manual_request(file_path: Path) -> dict:
    """Load a manual request JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def resolve_app_analysis_ids(analysis_manager, app_analysis_ids: list) -> list:
    """
    Convert app analysis IDs to real analysis IDs using search_analyses.

    The manual request JSON contains 'appAnalysisId' values (e.g., 5041, 5042)
    but the /regions endpoint requires actual 'analysisId' values.
    """
    real_analysis_ids = []
    for app_id in app_analysis_ids:
        results = analysis_manager.search_analyses(filter=f"appAnalysisId={app_id}")
        if results:
            real_id = results[0].get('analysisId')
            print(f"  appAnalysisId {app_id} -> analysisId {real_id}")
            real_analysis_ids.append(real_id)
        else:
            print(f"  appAnalysisId {app_id} -> NOT FOUND")
    return real_analysis_ids


def check_compound_perils(analysis_manager, app_analysis_ids: list) -> list:
    """
    Check which analyses have compound perils (subPeril contains "+").

    Returns list of (app_id, sub_peril, sub_peril_id, is_compound) tuples.
    """
    results = []
    for app_id in app_analysis_ids:
        search_results = analysis_manager.search_analyses(filter=f"appAnalysisId={app_id}")
        if search_results:
            sub_peril = search_results[0].get('subPeril', '')
            sub_peril_id = search_results[0].get('subPerilId', 0)
            # Compound perils have "+" in subPeril (e.g., "Surge + Wind")
            is_compound = '+' in sub_peril
            results.append((app_id, sub_peril, sub_peril_id, is_compound))
    return results


def validate_simulation_set(analysis_manager, manual_request_file: Path) -> bool:
    """
    Validate that build_region_peril_simulation_set produces the same output
    as the manually captured request from Moody's UI.

    Returns True if validation passes, False otherwise.
    """
    print(f"\n\n{'='*60}")
    print(f"Testing: {manual_request_file.name}")
    print('='*60)

    manual_request = load_manual_request(manual_request_file)

    app_analysis_ids = manual_request.get('analysisIds', [])
    expected_simulation_set = manual_request.get('regionPerilSimulationSet', [])

    if not app_analysis_ids:
        print(f"SKIP: No analysisIds in {manual_request_file.name}")
        return True

    print(f"\nApp Analysis IDs: {app_analysis_ids}")
    print(f"Expected simulation set entries: {len(expected_simulation_set)}")

    # Check for compound perils
    print("\n--- Checking for compound perils ---")
    peril_info = check_compound_perils(analysis_manager, app_analysis_ids)
    compound_count = sum(1 for _, _, _, is_compound in peril_info if is_compound)
    total_count = len(peril_info)
    for app_id, sub_peril, sub_peril_id, is_compound in peril_info:
        compound_marker = " [COMPOUND]" if is_compound else ""
        print(f"  appAnalysisId {app_id}: subPeril='{sub_peril}'{compound_marker}")
    if compound_count == total_count and total_count > 0:
        print(f"  => ALL analyses are compound - expecting empty regionPerilSimulationSet")
    elif compound_count > 0:
        print(f"  => {compound_count}/{total_count} compound - all analyses contribute entries")

    # Convert app analysis IDs to real analysis IDs
    print("\n--- Resolving app analysis IDs to real analysis IDs ---")
    analysis_ids = resolve_app_analysis_ids(analysis_manager, app_analysis_ids)

    if not analysis_ids:
        print("ERROR: Could not resolve any analysis IDs")
        return False

    # Get regions for each analysis (for debugging output)
    print("\n--- Fetching regions for each analysis ---")
    for analysis_id in analysis_ids:
        try:
            regions = analysis_manager.get_regions(analysis_id)
            print(f"Analysis {analysis_id}: {len(regions)} regions")
            if regions:
                # Group by framework
                frameworks = set(r.get('framework', 'ELT') for r in regions)
                print(f"  Frameworks: {frameworks}")
                sample = regions[0]
                print(f"  Sample: framework={sample.get('framework')}, "
                      f"region={sample.get('region')}, subRegion={sample.get('subRegion')}, "
                      f"peril={sample.get('peril')}, rateSchemeId={sample.get('rateSchemeId')}, "
                      f"petId={sample.get('petId')}, periods={sample.get('periods')}, "
                      f"engineVersion={sample.get('engineVersion')}")
        except Exception as e:
            print(f"Analysis {analysis_id}: Error - {e}")

    # Build simulation set using our method
    print("\n--- Building regionPerilSimulationSet ---")
    built_simulation_set = analysis_manager.build_region_peril_simulation_set(analysis_ids)
    print(f"Built simulation set entries: {len(built_simulation_set)}")

    # Compare
    print("\n--- Comparison ---")
    expected_normalized = normalize_simulation_set(expected_simulation_set)
    built_normalized = normalize_simulation_set(built_simulation_set)

    missing_from_built = expected_normalized - built_normalized
    extra_in_built = built_normalized - expected_normalized

    if missing_from_built:
        print(f"\n  X Missing from built ({len(missing_from_built)} entries):")
        for entry in sorted(missing_from_built)[:10]:
            print(f"    engineVersion={entry[0]}, eventRateSchemeId={entry[1]}, "
                  f"modelRegionCode={entry[2]}, modelVersion={entry[3]}, "
                  f"perilCode={entry[4]}, regionCode={entry[5]}, "
                  f"simulationPeriods={entry[6]}, simulationSetId={entry[7]}")
        if len(missing_from_built) > 10:
            print(f"    ... and {len(missing_from_built) - 10} more")

    if extra_in_built:
        print(f"\n  X Extra in built ({len(extra_in_built)} entries):")
        for entry in sorted(extra_in_built)[:10]:
            print(f"    engineVersion={entry[0]}, eventRateSchemeId={entry[1]}, "
                  f"modelRegionCode={entry[2]}, modelVersion={entry[3]}, "
                  f"perilCode={entry[4]}, regionCode={entry[5]}, "
                  f"simulationPeriods={entry[6]}, simulationSetId={entry[7]}")
        if len(extra_in_built) > 10:
            print(f"    ... and {len(extra_in_built) - 10} more")

    # Show sample of built entries for debugging
    if built_simulation_set:
        print("\n--- Sample of built entries ---")
        for entry in built_simulation_set[:3]:
            print(f"  {json.dumps(entry)}")

    # Check result
    if len(missing_from_built) == 0 and len(extra_in_built) == 0:
        print("\n[OK] SUCCESS: Built simulation set matches expected!")
        return True
    else:
        print(f"\n[FAIL] Mismatch: {len(missing_from_built)} missing, {len(extra_in_built)} extra")
        return False


def list_available_scenarios():
    """List all available test scenarios (manual request files)."""
    files = get_manual_request_files()
    print(f"\nAvailable test scenarios ({len(files)} files):")
    print("-" * 40)
    for f in files:
        manual_request = load_manual_request(f)
        analysis_ids = manual_request.get('analysisIds', [])
        simulation_set = manual_request.get('regionPerilSimulationSet', [])
        name = manual_request.get('name', 'N/A')
        print(f"  {f.name}")
        print(f"    Name: {name}")
        print(f"    Analysis IDs: {analysis_ids}")
        print(f"    Simulation set entries: {len(simulation_set)}")
        print()


def main():
    """Run validation tests for all manual request files."""
    print("=" * 60)
    print("Simulation Set Builder Validation")
    print("=" * 60)

    # List available scenarios
    list_available_scenarios()

    # Create IRP client
    print("\nInitializing IRP client...")
    irp_client = IRPClient()
    analysis_manager = irp_client.analysis

    # Run validation for each scenario
    files = get_manual_request_files()
    results = []

    for manual_request_file in files:
        try:
            passed = validate_simulation_set(analysis_manager, manual_request_file)
            results.append((manual_request_file.name, passed, None))
        except Exception as e:
            results.append((manual_request_file.name, False, str(e)))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed_count = sum(1 for _, passed, _ in results if passed)
    failed_count = len(results) - passed_count

    for name, passed, error in results:
        status = "[OK]" if passed else "[FAIL]"
        error_msg = f" - {error}" if error else ""
        print(f"  {status} {name}{error_msg}")

    print(f"\nTotal: {passed_count} passed, {failed_count} failed")

    return 0 if failed_count == 0 else 1


# Pytest fixtures and tests (for pytest compatibility)
try:
    import pytest

    @pytest.fixture(scope='module')
    def analysis_manager():
        """Create AnalysisManager with real API client."""
        irp_client = IRPClient()
        return irp_client.analysis

    @pytest.fixture(params=get_manual_request_files(), ids=lambda f: f.stem)
    def manual_request_file(request):
        """Parametrized fixture that yields each manual request file."""
        return request.param

    @pytest.mark.e2e
    def test_build_region_peril_simulation_set_matches_manual(analysis_manager, manual_request_file):
        """Pytest wrapper for validation."""
        passed = validate_simulation_set(analysis_manager, manual_request_file)
        assert passed, f"Validation failed for {manual_request_file.name}"

    @pytest.mark.e2e
    def test_list_available_scenarios():
        """List all available test scenarios."""
        list_available_scenarios()

except ImportError:
    # pytest not available, that's ok - we can still run directly
    pass


if __name__ == '__main__':
    sys.exit(main())
