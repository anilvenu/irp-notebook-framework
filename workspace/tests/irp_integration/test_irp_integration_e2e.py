"""
End-to-End Integration Tests for IRP Integration Module

This test file validates the complete workflow against Moody's API, mirroring
the workflow demonstrated in IRP_Integration_Demo.ipynb.

Workflow:
1. Create EDM(s) - batch submission
2. Create Portfolio(s) - batch submission
3. Upload files and execute MRI Import
4. Create Treaty/Treaties - batch submission
5. Upgrade EDM version - batch submission
6. GeoHaz (geocoding + hazard) - batch submission
7. Execute batch analyses
8. Create analysis group(s) - batch submission
9. Export to RDM

Requirements:
- RISK_MODELER_BASE_URL environment variable
- RISK_MODELER_API_KEY environment variable
- RISK_MODELER_RESOURCE_GROUP_ID environment variable
- Valid Moody's API credentials

Run these tests:
    pytest workspace/tests/irp_integration/test_irp_integration_e2e.py -v -s
    pytest workspace/tests/irp_integration/test_irp_integration_e2e.py -v -s -m moody_api
"""

import pytest
from pathlib import Path


# ============================================================================
# Test Configuration Constants
# ============================================================================

# EDM/Server configuration
SERVER_NAME = "databridge-1"
EDM_DATA_VERSION = "22"
GEOHAZ_VERSION = "22.0"

# Analysis configuration
ANALYSIS_PROFILE_NAME = "DLM CBHU v23"
OUTPUT_PROFILE_NAME = "Patched Portfolio Level Only (EP, ELT, Stats)"
EVENT_RATE_SCHEME_2023 = "RMS 2023 Stochastic Event Rates"
EVENT_RATE_SCHEME_2025 = "RMS 2025 Stochastic Event Rates"

# Treaty configuration
TREATY_TYPE_NAME = "Working Excess"
CURRENCY_NAME = "US Dollar"
ATTACHMENT_BASIS = "Losses Occurring"
ATTACHMENT_LEVEL = "Location"


# ============================================================================
# End-to-End Workflow Test
# ============================================================================

@pytest.mark.moody_api
@pytest.mark.e2e
@pytest.mark.slow
def test_complete_workflow_edm_to_rdm(irp_client, unique_name, cleanup_edms):
    """
    Complete E2E workflow from EDM creation to RDM export.

    This test validates the entire integration flow against Moody's API,
    following the same workflow as IRP_Integration_Demo.ipynb.

    Steps:
    1. Create EDM (batch submission)
    2. Create Portfolio (batch submission)
    3. MRI Import (upload files and import data)
    4. Create Treaty (batch submission)
    5. Upgrade EDM version (batch submission)
    6. GeoHaz (geocoding and hazard processing - batch submission)
    7. Execute batch analyses (2 analyses with different event rates)
    8. Create analysis group (batch submission)
    9. Export analyses to RDM

    Args:
        irp_client: IRPClient fixture
        unique_name: Unique name generator fixture
        cleanup_edms: List to track EDMs for cleanup
    """
    # Generate unique names for this test run
    edm_name = f"{unique_name}_EDM"
    portfolio_name = f"{unique_name}_Portfolio"
    treaty_name = f"{unique_name}_Treaty"
    analysis_job_name_2023 = f"{unique_name}_2023"
    analysis_job_name_2025 = f"{unique_name}_2025"
    analysis_group_name = f"{unique_name}_Group"
    analysis_tag_name = f"{unique_name}_TAG"
    rdm_name = f"{unique_name}_RDM"

    # Track for cleanup
    cleanup_edms.append(edm_name)

    print(f"\n{'='*80}")
    print(f"Starting E2E Workflow Test")
    print(f"EDM Name: {edm_name}")
    print(f"{'='*80}\n")

    # ========================================================================
    # Step 1: Create EDM
    # ========================================================================
    print("\n[Step 1/9] Creating EDM...")
    job_ids = irp_client.edm.submit_create_edm_jobs([
        {
            "edm_name": edm_name,
            "server_name": SERVER_NAME
        }
    ])

    assert len(job_ids) == 1, "Should submit exactly one EDM creation job"
    print(f"  Submitted EDM creation job: {job_ids[0]}")

    edm_responses = irp_client.job.poll_risk_data_job_batch_to_completion(job_ids)

    assert len(edm_responses) == 1, "Should receive exactly one EDM creation response"
    edm_response = edm_responses[0]
    assert edm_response['status'] == 'FINISHED', f"EDM creation should finish successfully, got {edm_response['status']}"

    print(f"✓ EDM created successfully: {edm_name}")

    # ========================================================================
    # Step 2: Create Portfolio
    # ========================================================================
    print("\n[Step 2/9] Creating Portfolio...")
    portfolio_ids = irp_client.portfolio.create_portfolios([
        {
            "edm_name": edm_name,
            "portfolio_name": portfolio_name,
            "portfolio_number": portfolio_name,
            "description": f"{portfolio_name} created via IRP Notebook Framework"
        }
    ])

    assert len(portfolio_ids) == 1, "Should create exactly one portfolio"
    portfolio_id = portfolio_ids[0]
    print(f"✓ Portfolio created successfully: {portfolio_name}")
    print(f"  Portfolio ID: {portfolio_id}")

    # ========================================================================
    # Step 3: MRI Import
    # ========================================================================
    print("\n[Step 3/9] Executing MRI Import...")

    import_response = irp_client.mri_import.import_from_files(
        edm_name=edm_name,
        portfolio_name=portfolio_name,
        accounts_file="test_accounts.csv",
        locations_file="test_locations.csv",
        mapping_file="mapping.json"
    )

    assert import_response is not None, "MRI import response should not be None"
    assert import_response['status'] == 'FINISHED', f"MRI import should finish successfully, got {import_response['status']}"

    print(f"✓ MRI Import completed successfully")

    # ========================================================================
    # Step 4: Create Treaty
    # ========================================================================
    print("\n[Step 4/9] Creating Treaty...")

    treaty_ids = irp_client.treaty.create_treaties([
        {
            "edm_name": edm_name,
            "treaty_name": treaty_name,
            "treaty_number": treaty_name,
            "treaty_type": TREATY_TYPE_NAME,
            "per_risk_limit": 30000000.00,
            "occurrence_limit": 90000000.00,
            "attachment_point": 2000000.00,
            "inception_date": "2025-01-01T00:00:00.000Z",
            "expiration_date": "2030-01-01T00:00:00.000Z",
            "currency_name": CURRENCY_NAME,
            "attachment_basis": ATTACHMENT_BASIS,
            "attachment_level": ATTACHMENT_LEVEL,
            "pct_covered": 100.0,
            "pct_placed": 95.00,
            "pct_share": 100.00,
            "pct_retention": 100.00,
            "premium": 0.00,
            "num_reinstatements": 99,
            "pct_reinstatement_charge": 0.00,
            "aggregate_limit": 0.00,
            "aggregate_deductible": 0.00,
            "priority": 1
        }
    ])

    assert len(treaty_ids) == 1, "Should create exactly one treaty"
    treaty_id = treaty_ids[0]

    print(f"✓ Treaty created successfully: {treaty_name}")
    print(f"  Treaty ID: {treaty_id}")

    # ========================================================================
    # Step 5: Upgrade EDM Version
    # ========================================================================
    print("\n[Step 5/9] Upgrading EDM version...")
    upgrade_job_ids = irp_client.edm.submit_upgrade_edm_data_version_jobs([
        {
            "edm_name": edm_name,
            "edm_version": EDM_DATA_VERSION
        }
    ])

    assert len(upgrade_job_ids) == 1, "Should submit exactly one upgrade job"
    print(f"  Submitted upgrade job: {upgrade_job_ids[0]}")

    upgrade_responses = irp_client.edm.poll_data_version_upgrade_job_batch_to_completion(upgrade_job_ids)

    assert len(upgrade_responses) == 1, "Should receive exactly one upgrade response"
    upgrade_response = upgrade_responses[0]
    assert upgrade_response['status'] == 'FINISHED', f"EDM upgrade should finish successfully, got {upgrade_response['status']}"

    print(f"✓ EDM upgraded successfully to version {EDM_DATA_VERSION}")

    # ========================================================================
    # Step 6: GeoHaz (Geocoding + Hazard)
    # ========================================================================
    print("\n[Step 6/9] Running GeoHaz workflow...")
    geohaz_job_ids = irp_client.portfolio.submit_geohaz_jobs([
        {
            "edm_name": edm_name,
            "portfolio_name": portfolio_name,
            "version": GEOHAZ_VERSION,
            "hazard_eq": True,
            "hazard_ws": True
        }
    ])

    assert len(geohaz_job_ids) == 1, "Should submit exactly one GeoHaz job"
    print(f"  Submitted GeoHaz job: {geohaz_job_ids[0]}")

    geohaz_responses = irp_client.portfolio.poll_geohaz_job_batch_to_completion(geohaz_job_ids)

    assert len(geohaz_responses) == 1, "Should receive exactly one GeoHaz response"
    geohaz_response = geohaz_responses[0]
    assert geohaz_response['status'] == 'FINISHED', f"GeoHaz should finish successfully, got {geohaz_response['status']}"

    print(f"✓ GeoHaz completed successfully")

    # ========================================================================
    # Step 7: Execute Batch Analyses
    # ========================================================================
    print("\n[Step 7/9] Executing batch analyses...")

    analysis_job_ids = irp_client.analysis.submit_portfolio_analysis_jobs([
        {
            "edm_name": edm_name,
            "job_name": analysis_job_name_2023,
            "portfolio_name": portfolio_name,
            "analysis_profile_name": ANALYSIS_PROFILE_NAME,
            "output_profile_name": OUTPUT_PROFILE_NAME,
            "event_rate_scheme_name": EVENT_RATE_SCHEME_2023,
            "treaty_names": [treaty_name],
            "tag_names": [analysis_tag_name]
        },
        {
            "edm_name": edm_name,
            "job_name": analysis_job_name_2025,
            "portfolio_name": portfolio_name,
            "analysis_profile_name": ANALYSIS_PROFILE_NAME,
            "output_profile_name": OUTPUT_PROFILE_NAME,
            "event_rate_scheme_name": EVENT_RATE_SCHEME_2025,
            "treaty_names": [treaty_name],
            "tag_names": [analysis_tag_name]
        }
    ])

    assert len(analysis_job_ids) == 2, "Should submit exactly two analysis jobs"
    print(f"  Submitted analysis jobs: {analysis_job_ids}")

    analysis_batch_response = irp_client.analysis.poll_analysis_job_batch_to_completion(analysis_job_ids)

    assert len(analysis_batch_response) == 2, "Should receive exactly two analysis responses"
    for analysis_response in analysis_batch_response:
        assert analysis_response['status'] == 'FINISHED', f"Analysis should finish successfully, got {analysis_response['status']}"

    print(f"✓ Batch analyses completed successfully")

    # ========================================================================
    # Step 8: Create Analysis Group
    # ========================================================================
    print("\n[Step 8/9] Creating analysis group...")

    grouping_job_ids = irp_client.analysis.submit_analysis_grouping_jobs([
        {
            "group_name": analysis_group_name,
            "analysis_names": [analysis_job_name_2023, analysis_job_name_2025]
        }
    ])

    assert len(grouping_job_ids) == 1, "Should submit exactly one grouping job"
    print(f"  Submitted grouping job: {grouping_job_ids[0]}")

    grouping_responses = irp_client.analysis.poll_analysis_grouping_job_batch_to_completion(grouping_job_ids)

    assert len(grouping_responses) == 1, "Should receive exactly one grouping response"
    grouping_response = grouping_responses[0]
    assert grouping_response['status'] == 'FINISHED', f"Grouping should finish successfully, got {grouping_response['status']}"

    print(f"✓ Analysis group created successfully: {analysis_group_name}")

    # ========================================================================
    # Step 9: Export to RDM
    # ========================================================================
    print("\n[Step 9/9] Exporting analyses to RDM...")

    # Export all analyses including the group
    analysis_names = [analysis_group_name, analysis_job_name_2023, analysis_job_name_2025]
    rdm_export_response = irp_client.rdm.export_analyses_to_rdm(
        server_name=SERVER_NAME,
        rdm_name=rdm_name,
        analysis_names=analysis_names
    )

    assert rdm_export_response is not None, "RDM export response should not be None"
    assert rdm_export_response['status'] == 'FINISHED', f"RDM export should finish successfully, got {rdm_export_response['status']}"

    print(f"✓ Export to RDM completed successfully")
    print(f"  RDM Name: {rdm_name}")
    print(f"  Analyses exported: {len(analysis_names)}")

    # ========================================================================
    # Test Complete
    # ========================================================================
    print(f"\n{'='*80}")
    print(f"E2E Workflow Test Completed Successfully!")
    print(f"All 9 steps executed without errors")
    print(f"{'='*80}\n")

    # Note: EDM cleanup will be handled by cleanup_edms fixture
