"""
End-to-End Integration Tests for IRP Integration Module

This test file validates the complete workflow against Moody's API, mirroring
the workflow demonstrated in IRP_Integration_Demo.ipynb.

Workflow:
1. Create EDM
2. Create Portfolio
3. Upload files and execute MRI Import
4. Create Treaty
5. Upgrade EDM version
6. GeoHaz (geocoding + hazard)
7. Execute single analysis
8. Execute batch analyses
9. Create analysis group
10. Export to RDM

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
def test_complete_workflow_edm_to_rdm(irp_client, unique_name, test_data_dir, cleanup_edms):
    """
    Complete E2E workflow from EDM creation to RDM export.

    This test validates the entire integration flow against Moody's API,
    following the same workflow as IRP_Integration_Demo.ipynb.

    Steps:
    1. Create EDM
    2. Create Portfolio
    3. MRI Import (upload files and import data)
    4. Create Treaty
    5. Upgrade EDM version
    6. GeoHaz (geocoding and hazard processing)
    7. Execute single analysis
    8. Execute batch analyses (2 analyses with different event rates)
    9. Create analysis group
    10. Export analyses to RDM

    Args:
        irp_client: IRPClient fixture
        unique_name: Unique name generator fixture
        test_data_dir: Path to test data fixtures
        cleanup_edms: List to track EDMs for cleanup
    """
    # Generate unique names for this test run
    edm_name = f"{unique_name}_EDM"
    portfolio_name = f"{unique_name}_Portfolio"
    treaty_name = f"{unique_name}_Treaty"
    analysis_job_name = f"{unique_name}_Analysis"
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
    print("\n[Step 1/10] Creating EDM...")
    edm_response = irp_client.edm.create_edm(edm_name, SERVER_NAME)

    assert edm_response is not None, "EDM creation response should not be None"
    assert edm_response['status'] == 'FINISHED', f"EDM creation should finish successfully, got {edm_response['status']}"
    assert 'summary' in edm_response, "EDM response should contain summary"
    assert 'exposureSetId' in edm_response['summary'], "EDM summary should contain exposureSetId"

    exposure_set_id = edm_response['summary']['exposureSetId']
    print(f"✓ EDM created successfully: {edm_name}")
    print(f"  Exposure Set ID: {exposure_set_id}")

    # ========================================================================
    # Step 2: Create Portfolio
    # ========================================================================
    print("\n[Step 2/10] Creating Portfolio...")
    portfolio_response = irp_client.portfolio.create_portfolio(edm_name, portfolio_name)

    assert portfolio_response is not None, "Portfolio creation response should not be None"
    assert 'id' in portfolio_response, "Portfolio response should contain id"

    portfolio_id = portfolio_response['id']
    print(f"✓ Portfolio created successfully: {portfolio_name}")
    print(f"  Portfolio ID: {portfolio_id}")

    # Verify portfolio was created
    portfolios = irp_client.portfolio.get_portfolios_by_edm_name(edm_name)
    assert 'searchItems' in portfolios, "Get portfolios should return searchItems"
    assert len(portfolios['searchItems']) > 0, "Should find at least one portfolio"
    print(f"  Verified portfolio exists in EDM")

    # ========================================================================
    # Step 3: MRI Import
    # ========================================================================
    print("\n[Step 3/10] Executing MRI Import...")

    import_response = irp_client.mri_import.import_from_files(
        edm_name=edm_name,
        portfolio_id=portfolio_id,
        accounts_file="test_accounts.csv",
        locations_file="test_locations.csv",
        mapping_file="mapping.json",
        working_dir=str(test_data_dir)
    )

    assert import_response is not None, "MRI import response should not be None"
    assert import_response['status'] == 'FINISHED', f"MRI import should finish successfully, got {import_response['status']}"
    assert 'summary' in import_response, "Import response should contain summary"
    assert 'importSummary' in import_response['summary'], "Import summary should contain importSummary"

    import_summary = import_response['summary']['importSummary']
    print(f"✓ MRI Import completed successfully")
    print(f"  {import_summary}")

    # ========================================================================
    # Step 4: Create Treaty
    # ========================================================================
    print("\n[Step 4/10] Creating Treaty...")

    treaty_response = irp_client.treaty.create_treaty_from_names(
        edm_name=edm_name,
        treaty_name=treaty_name,
        treaty_type_name=TREATY_TYPE_NAME,
        currency_name=CURRENCY_NAME,
        attachment_basis_name=ATTACHMENT_BASIS,
        attachment_level_name=ATTACHMENT_LEVEL,
        risk_limit=3000000,
        occur_limit=9000000,
        attach_pt=2000000,
        pcnt_covered=100,
        pcnt_placed=95,
        pcnt_ri_share=100,
        pcnt_retent=100,
        premium=0,
        num_of_reinst=99,
        reinst_charge=0,
        aggregate_limit=0,
        aggregate_deductible=0,
        priority=1,
        effect_date="2025-10-15T17:49:10.637Z",
        expire_date="2026-10-15T17:49:10.637Z",
        auto_assign_lobs=True
    )

    assert 'id' in treaty_response, "Treaty response should contain id"
    treaty_id = treaty_response['id']

    print(f"✓ Treaty created successfully: {treaty_name}")
    print(f"  Treaty ID: {treaty_id}")
    print(f"  LOBs assigned: {treaty_response.get('lobs_assigned', 'N/A')}")

    # ========================================================================
    # Step 5: Upgrade EDM Version
    # ========================================================================
    print("\n[Step 5/10] Upgrading EDM version...")
    upgrade_response = irp_client.edm.upgrade_edm_version(edm_name)

    assert upgrade_response is not None, "Upgrade response should not be None"
    assert upgrade_response['status'] == 'FINISHED', f"EDM upgrade should finish successfully, got {upgrade_response['status']}"

    if 'jobs' in upgrade_response and len(upgrade_response['jobs']) > 0:
        data_version = upgrade_response['jobs'][0].get('output', {}).get('dataVersion', 'Unknown')
        print(f"✓ EDM upgraded successfully")
        print(f"  Data Version: {data_version}")
    else:
        print(f"✓ EDM upgraded successfully")

    # ========================================================================
    # Step 6: GeoHaz (Geocoding + Hazard)
    # ========================================================================
    print("\n[Step 6/10] Running GeoHaz workflow...")
    geohaz_response = irp_client.portfolio.geohaz_portfolio(
        edm_name,
        portfolio_id,
        geocode=True,
        hazard_eq=True,
        hazard_ws=True
    )

    assert geohaz_response is not None, "GeoHaz response should not be None"
    assert geohaz_response['status'] == 'FINISHED', f"GeoHaz should finish successfully, got {geohaz_response['status']}"
    assert 'summary' in geohaz_response, "GeoHaz response should contain summary"

    print(f"✓ GeoHaz completed successfully")
    if 'Li Geocode Summary' in geohaz_response['summary']:
        print(f"  Geocode: {geohaz_response['summary']['Li Geocode Summary']}")
    if 'Li Hazard Summary' in geohaz_response['summary']:
        print(f"  Hazard: {geohaz_response['summary']['Li Hazard Summary']}")

    # ========================================================================
    # Step 7: Execute Single Analysis
    # ========================================================================
    print("\n[Step 7/10] Executing single analysis...")
    analysis_response = irp_client.analysis.execute_analysis(
        analysis_job_name,
        edm_name,
        portfolio_id,
        ANALYSIS_PROFILE_NAME,
        OUTPUT_PROFILE_NAME,
        EVENT_RATE_SCHEME_2025,
        [treaty_id]
    )

    assert analysis_response is not None, "Analysis response should not be None"
    assert analysis_response['status'] == 'FINISHED', f"Analysis should finish successfully, got {analysis_response['status']}"
    assert 'output' in analysis_response, "Analysis response should contain output"
    assert 'analysisId' in analysis_response['output'], "Analysis output should contain analysisId"

    single_analysis_id = analysis_response['output']['analysisId']
    print(f"✓ Analysis completed successfully")
    print(f"  Analysis ID: {single_analysis_id}")

    # ========================================================================
    # Step 8: Execute Batch Analyses
    # ========================================================================
    print("\n[Step 8/10] Executing batch analyses...")

    # Submit first analysis
    print("  Submitting analysis with 2023 event rates...")
    workflow_id1 = irp_client.analysis.submit_analysis_job(
        f"{analysis_job_name}_2023",
        edm_name,
        portfolio_id,
        ANALYSIS_PROFILE_NAME,
        OUTPUT_PROFILE_NAME,
        EVENT_RATE_SCHEME_2023,
        [treaty_id],
        [f"{unique_name}_tag"]
    )
    assert workflow_id1 > 0, "Workflow ID should be positive"
    print(f"  ✓ Workflow 1 submitted: {workflow_id1}")

    # Submit second analysis
    print("  Submitting analysis with 2025 event rates...")
    workflow_id2 = irp_client.analysis.submit_analysis_job(
        f"{analysis_job_name}_2025",
        edm_name,
        portfolio_id,
        ANALYSIS_PROFILE_NAME,
        OUTPUT_PROFILE_NAME,
        EVENT_RATE_SCHEME_2025,
        [treaty_id],
        [f"{unique_name}_tag"]
    )
    assert workflow_id2 > 0, "Workflow ID should be positive"
    print(f"  ✓ Workflow 2 submitted: {workflow_id2}")

    # Poll batch workflows
    print("  Polling batch workflows...")
    batch_response = irp_client.analysis.poll_analysis_job_batch([workflow_id1, workflow_id2])

    assert batch_response is not None, "Batch response should not be None"
    assert 'workflows' in batch_response, "Batch response should contain workflows"
    assert len(batch_response['workflows']) == 2, "Should have 2 workflows in response"

    # Extract analysis IDs
    analysis_ids = []
    for workflow in batch_response['workflows']:
        assert workflow['status'] == 'FINISHED', f"Workflow {workflow['id']} should finish successfully"
        assert 'output' in workflow, f"Workflow {workflow['id']} should have output"
        assert 'analysisId' in workflow['output'], f"Workflow {workflow['id']} should have analysisId"
        analysis_ids.append(workflow['output']['analysisId'])

    print(f"✓ Batch analyses completed successfully")
    print(f"  Analysis IDs: {analysis_ids}")

    # ========================================================================
    # Step 9: Create Analysis Group
    # ========================================================================
    print("\n[Step 9/10] Creating analysis group...")
    group_name = f"{unique_name}_Group"
    group_response = irp_client.analysis.create_analysis_group(analysis_ids, group_name)

    assert group_response is not None, "Group response should not be None"
    assert group_response['status'] == 'FINISHED', f"Group creation should finish successfully, got {group_response['status']}"
    assert 'output' in group_response, "Group response should contain output"
    assert 'analysisId' in group_response['output'], "Group output should contain analysisId"

    group_analysis_id = group_response['output']['analysisId']
    print(f"✓ Analysis group created successfully")
    print(f"  Group Name: {group_name}")
    print(f"  Group Analysis ID: {group_analysis_id}")

    # ========================================================================
    # Step 10: Export to RDM
    # ========================================================================
    print("\n[Step 10/10] Exporting analyses to RDM...")

    # Export all analyses including the group
    all_analysis_ids = [single_analysis_id, group_analysis_id] + analysis_ids
    rdm_response = irp_client.rdm.export_analyses_to_rdm(rdm_name, all_analysis_ids)

    assert rdm_response is not None, "RDM export response should not be None"
    assert rdm_response['status'] == 'FINISHED', f"RDM export should finish successfully, got {rdm_response['status']}"

    print(f"✓ Export to RDM completed successfully")
    print(f"  RDM Name: {rdm_name}")
    print(f"  Analyses exported: {len(all_analysis_ids)}")

    # ========================================================================
    # Test Complete
    # ========================================================================
    print(f"\n{'='*80}")
    print(f"E2E Workflow Test Completed Successfully!")
    print(f"All 10 steps executed without errors")
    print(f"{'='*80}\n")

    # Note: EDM cleanup will be handled by cleanup_edms fixture
