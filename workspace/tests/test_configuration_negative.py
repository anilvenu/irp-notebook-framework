"""
Test suite for configuration validation - NEGATIVE test cases (pytest version)

This test file validates that invalid Excel configuration files are properly rejected.
Each test corresponds to a specific invalid Excel file that you will create manually.

Test Categories:
1. Structure Errors - Missing sheets, columns, keys
2. Type Errors - Wrong data types
3. Nullability Errors - Null values in required fields
4. Uniqueness Violations - Duplicate values in unique columns
5. Pattern/Format Violations - Values not matching required patterns
6. Range Constraint Violations - Values outside allowed ranges
7. Foreign Key Violations - References to non-existent records
8. Special Reference Violations - Cross-sheet reference errors
9. Groupings Reference Violations - Invalid grouping references
10. Business Rule Violations - Business logic violations

All tests run in the 'test_configuration' schema (auto-managed by test_schema fixture).

Run these tests:
    pytest workspace/tests/test_configuration_negative.py
    pytest workspace/tests/test_configuration_negative.py -v -k "missing_sheet"
    pytest workspace/tests/test_configuration_negative.py --preserve-schema
"""

import pytest
from pathlib import Path

from helpers.configuration import (
    validate_configuration_file,
    ConfigurationError
)


# Test files directory
TEST_FILES_DIR = Path(__file__).parent / 'files'


# ============================================================================
# TEST CATEGORY 1: STRUCTURE ERRORS
# ============================================================================

def test_invalid_config_missing_sheet():
    """Test: Missing required sheet (STRUCT-001)

    File to create: invalid_config_missing_sheet.xlsx
    Modification: Delete the "Databases" sheet from valid config
    Expected: ConfigurationError with "Missing required sheets" message
    """
    test_file = TEST_FILES_DIR / 'invalid_config_missing_sheet.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Missing required sheets|Databases"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_missing_columns():
    """Test: Missing required columns (STRUCT-002)

    File to create: invalid_config_missing_columns.xlsx
    Modification: Delete "Store in Data Bridge?" column from Databases sheet
    Expected: ConfigurationError mentioning missing columns
    """
    test_file = TEST_FILES_DIR / 'invalid_config_missing_columns.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Missing required columns|Store in Data Bridge"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_missing_metadata_key():
    """Test: Missing required key in metadata (STRUCT-003)

    File to create: invalid_config_missing_metadata_key.xlsx
    Modification: Delete "EDM Data Version" row from Metadata sheet
    Expected: ConfigurationError mentioning missing key
    """
    test_file = TEST_FILES_DIR / 'invalid_config_missing_metadata_key.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Missing required key|EDM Data Version"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 2: TYPE ERRORS
# ============================================================================

def test_invalid_config_wrong_type_metadata():
    """Test: Wrong data type in metadata (TYPE-002)

    File to create: invalid_config_wrong_type_metadata.xlsx
    Modification: Change "DLM Model Version" value from integer (123) to text ("abc")
    Expected: ConfigurationError about type mismatch
    """
    test_file = TEST_FILES_DIR / 'invalid_config_wrong_type_metadata.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"wrong type|DLM Model Version|integer"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_wrong_type_percentage():
    """Test: Cannot convert to numeric type (TYPE-003)

    File to create: invalid_config_wrong_type_percentage.xlsx
    Modification: Change "% of Grand Total" in GeoHaz Thresholds to text "fifty"
    Expected: ConfigurationError about type conversion
    """
    test_file = TEST_FILES_DIR / 'invalid_config_wrong_type_percentage.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"type|% of Grand Total|float"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 3: NULLABILITY ERRORS
# ============================================================================

def test_invalid_config_null_required():
    """Test: Null value in required column (NULL-001)

    File to create: invalid_config_null_required.xlsx
    Modification: Make "Database" column empty for row 1 in Databases sheet
    Expected: ConfigurationError about null in non-nullable column
    """
    test_file = TEST_FILES_DIR / 'invalid_config_null_required.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"null|Database|not nullable|required"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_null_portfolio():
    """Test: Null value in required Portfolio field (NULL-001)

    File to create: invalid_config_null_portfolio.xlsx
    Modification: Make "Portfolio" column empty for row 1 in Portfolios sheet
    Expected: ConfigurationError about null in required field
    """
    test_file = TEST_FILES_DIR / 'invalid_config_null_portfolio.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"null|Portfolio|not nullable|required"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_null_metadata():
    """Test: Null required metadata key (NULL-002)

    File to create: invalid_config_null_metadata.xlsx
    Modification: Make "Current Date Value" empty in Metadata sheet
    Expected: ConfigurationError about null required key
    """
    test_file = TEST_FILES_DIR / 'invalid_config_null_metadata.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"null|Current Date Value|required"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 4: UNIQUENESS VIOLATIONS
# ============================================================================

def test_invalid_config_duplicate_database():
    """Test: Duplicate database name

    File to create: invalid_config_duplicate_database.xlsx
    Modification: Add duplicate database name in Databases sheet
    Expected: ConfigurationError about duplicate values
    """
    test_file = TEST_FILES_DIR / 'invalid_config_duplicate_database.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Duplicate|Database"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_duplicate_portfolio():
    """Test: Duplicate portfolio name

    File to create: invalid_config_duplicate_portfolio.xlsx
    Modification: Add duplicate portfolio name in Portfolios sheet
    Expected: ConfigurationError about duplicate values
    """
    test_file = TEST_FILES_DIR / 'invalid_config_duplicate_portfolio.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Duplicate|Portfolio"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_duplicate_analysis():
    """Test: Duplicate analysis name

    File to create: invalid_config_duplicate_analysis.xlsx
    Modification: Add duplicate analysis name in Analysis Table sheet
    Expected: ConfigurationError about duplicate values
    """
    test_file = TEST_FILES_DIR / 'invalid_config_duplicate_analysis.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Duplicate|Analysis Name"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_duplicate_treaty():
    """Test: Duplicate treaty name

    File to create: invalid_config_duplicate_treaty.xlsx
    Modification: Add duplicate treaty name in Reinsurance Treaties sheet
    Expected: ConfigurationError about duplicate values
    """
    test_file = TEST_FILES_DIR / 'invalid_config_duplicate_treaty.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Duplicate|Treaty"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 5: PATTERN/FORMAT VIOLATIONS
# ============================================================================

def test_invalid_config_bad_database_pattern():
    """Test: Database name doesn't match required pattern (FMT-001)

    File to create: invalid_config_bad_database_pattern.xlsx
    Modification: Change database name to "BAD_NAME_123" (not RMS_EDM_YYYYMM_*)
    Expected: ConfigurationError about pattern mismatch
    """
    test_file = TEST_FILES_DIR / 'invalid_config_bad_database_pattern.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"pattern|Database|RMS?_EDM"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_bad_yes_no():
    """Test: Invalid Y/N pattern (FMT-001)

    File to create: invalid_config_bad_yes_no.xlsx
    Modification: Change "Validate DLM Model Versions?" to "YES" (should be "Y")
    Expected: ConfigurationError about pattern mismatch
    """
    test_file = TEST_FILES_DIR / 'invalid_config_bad_yes_no.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"pattern|Validate DLM Model Versions"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_bad_version_format():
    """Test: Invalid version format (FMT-002)

    File to create: invalid_config_bad_version_format.xlsx
    Modification: Change "EDM Data Version" to "v1.2.3.4" (too many segments)
    Expected: ConfigurationError about version format
    """
    test_file = TEST_FILES_DIR / 'invalid_config_bad_version_format.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"pattern|EDM Data Version"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_bad_treaty_type():
    """Test: Invalid treaty type (FMT-001)

    File to create: invalid_config_bad_treaty_type.xlsx
    Modification: Change "Type" to "Invalid Type" (should be "Working Excess" or "Quota Share")
    Expected: ConfigurationError about pattern mismatch
    """
    test_file = TEST_FILES_DIR / 'invalid_config_bad_treaty_type.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"pattern|Type|Working Excess|Quota Share"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_bad_rdm_name():
    """Test: RDM name doesn't start with RMS_RDM_ (FMT-001)

    File to create: invalid_config_bad_rdm_name.xlsx
    Modification: Change "Export RDM Name" to "BAD_RDM_NAME"
    Expected: ConfigurationError about pattern mismatch
    """
    test_file = TEST_FILES_DIR / 'invalid_config_bad_rdm_name.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"pattern|Export RDM Name|RMS_RDM"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 6: RANGE CONSTRAINT VIOLATIONS
# ============================================================================

def test_invalid_config_percentage_over_100():
    """Test: Percentage value > 100 (RANGE-002)

    File to create: invalid_config_percentage_over_100.xlsx
    Modification: Set "% Covered" to 150 in Reinsurance Treaties
    Expected: ConfigurationError about range violation
    """
    test_file = TEST_FILES_DIR / 'invalid_config_percentage_over_100.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"range|% Covered|100"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_percentage_negative():
    """Test: Percentage value < 0 (RANGE-002)

    File to create: invalid_config_percentage_negative.xlsx
    Modification: Set "Threshold %" to -5 in GeoHaz Thresholds
    Expected: ConfigurationError about range violation
    """
    test_file = TEST_FILES_DIR / 'invalid_config_percentage_negative.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"range|Threshold %"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_multiple_percentages_invalid():
    """Test: Multiple percentage fields invalid (RANGE-002)

    File to create: invalid_config_multiple_percentages_invalid.xlsx
    Modification: Set multiple % fields in Reinsurance Treaties to 120
    Expected: ConfigurationError about range violations
    """
    test_file = TEST_FILES_DIR / 'invalid_config_multiple_percentages_invalid.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"range|%"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 7: FOREIGN KEY VIOLATIONS
# ============================================================================

def test_invalid_config_broken_fk_database():
    """Test: Portfolio references non-existent database (REF-001)

    File to create: invalid_config_broken_fk_database.xlsx
    Modification: In Portfolios, reference non-existent database "RMS_EDM_999999_FAKE"
    Expected: ConfigurationError about broken foreign key
    """
    test_file = TEST_FILES_DIR / 'invalid_config_broken_fk_database.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"reference|Database|not found"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_broken_fk_portfolio():
    """Test: Analysis references non-existent portfolio (REF-001)

    File to create: invalid_config_broken_fk_portfolio.xlsx
    Modification: In Analysis Table, reference non-existent portfolio "FAKE_PORTFOLIO"
    Expected: ConfigurationError about broken foreign key
    """
    test_file = TEST_FILES_DIR / 'invalid_config_broken_fk_portfolio.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"reference|Portfolio|not found"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_broken_fk_import_file():
    """Test: GeoHaz references non-existent import file (REF-001)

    File to create: invalid_config_broken_fk_import_file.xlsx
    Modification: In GeoHaz Thresholds, reference non-existent import file
    Expected: ConfigurationError about broken foreign key
    """
    test_file = TEST_FILES_DIR / 'invalid_config_broken_fk_import_file.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"reference|Import File|not found"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 8: SPECIAL REFERENCE VIOLATIONS (Cross-Sheet)
# ============================================================================

def test_invalid_config_missing_model_profile():
    """Test: Analysis Profile not in Moody's Reference Data (REF-004)

    File to create: invalid_config_missing_model_profile.xlsx
    Modification: In Analysis Table, reference non-existent "Analysis Profile"
    Expected: ConfigurationError about missing model profile
    """
    test_file = TEST_FILES_DIR / 'invalid_config_missing_model_profile.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Analysis Profile|not found|Reference Data"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_missing_output_profile():
    """Test: Output Profile not in Moody's Reference Data (REF-005)

    File to create: invalid_config_missing_output_profile.xlsx
    Modification: In Analysis Table, reference non-existent "Output Profile"
    Expected: ConfigurationError about missing output profile
    """
    test_file = TEST_FILES_DIR / 'invalid_config_missing_output_profile.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Output Profile|not found|Reference Data"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_missing_event_rate():
    """Test: Event Rate not in Moody's Reference Data (REF-006)

    File to create: invalid_config_missing_event_rate.xlsx
    Modification: In Analysis Table, reference non-existent "Event Rate"
    Expected: ConfigurationError about missing event rate
    """
    test_file = TEST_FILES_DIR / 'invalid_config_missing_event_rate.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Event Rate|not found|Reference Data"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_missing_treaty_reference():
    """Test: Treaty reference not found in Reinsurance Treaties (REF-007)

    File to create: invalid_config_missing_treaty_reference.xlsx
    Modification: In Analysis Table, reference non-existent treaty in "Reinsurance Treaty 1"
    Expected: ConfigurationError about missing treaty
    """
    test_file = TEST_FILES_DIR / 'invalid_config_missing_treaty_reference.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Reinsurance Treaty|not found"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_broken_products_reference():
    """Test: Products & Perils references invalid analysis/group (REF-003)

    File to create: invalid_config_broken_products_reference.xlsx
    Modification: In Products and Perils, reference non-existent analysis name
    Expected: ConfigurationError about invalid reference
    """
    test_file = TEST_FILES_DIR / 'invalid_config_broken_products_reference.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Analysis Name|not found"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 9: GROUPINGS REFERENCE VIOLATIONS
# ============================================================================

def test_invalid_config_groupings_invalid_item():
    """Test: Groupings item references non-existent entity (REF-003)

    File to create: invalid_config_groupings_invalid_item.xlsx
    Modification: In Groupings, add item that doesn't reference valid portfolio/analysis/group
    Expected: ConfigurationError about invalid grouping item
    """
    test_file = TEST_FILES_DIR / 'invalid_config_groupings_invalid_item.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Groupings|not found"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_groupings_forward_ref():
    """Test: Forward reference in groupings (order-dependent) (REF-003)

    File to create: invalid_config_groupings_forward_ref.xlsx
    Modification: Reference a group that's defined later in the sheet
    Expected: ConfigurationError about invalid reference (order matters)
    """
    test_file = TEST_FILES_DIR / 'invalid_config_groupings_forward_ref.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Groupings|not found"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 10: BUSINESS RULE VIOLATIONS
# ============================================================================

def test_invalid_config_no_base_portfolio():
    """Test: Database has no base portfolio (BUS-001)

    File to create: invalid_config_no_base_portfolio.xlsx
    Modification: Set all portfolios for a database to "Base Portfolio?" = "N"
    Expected: ConfigurationError about missing base portfolio
    """
    test_file = TEST_FILES_DIR / 'invalid_config_no_base_portfolio.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Base Portfolio|at least one required"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


def test_invalid_config_treaty_date_order():
    """Test: Expiration Date before Inception Date (BUS-002)

    File to create: invalid_config_treaty_date_order.xlsx
    Modification: Set "Expiration Date" before "Inception Date" in Reinsurance Treaties
    Expected: ConfigurationError about date ordering
    """
    test_file = TEST_FILES_DIR / 'invalid_config_treaty_date_order.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError, match=r"Expiration Date|after Inception Date"):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# TEST CATEGORY 11: MULTIPLE/COMPLEX ERRORS
# ============================================================================

def test_invalid_config_multiple_sheets():
    """Test: Errors in multiple sheets

    File to create: invalid_config_multiple_sheets.xlsx
    Modification: Introduce errors in 3+ different sheets
    Expected: ConfigurationError with multiple error messages
    """
    test_file = TEST_FILES_DIR / 'invalid_config_multiple_sheets.xlsx'

    if not test_file.exists():
        pytest.skip(f"Test file not found: {test_file}")

    with pytest.raises(ConfigurationError):
        validate_configuration_file(
            excel_config_path=str(test_file)
        )


# ============================================================================
# SUMMARY TEST - Run all negative tests that have files available
# ============================================================================

def test_negative_test_file_summary():
    """Summary test: Count how many negative test files exist

    This is a utility test to help track which test files have been created.
    """
    test_files_created = []
    test_files_missing = []

    # List all expected test files
    expected_files = [
        'invalid_config_missing_sheet.xlsx',
        'invalid_config_missing_columns.xlsx',
        'invalid_config_missing_metadata_key.xlsx',
        'invalid_config_wrong_type_metadata.xlsx',
        'invalid_config_wrong_type_percentage.xlsx',
        'invalid_config_null_required.xlsx',
        'invalid_config_null_portfolio.xlsx',
        'invalid_config_null_metadata.xlsx',
        'invalid_config_duplicate_database.xlsx',
        'invalid_config_duplicate_portfolio.xlsx',
        'invalid_config_duplicate_analysis.xlsx',
        'invalid_config_duplicate_treaty.xlsx',
        'invalid_config_bad_database_pattern.xlsx',
        'invalid_config_bad_yes_no.xlsx',
        'invalid_config_bad_version_format.xlsx',
        'invalid_config_bad_treaty_type.xlsx',
        'invalid_config_bad_rdm_name.xlsx',
        'invalid_config_percentage_over_100.xlsx',
        'invalid_config_percentage_negative.xlsx',
        'invalid_config_multiple_percentages_invalid.xlsx',
        'invalid_config_broken_fk_database.xlsx',
        'invalid_config_broken_fk_portfolio.xlsx',
        'invalid_config_broken_fk_import_file.xlsx',
        'invalid_config_missing_model_profile.xlsx',
        'invalid_config_missing_output_profile.xlsx',
        'invalid_config_missing_event_rate.xlsx',
        'invalid_config_missing_treaty_reference.xlsx',
        'invalid_config_broken_products_reference.xlsx',
        'invalid_config_groupings_invalid_item.xlsx',
        'invalid_config_groupings_forward_ref.xlsx',
        'invalid_config_no_base_portfolio.xlsx',
        'invalid_config_treaty_date_order.xlsx',
        'invalid_config_multiple_sheets.xlsx',
    ]

    for filename in expected_files:
        filepath = TEST_FILES_DIR / filename
        if filepath.exists():
            test_files_created.append(filename)
        else:
            test_files_missing.append(filename)

    print(f"\n{'='*70}")
    print("NEGATIVE TEST FILES SUMMARY")
    print(f"{'='*70}")
    print(f"Total expected files: {len(expected_files)}")
    print(f"Files created: {len(test_files_created)}")
    print(f"Files missing: {len(test_files_missing)}")
    print(f"\nFiles created ({len(test_files_created)}):")
    for f in test_files_created:
        print(f"  ✓ {f}")
    print(f"\nFiles missing ({len(test_files_missing)}):")
    for f in test_files_missing:
        print(f"  ✗ {f}")
    print(f"{'='*70}\n")

    # This test always passes - it's just informational
    assert True
