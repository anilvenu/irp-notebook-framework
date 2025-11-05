# Test Files - Negative Configuration Scenarios

This directory contains Excel configuration files for testing validation logic.

## Valid Configuration Files

- **valid_excel_configuration.xlsx** - Valid baseline configuration
- **invalid_excel_configuration.xlsx** - Original invalid file

## Negative Test Scenarios

All `invalid_config_*.xlsx` files are copies of the valid configuration with specific modifications to test validation errors.

### Test Scenarios Table

| # | File Name | Sheet Modified | Error Type | Expected Error Code | Test Description |
|---|-----------|----------------|------------|-------------------|---------------------|
| **1. STRUCTURE ERRORS** |
| 1.1 | `invalid_config_missing_sheet.xlsx` | Removed 'Databases' | Structure | STRUCT-001 | Missing required sheet |
| 1.2 | `invalid_config_missing_columns.xlsx` | Databases | Structure | STRUCT-002 | Missing required columns |
| 1.3 | `invalid_config_missing_metadata_key.xlsx` | Metadata | Structure | STRUCT-003 | Missing required key in key-value structure |
| **2. TYPE ERRORS** |
| 2.1 | `invalid_config_wrong_type_metadata.xlsx` | Metadata | Type | TYPE-002 | Wrong type in metadata (expected integer) |
| 2.2 | `invalid_config_wrong_type_percentage.xlsx` | GeoHaz Thresholds | Type | TYPE-003 | Cannot convert to float |
| **3. NULLABILITY ERRORS** |
| 3.1 | `invalid_config_null_required.xlsx` | Databases | Null | NULL-001 | Null in non-nullable column |
| 3.2 | `invalid_config_null_portfolio.xlsx` | Portfolios | Null | NULL-001 | Null in required field |
| 3.3 | `invalid_config_null_metadata.xlsx` | Metadata | Null | NULL-002 | Null required metadata key |
| **4. UNIQUENESS VIOLATIONS** |
| 4.1 | `invalid_config_duplicate_database.xlsx` | Databases | Uniqueness | - | Duplicate values in unique column |
| 4.2 | `invalid_config_duplicate_portfolio.xlsx` | Portfolios | Uniqueness | - | Duplicate portfolio name |
| 4.3 | `invalid_config_duplicate_analysis.xlsx` | Analysis Table | Uniqueness | - | Duplicate analysis name |
| 4.4 | `invalid_config_duplicate_treaty.xlsx` | Reinsurance Treaties | Uniqueness | - | Duplicate treaty identifier |
| **5. PATTERN/FORMAT VIOLATIONS** |
| 5.1 | `invalid_config_bad_database_pattern.xlsx` | Databases | Pattern | FMT-001 | Database name doesn't match pattern |
| 5.2 | `invalid_config_bad_yes_no.xlsx` | Metadata | Pattern | FMT-001 | Invalid Y/N pattern |
| 5.3 | `invalid_config_bad_version_format.xlsx` | Metadata | Pattern | FMT-002 | Invalid version format |
| 5.4 | `invalid_config_bad_treaty_type.xlsx` | Reinsurance Treaties | Pattern | FMT-001 | Invalid treaty type (must be "Working Excess" or "Quota Share") |
| 5.5 | `invalid_config_bad_rdm_name.xlsx` | Metadata | Pattern | FMT-001 | RDM name doesn't start with RMS_RDM_ |
| **6. RANGE CONSTRAINT VIOLATIONS** |
| 6.1 | `invalid_config_percentage_over_100.xlsx` | Reinsurance Treaties | Range | RANGE-002 | Percentage > 100 |
| 6.2 | `invalid_config_percentage_negative.xlsx` | GeoHaz Thresholds | Range | RANGE-002 | Percentage < 0 |
| 6.3 | `invalid_config_multiple_percentages_invalid.xlsx` | Reinsurance Treaties | Range | RANGE-002 | Multiple percentage violations |
| **7. FOREIGN KEY VIOLATIONS** |
| 7.1 | `invalid_config_broken_fk_database.xlsx` | Portfolios | Foreign Key | REF-001 | Portfolio references non-existent database |
| 7.2 | `invalid_config_broken_fk_portfolio.xlsx` | Analysis Table | Foreign Key | REF-001 | Analysis references non-existent portfolio |
| 7.3 | `invalid_config_broken_fk_import_file.xlsx` | GeoHaz Thresholds | Foreign Key | REF-001 | GeoHaz references non-existent import file |
| **8. SPECIAL REFERENCE VIOLATIONS (Cross-Sheet)** |
| 8.1 | `invalid_config_missing_model_profile.xlsx` | Analysis Table | Special Ref | REF-004 | Analysis Profile not in Moody's Reference Data |
| 8.2 | `invalid_config_missing_output_profile.xlsx` | Analysis Table | Special Ref | REF-005 | Output Profile not in Moody's Reference Data |
| 8.3 | `invalid_config_missing_event_rate.xlsx` | Analysis Table | Special Ref | REF-006 | Event Rate not in Moody's Reference Data |
| 8.4 | `invalid_config_missing_treaty_reference.xlsx` | Analysis Table | Special Ref | REF-007 | Treaty not found in Reinsurance Treaties |
| 8.5 | `invalid_config_broken_products_reference.xlsx` | Products and Perils | Special Ref | REF-003 | Products & Perils references invalid analysis/group |
| **9. GROUPINGS REFERENCE VIOLATIONS** |
| 9.1 | `invalid_config_groupings_invalid_item.xlsx` | Groupings | Groupings | REF-003 | Groupings item not found |
| 9.2 | `invalid_config_groupings_forward_ref.xlsx` | Groupings | Groupings | REF-003 | Forward reference (order-dependent) |
| **10. BUSINESS RULE VIOLATIONS** |
| 10.1 | `invalid_config_no_base_portfolio.xlsx` | Portfolios | Business Rule | BUS-001 | Database has no base portfolio |
| 10.2 | `invalid_config_treaty_date_order.xlsx` | Reinsurance Treaties | Business Rule | BUS-002 | Expiration before inception |
| **11. MULTIPLE/COMPLEX ERRORS** |
| 11.1 | `invalid_config_multiple_sheets.xlsx` | Multiple | Multiple | Multiple | Multiple validation errors across sheets |
---

## Configuration Sheet Reference

Each Excel file should contain these 9 sheets:

1. **Metadata** - Key-value structure (no header)
2. **Databases** - Table with columns: Database, Store in Data Bridge?
3. **Portfolios** - Table with columns: Portfolio, Database, Import File, Base Portfolio?
4. **Reinsurance Treaties** - Complex table with 23 columns
5. **GeoHaz Thresholds** - Table with columns: Geocode Level, Import File, % of Grand Total, Threshold %
6. **Analysis Table** - Large table with Database, Portfolio, Analysis Name, profiles, treaties, tags
7. **Groupings** - Special structure: Group_Name + Item1-Item50 columns
8. **Products and Perils** - Table with columns: Analysis Name, Peril, Product Group
9. **Moody's Reference Data** - Dictionary of lists: Model Profiles, Output Profiles, Event Rate Schemes

---

## Validation Error Code Reference

| Code | Description |
|------|-------------|
| STRUCT-001 | Missing required sheet |
| STRUCT-002 | Missing required columns |
| STRUCT-003 | Missing required key |
| TYPE-001 | Column has wrong type |
| TYPE-002 | Key has wrong type |
| TYPE-003 | Cannot convert value to expected type |
| NULL-001 | Column has null values (not nullable) |
| NULL-002 | Key is null (required) |
| NULL-003 | Row has null value in required column |
| REF-001 | Broken reference (foreign key) |
| REF-002 | Circular reference detected |
| REF-003 | Invalid reference in Groupings |
| REF-004 | Analysis Profile not found |
| REF-005 | Output Profile not found |
| REF-006 | Event Rate Scheme not found |
| REF-007 | Reinsurance Treaty not found |
| BUS-001 | Database has no Base Portfolio |
| BUS-002 | Expiration Date before Inception Date |
| BUS-003 | Duplicate ACTIVE configuration |
| BUS-004 | Cycle ID mismatch |
| FMT-001 | Value doesn't match required pattern |
| FMT-002 | Invalid version format |
| FMT-003 | Invalid date format |
| FMT-004 | Database name pattern mismatch |
| RANGE-001 | Value out of range |
| RANGE-002 | Percentage not between 0-100 |
| RANGE-003 | Monetary value negative |
| RANGE-004 | Integer value invalid |
