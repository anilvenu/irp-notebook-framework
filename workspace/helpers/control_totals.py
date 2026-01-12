"""
Control Totals Validation Module

This module provides functions for:
- Comparing control totals between stages (3a vs 3b, 3b vs 3d)
- Validating control totals against configuration thresholds (GeoHaz)
"""

import pandas as pd
from typing import Dict, List, Any, Tuple, Optional


def validate_geohaz_thresholds(
    geocoding_results: pd.DataFrame,
    geohaz_thresholds: List[Dict[str, Any]],
    import_file_mapping: Dict[str, str] = None
) -> Tuple[pd.DataFrame, bool]:
    """
    Validate geocoding results against GeoHaz threshold configuration.

    This function compares actual geocoding distribution (from SQL results) against
    expected thresholds defined in the configuration. For each Import File and Geocode Level,
    it calculates whether the actual percentage falls within the acceptable range:
    [% of Grand Total - Threshold %, % of Grand Total + Threshold %]

    Args:
        geocoding_results: DataFrame from 3e_GeocodingSummary.sql with columns:
            - DBname: Database name (e.g., "RM_EDM_202511_Quarterly_USEQ_DamQ")
            - PORTNAME: Portfolio name (e.g., "USEQ", "USFL_Other")
            - GeoResolutionCode: Numeric code for geocode level
            - GeocodeDescription: Description (e.g., "Coordinate", "Street address")
            - RiskCount: Number of locations at this geocode level
            - TIV: Total Insured Value
            - TRV: Total Replacement Value

        geohaz_thresholds: List of threshold dictionaries from configuration with:
            - Import File: Import file name (e.g., "USEQ", "USFL_Other")
            - Geocode Level: Expected geocode level name
            - % of Grand Total: Expected percentage (e.g., 93.2 for 93.2%)
            - Threshold %: Tolerance percentage (e.g., 5.0 for ±5%)

        import_file_mapping: Optional dict mapping PORTNAME to Import File.
            If None, assumes PORTNAME is the Import File (works for base portfolios).
            Example: {"USEQ_Clay_Homes": "USEQ", "USFL_Commercial": "USFL_Commercial"}

    Returns:
        Tuple of (validation_results_df, all_passed):
            - validation_results_df: DataFrame with columns:
                - Import File: Import file identifier
                - Portfolio: Portfolio name from results
                - Geocode Level: Geocode level description
                - Expected %: Expected percentage from config
                - Threshold %: Tolerance from config
                - Min %: Minimum acceptable percentage
                - Max %: Maximum acceptable percentage
                - Actual %: Actual percentage from results
                - Risk Count: Number of locations at this level
                - Status: "PASS" or "FAIL"
            - all_passed: Boolean indicating if all validations passed

    Example:
        ```python
        # Execute SQL to get geocoding results
        geocoding_df = execute_query_from_file(
            'control_totals/3e_GeocodingSummary.sql',
            params={'WORKSPACE_EDM': 'WORKSPACE_EDM_xyz', ...}
        )[0]  # First result set

        # Get thresholds from configuration
        config = read_configuration(config_id)
        thresholds = config.get('GeoHaz Thresholds', [])

        # Validate
        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_df,
            thresholds
        )

        # Display results
        print(results_df)
        if not all_passed:
            print("WARNING: Some geocoding thresholds were not met!")
        ```
    """

    # If no mapping provided, assume PORTNAME is the Import File
    if import_file_mapping is None:
        import_file_mapping = {}

    # Convert thresholds list to DataFrame for easier processing
    thresholds_df = pd.DataFrame(geohaz_thresholds)

    # Group geocoding results by portfolio to calculate percentages
    validation_results = []

    # Get unique portfolios from results
    portfolios = geocoding_results['PORTNAME'].unique()

    for portfolio in portfolios:
        # Determine the import file for this portfolio
        import_file = import_file_mapping.get(portfolio, portfolio)

        # Filter results for this portfolio
        portfolio_results = geocoding_results[geocoding_results['PORTNAME'] == portfolio].copy()

        # Calculate grand total for this portfolio
        grand_total = portfolio_results['RiskCount'].sum()

        # Skip if no locations
        if grand_total == 0:
            continue

        # Calculate actual percentages
        portfolio_results['Actual %'] = (portfolio_results['RiskCount'] / grand_total) * 100

        # Get thresholds for this import file
        portfolio_thresholds = thresholds_df[thresholds_df['Import File'] == import_file]

        # Skip if no thresholds defined for this import file
        if portfolio_thresholds.empty:
            continue

        # For each threshold, check if we have matching results
        for _, threshold_row in portfolio_thresholds.iterrows():
            geocode_level = threshold_row['Geocode Level']
            expected_pct = threshold_row['% of Grand Total']
            threshold_pct = threshold_row['Threshold %']

            # Calculate acceptable range (min cannot be below 0, max cannot be above 100)
            min_pct = max(0.0, expected_pct - threshold_pct)
            max_pct = min(100.0, expected_pct + threshold_pct)

            # Find matching result
            matching_result = portfolio_results[
                portfolio_results['GeocodeDescription'] == geocode_level
            ]

            if not matching_result.empty:
                actual_pct = matching_result.iloc[0]['Actual %']
                risk_count = matching_result.iloc[0]['RiskCount']
            else:
                # No results for this geocode level (0 locations)
                actual_pct = 0.0
                risk_count = 0

            # Determine pass/fail
            status = 'PASS' if min_pct <= actual_pct <= max_pct else 'FAIL'

            validation_results.append({
                'Import File': import_file,
                'Portfolio': portfolio,
                'Geocode Level': geocode_level,
                'Expected %': expected_pct,
                'Threshold %': threshold_pct,
                'Min %': min_pct,
                'Max %': max_pct,
                'Actual %': round(actual_pct, 3),
                'Risk Count': int(risk_count),
                'Status': status
            })

    # Convert results to DataFrame
    results_df = pd.DataFrame(validation_results)

    # Determine overall pass/fail
    all_passed = True
    if not results_df.empty:
        all_passed = (results_df['Status'] == 'PASS').all()

    return results_df, all_passed


def get_import_file_mapping_from_config(configuration_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract portfolio to import file mapping from configuration data.

    Args:
        configuration_data: Full configuration dictionary from read_configuration()

    Returns:
        Dictionary mapping portfolio names to import file names.
        Example: {"USEQ_Clay_Homes": "USEQ", "USFL_Commercial": "USFL_Commercial"}
    """
    portfolios = configuration_data.get('Portfolios', [])
    mapping = {}

    for portfolio_config in portfolios:
        portfolio_name = portfolio_config.get('Portfolio')
        import_file = portfolio_config.get('Import File')

        if portfolio_name and import_file:
            mapping[portfolio_name] = import_file

    return mapping


def _is_flood_exposure_group(exposure_group: str) -> bool:
    """Check if an ExposureGroup is a Flood portfolio (USFL_*)."""
    return exposure_group.startswith('USFL_')


def compare_3a_vs_3b(
    results_3a: List[pd.DataFrame],
    results_3b: List[pd.DataFrame]
) -> Tuple[pd.DataFrame, bool]:
    """
    Compare control totals between 3a (Working Table) and 3b (Contract Import File).

    This function compares attributes between 3a and 3b results by ExposureGroup.

    For Non-Flood perils (CBEQ, CBHU, USEQ, USFF, USST, USHU, USWF), compares 7 attributes:
    - PolicyCount: 3a.LocationCount vs 3b.PolicyCount
    - PolicyPremium: 3a.PolicyPremium vs 3b.PolicyPremium
    - PolicyLimit: 3a.PolicyLimit vs 3b.PolicyLimit
    - LocationCountDistinct: 3a.LocationCount vs 3b.LocationCountDistinct
    - TotalReplacementValue: 3a.TotalReplacementValue vs 3b.TotalReplacementValue
    - LocationLimit: 3a.LocationLimit vs 3b.LocationLimit
    - LocationDeductible: 3a.LocationDeductible vs 3b.LocationDeductible

    For Flood perils (USFL_*), compares 10 attributes with 3 additional Flood-specific:
    - PolicyCount, PolicyPremium, PolicyLimit (same column names in 3a and 3b)
    - AttachmentPoint, PolicyDeductible, PolicySublimit (Flood-only)
    - LocationCountDistinct, TotalReplacementValue, LocationLimit, LocationDeductible

    Difference is calculated as: 3b_value - 3a_value (0 means match)

    Args:
        results_3a: List of DataFrames from 3a_Control_Totals_Working_Table.sql
            Each DataFrame has columns: ExposureGroup, PolicyPremium, PolicyLimit,
            LocationCountDistinct (labeled as such in some sections, LocationCount in others),
            TotalReplacementValue, LocationLimit, LocationDeductible.
            Flood sections also have: PolicyCount, AttachmentPoint, PolicyDeductible, PolicySublimit
        results_3b: List of DataFrames from 3b_Control_Totals_Contract_Import_File_Tables.sql
            Each DataFrame has columns: ExposureGroup, PolicyCount, PolicyPremium,
            PolicyLimit, LocationCountDistinct, TotalReplacementValue, LocationLimit,
            LocationDeductible.
            Flood sections also have: AttachmentPoint, PolicyDeductible, PolicySublimit

    Returns:
        Tuple of (comparison_df, all_matched):
            - comparison_df: DataFrame with columns:
                - ExposureGroup: The exposure group identifier
                - Attribute: The attribute being compared
                - 3a_Value: Value from Working Table
                - 3b_Value: Value from Contract Import File
                - Difference: 3b_Value - 3a_Value
                - Status: "MATCH" if Difference == 0, else "MISMATCH"
            - all_matched: Boolean indicating if all comparisons matched (all differences are 0)

    Example:
        ```python
        from helpers.sqlserver import execute_query_from_file
        from helpers.control_totals import compare_3a_vs_3b

        # Execute 3a SQL
        results_3a = execute_query_from_file(
            'control_totals/3a_Control_Totals_Working_Table.sql',
            params={'DATE_VALUE': '202503'},
            connection='DATABRIDGE',
            database='DW_EXP_MGMT_USER'
        )

        # Execute 3b SQL
        results_3b = execute_query_from_file(
            'control_totals/3b_Control_Totals_Contract_Import_File_Tables.sql',
            params={'DATE_VALUE': '202503', 'CYCLE_TYPE': 'Quarterly'},
            connection='DATABRIDGE',
            database='DW_EXP_MGMT_USER'
        )

        # Compare
        comparison_df, all_matched = compare_3a_vs_3b(results_3a, results_3b)
        display(comparison_df)

        if all_matched:
            print("All control totals match!")
        else:
            print("WARNING: Some control totals do not match!")
        ```
    """
    # Non-Flood attributes: 7 attributes
    # Format: (attribute_name, 3a_column, 3b_column)
    NON_FLOOD_ATTRIBUTES = [
        ('PolicyCount', 'LocationCount', 'PolicyCount'),
        ('PolicyPremium', 'PolicyPremium', 'PolicyPremium'),
        ('PolicyLimit', 'PolicyLimit', 'PolicyLimit'),
        ('LocationCountDistinct', 'LocationCount', 'LocationCountDistinct'),
        ('TotalReplacementValue', 'TotalReplacementValue', 'TotalReplacementValue'),
        ('LocationLimit', 'LocationLimit', 'LocationLimit'),
        ('LocationDeductible', 'LocationDeductible', 'LocationDeductible'),
    ]

    # Flood attributes: 10 attributes (column names match between 3a and 3b)
    FLOOD_ATTRIBUTES = [
        ('PolicyCount', 'PolicyCount', 'PolicyCount'),
        ('PolicyPremium', 'PolicyPremium', 'PolicyPremium'),
        ('AttachmentPoint', 'AttachmentPoint', 'AttachmentPoint'),
        ('PolicyDeductible', 'PolicyDeductible', 'PolicyDeductible'),
        ('PolicyLimit', 'PolicyLimit', 'PolicyLimit'),
        ('PolicySublimit', 'PolicySublimit', 'PolicySublimit'),
        ('LocationCountDistinct', 'LocationCountDistinct', 'LocationCountDistinct'),
        ('TotalReplacementValue', 'TotalReplacementValue', 'TotalReplacementValue'),
        ('LocationLimit', 'LocationLimit', 'LocationLimit'),
        ('LocationDeductible', 'LocationDeductible', 'LocationDeductible'),
    ]

    # Combine all 3a result sets into one DataFrame
    df_3a_combined = pd.concat(results_3a, ignore_index=True)

    # Combine all 3b result sets into one DataFrame
    df_3b_combined = pd.concat(results_3b, ignore_index=True)

    # Normalize column names - handle variations in column naming for non-Flood
    # 3a may have LocationCountDistinct or LocationCount depending on section
    # Only rename if LocationCount doesn't exist (to avoid overwriting Flood's LocationCountDistinct)
    if 'LocationCountDistinct' in df_3a_combined.columns and 'LocationCount' not in df_3a_combined.columns:
        df_3a_combined = df_3a_combined.rename(columns={'LocationCountDistinct': 'LocationCount'})

    # Build comparison results
    comparison_results = []

    # Get all unique ExposureGroups from both sources
    all_exposure_groups = set(df_3a_combined['ExposureGroup'].unique()) | set(df_3b_combined['ExposureGroup'].unique())

    for exposure_group in sorted(all_exposure_groups):
        # Get row from 3a for this exposure group
        row_3a = df_3a_combined[df_3a_combined['ExposureGroup'] == exposure_group]

        # Get row from 3b for this exposure group
        row_3b = df_3b_combined[df_3b_combined['ExposureGroup'] == exposure_group]

        # Select appropriate attribute list based on exposure group type
        if _is_flood_exposure_group(exposure_group):
            attributes = FLOOD_ATTRIBUTES
        else:
            attributes = NON_FLOOD_ATTRIBUTES

        for attr_name, col_3a, col_3b in attributes:
            # Get 3a value
            if row_3a.empty:
                val_3a = None
            elif col_3a in row_3a.columns:
                val_3a = row_3a.iloc[0][col_3a]
            else:
                val_3a = None

            # Get 3b value
            if row_3b.empty:
                val_3b = None
            elif col_3b in row_3b.columns:
                val_3b = row_3b.iloc[0][col_3b]
            else:
                val_3b = None

            # Calculate difference (handle None values)
            if val_3a is not None and val_3b is not None:
                # Convert to numeric, handling string values like '0'
                try:
                    val_3a_num = float(val_3a) if val_3a is not None else 0
                    val_3b_num = float(val_3b) if val_3b is not None else 0
                    difference = val_3b_num - val_3a_num
                    status = 'MATCH' if difference == 0 else 'MISMATCH'
                except (ValueError, TypeError):
                    difference = None
                    status = 'ERROR'
            else:
                difference = None
                status = 'MISSING' if val_3a is None or val_3b is None else 'ERROR'

            comparison_results.append({
                'ExposureGroup': exposure_group,
                'Attribute': attr_name,
                '3a_Value': val_3a,
                '3b_Value': val_3b,
                'Difference': difference,
                'Status': status
            })

    # Convert to DataFrame
    comparison_df = pd.DataFrame(comparison_results)

    # Determine if all matched
    all_matched = True
    if not comparison_df.empty:
        all_matched = (comparison_df['Status'] == 'MATCH').all()

    return comparison_df, all_matched


def compare_3a_vs_3b_pivot(
    results_3a: List[pd.DataFrame],
    results_3b: List[pd.DataFrame]
) -> Tuple[pd.DataFrame, bool]:
    """
    Compare control totals between 3a and 3b in a pivoted format (one row per ExposureGroup).

    This provides a more compact view where each row shows all attribute differences
    for a single ExposureGroup.

    Args:
        results_3a: List of DataFrames from 3a_Control_Totals_Working_Table.sql
        results_3b: List of DataFrames from 3b_Control_Totals_Contract_Import_File_Tables.sql

    Returns:
        Tuple of (comparison_df, all_matched):
            - comparison_df: DataFrame with columns:
                - ExposureGroup
                - PolicyCount_Diff
                - PolicyPremium_Diff
                - PolicyLimit_Diff
                - LocationCountDistinct_Diff
                - TotalReplacementValue_Diff
                - LocationLimit_Diff
                - LocationDeductible_Diff
                - Status: "MATCH" if all differences are 0, else "MISMATCH"
            - all_matched: Boolean indicating if all comparisons matched

    Example:
        ```python
        comparison_df, all_matched = compare_3a_vs_3b_pivot(results_3a, results_3b)
        display(comparison_df)
        ```
    """
    # Get the detailed comparison first
    detailed_df, _ = compare_3a_vs_3b(results_3a, results_3b)

    if detailed_df.empty:
        return pd.DataFrame(), True

    # Pivot to get one row per ExposureGroup
    pivot_df = detailed_df.pivot(
        index='ExposureGroup',
        columns='Attribute',
        values='Difference'
    ).reset_index()

    # Rename columns to include _Diff suffix
    rename_map = {col: f'{col}_Diff' for col in pivot_df.columns if col != 'ExposureGroup'}
    pivot_df = pivot_df.rename(columns=rename_map)

    # Add overall status column
    diff_columns = [col for col in pivot_df.columns if col.endswith('_Diff')]
    pivot_df['Status'] = pivot_df[diff_columns].apply(
        lambda row: 'MATCH' if all(v == 0 for v in row if v is not None and not pd.isna(v)) else 'MISMATCH',
        axis=1
    )

    # Determine if all matched
    all_matched = (pivot_df['Status'] == 'MATCH').all()

    return pivot_df, all_matched
