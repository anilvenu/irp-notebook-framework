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
