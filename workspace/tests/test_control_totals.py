"""
Tests for control_totals module (GeoHaz threshold validation).
"""

import pytest
import pandas as pd
from helpers.control_totals import (
    validate_geohaz_thresholds,
    get_import_file_mapping_from_config
)


class TestValidateGeohazThresholds:
    """Tests for validate_geohaz_thresholds function."""

    def test_basic_validation_all_pass(self):
        """Test basic validation where all thresholds pass."""
        # Create sample geocoding results
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 930,
                'TIV': 1000000.0,
                'TRV': 1000000.0
            },
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 2,
                'GeocodeDescription': 'Street address',
                'RiskCount': 70,
                'TIV': 100000.0,
                'TRV': 100000.0
            }
        ])

        # Create thresholds (expecting 93% ± 5% for Coordinate, 7% ± 2% for Street address)
        thresholds = [
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 93.0,
                'Threshold %': 5.0
            },
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Street address',
                '% of Grand Total': 7.0,
                'Threshold %': 2.0
            }
        ]

        # Run validation
        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Assertions
        assert all_passed == True
        assert len(results_df) == 2
        assert (results_df['Status'] == 'PASS').all()
        assert results_df.iloc[0]['Actual %'] == 93.0  # 930/1000 = 93%
        assert results_df.iloc[1]['Actual %'] == 7.0   # 70/1000 = 7%

    def test_validation_with_failures(self):
        """Test validation where some thresholds fail."""
        # Create sample geocoding results with poor geocoding
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 800,  # Only 80% (below 88% minimum)
                'TIV': 1000000.0,
                'TRV': 1000000.0
            },
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 2,
                'GeocodeDescription': 'Street address',
                'RiskCount': 200,  # 20% (above 9% maximum)
                'TIV': 100000.0,
                'TRV': 100000.0
            }
        ])

        # Create thresholds
        thresholds = [
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 93.0,
                'Threshold %': 5.0  # Range: 88-98%
            },
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Street address',
                '% of Grand Total': 7.0,
                'Threshold %': 2.0  # Range: 5-9%
            }
        ]

        # Run validation
        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Assertions
        assert all_passed == False
        assert len(results_df) == 2
        assert results_df.iloc[0]['Status'] == 'FAIL'  # 80% < 88%
        assert results_df.iloc[1]['Status'] == 'FAIL'  # 20% > 9%

    def test_min_percentage_clamped_at_zero(self):
        """Test that minimum percentage is never negative."""
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 0,
                'GeocodeDescription': 'Null',
                'RiskCount': 0,
                'TIV': 0.0,
                'TRV': 0.0
            },
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 1000,
                'TIV': 1000000.0,
                'TRV': 1000000.0
            }
        ])

        thresholds = [
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Null',
                '% of Grand Total': 0.0,
                'Threshold %': 0.5  # Would create range -0.5% to 0.5%
            }
        ]

        results_df, _ = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Min % should be clamped to 0.0, not -0.5
        assert results_df.iloc[0]['Min %'] == 0.0
        assert results_df.iloc[0]['Max %'] == 0.5

    def test_max_percentage_clamped_at_100(self):
        """Test that maximum percentage is never above 100."""
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USFL_Commercial',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 1000,
                'TIV': 1000000.0,
                'TRV': 1000000.0
            }
        ])

        thresholds = [
            {
                'Import File': 'USFL_Commercial',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 99.9,
                'Threshold %': 2.0  # Would create range 97.9% to 101.9%
            }
        ]

        results_df, _ = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Max % should be clamped to 100.0, not 101.9
        assert results_df.iloc[0]['Min %'] == 97.9
        assert results_df.iloc[0]['Max %'] == 100.0

    def test_missing_geocode_level_in_results(self):
        """Test handling when a threshold exists but no matching geocode level in results."""
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 1000,
                'TIV': 1000000.0,
                'TRV': 1000000.0
            }
        ])

        thresholds = [
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 100.0,
                'Threshold %': 0.5
            },
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Street address',  # Not in results
                '% of Grand Total': 0.0,
                'Threshold %': 0.1
            }
        ]

        results_df, _ = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Should have 2 rows
        assert len(results_df) == 2

        # Street address should show 0 actual % and 0 risk count
        street_address_row = results_df[results_df['Geocode Level'] == 'Street address'].iloc[0]
        assert street_address_row['Actual %'] == 0.0
        assert street_address_row['Risk Count'] == 0

    def test_multiple_portfolios(self):
        """Test validation across multiple portfolios."""
        geocoding_results = pd.DataFrame([
            # USEQ portfolio
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 930,
                'TIV': 1000000.0,
                'TRV': 1000000.0
            },
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 2,
                'GeocodeDescription': 'Street address',
                'RiskCount': 70,
                'TIV': 100000.0,
                'TRV': 100000.0
            },
            # USFL_Other portfolio
            {
                'DBname': 'RM_EDM_Test2',
                'PORTNAME': 'USFL_Other',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 900,
                'TIV': 2000000.0,
                'TRV': 2000000.0
            },
            {
                'DBname': 'RM_EDM_Test2',
                'PORTNAME': 'USFL_Other',
                'GeoResolutionCode': 2,
                'GeocodeDescription': 'Street address',
                'RiskCount': 100,
                'TIV': 200000.0,
                'TRV': 200000.0
            }
        ])

        thresholds = [
            # USEQ thresholds
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 93.0,
                'Threshold %': 5.0
            },
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Street address',
                '% of Grand Total': 7.0,
                'Threshold %': 2.0
            },
            # USFL_Other thresholds
            {
                'Import File': 'USFL_Other',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 90.0,
                'Threshold %': 5.0
            },
            {
                'Import File': 'USFL_Other',
                'Geocode Level': 'Street address',
                '% of Grand Total': 10.0,
                'Threshold %': 2.0
            }
        ]

        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Should have 4 validation results (2 per portfolio)
        assert len(results_df) == 4
        assert all_passed == True

        # Check USEQ results
        useq_results = results_df[results_df['Portfolio'] == 'USEQ']
        assert len(useq_results) == 2

        # Check USFL_Other results
        usfl_results = results_df[results_df['Portfolio'] == 'USFL_Other']
        assert len(usfl_results) == 2

    def test_with_import_file_mapping(self):
        """Test validation with portfolio-to-import-file mapping."""
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ_Clay_Homes',  # Sub-portfolio
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 930,
                'TIV': 1000000.0,
                'TRV': 1000000.0
            },
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ_Clay_Homes',
                'GeoResolutionCode': 2,
                'GeocodeDescription': 'Street address',
                'RiskCount': 70,
                'TIV': 100000.0,
                'TRV': 100000.0
            }
        ])

        thresholds = [
            {
                'Import File': 'USEQ',  # Parent import file
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 93.0,
                'Threshold %': 5.0
            },
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Street address',
                '% of Grand Total': 7.0,
                'Threshold %': 2.0
            }
        ]

        # Map sub-portfolio to parent import file
        import_file_mapping = {
            'USEQ_Clay_Homes': 'USEQ'
        }

        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds,
            import_file_mapping=import_file_mapping
        )

        assert all_passed == True
        assert len(results_df) == 2
        assert results_df.iloc[0]['Import File'] == 'USEQ'
        assert results_df.iloc[0]['Portfolio'] == 'USEQ_Clay_Homes'

    def test_no_thresholds_for_portfolio(self):
        """Test when there are no thresholds defined for a portfolio."""
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'UNKNOWN_PORTFOLIO',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 1000,
                'TIV': 1000000.0,
                'TRV': 1000000.0
            }
        ])

        thresholds = [
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 93.0,
                'Threshold %': 5.0
            }
        ]

        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Should return empty results and all_passed = True (no checks to fail)
        assert len(results_df) == 0
        assert all_passed is True

    def test_empty_geocoding_results(self):
        """Test handling of empty geocoding results."""
        geocoding_results = pd.DataFrame(columns=[
            'DBname', 'PORTNAME', 'GeoResolutionCode',
            'GeocodeDescription', 'RiskCount', 'TIV', 'TRV'
        ])

        thresholds = [
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 93.0,
                'Threshold %': 5.0
            }
        ]

        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        assert len(results_df) == 0
        assert all_passed is True

    def test_portfolio_with_zero_total_locations(self):
        """Test handling of portfolio with zero total locations."""
        geocoding_results = pd.DataFrame([
            {
                'DBname': 'RM_EDM_Test',
                'PORTNAME': 'USEQ',
                'GeoResolutionCode': 1,
                'GeocodeDescription': 'Coordinate',
                'RiskCount': 0,
                'TIV': 0.0,
                'TRV': 0.0
            }
        ])

        thresholds = [
            {
                'Import File': 'USEQ',
                'Geocode Level': 'Coordinate',
                '% of Grand Total': 93.0,
                'Threshold %': 5.0
            }
        ]

        results_df, all_passed = validate_geohaz_thresholds(
            geocoding_results=geocoding_results,
            geohaz_thresholds=thresholds
        )

        # Should skip portfolio with 0 total locations
        assert len(results_df) == 0
        assert all_passed is True


class TestGetImportFileMappingFromConfig:
    """Tests for get_import_file_mapping_from_config function."""

    def test_basic_mapping_extraction(self):
        """Test basic extraction of portfolio-to-import-file mapping."""
        configuration_data = {
            'Portfolios': [
                {
                    'Portfolio': 'USEQ',
                    'Import File': 'USEQ',
                    'Database': 'RM_EDM_Test',
                    'Base Portfolio?': 'Y'
                },
                {
                    'Portfolio': 'USEQ_Clay_Homes',
                    'Import File': 'USEQ',
                    'Database': 'RM_EDM_Test',
                    'Base Portfolio?': 'N'
                },
                {
                    'Portfolio': 'USFL_Commercial',
                    'Import File': 'USFL_Commercial',
                    'Database': 'RM_EDM_Test2',
                    'Base Portfolio?': 'Y'
                }
            ]
        }

        mapping = get_import_file_mapping_from_config(configuration_data)

        assert mapping == {
            'USEQ': 'USEQ',
            'USEQ_Clay_Homes': 'USEQ',
            'USFL_Commercial': 'USFL_Commercial'
        }

    def test_empty_portfolios(self):
        """Test when Portfolios list is empty."""
        configuration_data = {
            'Portfolios': []
        }

        mapping = get_import_file_mapping_from_config(configuration_data)
        assert mapping == {}

    def test_missing_portfolios_key(self):
        """Test when Portfolios key is missing."""
        configuration_data = {}

        mapping = get_import_file_mapping_from_config(configuration_data)
        assert mapping == {}

    def test_incomplete_portfolio_data(self):
        """Test handling of portfolios with missing fields."""
        configuration_data = {
            'Portfolios': [
                {
                    'Portfolio': 'USEQ',
                    'Import File': 'USEQ'
                },
                {
                    'Portfolio': 'USFL_Other',
                    # Missing Import File
                },
                {
                    # Missing Portfolio
                    'Import File': 'CBHU'
                },
                {
                    'Portfolio': 'USFL_Commercial',
                    'Import File': 'USFL_Commercial'
                }
            ]
        }

        mapping = get_import_file_mapping_from_config(configuration_data)

        # Should only include portfolios with both fields present
        assert mapping == {
            'USEQ': 'USEQ',
            'USFL_Commercial': 'USFL_Commercial'
        }
