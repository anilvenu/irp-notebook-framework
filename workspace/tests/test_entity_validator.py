"""
Unit tests for EntityValidator.

Tests entity existence validation logic with mocked API responses.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path

# Add workspace directory to Python path for imports
workspace_path = Path(__file__).parent.parent.resolve()
if str(workspace_path) not in sys.path:
    sys.path.insert(0, str(workspace_path))

from helpers.entity_validator import EntityValidator
from helpers.irp_integration.exceptions import IRPAPIError


class TestValidateEdmsNotExist:
    """Tests for validate_edms_not_exist method."""

    def test_empty_list_returns_no_errors(self):
        """Empty EDM list should return empty results."""
        validator = EntityValidator()
        existing, errors = validator.validate_edms_not_exist([])
        assert existing == []
        assert errors == []

    def test_no_existing_edms(self):
        """When no EDMs exist in Moody's, should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        existing, errors = validator.validate_edms_not_exist(['EDM1', 'EDM2'])

        assert existing == []
        assert errors == []
        validator._edm_manager.search_edms_paginated.assert_called_once()

    def test_some_edms_exist(self):
        """When some EDMs exist, should return them with error message."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        existing, errors = validator.validate_edms_not_exist(['EDM1', 'EDM2'])

        assert existing == ['EDM1']
        assert len(errors) == 1
        assert 'ENT-EDM-001' in errors[0]
        assert '  - EDM1' in errors[0]  # Multi-line format

    def test_all_edms_exist(self):
        """When all EDMs exist, should return all with error message."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]

        existing, errors = validator.validate_edms_not_exist(['EDM1', 'EDM2'])

        assert set(existing) == {'EDM1', 'EDM2'}
        assert len(errors) == 1
        assert 'ENT-EDM-001' in errors[0]

    def test_api_error_handled(self):
        """API errors should be caught and returned as error messages."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.side_effect = IRPAPIError("Connection failed")

        existing, errors = validator.validate_edms_not_exist(['EDM1'])

        assert existing == []
        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]
        assert 'Connection failed' in errors[0]


class TestValidatePortfoliosNotExist:
    """Tests for validate_portfolios_not_exist method."""

    def test_empty_inputs_returns_no_errors(self):
        """Empty portfolios or EDM IDs should return empty results."""
        validator = EntityValidator()

        existing, errors = validator.validate_portfolios_not_exist([], {})
        assert existing == []
        assert errors == []

        existing, errors = validator.validate_portfolios_not_exist(
            [{'Database': 'EDM1', 'Portfolio': 'Port1'}],
            {}  # No EDM IDs means EDMs don't exist
        )
        assert existing == []
        assert errors == []

    def test_no_existing_portfolios(self):
        """When no portfolios exist, should return no errors."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_portfolios_paginated.return_value = []

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'},
            {'Database': 'EDM1', 'Portfolio': 'Port2'}
        ]
        edm_ids = {'EDM1': 123}

        existing, errors = validator.validate_portfolios_not_exist(portfolios, edm_ids)

        assert existing == []
        assert errors == []

    def test_some_portfolios_exist(self):
        """When some portfolios exist, should return them with error."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'id': 1}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'},
            {'Database': 'EDM1', 'Portfolio': 'Port2'}
        ]
        edm_ids = {'EDM1': 123}

        existing, errors = validator.validate_portfolios_not_exist(portfolios, edm_ids)

        assert existing == ['EDM1/Port1']
        assert len(errors) == 1
        assert 'ENT-PORT-001' in errors[0]

    def test_portfolios_across_multiple_edms(self):
        """Should check portfolios in each EDM separately."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        # Return different results for each EDM
        validator._portfolio_manager.search_portfolios_paginated.side_effect = [
            [{'portfolioName': 'Port1', 'id': 1}],  # EDM1 has Port1
            []  # EDM2 has no matching portfolios
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'},
            {'Database': 'EDM2', 'Portfolio': 'Port2'}
        ]
        edm_ids = {'EDM1': 123, 'EDM2': 456}

        existing, errors = validator.validate_portfolios_not_exist(portfolios, edm_ids)

        assert existing == ['EDM1/Port1']
        assert len(errors) == 1


class TestValidateTreatiesNotExist:
    """Tests for validate_treaties_not_exist method."""

    def test_empty_inputs_returns_no_errors(self):
        """Empty treaties should return empty results."""
        validator = EntityValidator()
        existing, errors = validator.validate_treaties_not_exist([], {})
        assert existing == []
        assert errors == []

    def test_treaties_exist(self):
        """When treaties exist, should return them with error."""
        validator = EntityValidator()
        validator._treaty_manager = Mock()
        validator._treaty_manager.search_treaties_paginated.return_value = [
            {'treatyName': 'Treaty1', 'treatyId': 1}
        ]

        treaties = [
            {'Database': 'EDM1', 'Treaty Name': 'Treaty1'},
            {'Database': 'EDM1', 'Treaty Name': 'Treaty2'}
        ]
        edm_ids = {'EDM1': 123}

        existing, errors = validator.validate_treaties_not_exist(treaties, edm_ids)

        assert existing == ['EDM1/Treaty1']
        assert len(errors) == 1
        assert 'ENT-TREATY-001' in errors[0]


class TestValidateAnalysesNotExist:
    """Tests for validate_analyses_not_exist method."""

    def test_empty_inputs_returns_no_errors(self):
        """Empty analyses should return empty results."""
        validator = EntityValidator()
        existing, errors = validator.validate_analyses_not_exist([], {})
        assert existing == []
        assert errors == []

    def test_analyses_exist(self):
        """When analyses exist, should return them with error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Analysis1', 'analysisId': 1}
        ]

        analyses = [
            {'Database': 'EDM1', 'Analysis Name': 'Analysis1'},
            {'Database': 'EDM1', 'Analysis Name': 'Analysis2'}
        ]
        edm_ids = {'EDM1': 123}

        existing, errors = validator.validate_analyses_not_exist(analyses, edm_ids)

        assert existing == ['EDM1/Analysis1']
        assert len(errors) == 1
        assert 'ENT-ANALYSIS-001' in errors[0]


class TestValidateGroupsNotExist:
    """Tests for validate_groups_not_exist method."""

    def test_empty_inputs_returns_no_errors(self):
        """Empty groupings should return empty results."""
        validator = EntityValidator()
        existing, errors = validator.validate_groups_not_exist([])
        assert existing == []
        assert errors == []

    def test_groups_exist(self):
        """When groups exist, should return them with error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Group1', 'analysisId': 1}
        ]

        groupings = [
            {'Group Name': 'Group1'},
            {'Group Name': 'Group2'}
        ]

        existing, errors = validator.validate_groups_not_exist(groupings)

        assert existing == ['Group1']
        assert len(errors) == 1
        assert 'ENT-GROUP-001' in errors[0]

    def test_handles_duplicate_group_names(self):
        """Should deduplicate group names before checking."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = []

        groupings = [
            {'Group Name': 'Group1'},
            {'Group Name': 'Group1'},  # Duplicate
            {'Group Name': 'Group2'}
        ]

        existing, errors = validator.validate_groups_not_exist(groupings)

        # Should only search for unique names
        call_args = validator._analysis_manager.search_analyses_paginated.call_args
        filter_str = call_args[1]['filter']
        # Count occurrences of Group1 in the filter - should be 1
        assert filter_str.count('"Group1"') == 1


class TestValidateRdmNotExists:
    """Tests for validate_rdm_not_exists method."""

    def test_empty_rdm_name_returns_no_errors(self):
        """Empty RDM name should return empty results."""
        validator = EntityValidator()
        errors = validator.validate_rdm_not_exists('')
        assert errors == []

        errors = validator.validate_rdm_not_exists(None)
        assert errors == []

    def test_rdm_exists(self):
        """When RDM exists, should return error."""
        validator = EntityValidator()
        validator._rdm_manager = Mock()
        validator._rdm_manager.search_databases.return_value = [
            {'databaseName': 'MyRDM_20241201', 'databaseId': 1}
        ]

        errors = validator.validate_rdm_not_exists('MyRDM')

        assert len(errors) == 1
        assert 'ENT-RDM-001' in errors[0]
        assert 'MyRDM_20241201' in errors[0]

    def test_rdm_not_exists(self):
        """When RDM doesn't exist, should return no errors."""
        validator = EntityValidator()
        validator._rdm_manager = Mock()
        validator._rdm_manager.search_databases.return_value = []

        errors = validator.validate_rdm_not_exists('MyRDM')

        assert errors == []


class TestValidateConfigEntitiesNotExist:
    """Tests for validate_config_entities_not_exist cascade logic."""

    def test_empty_config_returns_no_errors(self):
        """Empty config should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        config_data = {
            'Databases': [],
            'Portfolios': [],
            'Reinsurance Treaties': [],
            'Analysis Table': [],
            'Groupings': [],
            'Metadata': {}
        }

        errors = validator.validate_config_entities_not_exist(config_data)
        assert errors == []

    def test_no_edms_exist_skips_downstream_checks(self):
        """If no EDMs exist, should not check portfolios/treaties/etc."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []
        validator._portfolio_manager = Mock()

        config_data = {
            'Databases': [{'Database': 'EDM1'}],
            'Portfolios': [{'Database': 'EDM1', 'Portfolio': 'Port1'}],
            'Reinsurance Treaties': [],
            'Analysis Table': [],
            'Groupings': [],
            'Metadata': {}
        }

        errors = validator.validate_config_entities_not_exist(config_data)

        assert errors == []
        # Portfolio manager should not have been called
        validator._portfolio_manager.search_portfolios_paginated.assert_not_called()

    def test_edms_exist_triggers_portfolio_treaty_checks(self):
        """If EDMs exist, should check portfolios and treaties."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_portfolios_paginated.return_value = []
        validator._treaty_manager = Mock()
        validator._treaty_manager.search_treaties_paginated.return_value = []

        config_data = {
            'Databases': [{'Database': 'EDM1'}],
            'Portfolios': [{'Database': 'EDM1', 'Portfolio': 'Port1'}],
            'Reinsurance Treaties': [{'Database': 'EDM1', 'Treaty Name': 'Treaty1'}],
            'Analysis Table': [],
            'Groupings': [],
            'Metadata': {}
        }

        errors = validator.validate_config_entities_not_exist(config_data)

        # Should have EDM error
        assert len(errors) == 1
        assert 'ENT-EDM-001' in errors[0]

        # Portfolio and treaty managers should have been called
        validator._portfolio_manager.search_portfolios_paginated.assert_called()
        validator._treaty_manager.search_treaties_paginated.assert_called()

    def test_full_cascade_when_all_exist(self):
        """When all entities exist, should report all errors."""
        validator = EntityValidator()

        # Mock all managers to return existing entities
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'id': 1}
        ]

        validator._treaty_manager = Mock()
        validator._treaty_manager.search_treaties_paginated.return_value = [
            {'treatyName': 'Treaty1', 'treatyId': 1}
        ]

        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'Analysis1', 'analysisId': 1}],  # For analyses check
            [{'analysisName': 'Group1', 'analysisId': 2}]  # For groups check
        ]

        validator._rdm_manager = Mock()
        validator._rdm_manager.search_databases.return_value = [
            {'databaseName': 'MyRDM_20241201', 'databaseId': 1}
        ]

        config_data = {
            'Databases': [{'Database': 'EDM1'}],
            'Portfolios': [{'Database': 'EDM1', 'Portfolio': 'Port1'}],
            'Reinsurance Treaties': [{'Database': 'EDM1', 'Treaty Name': 'Treaty1'}],
            'Analysis Table': [{'Database': 'EDM1', 'Analysis Name': 'Analysis1'}],
            'Groupings': [{'Group Name': 'Group1'}],
            'Metadata': {'Export RDM Name': 'MyRDM'}
        }

        errors = validator.validate_config_entities_not_exist(config_data)

        # Should have errors for EDM, Portfolio, Treaty, Analysis, Group, and RDM
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-EDM-001' in error_codes
        assert 'ENT-PORT-001' in error_codes
        assert 'ENT-TREATY-001' in error_codes
        assert 'ENT-ANALYSIS-001' in error_codes
        assert 'ENT-GROUP-001' in error_codes
        assert 'ENT-RDM-001' in error_codes


class TestGetExposureIds:
    """Tests for _get_exposure_ids helper method."""

    def test_empty_list_returns_empty_dict(self):
        """Empty EDM list should return empty dict."""
        validator = EntityValidator()
        result = validator._get_exposure_ids([])
        assert result == {}

    def test_returns_mapping_for_existing_edms(self):
        """Should return mapping of EDM names to exposure IDs."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]

        result = validator._get_exposure_ids(['EDM1', 'EDM2'])

        assert result == {'EDM1': 123, 'EDM2': 456}

    def test_api_error_returns_empty_dict(self):
        """API errors should be handled gracefully, returning empty dict."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.side_effect = IRPAPIError("Failed")

        result = validator._get_exposure_ids(['EDM1'])

        assert result == {}
