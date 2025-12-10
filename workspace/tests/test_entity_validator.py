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


class TestValidateServerExists:
    """Tests for validate_server_exists method."""

    def test_empty_server_name_returns_error(self):
        """Empty server name should return error."""
        validator = EntityValidator()

        errors = validator.validate_server_exists('')
        assert len(errors) == 1
        assert 'ENT-SERVER-001' in errors[0]

        errors = validator.validate_server_exists(None)
        assert len(errors) == 1
        assert 'ENT-SERVER-001' in errors[0]

    def test_server_exists(self):
        """When server exists, should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_database_servers.return_value = [
            {'serverName': 'databridge-1', 'serverId': 1}
        ]

        errors = validator.validate_server_exists('databridge-1')

        assert errors == []
        validator._edm_manager.search_database_servers.assert_called_once()

    def test_server_not_found(self):
        """When server doesn't exist, should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_database_servers.return_value = []

        errors = validator.validate_server_exists('nonexistent-server')

        assert len(errors) == 1
        assert 'ENT-SERVER-001' in errors[0]
        assert 'nonexistent-server' in errors[0]

    def test_api_error_handled(self):
        """API errors should be caught and returned as error messages."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_database_servers.side_effect = IRPAPIError("Connection failed")

        errors = validator.validate_server_exists('databridge-1')

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
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = []

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
        """If no EDMs exist, should not check portfolios/treaties/analyses but still check groups."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []
        validator._portfolio_manager = Mock()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = []

        config_data = {
            'Databases': [{'Database': 'EDM1'}],
            'Portfolios': [{'Database': 'EDM1', 'Portfolio': 'Port1'}],
            'Reinsurance Treaties': [],
            'Analysis Table': [],
            'Groupings': [{'Group Name': 'Group1'}],
            'Metadata': {}
        }

        errors = validator.validate_config_entities_not_exist(config_data)

        assert errors == []
        # Portfolio manager should not have been called (EDMs don't exist)
        validator._portfolio_manager.search_portfolios_paginated.assert_not_called()
        # But groups should always be checked
        validator._analysis_manager.search_analyses_paginated.assert_called_once()

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
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = []

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


class TestValidateEdmBatch:
    """Tests for validate_edm_batch method."""

    def test_valid_batch_returns_no_errors(self):
        """Valid EDM batch should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        # Server exists
        validator._edm_manager.search_database_servers.return_value = [
            {'serverName': 'databridge-1', 'serverId': 1}
        ]
        # EDMs don't exist
        validator._edm_manager.search_edms_paginated.return_value = []

        errors = validator.validate_edm_batch(
            edm_names=['EDM1', 'EDM2'],
            server_name='databridge-1'
        )

        assert errors == []

    def test_server_not_found_returns_error(self):
        """Missing server should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        # Server doesn't exist
        validator._edm_manager.search_database_servers.return_value = []
        # EDMs don't exist
        validator._edm_manager.search_edms_paginated.return_value = []

        errors = validator.validate_edm_batch(
            edm_names=['EDM1'],
            server_name='nonexistent-server'
        )

        assert len(errors) == 1
        assert 'ENT-SERVER-001' in errors[0]
        assert 'nonexistent-server' in errors[0]

    def test_edms_already_exist_returns_error(self):
        """Existing EDMs should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        # Server exists
        validator._edm_manager.search_database_servers.return_value = [
            {'serverName': 'databridge-1', 'serverId': 1}
        ]
        # Some EDMs already exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        errors = validator.validate_edm_batch(
            edm_names=['EDM1', 'EDM2'],
            server_name='databridge-1'
        )

        assert len(errors) == 1
        assert 'ENT-EDM-001' in errors[0]
        assert 'EDM1' in errors[0]

    def test_multiple_errors_returned(self):
        """Both server and EDM errors should be returned."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        # Server doesn't exist
        validator._edm_manager.search_database_servers.return_value = []
        # EDMs already exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        errors = validator.validate_edm_batch(
            edm_names=['EDM1'],
            server_name='nonexistent-server'
        )

        assert len(errors) == 2
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-SERVER-001' in error_codes
        assert 'ENT-EDM-001' in error_codes


class TestValidateEdmsExist:
    """Tests for validate_edms_exist method."""

    def test_empty_list_returns_no_errors(self):
        """Empty EDM list should return empty results."""
        validator = EntityValidator()
        edm_exposure_ids, errors = validator.validate_edms_exist([])
        assert edm_exposure_ids == {}
        assert errors == []

    def test_all_edms_exist(self):
        """When all EDMs exist, should return their exposure IDs and no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]

        edm_exposure_ids, errors = validator.validate_edms_exist(['EDM1', 'EDM2'])

        assert edm_exposure_ids == {'EDM1': 123, 'EDM2': 456}
        assert errors == []

    def test_some_edms_missing(self):
        """When some EDMs don't exist, should return error with missing names."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        edm_exposure_ids, errors = validator.validate_edms_exist(['EDM1', 'EDM2'])

        assert edm_exposure_ids == {'EDM1': 123}
        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM2' in errors[0]

    def test_all_edms_missing(self):
        """When no EDMs exist, should return error with all names."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        edm_exposure_ids, errors = validator.validate_edms_exist(['EDM1', 'EDM2'])

        assert edm_exposure_ids == {}
        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM1' in errors[0]
        assert 'EDM2' in errors[0]

    def test_api_error_handled(self):
        """API errors should be caught and returned as error messages."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.side_effect = IRPAPIError("Connection failed")

        edm_exposure_ids, errors = validator.validate_edms_exist(['EDM1'])

        assert edm_exposure_ids == {}
        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]

    def test_duplicate_edm_names_deduplicated(self):
        """Duplicate EDM names in input should be deduplicated."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        edm_exposure_ids, errors = validator.validate_edms_exist(['EDM1', 'EDM1', 'EDM1'])

        assert edm_exposure_ids == {'EDM1': 123}
        assert errors == []
        # Should only make one API call with deduplicated names
        validator._edm_manager.search_edms_paginated.assert_called_once()


class TestValidatePortfolioBatch:
    """Tests for validate_portfolio_batch method."""

    def test_empty_portfolios_returns_no_errors(self):
        """Empty portfolio list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_portfolio_batch([])
        assert errors == []

    def test_valid_batch_returns_no_errors(self):
        """Valid portfolio batch should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolios don't exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = []

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Portfolio1'},
            {'Database': 'EDM1', 'Portfolio': 'Portfolio2'}
        ]
        errors = validator.validate_portfolio_batch(portfolios)

        assert errors == []

    def test_edms_not_found_returns_error(self):
        """Missing EDMs should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()

        # EDMs don't exist
        validator._edm_manager.search_edms_paginated.return_value = []

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Portfolio1'}
        ]
        errors = validator.validate_portfolio_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM1' in errors[0]

    def test_portfolios_already_exist_returns_error(self):
        """Existing portfolios should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Some portfolios already exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Portfolio1', 'portfolioId': 456}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Portfolio1'},
            {'Database': 'EDM1', 'Portfolio': 'Portfolio2'}
        ]
        errors = validator.validate_portfolio_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-PORT-001' in errors[0]
        assert 'Portfolio1' in errors[0]

    def test_multiple_errors_returned(self):
        """Both EDM and portfolio errors should be returned."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # Only EDM1 exists, EDM2 is missing
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolio1 already exists in EDM1
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Portfolio1', 'portfolioId': 456}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Portfolio1'},
            {'Database': 'EDM2', 'Portfolio': 'Portfolio2'}
        ]
        errors = validator.validate_portfolio_batch(portfolios)

        assert len(errors) == 2
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-EDM-002' in error_codes
        assert 'ENT-PORT-001' in error_codes

    def test_multiple_edms_validated(self):
        """Portfolios across multiple EDMs should all be validated."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # Both EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]
        # No portfolios exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = []

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Portfolio1'},
            {'Database': 'EDM2', 'Portfolio': 'Portfolio2'}
        ]
        errors = validator.validate_portfolio_batch(portfolios)

        assert errors == []
        # Should check portfolios in both EDMs
        assert validator._portfolio_manager.search_portfolios_paginated.call_count == 2

    def test_base_and_sub_portfolios_validated(self):
        """Both base portfolios and sub-portfolios should be validated."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Sub-portfolio already exists
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'SubPortfolio1', 'portfolioId': 789}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'BasePortfolio', 'Base Portfolio?': 'Y'},
            {'Database': 'EDM1', 'Portfolio': 'SubPortfolio1', 'Base Portfolio?': 'N'},
            {'Database': 'EDM1', 'Portfolio': 'SubPortfolio2', 'Base Portfolio?': 'N'}
        ]
        errors = validator.validate_portfolio_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-PORT-001' in errors[0]
        assert 'SubPortfolio1' in errors[0]


class TestValidatePortfoliosExist:
    """Tests for validate_portfolios_exist method."""

    def test_empty_portfolios_returns_no_errors(self):
        """Empty portfolio list should return empty results."""
        validator = EntityValidator()
        portfolio_ids, errors = validator.validate_portfolios_exist([], {})
        assert portfolio_ids == {}
        assert errors == []

    def test_empty_edm_exposure_ids_returns_no_errors(self):
        """Empty EDM exposure IDs should return empty results."""
        validator = EntityValidator()
        portfolios = [{'Database': 'EDM1', 'Portfolio': 'Port1'}]
        portfolio_ids, errors = validator.validate_portfolios_exist(portfolios, {})
        assert portfolio_ids == {}
        assert errors == []

    def test_all_portfolios_exist(self):
        """When all portfolios exist, should return their IDs and no errors."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 100},
            {'portfolioName': 'Port2', 'portfolioId': 200}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'},
            {'Database': 'EDM1', 'Portfolio': 'Port2'}
        ]
        edm_exposure_ids = {'EDM1': 123}

        portfolio_ids, errors = validator.validate_portfolios_exist(portfolios, edm_exposure_ids)

        assert 'EDM1/Port1' in portfolio_ids
        assert portfolio_ids['EDM1/Port1'] == {'exposure_id': 123, 'portfolio_id': 100}
        assert 'EDM1/Port2' in portfolio_ids
        assert errors == []

    def test_some_portfolios_missing(self):
        """When some portfolios don't exist, should return error."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 100}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'},
            {'Database': 'EDM1', 'Portfolio': 'Port2'}
        ]
        edm_exposure_ids = {'EDM1': 123}

        portfolio_ids, errors = validator.validate_portfolios_exist(portfolios, edm_exposure_ids)

        assert 'EDM1/Port1' in portfolio_ids
        assert 'EDM1/Port2' not in portfolio_ids
        assert len(errors) == 1
        assert 'ENT-PORT-002' in errors[0]
        assert 'Port2' in errors[0]

    def test_edm_not_in_exposure_ids_adds_to_missing(self):
        """When EDM not in exposure IDs, portfolios should be marked as missing."""
        validator = EntityValidator()

        portfolios = [{'Database': 'EDM1', 'Portfolio': 'Port1'}]
        edm_exposure_ids = {'EDM2': 456}  # Different EDM

        portfolio_ids, errors = validator.validate_portfolios_exist(portfolios, edm_exposure_ids)

        assert portfolio_ids == {}
        assert len(errors) == 1
        assert 'ENT-PORT-002' in errors[0]
        assert 'EDM1/Port1' in errors[0]

    def test_api_error_handled(self):
        """API errors should be caught and returned as error messages."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_portfolios_paginated.side_effect = IRPAPIError("Connection failed")

        portfolios = [{'Database': 'EDM1', 'Portfolio': 'Port1'}]
        edm_exposure_ids = {'EDM1': 123}

        portfolio_ids, errors = validator.validate_portfolios_exist(portfolios, edm_exposure_ids)

        assert portfolio_ids == {}
        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]


class TestValidateAccountsNotExist:
    """Tests for validate_accounts_not_exist method."""

    def test_empty_portfolio_ids_returns_no_errors(self):
        """Empty portfolio IDs should return empty results."""
        validator = EntityValidator()
        has_accounts, errors = validator.validate_accounts_not_exist({})
        assert has_accounts == []
        assert errors == []

    def test_portfolios_have_no_accounts(self):
        """When portfolios have no accounts, should return no errors."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolio_ids = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 100}
        }

        has_accounts, errors = validator.validate_accounts_not_exist(portfolio_ids)

        assert has_accounts == []
        assert errors == []

    def test_portfolios_have_accounts_returns_error(self):
        """When portfolios have accounts, should return error."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'accountName': 'Account1'}
        ]

        portfolio_ids = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 100}
        }

        has_accounts, errors = validator.validate_accounts_not_exist(portfolio_ids)

        assert has_accounts == ['EDM1/Port1']
        assert len(errors) == 1
        assert 'ENT-ACCT-001' in errors[0]
        assert 'EDM1/Port1' in errors[0]

    def test_multiple_portfolios_with_accounts(self):
        """When multiple portfolios have accounts, all should be reported."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        # All portfolios have accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'accountName': 'Account1'}
        ]

        portfolio_ids = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 100},
            'EDM1/Port2': {'exposure_id': 123, 'portfolio_id': 200}
        }

        has_accounts, errors = validator.validate_accounts_not_exist(portfolio_ids)

        assert len(has_accounts) == 2
        assert 'EDM1/Port1' in has_accounts
        assert 'EDM1/Port2' in has_accounts
        assert len(errors) == 1
        assert 'ENT-ACCT-001' in errors[0]

    def test_api_error_handled(self):
        """API errors should be caught and returned as error messages."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.side_effect = IRPAPIError("Connection failed")

        portfolio_ids = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 100}
        }

        has_accounts, errors = validator.validate_accounts_not_exist(portfolio_ids)

        assert has_accounts == []
        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]


class TestValidateCsvFilesExist:
    """Tests for _validate_csv_files_exist method."""

    def test_empty_portfolios_returns_no_errors(self):
        """Empty portfolio list should return no errors."""
        validator = EntityValidator()
        errors = validator._validate_csv_files_exist([], '/tmp')
        assert errors == []

    def test_empty_working_dir_returns_no_errors(self):
        """Empty working dir should return no errors."""
        validator = EntityValidator()
        portfolios = [{'accounts_import_file': 'test.csv'}]
        errors = validator._validate_csv_files_exist(portfolios, '')
        assert errors == []

    def test_files_exist_returns_no_errors(self, tmp_path):
        """When all files exist, should return no errors."""
        validator = EntityValidator()

        # Create test files
        accounts_file = tmp_path / 'accounts.csv'
        locations_file = tmp_path / 'locations.csv'
        accounts_file.touch()
        locations_file.touch()

        portfolios = [
            {
                'accounts_import_file': 'accounts.csv',
                'locations_import_file': 'locations.csv'
            }
        ]

        errors = validator._validate_csv_files_exist(portfolios, str(tmp_path))

        assert errors == []

    def test_missing_files_returns_error(self, tmp_path):
        """When files don't exist, should return error."""
        validator = EntityValidator()

        portfolios = [
            {
                'accounts_import_file': 'missing_accounts.csv',
                'locations_import_file': 'missing_locations.csv'
            }
        ]

        errors = validator._validate_csv_files_exist(portfolios, str(tmp_path))

        assert len(errors) == 1
        assert 'ENT-FILE-001' in errors[0]
        assert 'missing_accounts.csv' in errors[0]
        assert 'missing_locations.csv' in errors[0]

    def test_partial_files_missing(self, tmp_path):
        """When some files exist and some don't, should report missing ones."""
        validator = EntityValidator()

        # Create only one file
        accounts_file = tmp_path / 'accounts.csv'
        accounts_file.touch()

        portfolios = [
            {
                'accounts_import_file': 'accounts.csv',
                'locations_import_file': 'missing_locations.csv'
            }
        ]

        errors = validator._validate_csv_files_exist(portfolios, str(tmp_path))

        assert len(errors) == 1
        assert 'ENT-FILE-001' in errors[0]
        assert 'accounts.csv' not in errors[0]
        assert 'missing_locations.csv' in errors[0]


class TestValidateMriImportBatch:
    """Tests for validate_mri_import_batch method."""

    def test_empty_portfolios_returns_no_errors(self):
        """Empty portfolio list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_mri_import_batch([], '/tmp')
        assert errors == []

    def test_valid_batch_returns_no_errors(self, tmp_path):
        """Valid MRI Import batch should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolios exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 100}
        ]
        # No accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        # Create CSV files
        accounts_file = tmp_path / 'accounts.csv'
        locations_file = tmp_path / 'locations.csv'
        accounts_file.touch()
        locations_file.touch()

        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'Port1',
                'accounts_import_file': 'accounts.csv',
                'locations_import_file': 'locations.csv'
            }
        ]

        errors = validator.validate_mri_import_batch(portfolios, str(tmp_path))

        assert errors == []

    def test_edm_not_found_returns_error(self, tmp_path):
        """Missing EDM should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        # Create CSV files
        accounts_file = tmp_path / 'accounts.csv'
        locations_file = tmp_path / 'locations.csv'
        accounts_file.touch()
        locations_file.touch()

        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'Port1',
                'accounts_import_file': 'accounts.csv',
                'locations_import_file': 'locations.csv'
            }
        ]

        errors = validator.validate_mri_import_batch(portfolios, str(tmp_path))

        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]

    def test_portfolio_not_found_returns_error(self, tmp_path):
        """Missing portfolio should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolios don't exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = []

        # Create CSV files
        accounts_file = tmp_path / 'accounts.csv'
        locations_file = tmp_path / 'locations.csv'
        accounts_file.touch()
        locations_file.touch()

        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'Port1',
                'accounts_import_file': 'accounts.csv',
                'locations_import_file': 'locations.csv'
            }
        ]

        errors = validator.validate_mri_import_batch(portfolios, str(tmp_path))

        assert len(errors) == 1
        assert 'ENT-PORT-002' in errors[0]

    def test_accounts_exist_returns_error(self, tmp_path):
        """Portfolios with accounts should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolios exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 100}
        ]
        # Accounts exist (should fail)
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'accountName': 'Account1'}
        ]

        # Create CSV files
        accounts_file = tmp_path / 'accounts.csv'
        locations_file = tmp_path / 'locations.csv'
        accounts_file.touch()
        locations_file.touch()

        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'Port1',
                'accounts_import_file': 'accounts.csv',
                'locations_import_file': 'locations.csv'
            }
        ]

        errors = validator.validate_mri_import_batch(portfolios, str(tmp_path))

        assert len(errors) == 1
        assert 'ENT-ACCT-001' in errors[0]

    def test_csv_files_missing_returns_error(self):
        """Missing CSV files should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolios exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 100}
        ]
        # No accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'Port1',
                'accounts_import_file': 'missing_accounts.csv',
                'locations_import_file': 'missing_locations.csv'
            }
        ]

        # Use non-existent directory
        errors = validator.validate_mri_import_batch(portfolios, '/nonexistent/path')

        assert len(errors) == 1
        assert 'ENT-FILE-001' in errors[0]

    def test_multiple_errors_returned(self, tmp_path):
        """Multiple validation failures should all be reported."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # Only EDM1 exists, EDM2 is missing
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolio exists in EDM1
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 100}
        ]
        # Accounts exist (should fail)
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'accountName': 'Account1'}
        ]

        # Only create some files
        accounts_file = tmp_path / 'accounts1.csv'
        accounts_file.touch()

        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'Port1',
                'accounts_import_file': 'accounts1.csv',
                'locations_import_file': 'missing_locations1.csv'
            },
            {
                'Database': 'EDM2',
                'Portfolio': 'Port2',
                'accounts_import_file': 'missing_accounts2.csv',
                'locations_import_file': 'missing_locations2.csv'
            }
        ]

        errors = validator.validate_mri_import_batch(portfolios, str(tmp_path))

        # Should have: EDM2 missing, Port2 missing (because EDM2 missing), CSV files missing, accounts exist
        assert len(errors) == 4
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-EDM-002' in error_codes
        assert 'ENT-PORT-002' in error_codes  # Port2 missing since EDM2 doesn't exist
        assert 'ENT-FILE-001' in error_codes
        assert 'ENT-ACCT-001' in error_codes


class TestValidateEdmDbUpgradeBatch:
    """Tests for validate_edm_db_upgrade_batch method."""

    def test_empty_list_returns_no_errors(self):
        """Empty EDM list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_edm_db_upgrade_batch([])
        assert errors == []

    def test_all_edms_exist_returns_no_errors(self):
        """When all EDMs exist, should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]

        errors = validator.validate_edm_db_upgrade_batch(['EDM1', 'EDM2'])

        assert errors == []
        validator._edm_manager.search_edms_paginated.assert_called_once()

    def test_missing_edm_returns_error(self):
        """When EDM doesn't exist, should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        errors = validator.validate_edm_db_upgrade_batch(['EDM1'])

        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM1' in errors[0]

    def test_some_edms_missing_returns_error(self):
        """When some EDMs don't exist, should report missing ones."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        errors = validator.validate_edm_db_upgrade_batch(['EDM1', 'EDM2', 'EDM3'])

        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM2' in errors[0]
        assert 'EDM3' in errors[0]
        assert 'EDM1' not in errors[0]

    def test_api_error_returns_error(self):
        """API error should be captured in error list."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.side_effect = IRPAPIError("Connection failed")

        errors = validator.validate_edm_db_upgrade_batch(['EDM1'])

        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]
        assert 'Connection failed' in errors[0]

    def test_deduplicates_edm_names(self):
        """Should deduplicate EDM names before checking."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]

        errors = validator.validate_edm_db_upgrade_batch(['EDM1', 'EDM1', 'EDM1'])

        assert errors == []
        # Should only check once despite duplicates
        validator._edm_manager.search_edms_paginated.assert_called_once()
