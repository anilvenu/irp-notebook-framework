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
        existing, errors = validator.validate_analyses_not_exist([])
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

        existing, errors = validator.validate_analyses_not_exist(analyses)

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


class TestValidateSingleCedantPerEdm:
    """Tests for validate_single_cedant_per_edm method."""

    def test_empty_exposure_ids_returns_no_errors(self):
        """Empty exposure IDs should return no errors."""
        validator = EntityValidator()
        cedant_info, errors = validator.validate_single_cedant_per_edm({})
        assert cedant_info == {}
        assert errors == []

    def test_single_cedant_returns_no_errors(self):
        """EDM with exactly one cedant should pass validation."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Test Cedant'}
        ]

        cedant_info, errors = validator.validate_single_cedant_per_edm({'EDM1': 123})

        assert errors == []
        assert 'EDM1' in cedant_info
        assert cedant_info['EDM1']['cedantId'] == 1
        assert cedant_info['EDM1']['cedantName'] == 'Test Cedant'

    def test_no_cedants_returns_error(self):
        """EDM with no cedants should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.get_cedants_by_edm.return_value = []

        cedant_info, errors = validator.validate_single_cedant_per_edm({'EDM1': 123})

        assert len(errors) == 1
        assert 'ENT-CEDANT-001' in errors[0]
        assert 'EDM1' in errors[0]
        assert 'EDM1' not in cedant_info

    def test_multiple_cedants_returns_error(self):
        """EDM with multiple cedants should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Cedant 1'},
            {'cedantId': 2, 'cedantName': 'Cedant 2'}
        ]

        cedant_info, errors = validator.validate_single_cedant_per_edm({'EDM1': 123})

        assert len(errors) == 1
        assert 'ENT-CEDANT-002' in errors[0]
        assert 'EDM1' in errors[0]
        assert '2 cedants' in errors[0]
        assert 'EDM1' not in cedant_info

    def test_multiple_edms_validated(self):
        """Multiple EDMs should each be validated."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        # EDM1 has 1 cedant, EDM2 has 1 cedant
        validator._edm_manager.get_cedants_by_edm.side_effect = [
            [{'cedantId': 1, 'cedantName': 'Cedant 1'}],
            [{'cedantId': 2, 'cedantName': 'Cedant 2'}]
        ]

        cedant_info, errors = validator.validate_single_cedant_per_edm({
            'EDM1': 123,
            'EDM2': 456
        })

        assert errors == []
        assert len(cedant_info) == 2
        assert validator._edm_manager.get_cedants_by_edm.call_count == 2

    def test_mixed_cedant_issues_returns_all_errors(self):
        """EDMs with different cedant issues should all be reported."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        # EDM1 has 0 cedants, EDM2 has 2 cedants, EDM3 has 1 cedant
        validator._edm_manager.get_cedants_by_edm.side_effect = [
            [],
            [{'cedantId': 1, 'cedantName': 'C1'}, {'cedantId': 2, 'cedantName': 'C2'}],
            [{'cedantId': 3, 'cedantName': 'C3'}]
        ]

        cedant_info, errors = validator.validate_single_cedant_per_edm({
            'EDM1': 123,
            'EDM2': 456,
            'EDM3': 789
        })

        assert len(errors) == 2
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-CEDANT-001' in error_codes  # no cedants
        assert 'ENT-CEDANT-002' in error_codes  # multiple cedants
        # Only EDM3 should be in cedant_info
        assert len(cedant_info) == 1
        assert 'EDM3' in cedant_info

    def test_api_error_returns_error(self):
        """API error should be captured in error list."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.get_cedants_by_edm.side_effect = IRPAPIError("Connection failed")

        cedant_info, errors = validator.validate_single_cedant_per_edm({'EDM1': 123})

        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]
        assert 'Connection failed' in errors[0]


class TestValidateTreatyBatch:
    """Tests for validate_treaty_batch method."""

    def test_empty_treaties_returns_no_errors(self):
        """Empty treaty list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_treaty_batch([])
        assert errors == []

    def test_valid_batch_returns_no_errors(self):
        """Valid treaty batch should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Each EDM has exactly one cedant
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Test Cedant'}
        ]
        # Treaties don't exist
        validator._treaty_manager.search_treaties_paginated.return_value = []

        treaties = [
            {'Database': 'EDM1', 'Treaty Name': 'Treaty1'},
            {'Database': 'EDM1', 'Treaty Name': 'Treaty2'}
        ]

        errors = validator.validate_treaty_batch(treaties)

        assert errors == []

    def test_missing_edm_returns_error(self):
        """Missing EDM should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        treaties = [{'Database': 'EDM1', 'Treaty Name': 'Treaty1'}]

        errors = validator.validate_treaty_batch(treaties)

        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM1' in errors[0]

    def test_treaty_exists_returns_error(self):
        """Existing treaty should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Each EDM has exactly one cedant
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Test Cedant'}
        ]
        # Treaty already exists
        validator._treaty_manager.search_treaties_paginated.return_value = [
            {'treatyName': 'Treaty1', 'treatyId': 100}
        ]

        treaties = [{'Database': 'EDM1', 'Treaty Name': 'Treaty1'}]

        errors = validator.validate_treaty_batch(treaties)

        assert len(errors) == 1
        assert 'ENT-TREATY-001' in errors[0]
        assert 'Treaty1' in errors[0]

    def test_multiple_edms_validated(self):
        """Treaties across multiple EDMs should all be validated."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # Both EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]
        # Each EDM has exactly one cedant
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Test Cedant'}
        ]
        # No treaties exist
        validator._treaty_manager.search_treaties_paginated.return_value = []

        treaties = [
            {'Database': 'EDM1', 'Treaty Name': 'Treaty1'},
            {'Database': 'EDM2', 'Treaty Name': 'Treaty2'}
        ]

        errors = validator.validate_treaty_batch(treaties)

        assert errors == []
        # Should query treaties for each EDM
        assert validator._treaty_manager.search_treaties_paginated.call_count == 2

    def test_missing_edm_skips_treaty_check(self):
        """If EDM is missing, treaty check should be skipped for that EDM."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # Only EDM1 exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # EDM1 has exactly one cedant
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Test Cedant'}
        ]
        # No treaties exist
        validator._treaty_manager.search_treaties_paginated.return_value = []

        treaties = [
            {'Database': 'EDM1', 'Treaty Name': 'Treaty1'},
            {'Database': 'EDM2', 'Treaty Name': 'Treaty2'}  # EDM2 doesn't exist
        ]

        errors = validator.validate_treaty_batch(treaties)

        # Should have error for missing EDM2
        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM2' in errors[0]
        # Should only check treaties in EDM1 (the one that exists)
        assert validator._treaty_manager.search_treaties_paginated.call_count == 1

    def test_multiple_errors_returned(self):
        """Multiple validation failures should all be reported."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # Only EDM1 exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # EDM1 has exactly one cedant
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Test Cedant'}
        ]
        # Treaty1 already exists
        validator._treaty_manager.search_treaties_paginated.return_value = [
            {'treatyName': 'Treaty1', 'treatyId': 100}
        ]

        treaties = [
            {'Database': 'EDM1', 'Treaty Name': 'Treaty1'},  # Exists
            {'Database': 'EDM2', 'Treaty Name': 'Treaty2'}   # EDM missing
        ]

        errors = validator.validate_treaty_batch(treaties)

        # Should have: EDM2 missing, Treaty1 exists
        assert len(errors) == 2
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-EDM-002' in error_codes
        assert 'ENT-TREATY-001' in error_codes

    def test_no_cedant_returns_error(self):
        """EDM with no cedants should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # EDM has no cedants
        validator._edm_manager.get_cedants_by_edm.return_value = []
        # Treaties don't exist
        validator._treaty_manager.search_treaties_paginated.return_value = []

        treaties = [{'Database': 'EDM1', 'Treaty Name': 'Treaty1'}]

        errors = validator.validate_treaty_batch(treaties)

        assert len(errors) == 1
        assert 'ENT-CEDANT-001' in errors[0]
        assert 'EDM1' in errors[0]

    def test_multiple_cedants_returns_error(self):
        """EDM with multiple cedants should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # EDM has multiple cedants
        validator._edm_manager.get_cedants_by_edm.return_value = [
            {'cedantId': 1, 'cedantName': 'Cedant 1'},
            {'cedantId': 2, 'cedantName': 'Cedant 2'}
        ]
        # Treaties don't exist
        validator._treaty_manager.search_treaties_paginated.return_value = []

        treaties = [{'Database': 'EDM1', 'Treaty Name': 'Treaty1'}]

        errors = validator.validate_treaty_batch(treaties)

        assert len(errors) == 1
        assert 'ENT-CEDANT-002' in errors[0]
        assert 'EDM1' in errors[0]
        assert '2 cedants' in errors[0]

    def test_cedant_and_treaty_errors_combined(self):
        """Cedant errors and treaty errors should both be reported."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._treaty_manager = Mock()

        # Both EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]
        # EDM1 has no cedants, EDM2 has 1 cedant
        validator._edm_manager.get_cedants_by_edm.side_effect = [
            [],  # EDM1 - no cedants
            [{'cedantId': 1, 'cedantName': 'Cedant 1'}]  # EDM2 - valid
        ]
        # Treaty2 exists in EDM2
        def mock_treaty_search(exposure_id, filter):
            if exposure_id == 456:
                return [{'treatyName': 'Treaty2', 'treatyId': 200}]
            return []
        validator._treaty_manager.search_treaties_paginated.side_effect = mock_treaty_search

        treaties = [
            {'Database': 'EDM1', 'Treaty Name': 'Treaty1'},
            {'Database': 'EDM2', 'Treaty Name': 'Treaty2'}
        ]

        errors = validator.validate_treaty_batch(treaties)

        # Should have: EDM1 no cedants, Treaty2 exists
        assert len(errors) == 2
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-CEDANT-001' in error_codes
        assert 'ENT-TREATY-001' in error_codes


class TestValidatePortfoliosHaveLocations:
    """Tests for validate_portfolios_have_locations method."""

    def test_empty_map_returns_no_errors(self):
        """Empty portfolio map should return no errors."""
        validator = EntityValidator()
        no_locations, errors = validator.validate_portfolios_have_locations({})
        assert no_locations == []
        assert errors == []

    def test_portfolio_with_locations_returns_no_errors(self):
        """Portfolio with locations should pass validation."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'locationsCount': 10}
        ]

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_locations, errors = validator.validate_portfolios_have_locations(portfolio_map)

        assert no_locations == []
        assert errors == []

    def test_portfolio_no_accounts_returns_error(self):
        """Portfolio with no accounts should return error."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_locations, errors = validator.validate_portfolios_have_locations(portfolio_map)

        assert len(no_locations) == 1
        assert 'EDM1/Port1 (no accounts)' in no_locations
        assert len(errors) == 1
        assert 'ENT-LOC-001' in errors[0]

    def test_portfolio_zero_locations_returns_error(self):
        """Portfolio with accounts but zero locations should return error."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'locationsCount': 0},
            {'accountId': 2, 'locationsCount': 0}
        ]

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_locations, errors = validator.validate_portfolios_have_locations(portfolio_map)

        assert len(no_locations) == 1
        assert 'EDM1/Port1 (0 locations)' in no_locations
        assert len(errors) == 1
        assert 'ENT-LOC-001' in errors[0]

    def test_multiple_accounts_locations_summed(self):
        """Locations count should be summed across all accounts."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        # Multiple accounts with some locations each
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'locationsCount': 5},
            {'accountId': 2, 'locationsCount': 3}
        ]

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_locations, errors = validator.validate_portfolios_have_locations(portfolio_map)

        assert no_locations == []
        assert errors == []

    def test_multiple_portfolios_validated(self):
        """Multiple portfolios should each be validated."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        # Port1 has locations, Port2 has none
        validator._portfolio_manager.search_accounts_by_portfolio.side_effect = [
            [{'accountId': 1, 'locationsCount': 10}],
            []
        ]

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456},
            'EDM1/Port2': {'exposure_id': 123, 'portfolio_id': 789}
        }

        no_locations, errors = validator.validate_portfolios_have_locations(portfolio_map)

        assert len(no_locations) == 1
        assert 'EDM1/Port2 (no accounts)' in no_locations
        assert validator._portfolio_manager.search_accounts_by_portfolio.call_count == 2

    def test_api_error_returns_error(self):
        """API error should be captured in error list."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.side_effect = IRPAPIError("Connection failed")

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_locations, errors = validator.validate_portfolios_have_locations(portfolio_map)

        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]
        assert 'Connection failed' in errors[0]


class TestValidateGeohazBatch:
    """Tests for validate_geohaz_batch method."""

    def test_empty_portfolios_returns_no_errors(self):
        """Empty portfolio list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_geohaz_batch([])
        assert errors == []

    def test_valid_batch_returns_no_errors(self):
        """Valid GeoHaz batch should return no errors."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolios exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 456}
        ]
        # Portfolios have locations
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'locationsCount': 100}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'}
        ]

        errors = validator.validate_geohaz_batch(portfolios)

        assert errors == []

    def test_missing_edm_returns_error(self):
        """Missing EDM should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        portfolios = [{'Database': 'EDM1', 'Portfolio': 'Port1'}]

        errors = validator.validate_geohaz_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        assert 'EDM1' in errors[0]

    def test_missing_portfolio_returns_error(self):
        """Missing portfolio should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolio doesn't exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = []

        portfolios = [{'Database': 'EDM1', 'Portfolio': 'Port1'}]

        errors = validator.validate_geohaz_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-PORT-002' in errors[0]
        assert 'Port1' in errors[0]

    def test_no_locations_returns_error(self):
        """Portfolio without locations should return error."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Portfolio exists
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 456}
        ]
        # Portfolio has no locations
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolios = [{'Database': 'EDM1', 'Portfolio': 'Port1'}]

        errors = validator.validate_geohaz_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-LOC-001' in errors[0]
        assert 'Port1' in errors[0]

    def test_multiple_portfolios_validated(self):
        """Multiple portfolios should all be validated."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # Both EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]
        # Both portfolios exist
        validator._portfolio_manager.search_portfolios_paginated.side_effect = [
            [{'portfolioName': 'Port1', 'portfolioId': 100}],
            [{'portfolioName': 'Port2', 'portfolioId': 200}]
        ]
        # Both have locations
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'locationsCount': 50}
        ]

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'},
            {'Database': 'EDM2', 'Portfolio': 'Port2'}
        ]

        errors = validator.validate_geohaz_batch(portfolios)

        assert errors == []

    def test_multiple_errors_returned(self):
        """Multiple validation failures should all be reported."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # Only EDM1 exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Port1 exists but has no locations
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'Port1', 'portfolioId': 456}
        ]
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolios = [
            {'Database': 'EDM1', 'Portfolio': 'Port1'},  # No locations
            {'Database': 'EDM2', 'Portfolio': 'Port2'}   # EDM missing
        ]

        errors = validator.validate_geohaz_batch(portfolios)

        # Should have: EDM2 missing, Port2 missing (because EDM2 doesn't exist), Port1 no locations
        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-EDM-002' in error_codes
        assert 'ENT-LOC-001' in error_codes

    def test_edm_missing_skips_portfolio_check(self):
        """If EDM is missing, portfolio and location checks should be skipped for that EDM."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # No EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = []

        portfolios = [{'Database': 'EDM1', 'Portfolio': 'Port1'}]

        errors = validator.validate_geohaz_batch(portfolios)

        # Should only have EDM missing error (no portfolio/location checks)
        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        # Portfolio manager should not have been called
        validator._portfolio_manager.search_portfolios_paginated.assert_not_called()
        validator._portfolio_manager.search_accounts_by_portfolio.assert_not_called()


class TestValidatePortfoliosHaveAccounts:
    """Tests for validate_portfolios_have_accounts method."""

    def test_empty_map_returns_no_errors(self):
        """Empty portfolio map should return no errors."""
        validator = EntityValidator()
        no_accounts, errors = validator.validate_portfolios_have_accounts({})
        assert no_accounts == []
        assert errors == []

    def test_portfolios_with_accounts_returns_no_errors(self):
        """Portfolios with accounts should return no errors."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1, 'accountName': 'Account1'}
        ]

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_accounts, errors = validator.validate_portfolios_have_accounts(portfolio_map)

        assert no_accounts == []
        assert errors == []

    def test_portfolio_without_accounts_returns_error(self):
        """Portfolio without accounts should return error."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_accounts, errors = validator.validate_portfolios_have_accounts(portfolio_map)

        assert no_accounts == ['EDM1/Port1']
        assert len(errors) == 1
        assert 'ENT-ACCT-002' in errors[0]
        assert 'EDM1/Port1' in errors[0]

    def test_multiple_portfolios_checked(self):
        """Multiple portfolios should all be checked."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.side_effect = [
            [{'accountId': 1}],  # Port1 has accounts
            [],                   # Port2 has no accounts
            [{'accountId': 2}]   # Port3 has accounts
        ]

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 1},
            'EDM1/Port2': {'exposure_id': 123, 'portfolio_id': 2},
            'EDM1/Port3': {'exposure_id': 123, 'portfolio_id': 3}
        }

        no_accounts, errors = validator.validate_portfolios_have_accounts(portfolio_map)

        assert no_accounts == ['EDM1/Port2']
        assert len(errors) == 1
        assert 'ENT-ACCT-002' in errors[0]

    def test_api_error_handled(self):
        """API errors should be captured as errors."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()
        validator._portfolio_manager.search_accounts_by_portfolio.side_effect = IRPAPIError("API failed")

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': 123, 'portfolio_id': 456}
        }

        no_accounts, errors = validator.validate_portfolios_have_accounts(portfolio_map)

        assert no_accounts == []
        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]
        assert 'Failed to check accounts' in errors[0]

    def test_missing_ids_skipped(self):
        """Portfolios with missing exposure_id or portfolio_id should be skipped."""
        validator = EntityValidator()
        validator._portfolio_manager = Mock()

        portfolio_map = {
            'EDM1/Port1': {'exposure_id': None, 'portfolio_id': 456},
            'EDM1/Port2': {'exposure_id': 123, 'portfolio_id': None}
        }

        no_accounts, errors = validator.validate_portfolios_have_accounts(portfolio_map)

        assert no_accounts == []
        assert errors == []
        validator._portfolio_manager.search_accounts_by_portfolio.assert_not_called()


class TestValidatePortfolioMappingBatch:
    """Tests for validate_portfolio_mapping_batch method."""

    # Helper to create metadata with cycle type
    @staticmethod
    def _make_metadata(cycle_type='Quarterly'):
        return {'Cycle Type': cycle_type}

    # Helper to create base portfolio with required fields
    @staticmethod
    def _make_base_portfolio(database, portfolio, import_file, cycle_type='Quarterly'):
        return {
            'Database': database,
            'Portfolio': portfolio,
            'Base Portfolio?': 'Y',
            'Import File': import_file,
            'Metadata': {'Cycle Type': cycle_type}
        }

    # Helper to create sub portfolio
    @staticmethod
    def _make_sub_portfolio(database, portfolio):
        return {
            'Database': database,
            'Portfolio': portfolio,
            'Base Portfolio?': 'N'
        }

    def test_empty_portfolios_returns_no_errors(self):
        """Empty portfolio list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_portfolio_mapping_batch([])
        assert errors == []

    @patch('helpers.sqlserver.sql_file_exists')
    def test_valid_batch_returns_no_errors(self, mock_sql_exists):
        """Valid Portfolio Mapping batch should return no errors."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Base portfolio exists
        validator._portfolio_manager.search_portfolios_paginated.side_effect = [
            [{'portfolioName': 'BasePort', 'portfolioId': 456}],  # Base exists
            []  # Sub doesn't exist
        ]
        # Base portfolio has accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1}
        ]

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ'),
            self._make_sub_portfolio('EDM1', 'SubPort')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        assert errors == []

    @patch('helpers.sqlserver.sql_file_exists')
    def test_missing_edm_returns_error(self, mock_sql_exists):
        """Missing EDM should return error."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._edm_manager.search_edms_paginated.return_value = []

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        # Should have SQL validation pass + EDM missing error
        assert any('ENT-EDM-002' in e for e in errors)
        assert any('EDM1' in e for e in errors)

    @patch('helpers.sqlserver.sql_file_exists')
    def test_missing_base_portfolio_returns_error(self, mock_sql_exists):
        """Missing base portfolio should return error."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Base portfolio doesn't exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = []

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-PORT-002' in errors[0]
        assert 'BasePort' in errors[0]

    @patch('helpers.sqlserver.sql_file_exists')
    def test_base_portfolio_no_accounts_returns_error(self, mock_sql_exists):
        """Base portfolio without accounts should return error."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Base portfolio exists
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'BasePort', 'portfolioId': 456}
        ]
        # No accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-ACCT-002' in errors[0]
        assert 'BasePort' in errors[0]

    @patch('helpers.sqlserver.sql_file_exists')
    def test_existing_sub_portfolio_returns_error(self, mock_sql_exists):
        """Sub-portfolio that already exists should return error."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Base portfolio exists, sub portfolio also exists
        validator._portfolio_manager.search_portfolios_paginated.side_effect = [
            [{'portfolioName': 'BasePort', 'portfolioId': 456}],  # Base exists
            [{'portfolioName': 'SubPort', 'portfolioId': 789}]    # Sub also exists (bad!)
        ]
        # Base has accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1}
        ]

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ'),
            self._make_sub_portfolio('EDM1', 'SubPort')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        assert len(errors) == 1
        assert 'ENT-PORT-001' in errors[0]
        assert 'SubPort' in errors[0]

    @patch('helpers.sqlserver.sql_file_exists')
    def test_multiple_edms_validated(self, mock_sql_exists):
        """Multiple EDMs should all be validated."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # Both EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123},
            {'exposureName': 'EDM2', 'exposureId': 456}
        ]
        # All base portfolios exist, no sub portfolios exist
        validator._portfolio_manager.search_portfolios_paginated.side_effect = [
            [{'portfolioName': 'Base1', 'portfolioId': 100}],
            [{'portfolioName': 'Base2', 'portfolioId': 200}],
            [],  # Sub1 doesn't exist
            []   # Sub2 doesn't exist
        ]
        # Base portfolios have accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1}
        ]

        portfolios = [
            self._make_base_portfolio('EDM1', 'Base1', 'USEQ'),
            self._make_base_portfolio('EDM2', 'Base2', 'USHU_Full'),
            self._make_sub_portfolio('EDM1', 'Sub1'),
            self._make_sub_portfolio('EDM2', 'Sub2')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        assert errors == []

    @patch('helpers.sqlserver.sql_file_exists')
    def test_only_base_portfolios_no_sub(self, mock_sql_exists):
        """Batch with only base portfolios (no subs) should validate correctly."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Base portfolio exists
        validator._portfolio_manager.search_portfolios_paginated.return_value = [
            {'portfolioName': 'BasePort', 'portfolioId': 456}
        ]
        # Has accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = [
            {'accountId': 1}
        ]

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        assert errors == []

    def test_only_sub_portfolios_no_base(self):
        """Batch with only sub portfolios should validate sub doesn't exist."""
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # EDM exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Sub portfolio doesn't exist
        validator._portfolio_manager.search_portfolios_paginated.return_value = []

        portfolios = [
            self._make_sub_portfolio('EDM1', 'SubPort')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        assert errors == []

    @patch('helpers.sqlserver.sql_file_exists')
    def test_multiple_errors_returned(self, mock_sql_exists):
        """Multiple validation failures should all be reported."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # Only EDM1 exists
        validator._edm_manager.search_edms_paginated.return_value = [
            {'exposureName': 'EDM1', 'exposureId': 123}
        ]
        # Base portfolio exists but has no accounts, sub portfolio already exists
        validator._portfolio_manager.search_portfolios_paginated.side_effect = [
            [{'portfolioName': 'BasePort', 'portfolioId': 456}],  # Base exists
            [{'portfolioName': 'SubPort', 'portfolioId': 789}]    # Sub exists (error)
        ]
        # No accounts
        validator._portfolio_manager.search_accounts_by_portfolio.return_value = []

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ'),
            self._make_sub_portfolio('EDM1', 'SubPort'),
            self._make_base_portfolio('EDM2', 'Base2', 'USHU_Full')  # EDM missing
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-EDM-002' in error_codes      # EDM2 missing
        assert 'ENT-ACCT-002' in error_codes     # BasePort no accounts
        assert 'ENT-PORT-001' in error_codes     # SubPort exists

    @patch('helpers.sqlserver.sql_file_exists')
    def test_edm_missing_skips_portfolio_checks(self, mock_sql_exists):
        """If EDM is missing, portfolio checks should be skipped for that EDM."""
        mock_sql_exists.return_value = True
        validator = EntityValidator()
        validator._edm_manager = Mock()
        validator._portfolio_manager = Mock()

        # No EDMs exist
        validator._edm_manager.search_edms_paginated.return_value = []

        portfolios = [
            self._make_base_portfolio('EDM1', 'BasePort', 'USEQ'),
            self._make_sub_portfolio('EDM1', 'SubPort')
        ]

        errors = validator.validate_portfolio_mapping_batch(portfolios)

        # Should only have EDM missing error
        assert len(errors) == 1
        assert 'ENT-EDM-002' in errors[0]
        # Portfolio manager should not have been called
        validator._portfolio_manager.search_portfolios_paginated.assert_not_called()
        validator._portfolio_manager.search_accounts_by_portfolio.assert_not_called()


class TestValidatePortfolioMappingSqlScripts:
    """Tests for SQL script validation in portfolio mapping.

    Note: Individual SQL script existence is NOT validated (portfolios without
    scripts are skipped at execution time). Only cycle type directory existence
    is validated.
    """

    def test_missing_cycle_type_returns_error(self):
        """Missing Cycle Type in Metadata should return error."""
        validator = EntityValidator()
        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'BasePort',
                'Base Portfolio?': 'Y',
                'Import File': 'USEQ',
                'Metadata': {}  # No Cycle Type
            }
        ]

        errors = validator._validate_portfolio_mapping_sql_scripts(portfolios)

        assert len(errors) == 1
        assert "Cycle Type" in errors[0]

    @patch('helpers.entity_validator.WORKSPACE_PATH')
    def test_invalid_cycle_type_directory_returns_error(self, mock_workspace):
        """Invalid cycle type directory should return error."""
        # Setup mock: base directory exists but no matching subdirectory
        mock_base_path = MagicMock()
        mock_base_path.exists.return_value = True
        # Return empty list - no matching directories
        mock_base_path.iterdir.return_value = []

        # WORKSPACE_PATH / 'sql' / 'portfolio_mapping' returns mock_base_path
        mock_sql_path = MagicMock()
        mock_sql_path.__truediv__ = MagicMock(return_value=mock_base_path)
        mock_workspace_sql = MagicMock()
        mock_workspace_sql.__truediv__ = MagicMock(return_value=mock_sql_path)
        mock_workspace.__truediv__ = MagicMock(return_value=mock_workspace_sql)

        validator = EntityValidator()
        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'BasePort',
                'Base Portfolio?': 'Y',
                'Import File': 'USEQ',
                'Metadata': {'Cycle Type': 'InvalidType'}
            }
        ]

        errors = validator._validate_portfolio_mapping_sql_scripts(portfolios)

        assert len(errors) == 1
        assert "directory not found" in errors[0]
        assert "invalidtype" in errors[0]

    def test_test_cycle_type_uses_adhoc_directory(self):
        """Cycle type containing 'test' should use adhoc directory (no error if dir exists)."""
        validator = EntityValidator()
        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'BasePort',
                'Base Portfolio?': 'Y',
                'Import File': 'NONEXISTENT_TEST_FILE',
                'Metadata': {'Cycle Type': 'Test_Q1_2025'}
            }
        ]

        errors = validator._validate_portfolio_mapping_sql_scripts(portfolios)

        # No error - adhoc directory exists and missing SQL scripts are not validated
        assert len(errors) == 0

    def test_quarterly_cycle_type_uses_quarterly_directory(self):
        """Quarterly cycle type should use quarterly directory (no error if dir exists)."""
        validator = EntityValidator()
        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'BasePort',
                'Base Portfolio?': 'Y',
                'Import File': 'NONEXISTENT_QUARTERLY_FILE',
                'Metadata': {'Cycle Type': 'Quarterly'}
            }
        ]

        errors = validator._validate_portfolio_mapping_sql_scripts(portfolios)

        # No error - quarterly directory exists and missing SQL scripts are not validated
        assert len(errors) == 0

    def test_missing_sql_script_does_not_return_error(self):
        """Missing SQL script should NOT return error (skipped at execution time)."""
        validator = EntityValidator()
        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'BasePort',
                'Base Portfolio?': 'Y',
                'Import File': 'NONEXISTENT_FILE',  # Use non-existent file
                'Metadata': {'Cycle Type': 'Quarterly'}
            }
        ]

        errors = validator._validate_portfolio_mapping_sql_scripts(portfolios)

        # No error - missing SQL scripts are skipped at execution time
        assert len(errors) == 0

    def test_missing_import_file_does_not_return_error(self):
        """Missing Import File should NOT return error (handled at execution time)."""
        validator = EntityValidator()
        portfolios = [
            {
                'Database': 'EDM1',
                'Portfolio': 'BasePort',
                'Base Portfolio?': 'Y',
                # No Import File
                'Metadata': {'Cycle Type': 'Quarterly'}
            }
        ]

        errors = validator._validate_portfolio_mapping_sql_scripts(portfolios)

        # No error - missing import files are handled at execution time
        assert len(errors) == 0

    def test_empty_base_portfolios_returns_no_errors(self):
        """Empty base portfolios list should return no errors."""
        validator = EntityValidator()
        errors = validator._validate_portfolio_mapping_sql_scripts([])
        assert errors == []


class TestValidateAnalysesExist:
    """Tests for validate_analyses_exist method."""

    def test_empty_list_returns_no_errors(self):
        """Empty analysis list should return no errors."""
        validator = EntityValidator()
        missing, errors = validator.validate_analyses_exist([], {})
        assert missing == []
        assert errors == []

    def test_all_analyses_exist_returns_no_errors(self):
        """All analyses existing should return no errors."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Analysis1'},
            {'analysisName': 'Analysis2'}
        ]

        analysis_names = ['Analysis1', 'Analysis2']
        analysis_edm_map = {'Analysis1': 'EDM1', 'Analysis2': 'EDM1'}

        missing, errors = validator.validate_analyses_exist(analysis_names, analysis_edm_map)

        assert missing == []
        assert errors == []

    def test_missing_analysis_returns_error(self):
        """Missing analysis should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Analysis1'}
        ]

        analysis_names = ['Analysis1', 'Analysis2']
        analysis_edm_map = {'Analysis1': 'EDM1', 'Analysis2': 'EDM1'}

        missing, errors = validator.validate_analyses_exist(analysis_names, analysis_edm_map)

        assert 'EDM1/Analysis2' in missing
        assert len(errors) == 1
        assert 'ENT-ANALYSIS-002' in errors[0]

    def test_no_edm_mapping_returns_error(self):
        """Analysis without EDM mapping should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        analysis_names = ['Analysis1']
        analysis_edm_map = {}  # No mapping

        missing, errors = validator.validate_analyses_exist(analysis_names, analysis_edm_map)

        assert '?/Analysis1 (no EDM mapping)' in missing
        assert len(errors) == 1
        assert 'ENT-ANALYSIS-002' in errors[0]

    def test_multiple_edms_checked(self):
        """Analyses across multiple EDMs should all be checked."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'A1'}],  # EDM1
            [{'analysisName': 'A2'}]   # EDM2
        ]

        analysis_names = ['A1', 'A2']
        analysis_edm_map = {'A1': 'EDM1', 'A2': 'EDM2'}

        missing, errors = validator.validate_analyses_exist(analysis_names, analysis_edm_map)

        assert missing == []
        assert errors == []

    def test_api_error_handled(self):
        """API errors should be captured."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.side_effect = IRPAPIError("API failed")

        analysis_names = ['Analysis1']
        analysis_edm_map = {'Analysis1': 'EDM1'}

        missing, errors = validator.validate_analyses_exist(analysis_names, analysis_edm_map)

        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]


class TestValidateGroupsExist:
    """Tests for validate_groups_exist method."""

    def test_empty_list_returns_no_errors(self):
        """Empty group list should return no errors."""
        validator = EntityValidator()
        missing, errors = validator.validate_groups_exist([])
        assert missing == []
        assert errors == []

    def test_all_groups_exist_returns_no_errors(self):
        """All groups existing should return no errors."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Group1'},
            {'analysisName': 'Group2'}
        ]

        group_names = ['Group1', 'Group2']

        missing, errors = validator.validate_groups_exist(group_names)

        assert missing == []
        assert errors == []

    def test_missing_group_returns_error(self):
        """Missing group should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Group1'}
        ]

        group_names = ['Group1', 'Group2']

        missing, errors = validator.validate_groups_exist(group_names)

        assert 'Group2' in missing
        assert len(errors) == 1
        assert 'ENT-GROUP-002' in errors[0]

    def test_api_error_handled(self):
        """API errors should be captured."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._analysis_manager.search_analyses_paginated.side_effect = IRPAPIError("API failed")

        group_names = ['Group1']

        missing, errors = validator.validate_groups_exist(group_names)

        assert len(errors) == 1
        assert 'ENT-API-001' in errors[0]


class TestValidateGroupingBatch:
    """Tests for validate_grouping_batch method."""

    def test_empty_groupings_returns_no_errors(self):
        """Empty grouping list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_grouping_batch([])
        assert errors == []

    def test_valid_batch_returns_no_errors(self):
        """Valid grouping batch should return no errors."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Analyses exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'A1'}, {'analysisName': 'A2'}],  # Check analyses exist
            []  # Check group doesn't exist
        ]

        groupings = [{
            'Group_Name': 'NewGroup',
            'items': ['A1', 'A2'],
            'analysis_edm_map': {'A1': 'EDM1', 'A2': 'EDM1'}
        }]

        errors = validator.validate_grouping_batch(groupings)

        assert errors == []

    def test_missing_analysis_returns_error(self):
        """Missing analysis should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # A2 doesn't exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'A1'}],  # Only A1 exists
            []  # Group doesn't exist
        ]

        groupings = [{
            'Group_Name': 'NewGroup',
            'items': ['A1', 'A2'],
            'analysis_edm_map': {'A1': 'EDM1', 'A2': 'EDM1'}
        }]

        errors = validator.validate_grouping_batch(groupings)

        assert len(errors) == 1
        assert 'ENT-ANALYSIS-002' in errors[0]
        assert 'A2' in errors[0]

    def test_existing_group_returns_error(self):
        """Existing group name should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Analyses exist, but group also exists
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'A1'}],  # Analysis exists
            [{'analysisName': 'ExistingGroup'}]  # Group already exists!
        ]

        groupings = [{
            'Group_Name': 'ExistingGroup',
            'items': ['A1'],
            'analysis_edm_map': {'A1': 'EDM1'}
        }]

        errors = validator.validate_grouping_batch(groupings)

        assert len(errors) == 1
        assert 'ENT-GROUP-001' in errors[0]
        assert 'ExistingGroup' in errors[0]

    def test_multiple_groups_validated(self):
        """Multiple groups should all be validated."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # All analyses exist, no groups exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'A1'}, {'analysisName': 'A2'}],  # Analyses exist
            []  # No groups exist
        ]

        groupings = [
            {
                'Group_Name': 'Group1',
                'items': ['A1'],
                'analysis_edm_map': {'A1': 'EDM1', 'A2': 'EDM1'}
            },
            {
                'Group_Name': 'Group2',
                'items': ['A2'],
                'analysis_edm_map': {'A1': 'EDM1', 'A2': 'EDM1'}
            }
        ]

        errors = validator.validate_grouping_batch(groupings)

        assert errors == []

    def test_multiple_errors_returned(self):
        """Multiple validation failures should all be reported."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # A2 doesn't exist, and group already exists
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'A1'}],  # Only A1 exists
            [{'analysisName': 'ExistingGroup'}]  # Group exists
        ]

        groupings = [{
            'Group_Name': 'ExistingGroup',
            'items': ['A1', 'A2'],
            'analysis_edm_map': {'A1': 'EDM1', 'A2': 'EDM1'}
        }]

        errors = validator.validate_grouping_batch(groupings)

        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-ANALYSIS-002' in error_codes
        assert 'ENT-GROUP-001' in error_codes


class TestValidateGroupingRollupBatch:
    """Tests for validate_grouping_rollup_batch method."""

    def test_empty_groupings_returns_no_errors(self):
        """Empty grouping list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_grouping_rollup_batch([])
        assert errors == []

    def test_valid_batch_returns_no_errors(self):
        """Valid rollup batch should return no errors."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Child group exists, analysis exists, rollup group doesn't exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'ChildGroup'}],  # Child group exists
            [{'analysisName': 'Analysis1'}],   # Analysis exists
            []  # Rollup group doesn't exist
        ]

        groupings = [{
            'Group_Name': 'RollupGroup',
            'items': ['ChildGroup', 'Analysis1'],
            'group_names': ['ChildGroup', 'RollupGroup'],
            'analysis_edm_map': {'Analysis1': 'EDM1'}
        }]

        errors = validator.validate_grouping_rollup_batch(groupings)

        assert errors == []

    def test_missing_child_group_returns_error(self):
        """Missing child group should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Child group doesn't exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [],  # Child group doesn't exist
            []   # Rollup group doesn't exist
        ]

        groupings = [{
            'Group_Name': 'RollupGroup',
            'items': ['MissingChildGroup'],
            'group_names': ['MissingChildGroup', 'RollupGroup'],
            'analysis_edm_map': {}
        }]

        errors = validator.validate_grouping_rollup_batch(groupings)

        assert len(errors) == 1
        assert 'ENT-GROUP-002' in errors[0]
        assert 'MissingChildGroup' in errors[0]

    def test_missing_analysis_returns_error(self):
        """Missing analysis in rollup should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Analysis doesn't exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [],  # Analysis doesn't exist
            []   # Rollup group doesn't exist
        ]

        groupings = [{
            'Group_Name': 'RollupGroup',
            'items': ['MissingAnalysis'],
            'group_names': ['RollupGroup'],  # MissingAnalysis not in group_names
            'analysis_edm_map': {'MissingAnalysis': 'EDM1'}
        }]

        errors = validator.validate_grouping_rollup_batch(groupings)

        assert len(errors) == 1
        assert 'ENT-ANALYSIS-002' in errors[0]

    def test_existing_rollup_group_returns_error(self):
        """Existing rollup group name should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # All prereqs exist, but rollup group also exists
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'ChildGroup'}],     # Child group exists
            [{'analysisName': 'ExistingRollup'}]  # Rollup already exists!
        ]

        groupings = [{
            'Group_Name': 'ExistingRollup',
            'items': ['ChildGroup'],
            'group_names': ['ChildGroup', 'ExistingRollup'],
            'analysis_edm_map': {}
        }]

        errors = validator.validate_grouping_rollup_batch(groupings)

        assert len(errors) == 1
        assert 'ENT-GROUP-001' in errors[0]
        assert 'ExistingRollup' in errors[0]

    def test_mixed_items_validated(self):
        """Rollup with both groups and analyses should validate all."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Both child group and analysis exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'ChildGroup'}],  # Child group exists
            [{'analysisName': 'Analysis1'}],   # Analysis exists
            []  # Rollup doesn't exist
        ]

        groupings = [{
            'Group_Name': 'RollupGroup',
            'items': ['ChildGroup', 'Analysis1'],
            'group_names': ['ChildGroup', 'RollupGroup'],
            'analysis_edm_map': {'Analysis1': 'EDM1'}
        }]

        errors = validator.validate_grouping_rollup_batch(groupings)

        assert errors == []

    def test_multiple_errors_returned(self):
        """Multiple validation failures should all be reported."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Child group missing, analysis missing, and rollup exists
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [],  # Child group doesn't exist
            [],  # Analysis doesn't exist
            [{'analysisName': 'ExistingRollup'}]  # Rollup exists
        ]

        groupings = [{
            'Group_Name': 'ExistingRollup',
            'items': ['MissingGroup', 'MissingAnalysis'],
            'group_names': ['MissingGroup', 'ExistingRollup'],
            'analysis_edm_map': {'MissingAnalysis': 'EDM1'}
        }]

        errors = validator.validate_grouping_rollup_batch(groupings)

        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-GROUP-002' in error_codes   # Missing child group
        assert 'ENT-ANALYSIS-002' in error_codes  # Missing analysis
        assert 'ENT-GROUP-001' in error_codes   # Rollup already exists

    def test_only_analyses_no_child_groups(self):
        """Rollup with only analyses (no child groups) should work."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()

        # Only analyses, no child groups
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'A1'}, {'analysisName': 'A2'}],  # Analyses exist
            []  # Rollup doesn't exist
        ]

        groupings = [{
            'Group_Name': 'RollupGroup',
            'items': ['A1', 'A2'],
            'group_names': ['RollupGroup'],  # Only rollup in group_names
            'analysis_edm_map': {'A1': 'EDM1', 'A2': 'EDM1'}
        }]

        errors = validator.validate_grouping_rollup_batch(groupings)

        assert errors == []


class TestValidateRdmExportBatch:
    """Tests for validate_rdm_export_batch method."""

    def test_empty_export_jobs_returns_no_errors(self):
        """Empty export jobs list should return no errors."""
        validator = EntityValidator()
        errors = validator.validate_rdm_export_batch([])
        assert errors == []

    def test_valid_batch_returns_no_errors(self):
        """Valid RDM export batch should return no errors."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Groups and analyses exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'Group1'}],      # Group exists
            [{'analysisName': 'Analysis1'}]     # Analysis exists
        ]
        # RDM doesn't exist
        validator._rdm_manager.search_databases.return_value = []

        export_jobs = [{
            'rdm_name': 'RM_RDM_Test',
            'server_name': 'databridge-1',
            'analysis_names': ['Group1', 'Analysis1'],
            'analysis_edm_map': {'Analysis1': 'EDM1'},
            'group_names_set': ['Group1']
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert errors == []

    def test_missing_group_returns_error(self):
        """Missing group should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Group doesn't exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [],  # Group doesn't exist
            []   # (no analyses to check)
        ]
        # RDM doesn't exist
        validator._rdm_manager.search_databases.return_value = []

        export_jobs = [{
            'rdm_name': 'RM_RDM_Test',
            'server_name': 'databridge-1',
            'analysis_names': ['MissingGroup'],
            'analysis_edm_map': {},
            'group_names_set': ['MissingGroup']
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert len(errors) == 1
        assert 'ENT-GROUP-002' in errors[0]
        assert 'MissingGroup' in errors[0]

    def test_missing_analysis_returns_error(self):
        """Missing analysis should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Analysis doesn't exist
        validator._analysis_manager.search_analyses_paginated.return_value = []
        # RDM doesn't exist
        validator._rdm_manager.search_databases.return_value = []

        export_jobs = [{
            'rdm_name': 'RM_RDM_Test',
            'server_name': 'databridge-1',
            'analysis_names': ['MissingAnalysis'],
            'analysis_edm_map': {'MissingAnalysis': 'EDM1'},
            'group_names_set': []
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert len(errors) == 1
        assert 'ENT-ANALYSIS-002' in errors[0]

    def test_existing_rdm_returns_error(self):
        """Existing RDM should return error."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Analysis exists
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Analysis1'}
        ]
        # RDM already exists
        validator._rdm_manager.search_databases.return_value = [
            {'databaseName': 'RM_RDM_Test_123'}
        ]

        export_jobs = [{
            'rdm_name': 'RM_RDM_Test',
            'server_name': 'databridge-1',
            'analysis_names': ['Analysis1'],
            'analysis_edm_map': {'Analysis1': 'EDM1'},
            'group_names_set': []
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert len(errors) == 1
        assert 'ENT-RDM-001' in errors[0]

    def test_multiple_jobs_aggregated(self):
        """Items from multiple jobs should be aggregated and validated together."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # All exist
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [{'analysisName': 'Group1'}],  # Groups exist
            [{'analysisName': 'A1'}, {'analysisName': 'A2'}]  # Analyses exist
        ]
        # RDM doesn't exist
        validator._rdm_manager.search_databases.return_value = []

        # Multiple jobs (like chunked export)
        export_jobs = [
            {
                'rdm_name': 'RM_RDM_Test',
                'server_name': 'databridge-1',
                'analysis_names': ['Group1', 'A1'],
                'analysis_edm_map': {'A1': 'EDM1'},
                'group_names_set': ['Group1']
            },
            {
                'rdm_name': 'RM_RDM_Test',
                'server_name': 'databridge-1',
                'analysis_names': ['A2'],
                'analysis_edm_map': {'A2': 'EDM1'},
                'group_names_set': ['Group1']
            }
        ]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert errors == []

    def test_multiple_errors_returned(self):
        """Multiple validation failures should all be reported."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Group missing, analysis missing
        validator._analysis_manager.search_analyses_paginated.side_effect = [
            [],  # Group doesn't exist
            []   # Analysis doesn't exist
        ]
        # RDM already exists
        validator._rdm_manager.search_databases.return_value = [
            {'databaseName': 'RM_RDM_Existing'}
        ]

        export_jobs = [{
            'rdm_name': 'RM_RDM_Existing',
            'server_name': 'databridge-1',
            'analysis_names': ['MissingGroup', 'MissingAnalysis'],
            'analysis_edm_map': {'MissingAnalysis': 'EDM1'},
            'group_names_set': ['MissingGroup']
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        error_codes = [e.split(':')[0] for e in errors]
        assert 'ENT-GROUP-002' in error_codes
        assert 'ENT-ANALYSIS-002' in error_codes
        assert 'ENT-RDM-001' in error_codes

    def test_only_groups_no_analyses(self):
        """Export with only groups should work."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Group exists
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Group1'}
        ]
        # RDM doesn't exist
        validator._rdm_manager.search_databases.return_value = []

        export_jobs = [{
            'rdm_name': 'RM_RDM_Test',
            'server_name': 'databridge-1',
            'analysis_names': ['Group1'],
            'analysis_edm_map': {},
            'group_names_set': ['Group1']
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert errors == []

    def test_only_analyses_no_groups(self):
        """Export with only analyses should work."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Analysis exists
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Analysis1'}
        ]
        # RDM doesn't exist
        validator._rdm_manager.search_databases.return_value = []

        export_jobs = [{
            'rdm_name': 'RM_RDM_Test',
            'server_name': 'databridge-1',
            'analysis_names': ['Analysis1'],
            'analysis_edm_map': {'Analysis1': 'EDM1'},
            'group_names_set': []
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert errors == []

    def test_no_rdm_name_skips_rdm_check(self):
        """Export without RDM name should skip RDM existence check."""
        validator = EntityValidator()
        validator._analysis_manager = Mock()
        validator._rdm_manager = Mock()

        # Analysis exists
        validator._analysis_manager.search_analyses_paginated.return_value = [
            {'analysisName': 'Analysis1'}
        ]

        export_jobs = [{
            'rdm_name': None,  # No RDM name
            'server_name': 'databridge-1',
            'analysis_names': ['Analysis1'],
            'analysis_edm_map': {'Analysis1': 'EDM1'},
            'group_names_set': []
        }]

        errors = validator.validate_rdm_export_batch(export_jobs)

        assert errors == []
        # RDM manager should not have been called
        validator._rdm_manager.search_databases.assert_not_called()
