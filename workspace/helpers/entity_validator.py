"""
Entity Existence Validation for IRP Configuration.

Validates that entities from configuration files don't already exist in Moody's.
Used during configuration file validation to prevent conflicts with existing data.
"""

from typing import Dict, Any, List, Tuple, Optional

from helpers.constants import DEFAULT_DATABASE_SERVER, BatchType
from helpers.irp_integration.client import Client
from helpers.irp_integration.exceptions import IRPAPIError
from helpers.irp_integration.portfolio import resolve_cycle_type_directory


def _format_entity_list(entities: List[str], indent: str = "  - ") -> str:
    """Format a list of entities with one per line for readable error messages."""
    return "\n" + "\n".join(f"{indent}{e}" for e in entities)


class EntityValidator:
    """Validates entity existence in Moody's Risk Modeler."""

    def __init__(self, client: Optional[Client] = None):
        """
        Initialize entity validator.

        Args:
            client: Optional IRP API client instance. If not provided, one will be created.
        """
        self.client = client or Client()
        self._edm_manager = None
        self._portfolio_manager = None
        self._treaty_manager = None
        self._analysis_manager = None
        self._rdm_manager = None
        self._reference_data_manager = None

    @property
    def edm_manager(self):
        """Lazy-loaded EDM manager to avoid circular imports."""
        if self._edm_manager is None:
            from helpers.irp_integration.edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager

    @property
    def portfolio_manager(self):
        """Lazy-loaded portfolio manager to avoid circular imports."""
        if self._portfolio_manager is None:
            from helpers.irp_integration.portfolio import PortfolioManager
            self._portfolio_manager = PortfolioManager(self.client)
        return self._portfolio_manager

    @property
    def treaty_manager(self):
        """Lazy-loaded treaty manager to avoid circular imports."""
        if self._treaty_manager is None:
            from helpers.irp_integration.treaty import TreatyManager
            self._treaty_manager = TreatyManager(self.client)
        return self._treaty_manager

    @property
    def analysis_manager(self):
        """Lazy-loaded analysis manager to avoid circular imports."""
        if self._analysis_manager is None:
            from helpers.irp_integration.analysis import AnalysisManager
            self._analysis_manager = AnalysisManager(self.client)
        return self._analysis_manager

    @property
    def rdm_manager(self):
        """Lazy-loaded RDM manager to avoid circular imports."""
        if self._rdm_manager is None:
            from helpers.irp_integration.rdm import RDMManager
            self._rdm_manager = RDMManager(self.client)
        return self._rdm_manager

    @property
    def reference_data_manager(self):
        """Lazy-loaded reference data manager to avoid circular imports."""
        if self._reference_data_manager is None:
            from helpers.irp_integration.reference_data import ReferenceDataManager
            self._reference_data_manager = ReferenceDataManager(self.client)
        return self._reference_data_manager

    def validate_edms_not_exist(self, edm_names: List[str]) -> Tuple[List[str], List[str]]:
        """
        Check that EDM names don't already exist in Moody's.

        Args:
            edm_names: List of EDM names to check

        Returns:
            Tuple of (existing_edm_names, error_messages)
        """
        if not edm_names:
            return [], []

        errors = []
        existing = []

        # Build IN filter for efficiency
        quoted = ", ".join(f'"{name}"' for name in edm_names)
        filter_str = f"exposureName IN ({quoted})"

        try:
            edms = self.edm_manager.search_edms_paginated(filter=filter_str)
            existing = [edm['exposureName'] for edm in edms if edm.get('exposureName') in edm_names]

            if existing:
                errors.append(
                    f"ENT-EDM-001: The following EDMs already exist:{_format_entity_list(existing)}"
                )
        except IRPAPIError as e:
            errors.append(f"ENT-API-001: Failed to check EDM existence: {e}")

        return existing, errors

    def validate_edms_exist(self, edm_names: List[str]) -> Tuple[Dict[str, int], List[str]]:
        """
        Check that EDM names exist in Moody's (for pre-requisite validation).

        Args:
            edm_names: List of EDM names that must exist

        Returns:
            Tuple of (edm_exposure_ids mapping, error_messages)
            edm_exposure_ids maps EDM names to their exposure IDs for found EDMs
        """
        if not edm_names:
            return {}, []

        errors = []
        edm_exposure_ids = {}

        # Build IN filter for efficiency
        unique_names = list(set(edm_names))
        quoted = ", ".join(f'"{name}"' for name in unique_names)
        filter_str = f"exposureName IN ({quoted})"

        try:
            edms = self.edm_manager.search_edms_paginated(filter=filter_str)
            for edm in edms:
                name = edm.get('exposureName')
                exposure_id = edm.get('exposureId')
                if name and exposure_id:
                    edm_exposure_ids[name] = exposure_id

            # Find which EDMs were not found
            missing = [name for name in unique_names if name not in edm_exposure_ids]

            if missing:
                errors.append(
                    f"ENT-EDM-002: The following required EDMs were not found:{_format_entity_list(missing)}"
                )
        except IRPAPIError as e:
            errors.append(f"ENT-API-001: Failed to check EDM existence: {e}")

        return edm_exposure_ids, errors

    def validate_server_exists(self, server_name: str) -> List[str]:
        """
        Check that a database server exists in Moody's.

        Args:
            server_name: Name of the database server to check

        Returns:
            List of error messages (empty if server exists)
        """
        if not server_name:
            return ["ENT-SERVER-001: Database server name is required"]

        try:
            servers = self.edm_manager.search_database_servers(
                filter=f'serverName="{server_name}"'
            )
            if not servers:
                return [f"ENT-SERVER-001: Database server '{server_name}' not found"]
            return []
        except IRPAPIError as e:
            return [f"ENT-API-001: Failed to check server existence: {e}"]

    def validate_portfolios_not_exist(
        self,
        portfolios: List[Dict[str, str]],
        edm_exposure_ids: Dict[str, int]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that portfolios don't already exist within their EDMs.

        Args:
            portfolios: List of dicts with 'Database' and 'Portfolio' keys
            edm_exposure_ids: Mapping of EDM names to exposure IDs

        Returns:
            Tuple of (existing_portfolio_identifiers, error_messages)
            Identifiers are in format "EDM_NAME/PORTFOLIO_NAME"
        """
        if not portfolios or not edm_exposure_ids:
            return [], []

        errors = []
        existing = []

        # Group portfolios by EDM for efficient lookup
        by_edm: Dict[str, List[str]] = {}
        for p in portfolios:
            edm = p.get('Database')
            portfolio = p.get('Portfolio')
            if edm and portfolio:
                by_edm.setdefault(edm, []).append(portfolio)

        for edm_name, portfolio_names in by_edm.items():
            exposure_id = edm_exposure_ids.get(edm_name)
            if not exposure_id:
                continue  # EDM doesn't exist in Moody's, so portfolios can't exist either

            try:
                # Build IN filter for all portfolio names in this EDM
                quoted = ", ".join(f'"{name}"' for name in portfolio_names)
                filter_str = f"portfolioName IN ({quoted})"

                found = self.portfolio_manager.search_portfolios_paginated(
                    exposure_id=exposure_id,
                    filter=filter_str
                )

                for p in found:
                    name = p.get('portfolioName')
                    if name in portfolio_names:
                        existing.append(f"{edm_name}/{name}")

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check portfolios in {edm_name}: {e}")

        if existing:
            errors.insert(0,
                f"ENT-PORT-001: The following portfolios already exist:{_format_entity_list(existing)}"
            )

        return existing, errors

    def validate_portfolios_exist(
        self,
        portfolios: List[Dict[str, str]],
        edm_exposure_ids: Dict[str, int]
    ) -> Tuple[Dict[str, Dict[str, int]], List[str]]:
        """
        Check that portfolios exist within their EDMs (for pre-requisite validation).

        Args:
            portfolios: List of dicts with 'Database' and 'Portfolio' keys
            edm_exposure_ids: Mapping of EDM names to exposure IDs

        Returns:
            Tuple of (portfolio_ids mapping, error_messages)
            portfolio_ids maps "EDM_NAME/PORTFOLIO_NAME" to {'exposure_id': X, 'portfolio_id': Y}
        """
        if not portfolios or not edm_exposure_ids:
            return {}, []

        errors = []
        portfolio_ids: Dict[str, Dict[str, int]] = {}
        missing = []

        # Group portfolios by EDM for efficient lookup
        by_edm: Dict[str, List[str]] = {}
        for p in portfolios:
            edm = p.get('Database')
            portfolio = p.get('Portfolio')
            if edm and portfolio:
                by_edm.setdefault(edm, []).append(portfolio)

        for edm_name, portfolio_names in by_edm.items():
            exposure_id = edm_exposure_ids.get(edm_name)
            if not exposure_id:
                # EDM doesn't exist, so portfolios can't exist - add all to missing
                for name in portfolio_names:
                    missing.append(f"{edm_name}/{name}")
                continue

            try:
                # Build IN filter for all portfolio names in this EDM
                quoted = ", ".join(f'"{name}"' for name in portfolio_names)
                filter_str = f"portfolioName IN ({quoted})"

                found = self.portfolio_manager.search_portfolios_paginated(
                    exposure_id=exposure_id,
                    filter=filter_str
                )

                # Build set of found portfolio names
                found_names = {p.get('portfolioName'): p.get('portfolioId') for p in found}

                for name in portfolio_names:
                    key = f"{edm_name}/{name}"
                    if name in found_names:
                        portfolio_ids[key] = {
                            'exposure_id': exposure_id,
                            'portfolio_id': found_names[name]
                        }
                    else:
                        missing.append(key)

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check portfolios in {edm_name}: {e}")

        if missing:
            errors.insert(0,
                f"ENT-PORT-002: The following required portfolios were not found:{_format_entity_list(missing)}"
            )

        return portfolio_ids, errors

    def validate_accounts_not_exist(
        self,
        portfolio_ids: Dict[str, Dict[str, int]]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that portfolios have no accounts (for MRI Import validation).

        Args:
            portfolio_ids: Mapping of "EDM_NAME/PORTFOLIO_NAME" to {'exposure_id': X, 'portfolio_id': Y}

        Returns:
            Tuple of (portfolios_with_accounts, error_messages)
        """
        if not portfolio_ids:
            return [], []

        errors = []
        has_accounts = []

        for portfolio_key, ids in portfolio_ids.items():
            exposure_id = ids.get('exposure_id')
            portfolio_id = ids.get('portfolio_id')

            if not exposure_id or not portfolio_id:
                continue

            try:
                accounts = self.portfolio_manager.search_accounts_by_portfolio(
                    exposure_id=exposure_id,
                    portfolio_id=portfolio_id
                )

                if accounts and len(accounts) > 0:
                    has_accounts.append(portfolio_key)

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check accounts for {portfolio_key}: {e}")

        if has_accounts:
            errors.insert(0,
                f"ENT-ACCT-001: The following portfolios already have accounts (must be empty for import):{_format_entity_list(has_accounts)}"
            )

        return has_accounts, errors

    def validate_portfolios_have_locations(
        self,
        portfolio_exposure_map: Dict[str, Dict[str, int]]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that portfolios have accounts with locations (for GeoHaz validation).

        Args:
            portfolio_exposure_map: Mapping of "EDM_NAME/PORTFOLIO_NAME" to
                                   {'exposure_id': X, 'portfolio_id': Y}

        Returns:
            Tuple of (portfolios_without_locations, error_messages)
        """
        if not portfolio_exposure_map:
            return [], []

        errors = []
        no_locations = []

        for portfolio_key, ids in portfolio_exposure_map.items():
            exposure_id = ids.get('exposure_id')
            portfolio_id = ids.get('portfolio_id')

            if not exposure_id or not portfolio_id:
                continue

            try:
                accounts = self.portfolio_manager.search_accounts_by_portfolio(
                    exposure_id=exposure_id,
                    portfolio_id=portfolio_id
                )

                if not accounts or len(accounts) == 0:
                    no_locations.append(f"{portfolio_key} (no accounts)")
                else:
                    # Sum up locations count from all accounts
                    total_locations = sum(
                        acc.get('locationsCount', 0) for acc in accounts
                    )
                    if total_locations == 0:
                        no_locations.append(f"{portfolio_key} (0 locations)")

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check locations for {portfolio_key}: {e}")

        if no_locations:
            errors.insert(0,
                f"ENT-LOC-001: The following portfolios have no locations (required for GeoHaz):{_format_entity_list(no_locations)}"
            )

        return no_locations, errors

    def validate_portfolios_have_accounts(
        self,
        portfolio_exposure_map: Dict[str, Dict[str, int]]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that portfolios have accounts (for Portfolio Mapping validation).

        Args:
            portfolio_exposure_map: Mapping of "EDM_NAME/PORTFOLIO_NAME" to
                                   {'exposure_id': X, 'portfolio_id': Y}

        Returns:
            Tuple of (portfolios_without_accounts, error_messages)
        """
        if not portfolio_exposure_map:
            return [], []

        errors = []
        no_accounts = []

        for portfolio_key, ids in portfolio_exposure_map.items():
            exposure_id = ids.get('exposure_id')
            portfolio_id = ids.get('portfolio_id')

            if not exposure_id or not portfolio_id:
                continue

            try:
                accounts = self.portfolio_manager.search_accounts_by_portfolio(
                    exposure_id=exposure_id,
                    portfolio_id=portfolio_id
                )

                if not accounts or len(accounts) == 0:
                    no_accounts.append(portfolio_key)

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check accounts for {portfolio_key}: {e}")

        if no_accounts:
            errors.insert(0,
                f"ENT-ACCT-002: The following portfolios have no accounts (required for sub-portfolio creation):{_format_entity_list(no_accounts)}"
            )

        return no_accounts, errors

    def validate_single_cedant_per_edm(
        self,
        edm_exposure_ids: Dict[str, int]
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        """
        Validate that each EDM has exactly one cedant (required for treaty creation).

        Args:
            edm_exposure_ids: Mapping of EDM names to exposure IDs

        Returns:
            Tuple of (cedant_info mapping, error_messages)
            cedant_info maps EDM names to {'cedantId': X, 'cedantName': Y} for valid EDMs
        """
        if not edm_exposure_ids:
            return {}, []

        errors = []
        cedant_info: Dict[str, Dict[str, Any]] = {}
        no_cedants = []
        multiple_cedants = []

        for edm_name, exposure_id in edm_exposure_ids.items():
            try:
                cedants = self.edm_manager.get_cedants_by_edm(exposure_id)

                if not cedants:
                    no_cedants.append(edm_name)
                elif len(cedants) > 1:
                    multiple_cedants.append(f"{edm_name} ({len(cedants)} cedants)")
                else:
                    cedant_info[edm_name] = {
                        'cedantId': cedants[0].get('cedantId'),
                        'cedantName': cedants[0].get('cedantName')
                    }

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check cedants for {edm_name}: {e}")

        if no_cedants:
            errors.insert(0,
                f"ENT-CEDANT-001: The following EDMs have no cedants:{_format_entity_list(no_cedants)}"
            )

        if multiple_cedants:
            errors.insert(0,
                f"ENT-CEDANT-002: The following EDMs have multiple cedants (exactly one required for treaty creation):{_format_entity_list(multiple_cedants)}"
            )

        return cedant_info, errors

    def validate_treaties_not_exist(
        self,
        treaties: List[Dict[str, str]],
        edm_exposure_ids: Dict[str, int]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that treaties don't already exist within their EDMs.

        Args:
            treaties: List of dicts with 'Database' and 'Treaty Name' keys
            edm_exposure_ids: Mapping of EDM names to exposure IDs

        Returns:
            Tuple of (existing_treaty_identifiers, error_messages)
            Identifiers are in format "EDM_NAME/TREATY_NAME"
        """
        if not treaties or not edm_exposure_ids:
            return [], []

        errors = []
        existing = []

        # Group treaties by EDM for efficient lookup
        by_edm: Dict[str, List[str]] = {}
        for t in treaties:
            edm = t.get('Database')
            treaty_name = t.get('Treaty Name')
            if edm and treaty_name:
                by_edm.setdefault(edm, []).append(treaty_name)

        for edm_name, treaty_names in by_edm.items():
            exposure_id = edm_exposure_ids.get(edm_name)
            if not exposure_id:
                continue  # EDM doesn't exist in Moody's, so treaties can't exist either

            try:
                # Build IN filter for all treaty names in this EDM
                quoted = ", ".join(f'"{name}"' for name in treaty_names)
                filter_str = f"treatyName IN ({quoted})"

                found = self.treaty_manager.search_treaties_paginated(
                    exposure_id=exposure_id,
                    filter=filter_str
                )

                for t in found:
                    name = t.get('treatyName')
                    if name in treaty_names:
                        existing.append(f"{edm_name}/{name}")

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check treaties in {edm_name}: {e}")

        if existing:
            errors.insert(0,
                f"ENT-TREATY-001: The following treaties already exist:{_format_entity_list(existing)}"
            )

        return existing, errors

    def validate_treaties_exist(
        self,
        treaties: List[Dict[str, str]],
        edm_exposure_ids: Dict[str, int]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that treaties exist within their EDMs (for pre-requisite validation).

        Args:
            treaties: List of dicts with 'Database' and treaty name key
                     (supports both 'Treaty Name' and 'Reinsurance Treaty N' formats)
            edm_exposure_ids: Mapping of EDM names to exposure IDs

        Returns:
            Tuple of (missing_treaty_identifiers, error_messages)
            Identifiers are in format "EDM_NAME/TREATY_NAME"
        """
        if not treaties or not edm_exposure_ids:
            return [], []

        errors = []
        missing = []

        # Group treaties by EDM for efficient lookup
        by_edm: Dict[str, List[str]] = {}
        for t in treaties:
            edm = t.get('Database')
            # Support both 'Treaty Name' and direct treaty name value
            treaty_name = t.get('Treaty Name') or t.get('treaty_name')
            if edm and treaty_name:
                by_edm.setdefault(edm, []).append(treaty_name)

        for edm_name, treaty_names in by_edm.items():
            exposure_id = edm_exposure_ids.get(edm_name)
            if not exposure_id:
                # EDM doesn't exist, all treaties in this EDM are missing
                for name in treaty_names:
                    missing.append(f"{edm_name}/{name}")
                continue

            try:
                # Build IN filter for all treaty names in this EDM
                unique_names = list(set(treaty_names))
                quoted = ", ".join(f'"{name}"' for name in unique_names)
                filter_str = f"treatyName IN ({quoted})"

                found = self.treaty_manager.search_treaties_paginated(
                    exposure_id=exposure_id,
                    filter=filter_str
                )

                # Build set of found treaty names
                found_names = {t.get('treatyName') for t in found}

                # Find which treaties were not found
                for name in unique_names:
                    if name not in found_names:
                        missing.append(f"{edm_name}/{name}")

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check treaties in {edm_name}: {e}")

        if missing:
            errors.insert(0,
                f"ENT-TREATY-002: The following treaties were not found:{_format_entity_list(missing)}"
            )

        return missing, errors

    def validate_analyses_not_exist(
        self,
        analyses: List[Dict[str, str]]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that analyses don't already exist.

        Searches by analysis name and EDM name (exposureName), which doesn't
        require the EDM to exist or have an exposure ID.

        Args:
            analyses: List of dicts with 'Database' and 'Analysis Name' keys

        Returns:
            Tuple of (existing_analysis_identifiers, error_messages)
            Identifiers are in format "EDM_NAME/ANALYSIS_NAME"
        """
        if not analyses:
            return [], []

        errors = []
        existing = []

        # Group analyses by EDM for efficient lookup
        by_edm: Dict[str, List[str]] = {}
        for a in analyses:
            edm = a.get('Database')
            analysis_name = a.get('Analysis Name')
            if edm and analysis_name:
                by_edm.setdefault(edm, []).append(analysis_name)

        # Batch the lookups to avoid "Request Header Fields Too Large" errors
        BATCH_SIZE = 25

        for edm_name, analysis_names in by_edm.items():
            analysis_names_set = set(analysis_names)
            try:
                # Process in batches
                for i in range(0, len(analysis_names), BATCH_SIZE):
                    batch = analysis_names[i:i + BATCH_SIZE]
                    quoted = ", ".join(f'"{name}"' for name in batch)
                    filter_str = f'analysisName IN ({quoted}) AND exposureName = "{edm_name}"'

                    found = self.analysis_manager.search_analyses_paginated(filter=filter_str)

                    for a in found:
                        name = a.get('analysisName')
                        if name in analysis_names_set:
                            existing.append(f"{edm_name}/{name}")

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check analyses in {edm_name}: {e}")

        if existing:
            errors.insert(0,
                f"ENT-ANALYSIS-001: The following analyses already exist:{_format_entity_list(existing)}"
            )

        return existing, errors

    def validate_groups_not_exist(
        self,
        groupings: List[Dict[str, str]]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that analysis group names don't already exist.

        Group names are globally unique (not scoped to an EDM).

        Args:
            groupings: List of dicts with 'Group Name' key

        Returns:
            Tuple of (existing_group_names, error_messages)
        """
        if not groupings:
            return [], []

        errors = []
        existing = []

        # Extract unique group names
        group_names = list(set(
            g.get('Group Name') for g in groupings if g.get('Group Name')
        ))

        if not group_names:
            return [], []

        # Batch the lookups to avoid "Request Header Fields Too Large" errors
        # when there are many group names (URL query string has size limits)
        BATCH_SIZE = 25
        group_names_set = set(group_names)

        try:
            for i in range(0, len(group_names), BATCH_SIZE):
                batch = group_names[i:i + BATCH_SIZE]
                quoted = ", ".join(f'"{name}"' for name in batch)
                filter_str = f"analysisName IN ({quoted})"

                found = self.analysis_manager.search_analyses_paginated(filter=filter_str)

                for g in found:
                    name = g.get('analysisName')
                    if name in group_names_set:
                        existing.append(name)

        except IRPAPIError as e:
            errors.append(f"ENT-API-001: Failed to check group existence: {e}")

        if existing:
            errors.insert(0,
                f"ENT-GROUP-001: The following groups already exist:{_format_entity_list(existing)}"
            )

        return existing, errors

    def validate_analyses_exist(
        self,
        analysis_names: List[str],
        analysis_edm_map: Dict[str, str]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that analyses exist (for grouping validation).

        Args:
            analysis_names: List of analysis names to check
            analysis_edm_map: Mapping of analysis names to EDM names

        Returns:
            Tuple of (missing_analysis_identifiers, error_messages)
            Identifiers are in format "EDM_NAME/ANALYSIS_NAME"
        """
        if not analysis_names:
            return [], []

        errors = []
        missing = []

        # Group analyses by EDM for efficient lookup
        by_edm: Dict[str, List[str]] = {}
        for analysis_name in analysis_names:
            edm_name = analysis_edm_map.get(analysis_name)
            if edm_name:
                by_edm.setdefault(edm_name, []).append(analysis_name)
            else:
                # No EDM mapping - can't look up
                missing.append(f"?/{analysis_name} (no EDM mapping)")

        # Batch the lookups to avoid "Request Header Fields Too Large" errors
        BATCH_SIZE = 25

        for edm_name, names in by_edm.items():
            try:
                found_names = set()

                # Process in batches
                for i in range(0, len(names), BATCH_SIZE):
                    batch = names[i:i + BATCH_SIZE]
                    quoted = ", ".join(f'"{name}"' for name in batch)
                    filter_str = f'analysisName IN ({quoted}) AND exposureName = "{edm_name}"'

                    found = self.analysis_manager.search_analyses_paginated(filter=filter_str)
                    found_names.update(a.get('analysisName') for a in found)

                for name in names:
                    if name not in found_names:
                        missing.append(f"{edm_name}/{name}")

            except IRPAPIError as e:
                errors.append(f"ENT-API-001: Failed to check analyses in {edm_name}: {e}")

        if missing:
            errors.insert(0,
                f"ENT-ANALYSIS-002: The following analyses were not found:{_format_entity_list(missing)}"
            )

        return missing, errors

    def validate_groups_exist(
        self,
        group_names: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that analysis groups exist (for rollup grouping validation).

        Group names are globally unique (not scoped to an EDM).

        Args:
            group_names: List of group names to check

        Returns:
            Tuple of (missing_group_names, error_messages)
        """
        if not group_names:
            return [], []

        errors = []
        missing = []

        # Batch the lookups to avoid "Request Header Fields Too Large" errors
        BATCH_SIZE = 25

        try:
            found_names = set()

            for i in range(0, len(group_names), BATCH_SIZE):
                batch = group_names[i:i + BATCH_SIZE]
                quoted = ", ".join(f'"{name}"' for name in batch)
                filter_str = f"analysisName IN ({quoted})"

                found = self.analysis_manager.search_analyses_paginated(filter=filter_str)
                found_names.update(g.get('analysisName') for g in found)

            for name in group_names:
                if name not in found_names:
                    missing.append(name)

        except IRPAPIError as e:
            errors.append(f"ENT-API-001: Failed to check group existence: {e}")

        if missing:
            errors.insert(0,
                f"ENT-GROUP-002: The following groups were not found:{_format_entity_list(missing)}"
            )

        return missing, errors

    def validate_rdm_not_exists(
        self,
        rdm_name: str,
        server_name: str = DEFAULT_DATABASE_SERVER
    ) -> List[str]:
        """
        Check that an RDM with the given name doesn't already exist.

        Args:
            rdm_name: Name of the RDM to check
            server_name: Database server name (default: DEFAULT_DATABASE_SERVER)

        Returns:
            List of error messages (empty if RDM doesn't exist)
        """
        if not rdm_name:
            return []

        errors = []

        try:
            # RDM names are prefixed, so use LIKE filter
            filter_str = f'databaseName LIKE "{rdm_name}*"'
            found = self.rdm_manager.search_databases(
                server_name=server_name,
                filter=filter_str
            )

            if found:
                existing_name = found[0].get('databaseName', rdm_name)
                errors.append(
                    f"ENT-RDM-001: RDM already exists: {existing_name}"
                )

        except IRPAPIError as e:
            errors.append(f"ENT-API-001: Failed to check RDM existence: {e}")

        return errors

    def _get_exposure_ids(self, edm_names: List[str]) -> Dict[str, int]:
        """
        Get exposure IDs for a list of EDM names that exist in Moody's.

        Args:
            edm_names: List of EDM names to look up

        Returns:
            Mapping of EDM names to their exposure IDs (only includes EDMs that exist)
        """
        result = {}

        if not edm_names:
            return result

        # Build IN filter for efficiency
        quoted = ", ".join(f'"{name}"' for name in edm_names)
        filter_str = f"exposureName IN ({quoted})"

        try:
            edms = self.edm_manager.search_edms_paginated(filter=filter_str)
            for edm in edms:
                name = edm.get('exposureName')
                exposure_id = edm.get('exposureId')
                if name and exposure_id:
                    result[name] = exposure_id
        except IRPAPIError:
            pass  # If we can't get IDs, we'll just have an empty map

        return result

    def validate_config_entities_not_exist(
        self,
        config_data: Dict[str, Any]
    ) -> List[str]:
        """
        Validate that entities from configuration don't already exist in Moody's.

        Follows cascade logic:
        1. Check EDMs from config
        2. If any EDMs exist in Moody's:
           - Check Portfolios don't exist (in those EDMs)
           - Check Treaties don't exist (in those EDMs)
        3. If any Portfolios exist:
           - Check Analyses don't exist
        4. Always check Groups (they are global, not scoped to EDMs)
        5. If any Analyses or Groups exist:
           - Check RDM doesn't exist

        Args:
            config_data: Parsed configuration dictionary from Excel file

        Returns:
            List of error messages (empty if all validation passes)
        """
        all_errors = []

        # Extract entity data from config
        databases = config_data.get('Databases', [])
        edm_names = [d['Database'] for d in databases if d.get('Database')]

        portfolios = config_data.get('Portfolios', [])
        treaties = config_data.get('Reinsurance Treaties', [])
        analyses = config_data.get('Analysis Table', [])
        groupings = config_data.get('Groupings', [])

        metadata = config_data.get('Metadata', {})
        rdm_name = metadata.get('Export RDM Name') if isinstance(metadata, dict) else None

        # Step 1: Check EDMs
        existing_edms, edm_errors = self.validate_edms_not_exist(edm_names)
        all_errors.extend(edm_errors)

        # If any EDMs exist, we need to check Portfolios and Treaties in those EDMs
        if existing_edms:
            # Get exposure IDs for existing EDMs
            edm_exposure_ids = self._get_exposure_ids(existing_edms)

            # Step 2a: Check Portfolios
            _, portfolio_errors = self.validate_portfolios_not_exist(
                portfolios, edm_exposure_ids
            )
            all_errors.extend(portfolio_errors)

            # Step 2b: Check Treaties
            _, treaty_errors = self.validate_treaties_not_exist(
                treaties, edm_exposure_ids
            )
            all_errors.extend(treaty_errors)

        # Step 3: Always check Analyses (searches by EDM name, doesn't require EDM to exist)
        _, analysis_errors = self.validate_analyses_not_exist(analyses)
        all_errors.extend(analysis_errors)

        # Step 4: Always check Groups (they are global, not scoped to EDMs)
        _, group_errors = self.validate_groups_not_exist(groupings)
        all_errors.extend(group_errors)

        # Step 5: If analyses or groups exist, check RDM
        if rdm_name:
            rdm_errors = self.validate_rdm_not_exists(rdm_name)
            all_errors.extend(rdm_errors)

        return all_errors

    # =========================================================================
    # Batch Submission Validations
    # =========================================================================

    def validate_edm_batch(
        self,
        edm_names: List[str],
        server_name: str
    ) -> List[str]:
        """
        Validate EDM batch submission.

        Pre-requisites (must exist):
        - Database server

        Entities to be created (must NOT exist):
        - EDM names

        Args:
            edm_names: List of EDM names to be created
            server_name: Database server name

        Returns:
            List of error messages (empty if all validation passes)
        """
        all_errors = []

        # Pre-requisite: Server must exist
        server_errors = self.validate_server_exists(server_name)
        all_errors.extend(server_errors)

        # EDMs must not already exist
        _, edm_errors = self.validate_edms_not_exist(edm_names)
        all_errors.extend(edm_errors)

        return all_errors

    def validate_portfolio_batch(
        self,
        portfolios: List[Dict[str, str]]
    ) -> List[str]:
        """
        Validate Portfolio Creation batch submission.

        Pre-requisites (must exist):
        - EDMs that portfolios will be created in

        Entities to be created (must NOT exist):
        - Portfolio names (both base portfolios and sub-portfolios)

        Args:
            portfolios: List of portfolio dicts with 'Database' and 'Portfolio' keys

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not portfolios:
            return []

        all_errors = []

        # Extract unique EDM names from portfolios
        edm_names = list(set(
            p.get('Database') for p in portfolios if p.get('Database')
        ))

        # Pre-requisite: EDMs must exist
        edm_exposure_ids, edm_errors = self.validate_edms_exist(edm_names)
        all_errors.extend(edm_errors)

        # Portfolios must not already exist (check in EDMs that were found)
        if edm_exposure_ids:
            _, portfolio_errors = self.validate_portfolios_not_exist(
                portfolios, edm_exposure_ids
            )
            all_errors.extend(portfolio_errors)

        return all_errors

    def validate_mri_import_batch(
        self,
        portfolios: List[Dict[str, str]],
        working_files_dir: str
    ) -> List[str]:
        """
        Validate MRI Import batch submission.

        Pre-requisites (must exist):
        - EDMs that portfolios belong to
        - Portfolios within their EDMs
        - CSV import files in working_files directory

        Entities to be created (must NOT exist):
        - Accounts in portfolios (portfolios must be empty)

        Args:
            portfolios: List of portfolio dicts with 'Database', 'Portfolio',
                       'accounts_import_file', and 'locations_import_file' keys
            working_files_dir: Path to the working_files directory

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not portfolios:
            return []

        all_errors = []

        # Extract unique EDM names from portfolios
        edm_names = list(set(
            p.get('Database') for p in portfolios if p.get('Database')
        ))

        # Pre-requisite 1: EDMs must exist
        edm_exposure_ids, edm_errors = self.validate_edms_exist(edm_names)
        all_errors.extend(edm_errors)

        # Pre-requisite 2: Portfolios must exist (and get their IDs for account check)
        portfolio_ids = {}
        if edm_exposure_ids:
            portfolio_ids, portfolio_errors = self.validate_portfolios_exist(
                portfolios, edm_exposure_ids
            )
            all_errors.extend(portfolio_errors)

        # Pre-requisite 3: CSV files must exist
        csv_errors = self._validate_csv_files_exist(portfolios, working_files_dir)
        all_errors.extend(csv_errors)

        # Accounts must NOT exist (portfolios must be empty)
        if portfolio_ids:
            _, account_errors = self.validate_accounts_not_exist(portfolio_ids)
            all_errors.extend(account_errors)

        return all_errors

    def validate_edm_db_upgrade_batch(
        self,
        edm_names: List[str]
    ) -> List[str]:
        """
        Validate EDM DB Upgrade batch submission.

        Pre-requisites (must exist):
        - EDMs to be upgraded

        Args:
            edm_names: List of EDM names to be upgraded

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not edm_names:
            return []

        # Pre-requisite: EDMs must exist
        _, edm_errors = self.validate_edms_exist(edm_names)
        return edm_errors

    def validate_treaty_batch(
        self,
        treaties: List[Dict[str, str]]
    ) -> List[str]:
        """
        Validate Reinsurance Treaty Creation batch submission.

        Pre-requisites (must exist):
        - EDMs that treaties will be created in
        - Exactly one cedant per EDM

        Entities to be created (must NOT exist):
        - Treaty names within their EDMs

        Args:
            treaties: List of treaty dicts with 'Database' and 'Treaty Name' keys

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not treaties:
            return []

        all_errors = []

        # Extract unique EDM names from treaties
        edm_names = list(set(
            t.get('Database') for t in treaties if t.get('Database')
        ))

        # Pre-requisite 1: EDMs must exist
        edm_exposure_ids, edm_errors = self.validate_edms_exist(edm_names)
        all_errors.extend(edm_errors)

        if edm_exposure_ids:
            # Pre-requisite 2: Each EDM must have exactly one cedant
            _, cedant_errors = self.validate_single_cedant_per_edm(edm_exposure_ids)
            all_errors.extend(cedant_errors)

            # Treaties must not already exist (check in EDMs that were found)
            _, treaty_errors = self.validate_treaties_not_exist(
                treaties, edm_exposure_ids
            )
            all_errors.extend(treaty_errors)

        return all_errors

    def validate_geohaz_batch(
        self,
        portfolios: List[Dict[str, str]]
    ) -> List[str]:
        """
        Validate GeoHaz batch submission.

        Pre-requisites (must exist):
        - EDMs that portfolios belong to
        - Portfolios within their EDMs
        - Portfolios must have accounts with locations

        Args:
            portfolios: List of portfolio dicts with 'Database' and 'Portfolio' keys

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not portfolios:
            return []

        all_errors = []

        # Extract unique EDM names from portfolios
        edm_names = list(set(
            p.get('Database') for p in portfolios if p.get('Database')
        ))

        # Pre-requisite 1: EDMs must exist
        edm_exposure_ids, edm_errors = self.validate_edms_exist(edm_names)
        all_errors.extend(edm_errors)

        # Pre-requisite 2: Portfolios must exist (and get their IDs for location check)
        portfolio_exposure_map = {}
        if edm_exposure_ids:
            portfolio_exposure_map, portfolio_errors = self.validate_portfolios_exist(
                portfolios, edm_exposure_ids
            )
            all_errors.extend(portfolio_errors)

        # Pre-requisite 3: Portfolios must have locations
        if portfolio_exposure_map:
            _, location_errors = self.validate_portfolios_have_locations(
                portfolio_exposure_map
            )
            all_errors.extend(location_errors)

        return all_errors

    def validate_portfolio_mapping_batch(
        self,
        portfolios: List[Dict[str, str]]
    ) -> List[str]:
        """
        Validate Portfolio Mapping batch submission.

        Pre-requisites (must exist):
        - Cycle type directory in portfolio_mapping/ (quarterly, annual, adhoc)
        - SQL scripts for each base portfolio's Import File
        - EDMs that portfolios belong to
        - Base portfolios (Base Portfolio? = Y) must exist within their EDMs
        - Base portfolios must have accounts (for sub-portfolio creation)

        Entities to be created (must NOT exist):
        - Sub-portfolios (Base Portfolio? = N) must not already exist

        Args:
            portfolios: List of portfolio dicts with 'Database', 'Portfolio',
                       'Base Portfolio?', 'Import File', and 'Metadata' keys

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not portfolios:
            return []

        all_errors = []

        # Split portfolios into base (exist) vs sub (to be created)
        base_portfolios = [p for p in portfolios if p.get('Base Portfolio?') == 'Y']
        sub_portfolios = [p for p in portfolios if p.get('Base Portfolio?') != 'Y']

        # Pre-requisite 0: Validate cycle type directory and SQL scripts exist
        sql_errors = self._validate_portfolio_mapping_sql_scripts(base_portfolios)
        all_errors.extend(sql_errors)

        # Extract unique EDM names from all portfolios
        edm_names = list(set(
            p.get('Database') for p in portfolios if p.get('Database')
        ))

        # Pre-requisite 1: EDMs must exist
        edm_exposure_ids, edm_errors = self.validate_edms_exist(edm_names)
        all_errors.extend(edm_errors)

        if edm_exposure_ids:
            # Pre-requisite 2: Base portfolios must exist
            base_portfolio_map = {}
            if base_portfolios:
                base_portfolio_map, base_errors = self.validate_portfolios_exist(
                    base_portfolios, edm_exposure_ids
                )
                all_errors.extend(base_errors)

            # Pre-requisite 3: Base portfolios must have accounts
            if base_portfolio_map:
                _, account_errors = self.validate_portfolios_have_accounts(
                    base_portfolio_map
                )
                all_errors.extend(account_errors)

            # Sub-portfolios must NOT exist
            if sub_portfolios:
                _, sub_errors = self.validate_portfolios_not_exist(
                    sub_portfolios, edm_exposure_ids
                )
                all_errors.extend(sub_errors)

        return all_errors

    def _validate_portfolio_mapping_sql_scripts(
        self,
        base_portfolios: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Validate portfolio mapping SQL script configuration.

        Checks:
        - Cycle type is present in Metadata
        - Cycle type directory exists (quarterly, annual, adhoc)

        Note: Missing SQL scripts for individual portfolios are NOT validation errors.
        Some base portfolios may not have associated mapping scripts, and will be
        skipped during execution.

        Args:
            base_portfolios: List of base portfolio job configs

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not base_portfolios:
            return []

        errors = []

        # Get cycle type from first portfolio's Metadata
        first_portfolio = base_portfolios[0]
        metadata = first_portfolio.get('Metadata', {})
        cycle_type = metadata.get('Cycle Type')

        if not cycle_type:
            errors.append("Missing 'Cycle Type' in Metadata - required for portfolio mapping")
            return errors

        # Validate that cycle type directory exists using shared function
        try:
            resolve_cycle_type_directory(cycle_type)
        except Exception as e:
            errors.append(str(e))
            return errors

        # Note: We intentionally do NOT validate that SQL scripts exist for each portfolio.
        # Some base portfolios may not have mapping scripts and will be skipped at execution time.

        return errors

    def validate_grouping_batch(
        self,
        groupings: List[Dict[str, Any]]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate Grouping batch submission (analysis-only groups).

        Pre-requisites:
        - Analyses referenced in items should exist (missing analyses generate
          warnings, not errors - they will be skipped during submission)

        Entities to be created (must NOT exist):
        - Group names must not already exist

        Args:
            groupings: List of grouping dicts with 'Group_Name', 'items',
                      and 'analysis_edm_map' keys

        Returns:
            Tuple of (error_messages, existing_group_names)
            existing_group_names are group names that already exist in Moody's.
            This allows callers to use the existing list for interactive recovery.
            Warnings are prefixed with 'WARN-' and indicate missing analyses
            that will be skipped during submission.
            Errors (without WARN- prefix) block submission.
        """
        if not groupings:
            return [], []

        all_messages = []
        existing_groups = []

        # Collect all analysis names and the edm map from job configs
        all_analysis_names = []
        analysis_edm_map = {}
        group_names_to_create = []

        for group in groupings:
            group_name = group.get('Group_Name')
            if group_name:
                group_names_to_create.append({'Group Name': group_name})

            items = group.get('items', [])
            all_analysis_names.extend(items)

            # Merge analysis_edm_map from each job config
            edm_map = group.get('analysis_edm_map', {})
            analysis_edm_map.update(edm_map)

        # Check analyses existence - missing analyses are warnings, not errors
        # They will be skipped during submission
        if all_analysis_names:
            # Deduplicate
            unique_analyses = list(set(all_analysis_names))
            missing_analyses, _ = self.validate_analyses_exist(
                unique_analyses, analysis_edm_map
            )
            if missing_analyses:
                all_messages.append(
                    f"WARN-ANALYSIS-001: The following analyses were not found and will be skipped during grouping:{_format_entity_list(missing_analyses)}"
                )

        # Groups must NOT exist - this is still an error
        if group_names_to_create:
            existing_groups, group_errors = self.validate_groups_not_exist(group_names_to_create)
            all_messages.extend(group_errors)

        return all_messages, existing_groups

    def validate_grouping_rollup_batch(
        self,
        groupings: List[Dict[str, Any]]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate Grouping Rollup batch submission.

        Pre-requisites:
        - Child groups referenced in items should exist (missing groups generate
          warnings - they will be skipped during submission)
        - Analyses referenced in items should exist (missing analyses generate
          warnings - they will be skipped during submission)

        Entities to be created (must NOT exist):
        - Rollup group names must not already exist

        Args:
            groupings: List of grouping dicts with 'Group_Name', 'items',
                      'analysis_edm_map', and 'group_names' keys

        Returns:
            Tuple of (error_messages, existing_rollup_group_names)
            existing_rollup_group_names are rollup group names that already exist in Moody's.
            This allows callers to auto-delete them before submission.
            Warnings are prefixed with 'WARN-' and indicate missing analyses/groups
            that will be skipped during submission.
            Errors (without WARN- prefix) block submission.
        """
        if not groupings:
            return [], []

        all_messages = []
        existing_rollup_groups = []

        # Collect items, separating groups from analyses
        all_child_groups = []
        all_analysis_names = []
        analysis_edm_map = {}
        known_group_names = set()
        group_names_to_create = []

        for group in groupings:
            group_name = group.get('Group_Name')
            if group_name:
                group_names_to_create.append({'Group Name': group_name})

            items = group.get('items', [])
            group_names_list = group.get('group_names', [])
            known_group_names.update(group_names_list)

            # Merge analysis_edm_map from each job config
            edm_map = group.get('analysis_edm_map', {})
            analysis_edm_map.update(edm_map)

            # Separate items into groups vs analyses
            for item in items:
                if item in known_group_names:
                    all_child_groups.append(item)
                else:
                    all_analysis_names.append(item)

        # Check child groups existence - missing groups are warnings, not errors
        if all_child_groups:
            unique_groups = list(set(all_child_groups))
            missing_groups, _ = self.validate_groups_exist(unique_groups)
            if missing_groups:
                all_messages.append(
                    f"WARN-GROUP-001: The following child groups were not found and will be skipped during grouping:{_format_entity_list(missing_groups)}"
                )

        # Check analyses existence - missing analyses are warnings, not errors
        if all_analysis_names:
            unique_analyses = list(set(all_analysis_names))
            missing_analyses, _ = self.validate_analyses_exist(
                unique_analyses, analysis_edm_map
            )
            if missing_analyses:
                all_messages.append(
                    f"WARN-ANALYSIS-001: The following analyses were not found and will be skipped during grouping:{_format_entity_list(missing_analyses)}"
                )

        # Rollup groups must NOT exist - this is still an error
        if group_names_to_create:
            existing_rollup_groups, group_errors = self.validate_groups_not_exist(group_names_to_create)
            all_messages.extend(group_errors)

        return all_messages, existing_rollup_groups

    def validate_analysis_batch(
        self,
        analyses: List[Dict[str, Any]]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate Analysis batch submission.

        Pre-requisites (must exist):
        - EDMs that analyses belong to
        - Portfolios within their EDMs
        - Treaties referenced (if any)
        - Reference data (model profiles, output profiles, event rate schemes)

        Entities to be created (must NOT exist):
        - Analyses must not already exist

        Args:
            analyses: List of analysis job dicts with 'Database', 'Portfolio',
                     'Analysis Name', 'Analysis Profile', 'Output Profile',
                     'Event Rate', and optional 'Reinsurance Treaty 1-5' keys

        Returns:
            Tuple of (error_messages, existing_analysis_identifiers)
            existing_analysis_identifiers are in format "EDM_NAME/ANALYSIS_NAME"
            This allows callers to use the existing list for interactive recovery.
        """
        if not analyses:
            return [], []

        all_errors = []
        existing_analyses = []

        # Extract unique EDM names (filter out None values)
        edm_names = list(set(
            str(a['Database']) for a in analyses
            if a.get('Database') is not None
        ))

        # Pre-requisite 1: EDMs must exist
        edm_exposure_ids, edm_errors = self.validate_edms_exist(edm_names)
        all_errors.extend(edm_errors)

        # Pre-requisite 2: Portfolios must exist (only check in EDMs that exist)
        if edm_exposure_ids:
            # Build portfolio list for validation
            portfolios: List[Dict[str, str]] = [
                {'Database': a['Database'], 'Portfolio': a['Portfolio']}
                for a in analyses
                if a.get('Database') and a.get('Portfolio')
            ]
            _, portfolio_errors = self.validate_portfolios_exist(portfolios, edm_exposure_ids)
            all_errors.extend(portfolio_errors)

        # Pre-requisite 3: Treaties must exist (if any are specified)
        if edm_exposure_ids:
            # Extract all treaty references from analyses (Reinsurance Treaty 1-5)
            treaties: List[Dict[str, str]] = []
            for a in analyses:
                edm = a.get('Database')
                if not edm:
                    continue
                for i in range(1, 6):
                    treaty_name = a.get(f'Reinsurance Treaty {i}')
                    if treaty_name:
                        treaties.append({'Database': edm, 'treaty_name': treaty_name})

            if treaties:
                _, treaty_errors = self.validate_treaties_exist(treaties, edm_exposure_ids)
                all_errors.extend(treaty_errors)

        # Pre-requisite 4: Reference data must exist (model profiles, output profiles, event rates)
        # Use the shared reference data validation from configuration.py
        # Pass None to let it create its own IRPClient (avoids type mismatch with Client)
        from helpers.configuration import validate_reference_data_with_api
        ref_data_errors = validate_reference_data_with_api(analyses, irp_client=None)
        all_errors.extend(ref_data_errors)

        # Analyses must NOT exist
        existing_analyses, analysis_errors = self.validate_analyses_not_exist(analyses)
        all_errors.extend(analysis_errors)

        return all_errors, existing_analyses

    def validate_rdm_export_batch(
        self,
        export_jobs: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Validate RDM Export batch submission.

        Pre-requisites (must exist):
        - Analyses referenced must exist
        - Groups referenced must exist

        Entities to be created (must NOT exist):
        - RDM name must not already exist

        Args:
            export_jobs: List of export job dicts with 'analysis_names', 'rdm_name',
                        'server_name', 'analysis_edm_map', and 'group_names_set' keys

        Returns:
            List of error messages (empty if all validation passes)
        """
        if not export_jobs:
            return []

        all_errors = []

        # Collect all items, analysis_edm_map, and group_names_set from all jobs
        all_items = []
        analysis_edm_map = {}
        group_names_set = set()
        rdm_name = None
        server_name = None

        for job in export_jobs:
            items = job.get('analysis_names', [])
            all_items.extend(items)

            # Merge analysis_edm_map from each job
            edm_map = job.get('analysis_edm_map', {})
            analysis_edm_map.update(edm_map)

            # Merge group_names_set from each job
            groups = job.get('group_names_set', [])
            group_names_set.update(groups)

            # Get rdm_name and server_name (same across all jobs)
            if not rdm_name:
                rdm_name = job.get('rdm_name')
            if not server_name:
                server_name = job.get('server_name', DEFAULT_DATABASE_SERVER)

        # Deduplicate items
        unique_items = list(set(all_items))

        # Separate items into groups vs analyses
        group_items = [item for item in unique_items if item in group_names_set]
        analysis_items = [item for item in unique_items if item not in group_names_set]

        # Pre-requisite 1: Groups must exist
        if group_items:
            _, group_errors = self.validate_groups_exist(group_items)
            all_errors.extend(group_errors)

        # Pre-requisite 2: Analyses must exist
        if analysis_items:
            _, analysis_errors = self.validate_analyses_exist(
                analysis_items, analysis_edm_map
            )
            all_errors.extend(analysis_errors)

        # RDM must NOT exist
        if rdm_name:
            rdm_errors = self.validate_rdm_not_exists(rdm_name, server_name)
            all_errors.extend(rdm_errors)

        return all_errors

    # =========================================================================
    # Entity Existence Check for FINISHED Jobs
    # =========================================================================

    def check_entity_exists_for_job(
        self,
        job_config_data: Dict[str, Any],
        batch_type: str,
        config_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Check if the entity that should have been created by a job actually exists.

        Used to verify FINISHED jobs - if the entity is missing (e.g., deleted
        externally), the job may need to be resubmitted.

        Args:
            job_config_data: Job configuration data dict
            batch_type: Batch type string (from BatchType constants)
            config_data: Optional full configuration data (needed for some batch types
                        like portfolio mapping to determine expected sub-portfolios)

        Returns:
            True if entity exists, False if entity is missing
        """
        try:
            if batch_type == BatchType.EDM_CREATION:
                return self._check_edm_exists(job_config_data)

            elif batch_type == BatchType.PORTFOLIO_CREATION:
                return self._check_portfolio_exists(job_config_data)

            elif batch_type == BatchType.CREATE_REINSURANCE_TREATIES:
                return self._check_treaty_exists(job_config_data)

            elif batch_type == BatchType.MRI_IMPORT:
                return self._check_mri_import_complete(job_config_data)

            elif batch_type == BatchType.EDM_DB_UPGRADE:
                return self._check_edm_version_upgraded(job_config_data)

            elif batch_type == BatchType.GEOHAZ:
                return self._check_geohaz_complete(job_config_data)

            elif batch_type == BatchType.PORTFOLIO_MAPPING:
                return self._check_sub_portfolios_exist(job_config_data, config_data)

            elif batch_type == BatchType.ANALYSIS:
                return self._check_analysis_exists(job_config_data)

            elif batch_type == BatchType.GROUPING or batch_type == BatchType.GROUPING_ROLLUP:
                return self._check_group_exists(job_config_data)

            elif batch_type == BatchType.EXPORT_TO_RDM:
                return self._check_rdm_exists(job_config_data)

            # For batch types we can't verify, assume entity exists
            return True

        except IRPAPIError:
            # If API call fails, assume entity exists to avoid false resubmissions
            return True

    def _check_edm_exists(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if EDM exists."""
        edm_name = job_config_data.get('Database')
        if not edm_name:
            return True  # Can't check without name, assume exists

        edm_ids, _ = self.validate_edms_exist([edm_name])
        return edm_name in edm_ids

    def _check_portfolio_exists(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if portfolio exists in its EDM."""
        edm_name = job_config_data.get('Database')
        portfolio_name = job_config_data.get('Portfolio')
        if not edm_name or not portfolio_name:
            return True  # Can't check without names, assume exists

        edm_ids, _ = self.validate_edms_exist([edm_name])
        if not edm_ids:
            return False  # EDM doesn't exist, so portfolio can't exist

        portfolio_ids, _ = self.validate_portfolios_exist(
            [{'Database': edm_name, 'Portfolio': portfolio_name}],
            edm_ids
        )
        return f"{edm_name}/{portfolio_name}" in portfolio_ids

    def _check_treaty_exists(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if treaty exists in its EDM."""
        edm_name = job_config_data.get('Database')
        treaty_name = job_config_data.get('Treaty Name')
        if not edm_name or not treaty_name:
            return True  # Can't check without names, assume exists

        edm_ids, _ = self.validate_edms_exist([edm_name])
        if not edm_ids:
            return False  # EDM doesn't exist, so treaty can't exist

        # validate_treaties_exist returns missing treaties
        missing, _ = self.validate_treaties_exist(
            [{'Database': edm_name, 'Treaty Name': treaty_name}],
            edm_ids
        )
        return len(missing) == 0  # Empty missing list = treaty exists

    def _check_mri_import_complete(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if MRI import completed (portfolio has accounts/locations)."""
        edm_name = job_config_data.get('Database')
        portfolio_name = job_config_data.get('Portfolio')
        if not edm_name or not portfolio_name:
            return True  # Can't check without names, assume exists

        edm_ids, _ = self.validate_edms_exist([edm_name])
        if not edm_ids:
            return False  # EDM doesn't exist

        portfolio_ids, _ = self.validate_portfolios_exist(
            [{'Database': edm_name, 'Portfolio': portfolio_name}],
            edm_ids
        )
        portfolio_key = f"{edm_name}/{portfolio_name}"
        if portfolio_key not in portfolio_ids:
            return False  # Portfolio doesn't exist

        # Check if portfolio has accounts (inverse of validate_accounts_not_exist)
        has_no_accounts, _ = self.validate_accounts_not_exist({portfolio_key: portfolio_ids[portfolio_key]})
        # If portfolio is in has_no_accounts list, it means it HAS accounts (validation failed)
        # Wait, that's backwards - validate_accounts_not_exist returns portfolios WITH accounts
        return len(has_no_accounts) > 0  # If has accounts, import is complete

    def _check_edm_version_upgraded(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if EDM has been upgraded to target version."""
        edm_name = job_config_data.get('Database')
        target_version = job_config_data.get('target_edm_version')
        if not edm_name or not target_version:
            return True  # Can't check without required fields, assume exists

        # Search for EDM and check its dataVersion
        try:
            filter_str = f'exposureName = "{edm_name}"'
            edms = self.edm_manager.search_edms_paginated(filter=filter_str)
            if not edms:
                return False  # EDM doesn't exist

            edm = edms[0]
            metrics = edm.get('metrics', {})
            data_version = metrics.get('dataVersion', '')

            # Compare major version (e.g., "22.0.0" -> "22")
            current_major = data_version.split('.')[0] if data_version else ''
            target_major = target_version.split('.')[0] if '.' in target_version else target_version

            return current_major == target_major

        except IRPAPIError:
            return True  # Assume upgraded if we can't check

    def _check_geohaz_complete(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if portfolio has been geocoded and hazard-coded."""
        edm_name = job_config_data.get('Database')
        portfolio_name = job_config_data.get('Portfolio')
        if not edm_name or not portfolio_name:
            return True  # Can't check without names, assume exists

        edm_ids, _ = self.validate_edms_exist([edm_name])
        if not edm_ids:
            return False  # EDM doesn't exist

        exposure_id = edm_ids[edm_name]

        # Search for portfolio and check geocodeVersion/hazardVersion
        try:
            filter_str = f'portfolioName = "{portfolio_name}"'
            portfolios = self.portfolio_manager.search_portfolios_paginated(
                exposure_id=exposure_id,
                filter=filter_str
            )
            if not portfolios:
                return False  # Portfolio doesn't exist

            portfolio = portfolios[0]
            geocode_version = portfolio.get('geocodeVersion', '')
            hazard_version = portfolio.get('hazardVersion', '')

            # Both must be non-empty for GeoHaz to be complete
            return bool(geocode_version) and bool(hazard_version)

        except IRPAPIError:
            return True  # Assume complete if we can't check

    def _check_sub_portfolios_exist(
        self,
        job_config_data: Dict[str, Any],
        config_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if sub-portfolios created by portfolio mapping exist."""
        edm_name = job_config_data.get('Database')
        if not edm_name:
            return True  # Can't check without EDM name

        base_portfolio = job_config_data.get('Portfolio')
        import_file = job_config_data.get('Import File')

        if not base_portfolio:
            return True  # Can't check without base portfolio name

        edm_ids, _ = self.validate_edms_exist([edm_name])
        if not edm_ids:
            return False  # EDM doesn't exist

        # Determine expected sub-portfolios from configuration
        if not config_data:
            return True  # Can't check without config data, assume exists

        portfolios = config_data.get('Portfolios', [])

        # Find sub-portfolios that belong to this base portfolio
        # (matching Database and Import File, with Base Portfolio? = N)
        expected_sub_portfolios = [
            p.get('Portfolio')
            for p in portfolios
            if (p.get('Database') == edm_name and
                p.get('Import File') == import_file and
                p.get('Base Portfolio?') == 'N')
        ]

        if not expected_sub_portfolios:
            return True  # No sub-portfolios expected, assume complete

        # Check if all expected sub-portfolios exist
        sub_portfolio_checks = [
            {'Database': edm_name, 'Portfolio': sub_name}
            for sub_name in expected_sub_portfolios
        ]

        portfolio_ids, _ = self.validate_portfolios_exist(sub_portfolio_checks, edm_ids)

        # Check if any sub-portfolio is missing
        for sub_name in expected_sub_portfolios:
            portfolio_key = f"{edm_name}/{sub_name}"
            if portfolio_key not in portfolio_ids:
                return False  # At least one sub-portfolio is missing

        return True  # All sub-portfolios exist

    def _check_analysis_exists(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if analysis exists."""
        edm_name = job_config_data.get('Database')
        analysis_name = job_config_data.get('Analysis Name')
        if not edm_name or not analysis_name:
            return True  # Can't check without names, assume exists

        # validate_analyses_not_exist returns existing analyses
        existing, _ = self.validate_analyses_not_exist(
            [{'Database': edm_name, 'Analysis Name': analysis_name}]
        )
        return len(existing) > 0  # If in existing list, it exists

    def _check_group_exists(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if analysis group exists."""
        group_name = job_config_data.get('Group_Name')
        if not group_name:
            return True  # Can't check without name, assume exists

        # validate_groups_not_exist returns existing groups
        existing, _ = self.validate_groups_not_exist([{'Group Name': group_name}])
        return len(existing) > 0  # If in existing list, it exists

    def _check_rdm_exists(self, job_config_data: Dict[str, Any]) -> bool:
        """Check if RDM exists."""
        rdm_name = job_config_data.get('rdm_name')
        server_name = job_config_data.get('server_name', DEFAULT_DATABASE_SERVER)
        if not rdm_name:
            return True  # Can't check without name, assume exists

        # validate_rdm_not_exists returns errors if RDM exists
        errors = self.validate_rdm_not_exists(rdm_name, server_name)
        return len(errors) > 0  # If errors (RDM exists), return True

    def _validate_csv_files_exist(
        self,
        portfolios: List[Dict[str, str]],
        working_files_dir: str
    ) -> List[str]:
        """
        Check that CSV import files exist in the working_files directory.

        Args:
            portfolios: List of portfolio dicts with 'accounts_import_file'
                       and 'locations_import_file' keys
            working_files_dir: Path to the working_files directory

        Returns:
            List of error messages (empty if all files exist)
        """
        from pathlib import Path

        if not portfolios or not working_files_dir:
            return []

        missing_files = []
        working_path = Path(working_files_dir)

        for portfolio in portfolios:
            accounts_file = portfolio.get('accounts_import_file')
            locations_file = portfolio.get('locations_import_file')

            if accounts_file:
                accounts_path = working_path / accounts_file
                if not accounts_path.exists():
                    missing_files.append(accounts_file)

            if locations_file:
                locations_path = working_path / locations_file
                if not locations_path.exists():
                    missing_files.append(locations_file)

        if missing_files:
            return [
                f"ENT-FILE-001: The following required CSV files were not found:{_format_entity_list(missing_files)}"
            ]

        return []