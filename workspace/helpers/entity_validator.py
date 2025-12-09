"""
Entity Existence Validation for IRP Configuration.

Validates that entities from configuration files don't already exist in Moody's.
Used during configuration file validation to prevent conflicts with existing data.
"""

from typing import Dict, Any, List, Tuple, Optional
from helpers.irp_integration.client import Client
from helpers.irp_integration.exceptions import IRPAPIError


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

    def validate_analyses_not_exist(
        self,
        analyses: List[Dict[str, str]],
        edm_exposure_ids: Dict[str, int]
    ) -> Tuple[List[str], List[str]]:
        """
        Check that analyses don't already exist.

        Args:
            analyses: List of dicts with 'Database' and 'Analysis Name' keys
            edm_exposure_ids: Mapping of EDM names to exposure IDs

        Returns:
            Tuple of (existing_analysis_identifiers, error_messages)
            Identifiers are in format "EDM_NAME/ANALYSIS_NAME"
        """
        if not analyses or not edm_exposure_ids:
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

        for edm_name, analysis_names in by_edm.items():
            exposure_id = edm_exposure_ids.get(edm_name)
            if not exposure_id:
                continue  # EDM doesn't exist in Moody's, so analyses can't exist either

            try:
                # Build filter for analyses in this EDM
                # Need to combine analysisName IN (...) with exposureName filter
                quoted = ", ".join(f'"{name}"' for name in analysis_names)
                filter_str = f'analysisName IN ({quoted}) AND exposureName = "{edm_name}"'

                found = self.analysis_manager.search_analyses_paginated(filter=filter_str)

                for a in found:
                    name = a.get('analysisName')
                    if name in analysis_names:
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

        try:
            # Build IN filter for all group names
            quoted = ", ".join(f'"{name}"' for name in group_names)
            filter_str = f"analysisName IN ({quoted})"

            found = self.analysis_manager.search_analyses_paginated(filter=filter_str)

            for g in found:
                name = g.get('analysisName')
                if name in group_names:
                    existing.append(name)

        except IRPAPIError as e:
            errors.append(f"ENT-API-001: Failed to check group existence: {e}")

        if existing:
            errors.insert(0,
                f"ENT-GROUP-001: The following groups already exist:{_format_entity_list(existing)}"
            )

        return existing, errors

    def validate_rdm_not_exists(
        self,
        rdm_name: str,
        server_name: str = "databridge-1"
    ) -> List[str]:
        """
        Check that an RDM with the given name doesn't already exist.

        Args:
            rdm_name: Name of the RDM to check
            server_name: Database server name (default: "databridge-1")

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
        4. If any Analyses exist:
           - Check Groups don't exist
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
            existing_portfolios, portfolio_errors = self.validate_portfolios_not_exist(
                portfolios, edm_exposure_ids
            )
            all_errors.extend(portfolio_errors)

            # Step 2b: Check Treaties
            existing_treaties, treaty_errors = self.validate_treaties_not_exist(
                treaties, edm_exposure_ids
            )
            all_errors.extend(treaty_errors)

            # Step 3: If portfolios exist, check Analyses
            if existing_portfolios:
                existing_analyses, analysis_errors = self.validate_analyses_not_exist(
                    analyses, edm_exposure_ids
                )
                all_errors.extend(analysis_errors)

                # Step 4: If analyses exist, check Groups
                if existing_analyses:
                    existing_groups, group_errors = self.validate_groups_not_exist(groupings)
                    all_errors.extend(group_errors)

                    # Step 5: If analyses or groups exist, check RDM
                    if (existing_analyses or existing_groups) and rdm_name:
                        rdm_errors = self.validate_rdm_not_exists(rdm_name)
                        all_errors.extend(rdm_errors)

        return all_errors