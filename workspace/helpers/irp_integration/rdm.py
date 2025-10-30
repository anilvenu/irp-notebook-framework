"""
RDM (Risk Data Model) export operations.

Handles exporting analysis results to RDM via databridge.
"""

import json
from typing import Dict, List, Any, Optional
from .client import Client
from .constants import EXPORT_TO_RDM
from .exceptions import IRPAPIError
from .validators import validate_non_empty_string, validate_list_not_empty
from .utils import get_nested_field

class RDMManager:
    """Manager for RDM export operations."""

    def __init__(self, client: Client, analysis_manager: Optional[Any] = None) -> None:
        """
        Initialize RDM manager.

        Args:
            client: IRP API client instance
            analysis_manager: Optional AnalysisManager instance
        """
        self.client = client
        self._analysis_manager = analysis_manager

    @property
    def analysis_manager(self):
        """Lazy-loaded analysis manager to avoid circular imports."""
        if self._analysis_manager is None:
            from .analysis import AnalysisManager
            self._analysis_manager = AnalysisManager(self.client)
        return self._analysis_manager

    def export_analyses_to_rdm(
        self,
        rdm_name: str,
        analysis_ids: List[int],
        server_id: int = 88094
    ) -> Dict[str, Any]:
        """
        Export analyses to RDM via databridge.

        Args:
            rdm_name: Target RDM name
            analysis_ids: List of analysis IDs to export
            server_id: RDM server ID (default: 88094)

        Returns:
            Workflow response dict

        Raises:
            IRPValidationError: If inputs are invalid
            IRPWorkflowError: If workflow fails or times out
        """
        validate_non_empty_string(rdm_name, "rdm_name")
        validate_list_not_empty(analysis_ids, "analysis_ids")

        try:
            analyses = self.analysis_manager.get_analyses_by_ids(analysis_ids)
            resourceUris = [
                get_nested_field(analysis, 'uri', required=True, context=f"analysis data")
                for analysis in analyses
            ]

            if not resourceUris:
                raise IRPAPIError(
                    f"No analysis URIs found for the provided IDs: {analysis_ids}"
                )

            data = {
                "exportType": "RDM_DATABRIDGE",
                "resourceType": "analyses",
                "resourceUris": resourceUris,
                "settings": {
                    "serverId": server_id,
                    "rdmName": rdm_name
                }
            }

            print(json.dumps(data, indent=2))

            response = self.client.execute_workflow('POST', EXPORT_TO_RDM, json=data)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to export analyses to RDM '{rdm_name}': {e}")