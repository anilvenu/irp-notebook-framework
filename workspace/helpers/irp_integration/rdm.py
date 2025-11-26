"""
RDM (Risk Data Model) export operations.

Handles exporting analysis results to RDM via databridge.
"""

import json
import time
from typing import Dict, List, Any, Optional

from helpers.irp_integration.utils import extract_id_from_location_header
from .client import Client
from .constants import CREATE_RDM_EXPORT_JOB, GET_EXPORT_JOB, WORKFLOW_COMPLETED_STATUSES
from .exceptions import IRPAPIError, IRPJobError
from .validators import validate_non_empty_string, validate_list_not_empty, validate_positive_int

class RDMManager:
    """Manager for RDM export operations."""

    def __init__(self, client: Client, analysis_manager: Optional[Any] = None, edm_manager: Optional[Any] = None) -> None:
        """
        Initialize RDM manager.

        Args:
            client: IRP API client instance
            analysis_manager: Optional AnalysisManager instance
        """
        self.client = client
        self._analysis_manager = analysis_manager
        self._edm_manager = edm_manager

    @property
    def analysis_manager(self):
        """Lazy-loaded analysis manager to avoid circular imports."""
        if self._analysis_manager is None:
            from .analysis import AnalysisManager
            self._analysis_manager = AnalysisManager(self.client)
        return self._analysis_manager
    
    @property
    def edm_manager(self):
        """Lazy-loaded edm manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager


    def export_analyses_to_rdm(
            self,
            server_name: str,
            rdm_name: str,
            analysis_names: List[str]
    ) -> Dict[str, Any]:
        """
        Export multiple analyses to RDM (Risk Data Model) and poll to completion.

        Args:
            server_name: Database server name
            rdm_name: Name for the RDM
            analysis_names: List of analysis names to export

        Returns:
            Dict containing final export job status

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If export fails or analyses not found
        """
        rdm_export_job_id = self.submit_rdm_export_job(
            server_name=server_name,
            rdm_name=rdm_name,
            analysis_names=analysis_names
        )
        return self.poll_rdm_export_job_to_completion(rdm_export_job_id)


    def submit_rdm_export_job(
            self,
            server_name: str,
            rdm_name: str,
            analysis_names: List[str]
    ) -> int:
        """
        Submit RDM export job.

        Performs validation (server lookup, RDM existence check, analysis URI
        resolution) and submits the export job.

        Args:
            server_name: Database server name
            rdm_name: Name for the RDM
            analysis_names: List of analysis names to export

        Returns:
            Job ID

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If job submission fails
        """
        validate_non_empty_string(server_name, "server_name")
        validate_non_empty_string(rdm_name, "rdm_name")
        validate_list_not_empty(analysis_names, "analysis_names")

        # Look up server ID
        database_servers = self.edm_manager.search_database_servers(filter=f"serverName=\"{server_name}\"")
        try:
            server_id = database_servers[0]['serverId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract server ID for server '{server_name}': {e}"
            ) from e

        # Check RDM doesn't already exist
        rdms = self.edm_manager.search_edms(filter=f"exposureName=\"{rdm_name}\"")
        if len(rdms) > 0:
            raise IRPAPIError(f"RDM with name {rdm_name} already exists")

        # Resolve analysis names to URIs
        resource_uris = []
        for name in analysis_names:
            analysis_response = self.analysis_manager.search_analyses(filter=f"analysisName = \"{name}\"")
            if len(analysis_response) == 0:
                raise IRPAPIError(f"Analysis with this name does not exist: {name}")
            if len(analysis_response) > 1:
                raise IRPAPIError(f"Duplicate analyses exist with name: {name}")
            try:
                resource_uris.append(analysis_response[0]['uri'])
            except (KeyError, IndexError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract analysis URI for analysis '{name}': {e}"
                ) from e

        data = {
            "exportType": "RDM_DATABRIDGE",
            "resourceType": "analyses",
            "settings": {
                "rdmName": rdm_name,
                "serverId": server_id
            },
            "resourceUris": resource_uris
        }

        try:
            response = self.client.request('POST', CREATE_RDM_EXPORT_JOB, json=data)
            job_id = extract_id_from_location_header(response, "analysis job submission")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to submit rdm export job : {e}")


    def get_rdm_export_job(self, job_id: int) -> Dict[str, Any]:
        """
        Retrieve RDM export job status by job ID.

        Args:
            job_id: Job ID

        Returns:
            Dict containing job status details

        Raises:
            IRPValidationError: If job_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(job_id, "job_id")

        try:
            response = self.client.request('GET', GET_EXPORT_JOB.format(jobId=job_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get rdm export job status for job ID {job_id}: {e}")


    def poll_rdm_export_job_to_completion(
            self,
            job_id: int,
            interval: int = 10,
            timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll RDM export job until completion or timeout.

        Args:
            job_id: Job ID
            interval: Polling interval in seconds (default: 10)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            Final job status details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If job times out
            IRPAPIError: If polling fails
        """
        validate_positive_int(job_id, "job_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling RDM export job ID {job_id}")
            job_data = self.get_rdm_export_job(job_id)
            try:
                status = job_data['status']
                progress = job_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in job response for job ID {job_id}: {e}"
                ) from e
            print(f"Job status: {status}; Percent complete: {progress}")
            if status in WORKFLOW_COMPLETED_STATUSES:
                return job_data
            
            if time.time() - start > timeout:
                raise IRPJobError(
                    f"RDM Export job ID {job_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)
