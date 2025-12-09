"""
RDM (Risk Data Model) export operations.

Handles exporting analysis results to RDM via databridge.
"""

import time
from typing import Dict, List, Any, Optional

from helpers.irp_integration.utils import extract_id_from_location_header
from .client import Client
from .constants import CREATE_RDM_EXPORT_JOB, GET_EXPORT_JOB, SEARCH_DATABASES, WORKFLOW_COMPLETED_STATUSES
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
            analysis_names: List[str],
            database_id: Optional[int] = None,
            analysis_edm_map: Optional[Dict[str, str]] = None,
            group_names: Optional[set] = None
    ) -> int:
        """
        Submit RDM export job.

        Performs validation (server lookup, RDM existence check, analysis URI
        resolution) and submits the export job.

        Args:
            server_name: Database server name
            rdm_name: Name for the RDM
            analysis_names: List of analysis and group names to export
            database_id: Optional database ID (for appending to existing RDM)
            analysis_edm_map: Optional mapping of analysis names to EDM names.
                Used to look up analyses by name + EDM (since analysis names are only
                unique within an EDM). If not provided, lookups use name only.
            group_names: Optional set of known group names. Items in this set are
                looked up as groups (by name only), all others are looked up as
                analyses (by name + EDM if mapping provided).

        Returns:
            Job ID

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If job submission fails
        """
        validate_non_empty_string(server_name, "server_name")
        validate_non_empty_string(rdm_name, "rdm_name")
        validate_list_not_empty(analysis_names, "analysis_names")

        # Initialize defaults
        if analysis_edm_map is None:
            analysis_edm_map = {}
        if group_names is None:
            group_names = set()

        # Look up server ID
        database_servers = self.edm_manager.search_database_servers(filter=f"serverName=\"{server_name}\"")
        try:
            server_id = database_servers[0]['serverId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract server ID for server '{server_name}': {e}"
            ) from e

        # Check if RDM with same name already exists (only for new RDM creation, not appending)
        if not database_id:
            existing_rdms = self.search_databases(
                server_name=server_name,
                filter=f"databaseName LIKE \"{rdm_name}*\""
            )
            if existing_rdms:
                existing_name = existing_rdms[0].get('databaseName', rdm_name)
                raise IRPAPIError(
                    f"RDM with name '{rdm_name}' already exists on server '{server_name}' "
                    f"(found: '{existing_name}'). Please use a different RDM name or delete "
                    f"the existing RDM first."
                )

        # Resolve analysis/group names to URIs
        resource_uris = []
        for name in analysis_names:
            # Determine if this is a group name or an analysis name
            if name in group_names:
                # Group names are globally unique - search by name only
                analysis_response = self.analysis_manager.search_analyses(filter=f"analysisName = \"{name}\"")
                if len(analysis_response) == 0:
                    raise IRPAPIError(f"Group with this name does not exist: {name}")
                if len(analysis_response) > 1:
                    raise IRPAPIError(f"Duplicate groups exist with name: {name}")
            else:
                # Analysis names - search by name + EDM if mapping provided
                edm_name = analysis_edm_map.get(name)
                if edm_name:
                    filter_str = f"analysisName = \"{name}\" AND exposureName = \"{edm_name}\""
                    analysis_response = self.analysis_manager.search_analyses(filter=filter_str)
                    if len(analysis_response) == 0:
                        raise IRPAPIError(f"Analysis '{name}' not found for EDM '{edm_name}'")
                    if len(analysis_response) > 1:
                        raise IRPAPIError(f"Multiple analyses found with name '{name}' for EDM '{edm_name}'")
                else:
                    # Fallback to name-only search (legacy behavior)
                    analysis_response = self.analysis_manager.search_analyses(filter=f"analysisName = \"{name}\"")
                    if len(analysis_response) == 0:
                        raise IRPAPIError(f"Analysis with this name does not exist: {name}")
                    if len(analysis_response) > 1:
                        raise IRPAPIError(f"Duplicate analyses exist with name: {name}.")

            try:
                resource_uris.append(analysis_response[0]['uri'])
            except (KeyError, IndexError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract URI for '{name}': {e}"
                ) from e

        # Build settings - use databaseId if provided (appending to existing RDM),
        # otherwise use rdmName (creating new RDM)
        if database_id:
            settings = {
                "databaseId": database_id,
                "serverId": server_id
            }
        else:
            settings = {
                "rdmName": rdm_name,
                "serverId": server_id
            }

        data = {
            "exportType": "RDM_DATABRIDGE",
            "resourceType": "analyses",
            "settings": settings,
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

    def get_rdm_database_id(self, rdm_name: str, server_name: str = "databridge-1") -> int:
        """
        Get database ID for an existing RDM by name.

        Args:
            rdm_name: Name of the RDM
            server_name: Name of the database server (default: "databridge-1")

        Returns:
            Database ID

        Raises:
            IRPAPIError: If RDM not found
        """
        databases = self.search_databases(
            server_name=server_name,
            filter=f"databaseName LIKE \"{rdm_name}*\""
        )
        if not databases:
            raise IRPAPIError(f"RDM '{rdm_name}' not found on server '{server_name}'")
        elif len(databases) > 1:
            raise IRPAPIError(f"Multiple RDMs found with name '{rdm_name}' on server '{server_name}'")

        try:
            return databases[0]['databaseId']
        except (KeyError, IndexError) as e:
            raise IRPAPIError(f"Failed to extract databaseId for RDM '{rdm_name}': {e}")

    def get_rdm_database_full_name(self, rdm_name: str, server_name: str = "databridge-1") -> str:
        """
        Get full database name for an existing RDM by name prefix.

        Args:
            rdm_name: Name prefix of the RDM
            server_name: Name of the database server (default: "databridge-1")

        Returns:
            Full database name

        Raises:
            IRPAPIError: If RDM not found
        """
        databases = self.search_databases(
            server_name=server_name,
            filter=f"databaseName LIKE \"{rdm_name}*\""
        )
        if not databases:
            raise IRPAPIError(f"RDM '{rdm_name}' not found on server '{server_name}'")
        elif len(databases) > 1:
            raise IRPAPIError(f"Multiple RDMs found with name '{rdm_name}' on server '{server_name}'")

        try:
            return databases[0]['databaseName']
        except (KeyError, IndexError) as e:
            raise IRPAPIError(f"Failed to extract databaseName for RDM '{rdm_name}': {e}")

    def search_databases(self, server_name: str, filter: str = "", limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search databases on a server.

        Args:
            server_name: Name of the database server
            filter: Optional filter string (e.g., 'databaseName="MyRDM"')
            limit: Maximum results per page (default: 100)
            offset: Offset for pagination (default: 0)

        Returns:
            List of database records

        Raises:
            IRPAPIError: If request fails
        """
        # Look up server ID first
        database_servers = self.edm_manager.search_database_servers(filter=f"serverName=\"{server_name}\"")
        if not database_servers:
            raise IRPAPIError(f"Database server '{server_name}' not found")

        try:
            server_id = database_servers[0]['serverId']
        except (KeyError, IndexError) as e:
            raise IRPAPIError(f"Failed to extract server ID: {e}")

        params: Dict[str, Any] = {'limit': limit, 'offset': offset}
        if filter:
            params['filter'] = filter

        try:
            response = self.client.request(
                'GET',
                SEARCH_DATABASES.format(serverId=server_id),
                params=params
            )
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to search databases: {e}")

    def search_databases_paginated(self, server_name: str, filter: str = "") -> List[Dict[str, Any]]:
        """
        Search all databases on a server with automatic pagination.

        Fetches all pages of results matching the filter criteria.

        Args:
            server_name: Name of the database server
            filter: Optional filter string (e.g., 'databaseName="MyRDM"')

        Returns:
            Complete list of all matching database records across all pages

        Raises:
            IRPAPIError: If request fails
        """
        all_results = []
        offset = 0
        limit = 100

        while True:
            results = self.search_databases(server_name=server_name, filter=filter, limit=limit, offset=offset)
            all_results.extend(results)

            # If we got fewer results than the limit, we've reached the end
            if len(results) < limit:
                break
            offset += limit

        return all_results
