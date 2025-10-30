"""
Client for IRP Integration API requests.

Handles HTTP requests with retry logic, workflow polling,
and batch workflow execution.
"""

import requests
import time
import os
from typing import Dict, List, Any, Optional, Union
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from .constants import GET_WORKFLOWS, WORKFLOW_COMPLETED_STATUSES, WORKFLOW_IN_PROGRESS_STATUSES
from .exceptions import IRPAPIError, IRPWorkflowError
from .validators import validate_non_empty_string, validate_positive_int
from .utils import get_location_header, get_nested_field

class Client:

    """Client for Moody's Risk Modeler API."""

    def __init__(self) -> None:
        """
        Initialize API client with credentials from environment.

        Environment variables:
            RISK_MODELER_BASE_URL: API base URL
            RISK_MODELER_API_KEY: API authentication key
            RISK_MODELER_RESOURCE_GROUP_ID: Resource group ID
        """
        self.base_url = os.environ.get('RISK_MODELER_BASE_URL', 'https://api-euw1.rms-ppe.com')
        self.api_key = os.environ.get('RISK_MODELER_API_KEY', 'your_api_key')
        self.resource_group_id = os.environ.get('RISK_MODELER_RESOURCE_GROUP_ID', 'your_resource_id')
        self.headers = {
            'Authorization': self.api_key,
            'x-rms-resource-group-id': self.resource_group_id
        }
        self.timeout = 200

        session = requests.Session()
        session.headers.update(self.headers or {})

        retry = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE"),
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session = session

    def request(
        self,
        method: str,
        path: str,
        *,
        full_url: Optional[str] = None,
        base_url: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Union[Dict[str, Any], List[Any]]] = None,
        headers: Dict[str, str] = {},
        timeout: Optional[int] = None,
        stream: bool = False
    ) -> requests.Response:
        """
        Make HTTP request to API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            path: API path (e.g., '/api/v1/datasources')
            full_url: Full URL (overrides path/base_url if provided)
            base_url: Base URL (overrides default if provided)
            params: Query parameters
            json: JSON request body
            headers: Additional headers
            timeout: Request timeout in seconds
            stream: Enable streaming response

        Returns:
            HTTP response object

        Raises:
            IRPAPIError: If HTTP request fails
        """
        validate_non_empty_string(method, "method")

        if full_url:
            url = full_url
        else:
            if base_url:
                url = f"{base_url}/{path.lstrip('/')}"
            else:
                url = f"{self.base_url}/{path.lstrip('/')}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=self.headers | headers,
                timeout=timeout or self.timeout,
                stream=stream,
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            # Enrich with server message if available
            msg = ""
            try:
                body = response.json()
                msg = f" | server: {body}"
            except Exception:
                msg = f" | text: {response.text[:500]}"
            raise IRPAPIError(f"HTTP request failed: {e} {msg}") from e
        except requests.RequestException as e:
            raise IRPAPIError(f"Request error: {e}") from e

        return response


    def poll_workflow(
        self,
        workflow_url: str,
        interval: int = 10,
        timeout: int = 600000
    ) -> requests.Response:
        """
        Poll workflow until completion or timeout.

        Args:
            workflow_url: Full URL to workflow endpoint
            interval: Polling interval in seconds
            timeout: Maximum timeout in seconds

        Returns:
            Final workflow response

        Raises:
            IRPValidationError: If workflow_url is invalid
            IRPWorkflowError: If workflow times out
        """
        validate_non_empty_string(workflow_url, "workflow_url")
        
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling workflow url {workflow_url}")
            response = self.request('GET', '', full_url=workflow_url)
            workflow_data = response.json()
            status = workflow_data.get('status', '')
            progress = workflow_data.get('progress', '')
            print(f"Workflow status: {status}; Percent complete: {progress}")

            if status in WORKFLOW_COMPLETED_STATUSES:
                return response

            if time.time() - start > timeout:
                raise IRPWorkflowError(
                    f"Workflow did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)

    def poll_workflow_batch(
        self,
        workflow_ids: List[int],
        interval: int = 20,
        timeout: int = 600000
    ) -> requests.Response:
        """
        Poll multiple workflows until all complete or timeout.

        Args:
            workflow_ids: List of workflow IDs to poll
            interval: Polling interval in seconds
            timeout: Maximum timeout in seconds

        Returns:
            Response with all workflows combined

        Raises:
            IRPValidationError: If inputs are invalid
            IRPWorkflowError: If workflows time out
        """
        from .validators import validate_list_not_empty
        validate_list_not_empty(workflow_ids, "workflow_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling batch workflow ids: {','.join(str(item) for item in workflow_ids)}")

            # Fetch all workflows across all pages
            all_workflows = []
            offset = 0
            limit = 100

            while True:
                params = {
                    'ids': ','.join(str(item) for item in workflow_ids),
                    'limit': limit,
                    'offset': offset
                }
                response = self.request('GET', GET_WORKFLOWS, params=params)
                response_data = response.json()
                total_match_count = get_nested_field(
                    response_data, 'totalMatchCount',
                    required=True,
                    context="workflow batch response"
                )
                workflows = get_nested_field(
                    response_data, 'workflows',
                    default=[],
                    context="workflow batch response"
                )

                all_workflows.extend(workflows)

                # Check if we've fetched all workflows
                if len(all_workflows) >= total_match_count:
                    break

                # Move to next page
                offset += limit

            # Check if all workflows are completed
            all_completed = True
            for workflow in all_workflows:
                status = workflow.get('status', '')
                if status in WORKFLOW_IN_PROGRESS_STATUSES:
                    all_completed = False
                    break

            if all_completed:
                # Return the last response but with all workflows combined
                response_data['workflows'] = all_workflows
                return response

            if time.time() - start > timeout:
                raise IRPWorkflowError(
                    f"Batch workflows did not complete within {timeout} seconds"
                )
            time.sleep(interval)

    def execute_workflow(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Union[Dict[str, Any], List[Any]]] = None,
        headers: Dict[str, str] = {},
        timeout: Optional[int] = None,
        stream: bool = False
    ) -> requests.Response:
        """
        Execute workflow: submit request and poll until completion.

        This is a convenience method that combines request submission
        with automatic workflow polling.

        Args:
            method: HTTP method (POST, DELETE, etc.)
            path: API path
            params: Query parameters
            json: JSON request body
            headers: Additional headers
            timeout: Request timeout in seconds
            stream: Enable streaming response

        Returns:
            Final workflow response after completion

        Raises:
            IRPAPIError: If request fails
            IRPWorkflowError: If workflow times out
        """
        print("Submitting workflow request...")
        response = self.request(
            method, path,
            params=params,
            json=json,
            headers=headers,
            timeout=timeout,
            stream=stream
        )

        if response.status_code not in (201, 202):
            return response

        workflow_url = get_location_header(response)
        if not workflow_url:
            raise IRPAPIError(
                "Workflow submission succeeded but Location header is missing"
            )

        return self.poll_workflow(workflow_url)