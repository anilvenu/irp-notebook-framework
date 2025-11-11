"""
MRI Import Manager for IRP Integration.

Handles Multi-Risk Insurance (MRI) data imports including file uploads
to AWS S3 and import execution via Moody's Risk Modeler API.
"""

from typing import Dict, Any, List, Optional
import boto3
from boto3.s3.transfer import TransferConfig
import requests
import json
import os
import time
from .client import Client
from .constants import (
    CREATE_AWS_BUCKET,
    CREATE_IMPORT_FOLDER,
    CREATE_IMPORT_JOB,
    CREATE_MAPPING,
    EXECUTE_IMPORT,
    GET_WORKFLOW_BY_ID,
    GET_WORKFLOWS,
    WORKFLOW_COMPLETED_STATUSES,
    WORKFLOW_IN_PROGRESS_STATUSES
)
from .exceptions import IRPFileError, IRPAPIError, IRPValidationError, IRPJobError
from .validators import (
    validate_non_empty_string,
    validate_file_exists,
    validate_positive_int,
    validate_list_not_empty
)
from .utils import decode_base64_field, decode_mri_credentials, decode_presign_params, extract_id_from_location_header, get_location_header, get_workspace_root


class MRIImportManager:
    """Manager for MRI import operations."""

    def __init__(self, client: Client, edm_manager: Optional[Any] = None, portfolio_manager: Optional[Any] = None):
        """
        Initialize MRI Import Manager.

        Args:
            client: Client instance for API requests
        """
        self.client = client
        self._edm_manager = edm_manager
        self._portfolio_manager = portfolio_manager

    @property
    def edm_manager(self):
        """Lazy-loaded edm manager to avoid circular imports."""
        if self._edm_manager is None:
            from .edm import EDMManager
            self._edm_manager = EDMManager(self.client)
        return self._edm_manager
    
    @property
    def portfolio_manager(self):
        """Lazy-loaded portfolio manager to avoid circular imports."""
        if self._portfolio_manager is None:
            from .portfolio import PortfolioManager
            self._portfolio_manager = PortfolioManager(self.client)
        return self._portfolio_manager


    def create_import_folder(
            self, 
            folder_type: str, 
            file_extension: str, 
            file_types: List[str]
    ) -> Dict[str, Any]:
        """
        Create an import folder for MRI files.

        Args:
            folder_type: Type of folder (e.g., 'MRI')
            file_extension: File extension (e.g., '.csv')
            file_types: List of file types (e.g., ['account', 'location'])

        Returns:
            Dict containing created folder details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If folder creation fails
        """
        validate_non_empty_string(folder_type, "folder_type")
        validate_non_empty_string(file_extension, "file_extension")
        validate_list_not_empty(file_types, "file_types")
        data = {
            "folderType": folder_type,
            "properties": {
                "fileExtension": file_extension,
                "fileTypes": file_types
            }
        }
        try:
            response = self.client.request('POST', CREATE_IMPORT_FOLDER, json=data)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to create import folder: {e}")


    def initialize_s3_session(self, presign_params: Dict[str, Any]) -> boto3.Session:
        """
        Initialize AWS S3 session using temporary credentials.

        Args:
            presign_params: Presigned parameters with AWS credentials

        Returns:
            Boto3 Session object

        Raises:
            IRPValidationError: If presign_params are invalid
            IRPFileError: If session creation fails
        """
        required_fields = ['aws_access_key_id', 'aws_secret_access_key', 'aws_session_token', 's3_region']
        missing = [f for f in required_fields if f not in presign_params]
        if missing:
            raise IRPValidationError(
                f"presign_params missing required fields: {', '.join(missing)}"
            )

        try:
            session = boto3.Session(
                aws_access_key_id=presign_params['aws_access_key_id'],
                aws_secret_access_key=presign_params['aws_secret_access_key'],
                aws_session_token=presign_params['aws_session_token'],
                region_name=presign_params['s3_region']
            )
            return session
        except Exception as e:
            raise IRPFileError(f"Failed to create AWS session: {e}")


    # def upload_file_to_s3(self, 
    #                        file_data: Dict[str, Any],
    #                        file_name: str,
    #                        file_id: str
    # ) -> None:
    #     """
    #     Upload MRI files to S3 bucket.

    #     Args:
    #         file_data: Dict containing presign parameters and upload URL
    #         file_name: Name of the local file to upload

    #     Raises:
    #         IRPValidationError: If parameters are invalid
    #         IRPFileError: If file upload fails
    #     """
    #     validate_non_empty_string(file_name, "file_name")

    #     try:
    #         decoded_presign_params = decode_presign_params(file_data['presignParams'])
    #         s3_client = self.initialize_s3_session(decoded_presign_params).client("s3")
    #     except Exception as e:
    #         raise IRPFileError(f"Failed to create S3 client: {e}")
        
    #     # Parse S3 path
    #     s3_path_parts = decoded_presign_params['s3_path'].split('/', 1)
    #     bucket = s3_path_parts[0]
    #     prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ""
    #     key = f"{prefix}/{file_id}-{file_name}"
    #     print(key)
    #     try:
    #         workspace_root = get_workspace_root()
    #         working_files_dir = workspace_root / "workflows" / "_Tools" / "files" / "working_files"
    #         file_path = os.path.join(working_files_dir, file_name)
    #         with open(file_path, 'rb') as file:
    #             s3_client.put_object(
    #                 Bucket=bucket,
    #                 Key=key,
    #                 Body=file,
    #                 ContentType='text/csv'
    #             )
    #     except FileNotFoundError:
    #         raise IRPFileError(f"File not found: {file_name}")
    #     except Exception as e:
    #         raise IRPFileError(f"Failed to upload file to S3: {e}")
        

    def create_import_job(self, file_data: Dict[str, Any], portfolio_uri: str) -> int:
        """
        Create MRI import job after files are uploaded.

        Args:
            file_data: Dict containing import job parameters
            
        Returns:
            Int of the job ID
            
        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If import job creation fails
        """

        data = {
            "importType": "MRI",
            "resourceUri": portfolio_uri,
            "settings": {
                "import files": {
                    "accountsFileId": file_data['uploadDetails']['accountsFile']['fileUri'].split('/')[-1],
                    "locationsFileId": file_data['uploadDetails']['locationsFile']['fileUri'].split('/')[-1],
                    # "mappingFileId": file_data['uploadDetails']['mappingFile']['fileUri'].split('-')[-1]
                },
                "folderId": file_data['folderId'],
                "currency": "USD",
                "delimeter": "COMMA",
                "skipLines": 1,
                "appendLocations": False,
                "geoHaz": False
            }
        }

        try:
            response = self.client.request('POST', CREATE_IMPORT_JOB, json=data)
            job_id = extract_id_from_location_header(response, "MRI import job creation")
            return int(job_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to create MRI import job: {e}")

    def create_aws_bucket(self) -> requests.Response:
        """
        Create an AWS S3 bucket for file uploads.

        Returns:
            Response with Location header containing bucket URL

        Raises:
            IRPAPIError: If bucket creation fails
        """
        return self.client.request('POST', CREATE_AWS_BUCKET)

    def get_file_credentials(
        self,
        bucket_url: str,
        filename: str,
        filesize: int,
        file_type: str
    ) -> Dict[str, Any]:
        """
        Get temporary AWS credentials for file upload.

        Args:
            bucket_url: Bucket URL from create_aws_bucket response
            filename: Name of file to upload
            filesize: File size in kilobytes
            file_type: Type of file ('account' or 'location')

        Returns:
            Dict with decoded credentials including:
                - filename: str
                - file_id: str
                - aws_access_key_id: str
                - aws_secret_access_key: str
                - aws_session_token: str
                - s3_path: str
                - s3_region: str

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If credential request fails or response is malformed
        """
        validate_non_empty_string(bucket_url, "bucket_url")
        validate_non_empty_string(filename, "filename")
        validate_positive_int(filesize, "filesize")
        validate_non_empty_string(file_type, "file_type")

        data = {
            "fileName": filename,
            "fileSize": filesize,
            "fileType": file_type
        }

        try:
            response = self.client.request('POST', 'path', base_url=bucket_url, json=data)
            response_json = response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get file credentials: {e}")

        # Extract file ID from location header
        file_id = extract_id_from_location_header(response)

        # Decode credentials
        decoded_creds = decode_mri_credentials(response_json)

        return {
            'filename': filename,
            'file_id': file_id,
            **decoded_creds
        }

    def upload_file_to_s3(self, credentials: Dict[str, str], file_path: str) -> None:
        """
        Upload file to S3 using temporary credentials.

        Args:
            credentials: Credentials dict from get_file_credentials
            file_path: Path to file to upload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If file upload fails
        """
        validate_file_exists(file_path, "file_path")

        # Validate required credential fields
        required_fields = ['aws_access_key_id', 'aws_secret_access_key',
                          'aws_session_token', 's3_region', 's3_path',
                          'file_id', 'filename']
        missing = [f for f in required_fields if f not in credentials]
        if missing:
            raise IRPValidationError(
                f"credentials missing required fields: {', '.join(missing)}"
            )

        try:
            print(f'Uploading file {file_path} to s3...')
            session = boto3.Session(
                aws_access_key_id=credentials['aws_access_key_id'],
                aws_secret_access_key=credentials['aws_secret_access_key'],
                aws_session_token=credentials['aws_session_token'],
                region_name=credentials['s3_region']
            )
            s3 = session.client("s3")

            # Parse S3 path
            s3_path_parts = credentials['s3_path'].split('/', 1)
            bucket = s3_path_parts[0]
            prefix = s3_path_parts[1] if len(s3_path_parts) > 1 else ""
            key = f"{prefix}/{credentials['file_id']}-{credentials['filename']}"

            # Configure transfer settings for optimized multipart uploads
            # Automatically handles multipart uploads for files > 8MB
            config = TransferConfig(
                multipart_threshold=8 * 1024 * 1024,  # 8MB threshold
                max_concurrency=10,                    # 10 concurrent threads
                multipart_chunksize=8 * 1024 * 1024,   # 8MB chunks
                use_threads=True
            )

            # Use upload_file for automatic multipart handling and better performance
            s3.upload_file(
                file_path,
                bucket,
                key,
                ExtraArgs={'ContentType': 'text/csv'},
                Config=config
            )
            print('File uploaded!')
        except FileNotFoundError:
            raise IRPFileError(f"File not found: {file_path}")
        except Exception as e:
            raise IRPFileError(f"Failed to upload file to S3: {e}")

    def upload_mapping_file(self, file_path: str, bucket_id: str) -> requests.Response:
        """
        Upload MRI mapping file to bucket.

        Args:
            file_path: Path to JSON mapping file
            bucket_id: Bucket ID from create_aws_bucket

        Returns:
            Response from mapping file upload

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If file cannot be read or is invalid JSON
            IRPAPIError: If upload fails
        """
        validate_file_exists(file_path, "file_path")
        validate_non_empty_string(bucket_id, "bucket_id")

        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            raise IRPFileError(f"Mapping file not found: {file_path}")
        except json.JSONDecodeError as e:
            raise IRPFileError(
                f"Invalid JSON in mapping file '{file_path}': {e}"
            )
        except Exception as e:
            raise IRPFileError(
                f"Failed to read mapping file '{file_path}': {e}"
            )

        try:
            return self.client.request(
                'POST',
                CREATE_MAPPING.format(bucket_id=bucket_id),
                json=data
            )
        except Exception as e:
            raise IRPAPIError(f"Failed to upload mapping file: {e}")

    def get_import_job(self, workflow_id: int) -> Dict[str, Any]:
        """
        Retrieve MRI import workflow status by workflow ID.

        Args:
            workflow_id: Workflow ID

        Returns:
            Dict containing workflow status details

        Raises:
            IRPValidationError: If workflow_id is invalid
            IRPAPIError: If request fails
        """
        validate_positive_int(workflow_id, "workflow_id")

        try:
            response = self.client.request('GET', GET_WORKFLOW_BY_ID.format(workflow_id=workflow_id))
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to get import job status for workflow ID {workflow_id}: {e}")

    def submit_import_job(
        self,
        edm_name: str,
        portfolio_id: int,
        bucket_id: int,
        accounts_file_id: int,
        locations_file_id: int,
        mapping_file_id: int,
        delimiter: str = "COMMA",
        skip_lines: int = 1,
        currency: str = "USD",
        append_locations: bool = False
    ) -> int:
        """
        Submit MRI import job without polling (returns immediately).

        This method submits the import workflow after files have been uploaded.
        Use poll_import_job_to_completion() to track the workflow to completion.
        For a complete end-to-end import with polling, use import_from_files() instead.

        Args:
            edm_name: Target EDM name
            portfolio_id: Target portfolio ID
            bucket_id: AWS bucket ID
            accounts_file_id: Uploaded accounts file ID
            locations_file_id: Uploaded locations file ID
            mapping_file_id: Uploaded mapping file ID
            delimiter: File delimiter (default: "COMMA")
            skip_lines: Number of header lines to skip (default: 1)
            currency: Currency code (default: "USD")
            append_locations: Append to existing locations (default: False)

        Returns:
            Workflow ID (int)

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If import submission fails
        """
        validate_non_empty_string(edm_name, "edm_name")
        validate_positive_int(portfolio_id, "portfolio_id")
        validate_positive_int(bucket_id, "bucket_id")
        validate_positive_int(accounts_file_id, "accounts_file_id")
        validate_positive_int(locations_file_id, "locations_file_id")
        validate_positive_int(mapping_file_id, "mapping_file_id")

        data = {
            "importType": "MRI",
            "bucketId": bucket_id,
            "dataSourceName": edm_name,
            "accountsFileId": accounts_file_id,
            "locationsFileId": locations_file_id,
            "mappingFileId": mapping_file_id,
            "delimiter": delimiter,
            "skipLines": skip_lines,
            "currency": currency,
            "portfolioId": portfolio_id,
            "appendLocations": append_locations
        }

        try:
            response = self.client.request('POST', EXECUTE_IMPORT, json=data)
            workflow_id = extract_id_from_location_header(response, "MRI import submission")
            return int(workflow_id)
        except Exception as e:
            raise IRPAPIError(f"Failed to submit MRI import: {e}")

    def poll_import_job_to_completion(
        self,
        workflow_id: int,
        interval: int = 10,
        timeout: int = 600000
    ) -> Dict[str, Any]:
        """
        Poll MRI import workflow until completion or timeout.

        Args:
            workflow_id: Workflow ID
            interval: Polling interval in seconds (default: 10)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            Final workflow status details

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If workflow times out
            IRPAPIError: If polling fails
        """
        validate_positive_int(workflow_id, "workflow_id")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling import workflow ID {workflow_id}")
            workflow_data = self.get_import_job(workflow_id)
            try:
                status = workflow_data['status']
                progress = workflow_data['progress']
            except (KeyError, TypeError) as e:
                raise IRPAPIError(
                    f"Missing 'status' or 'progress' in workflow response for workflow ID {workflow_id}: {e}"
                ) from e
            print(f"Workflow status: {status}; Percent complete: {progress}")
            if status in WORKFLOW_COMPLETED_STATUSES:
                return workflow_data

            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Import workflow ID {workflow_id} did not complete within {timeout} seconds. Last status: {status}"
                )
            time.sleep(interval)

    def submit_import_jobs(self, import_data_list: List[Dict[str, Any]]) -> List[int]:
        """
        Submit multiple MRI import jobs (without polling).

        Each import must have files already uploaded to S3.

        Args:
            import_data_list: List of import data dicts, each containing:
                - edm_name: str
                - portfolio_id: int
                - bucket_id: int
                - accounts_file_id: int
                - locations_file_id: int
                - mapping_file_id: int
                - delimiter: str (optional, default: "COMMA")
                - skip_lines: int (optional, default: 1)
                - currency: str (optional, default: "USD")
                - append_locations: bool (optional, default: False)

        Returns:
            List of workflow IDs

        Raises:
            IRPValidationError: If inputs are invalid
            IRPAPIError: If any submission fails
        """
        validate_list_not_empty(import_data_list, "import_data_list")

        workflow_ids = []
        for import_data in import_data_list:
            workflow_id = self.submit_import_job(
                edm_name=import_data['edm_name'],
                portfolio_id=import_data['portfolio_id'],
                bucket_id=import_data['bucket_id'],
                accounts_file_id=import_data['accounts_file_id'],
                locations_file_id=import_data['locations_file_id'],
                mapping_file_id=import_data['mapping_file_id'],
                delimiter=import_data.get('delimiter', 'COMMA'),
                skip_lines=import_data.get('skip_lines', 1),
                currency=import_data.get('currency', 'USD'),
                append_locations=import_data.get('append_locations', False)
            )
            workflow_ids.append(workflow_id)

        return workflow_ids

    def poll_import_job_batch_to_completion(
        self,
        workflow_ids: List[int],
        interval: int = 20,
        timeout: int = 600000
    ) -> List[Dict[str, Any]]:
        """
        Poll multiple MRI import workflows until all complete or timeout.

        Args:
            workflow_ids: List of workflow IDs
            interval: Polling interval in seconds (default: 20)
            timeout: Maximum timeout in seconds (default: 600000)

        Returns:
            List of final workflow status details for all workflows

        Raises:
            IRPValidationError: If parameters are invalid
            IRPJobError: If workflows time out
            IRPAPIError: If polling fails
        """
        validate_list_not_empty(workflow_ids, "workflow_ids")
        validate_positive_int(interval, "interval")
        validate_positive_int(timeout, "timeout")

        start = time.time()
        while True:
            print(f"Polling batch import workflow ids: {','.join(str(item) for item in workflow_ids)}")

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
                response = self.client.request('GET', GET_WORKFLOWS, params=params)
                response_data = response.json()

                try:
                    total_match_count = response_data['totalMatchCount']
                except (KeyError, TypeError) as e:
                    raise IRPAPIError(
                        f"Missing 'totalMatchCount' in workflow batch response: {e}"
                    ) from e

                workflows = response_data.get('workflows', [])
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
                return all_workflows

            if time.time() - start > timeout:
                raise IRPJobError(
                    f"Batch import workflows did not complete within {timeout} seconds"
                )
            time.sleep(interval)

    def execute_mri_import(
        self,
        edm_name: str,
        portfolio_id: int,
        bucket_id: int,
        accounts_file_id: int,
        locations_file_id: int,
        mapping_file_id: int,
        delimiter: str = "COMMA",
        skip_lines: int = 1,
        currency: str = "USD",
        append_locations: bool = False
    ) -> Dict[str, Any]:
        """
        Execute MRI import workflow (legacy method - submits and polls to completion).

        DEPRECATED: This method is maintained for backward compatibility.
        For new code, use submit_import_job() and poll_import_job_to_completion() separately.
        For a complete end-to-end import with file handling, use import_from_files() instead.

        Args:
            edm_name: Target EDM name
            portfolio_id: Target portfolio ID
            bucket_id: AWS bucket ID
            accounts_file_id: Uploaded accounts file ID
            locations_file_id: Uploaded locations file ID
            mapping_file_id: Uploaded mapping file ID
            delimiter: File delimiter (default: "COMMA")
            skip_lines: Number of header lines to skip (default: 1)
            currency: Currency code (default: "USD")
            append_locations: Append to existing locations (default: False)

        Returns:
            Import workflow response with status and summary

        Raises:
            IRPValidationError: If parameters are invalid
            IRPAPIError: If import submission fails
            IRPJobError: If workflow times out
        """
        # Submit the job
        workflow_id = self.submit_import_job(
            edm_name=edm_name,
            portfolio_id=portfolio_id,
            bucket_id=bucket_id,
            accounts_file_id=accounts_file_id,
            locations_file_id=locations_file_id,
            mapping_file_id=mapping_file_id,
            delimiter=delimiter,
            skip_lines=skip_lines,
            currency=currency,
            append_locations=append_locations
        )

        # Poll to completion (maintains backward compatibility)
        return self.poll_import_job_to_completion(workflow_id)

    def import_from_files(
        self,
        edm_name: str,
        portfolio_name: str,
        accounts_file: str,
        locations_file: str,
        mapping_file: str,
        delimiter: str = "COMMA",
        skip_lines: int = 1,
        currency: str = "USD",
        append_locations: bool = False
    ) -> Dict[str, Any]:
        """
        Import MRI data from files in one call (convenience method).

        This method handles the entire import process:
        1. Create AWS bucket
        2. Get file credentials for accounts and locations
        3. Upload files to S3
        4. Upload mapping file
        5. Submit and poll import workflow

        Args:
            edm_name: Target EDM name
            portfolio_id: Target portfolio ID
            accounts_file: Accounts file name (or full path)
            locations_file: Locations file name (or full path)
            mapping_file: Mapping file name (or full path)
            delimiter: File delimiter (default: "COMMA")
            skip_lines: Number of header lines to skip (default: 1)
            currency: Currency code (default: "USD")
            append_locations: Append to existing locations (default: False)

        Returns:
            Import workflow response with status and summary

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If files cannot be found or uploaded
            IRPAPIError: If any API call fails
        """
        # Validate inputs
        validate_non_empty_string(edm_name, "edm_name")
        validate_non_empty_string(portfolio_name, "portfolio_name")
        validate_non_empty_string(accounts_file, "accounts_file")
        validate_non_empty_string(locations_file, "locations_file")
        validate_non_empty_string(mapping_file, "mapping_file")

        edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
        if (len(edms) != 1):
            raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
        try:
            exposure_id = edms[0]['exposureId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract exposure ID for EDM '{edm_name}': {e}"
            ) from e

        portfolios = self.portfolio_manager.search_portfolios(exposure_id=exposure_id, filter=f"portfolioName=\"{portfolio_name}\"")
        if (len(portfolios) == 0):
            raise IRPAPIError(f"Portfolio with name {portfolio_name} not found")
        if (len(portfolios) > 1):
            raise IRPAPIError(f"{len(portfolios)} portfolios found with name {portfolio_name}, please use a unique name")
        try:
            portfolio_id = portfolios[0]['portfolioId']
        except (KeyError, IndexError, TypeError) as e:
            raise IRPAPIError(
                f"Failed to extract portfolio ID for portfolio '{portfolio_name}': {e}"
            ) from e

        # Use get_workspace_root() to get correct path in both VS Code and JupyterLab
        workspace_root = get_workspace_root()
        working_files_dir = str(workspace_root / "workflows" / "_Tools" / "files" / "working_files")

        accounts_file_path = os.path.join(working_files_dir, accounts_file)
        locations_file_path = os.path.join(working_files_dir, locations_file)
        mapping_file_path = os.path.join(working_files_dir, mapping_file)

        # Validate files exist
        validate_file_exists(accounts_file_path, "accounts_file")
        validate_file_exists(locations_file_path, "locations_file")
        validate_file_exists(mapping_file_path, "mapping_file")

        # Get file sizes
        accounts_size_kb = self.get_file_size_kb(accounts_file_path)
        locations_size_kb = self.get_file_size_kb(locations_file_path)

        if accounts_size_kb < 0 or locations_size_kb < 0:
            raise IRPFileError("Failed to determine file sizes")

        # Create AWS bucket
        print('Creating AWS bucket...')
        bucket_response = self.create_aws_bucket()
        print('AWS bucket created!')
        bucket_url = get_location_header(bucket_response, "AWS bucket creation response")
        bucket_id = extract_id_from_location_header(bucket_response, "AWS bucket creation response")

        # Upload accounts file
        accounts_credentials = self.get_file_credentials(
            bucket_url,
            os.path.basename(accounts_file),
            accounts_size_kb,
            "account"
        )
        print('Access credentials received')
        self.upload_file_to_s3(accounts_credentials, accounts_file_path)

        # Upload locations file
        locations_credentials = self.get_file_credentials(
            bucket_url,
            os.path.basename(locations_file),
            locations_size_kb,
            "location"
        )
        self.upload_file_to_s3(locations_credentials, locations_file_path)

        # Upload mapping file
        mapping_response = self.upload_mapping_file(mapping_file_path, bucket_id)
        mapping_file_id = mapping_response.json()

        # Submit MRI import (without polling)
        workflow_id = self.submit_import_job(
            edm_name,
            int(portfolio_id),
            int(bucket_id),
            int(accounts_credentials['file_id']),
            int(locations_credentials['file_id']),
            mapping_file_id,
            delimiter=delimiter,
            skip_lines=skip_lines,
            currency=currency,
            append_locations=append_locations
        )

        # Poll workflow to completion
        return self.poll_import_job_to_completion(workflow_id)

    def import_from_files_batch(
        self,
        import_requests: List[Dict[str, Any]],
        delimiter: str = "COMMA",
        skip_lines: int = 1,
        currency: str = "USD",
        append_locations: bool = False
    ) -> List[int]:
        """
        Import MRI data from multiple file sets (batch submission without polling).

        This method handles file preparation and submission for multiple imports,
        returning workflow IDs that can be polled separately using
        poll_import_job_batch_to_completion().

        Args:
            import_requests: List of import request dicts, each containing:
                - edm_name: str - Target EDM name
                - portfolio_name: str - Target portfolio name
                - accounts_file_name: str - Accounts file name
                - locations_file_name: str - Locations file name
                - mapping_file: str - Mapping file name
                - delimiter: str (optional, default from method parameter)
                - skip_lines: int (optional, default from method parameter)
                - currency: str (optional, default from method parameter)
                - append_locations: bool (optional, default from method parameter)
            delimiter: Default file delimiter (default: "COMMA")
            skip_lines: Default number of header lines to skip (default: 1)
            currency: Default currency code (default: "USD")
            append_locations: Default append to existing locations (default: False)

        Returns:
            List of workflow IDs (can be passed to poll_import_job_batch_to_completion)

        Raises:
            IRPValidationError: If parameters are invalid
            IRPFileError: If files cannot be found or uploaded
            IRPAPIError: If any API call fails

        Example:
            ```python
            import_requests = [
                {
                    "edm_name": "RM_EDM_202511_Quarterly_USFL",
                    "portfolio_name": "USFL_Other",
                    "accounts_file_name": "Modeling_202511_Moodys_Quarterly_OtherFld_Account.csv",
                    "locations_file_name": "Modeling_202511_Moodys_Quarterly_OtherFld_Location.csv",
                    "mapping_file": "mapping.json"
                },
                # ... more imports
            ]

            # Submit all imports
            workflow_ids = mri_manager.import_from_files_batch(import_requests)

            # Poll all to completion
            results = mri_manager.poll_import_job_batch_to_completion(workflow_ids)
            ```
        """
        validate_list_not_empty(import_requests, "import_requests")

        workflow_ids = []

        for idx, import_request in enumerate(import_requests):
            print(f"\n=== Processing import {idx + 1}/{len(import_requests)} ===")

            # Extract parameters
            edm_name = import_request['edm_name']
            portfolio_name = import_request['portfolio_name']
            accounts_file = import_request['accounts_file_name']
            locations_file = import_request['locations_file_name']
            mapping_file = import_request['mapping_file']

            # Use request-specific overrides or defaults
            req_delimiter = import_request.get('delimiter', delimiter)
            req_skip_lines = import_request.get('skip_lines', skip_lines)
            req_currency = import_request.get('currency', currency)
            req_append_locations = import_request.get('append_locations', append_locations)

            # Validate inputs
            validate_non_empty_string(edm_name, f"import_requests[{idx}].edm_name")
            validate_non_empty_string(portfolio_name, f"import_requests[{idx}].portfolio_name")
            validate_non_empty_string(accounts_file, f"import_requests[{idx}].accounts_file_name")
            validate_non_empty_string(locations_file, f"import_requests[{idx}].locations_file_name")
            validate_non_empty_string(mapping_file, f"import_requests[{idx}].mapping_file")

            # Lookup EDM
            print(f"Looking up EDM: {edm_name}")
            edms = self.edm_manager.search_edms(filter=f"exposureName=\"{edm_name}\"")
            if len(edms) != 1:
                raise IRPAPIError(f"Expected 1 EDM with name {edm_name}, found {len(edms)}")
            try:
                exposure_id = edms[0]['exposureId']
            except (KeyError, IndexError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract exposure ID for EDM '{edm_name}': {e}"
                ) from e

            # Lookup Portfolio
            print(f"Looking up portfolio: {portfolio_name}")
            portfolios = self.portfolio_manager.search_portfolios(
                exposure_id=exposure_id,
                filter=f"portfolioName=\"{portfolio_name}\""
            )
            if len(portfolios) == 0:
                raise IRPAPIError(f"Portfolio with name {portfolio_name} not found")
            if len(portfolios) > 1:
                raise IRPAPIError(
                    f"{len(portfolios)} portfolios found with name {portfolio_name}, please use a unique name"
                )
            try:
                portfolio_id = portfolios[0]['portfolioId']
            except (KeyError, IndexError, TypeError) as e:
                raise IRPAPIError(
                    f"Failed to extract portfolio ID for portfolio '{portfolio_name}': {e}"
                ) from e

            # Use get_workspace_root() to get correct path in both VS Code and JupyterLab
            workspace_root = get_workspace_root()
            working_files_dir = str(workspace_root / "workflows" / "_Tools" / "files" / "working_files")

            accounts_file_path = os.path.join(working_files_dir, accounts_file)
            locations_file_path = os.path.join(working_files_dir, locations_file)
            mapping_file_path = os.path.join(working_files_dir, mapping_file)

            # Validate files exist
            validate_file_exists(accounts_file_path, f"import_requests[{idx}].accounts_file_name")
            validate_file_exists(locations_file_path, f"import_requests[{idx}].locations_file_name")
            validate_file_exists(mapping_file_path, f"import_requests[{idx}].mapping_file")

            # Get file sizes
            accounts_size_kb = self.get_file_size_kb(accounts_file_path)
            locations_size_kb = self.get_file_size_kb(locations_file_path)

            if accounts_size_kb < 0 or locations_size_kb < 0:
                raise IRPFileError(f"Failed to determine file sizes for import {idx + 1}")

            # Create AWS bucket
            print('Creating AWS bucket...')
            bucket_response = self.create_aws_bucket()
            print('AWS bucket created!')
            bucket_url = get_location_header(bucket_response, "AWS bucket creation response")
            bucket_id = extract_id_from_location_header(bucket_response, "AWS bucket creation response")

            # Upload accounts file
            print(f'Uploading accounts file: {accounts_file}')
            accounts_credentials = self.get_file_credentials(
                bucket_url,
                os.path.basename(accounts_file),
                accounts_size_kb,
                "account"
            )
            print('Access credentials received')
            self.upload_file_to_s3(accounts_credentials, accounts_file_path)

            # Upload locations file
            print(f'Uploading locations file: {locations_file}')
            locations_credentials = self.get_file_credentials(
                bucket_url,
                os.path.basename(locations_file),
                locations_size_kb,
                "location"
            )
            self.upload_file_to_s3(locations_credentials, locations_file_path)

            # Upload mapping file
            print(f'Uploading mapping file: {mapping_file}')
            mapping_response = self.upload_mapping_file(mapping_file_path, bucket_id)
            mapping_file_id = mapping_response.json()

            # Submit MRI import (without polling)
            print(f'Submitting import job for {edm_name}/{portfolio_name}...')
            workflow_id = self.submit_import_job(
                edm_name,
                int(portfolio_id),
                int(bucket_id),
                int(accounts_credentials['file_id']),
                int(locations_credentials['file_id']),
                mapping_file_id,
                delimiter=req_delimiter,
                skip_lines=req_skip_lines,
                currency=req_currency,
                append_locations=req_append_locations
            )
            print(f'Import job submitted with workflow ID: {workflow_id}')
            workflow_ids.append(workflow_id)

        print(f"\n=== All {len(workflow_ids)} import jobs submitted ===")
        print(f"Workflow IDs: {workflow_ids}")
        return workflow_ids

    def get_file_size_kb(self, file_path: str) -> int:
        """
        Get file size in kilobytes.

        Args:
            file_path: Path to file

        Returns:
            File size in kilobytes, or -1 if file does not exist

        Note:
            This method returns -1 for backwards compatibility.
            Consider using validate_file_exists() instead for better error handling.
        """
        if not os.path.exists(file_path):
            return -1

        file_size_bytes = os.path.getsize(file_path)
        file_size_kb = int(file_size_bytes / 1024)
        return file_size_kb
