"""
MRI Import Manager for IRP Integration.

Handles Multi-Risk Insurance (MRI) data imports including file uploads
to AWS S3 and import execution via Moody's Risk Modeler API.
"""

from typing import Dict, Any, Optional
import boto3
import requests
import json
import os
from .client import Client
from .constants import CREATE_AWS_BUCKET, CREATE_MAPPING, EXECUTE_IMPORT
from .exceptions import IRPFileError, IRPAPIError, IRPValidationError
from .validators import (
    validate_non_empty_string,
    validate_file_exists,
    validate_positive_int
)
from .utils import decode_mri_credentials, extract_id_from_location_header, get_location_header


class MRIImportManager:
    """Manager for MRI import operations."""

    def __init__(self, client: Client):
        """
        Initialize MRI Import Manager.

        Args:
            client: Client instance for API requests
        """
        self.client = client

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

            with open(file_path, 'rb') as file:
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=file,
                    ContentType='text/csv'
                )
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
        Execute MRI import workflow (low-level method with file IDs).

        This method submits the import workflow after files have been uploaded.
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
            response = self.client.execute_workflow('POST', EXECUTE_IMPORT, json=data)
            return response.json()
        except Exception as e:
            raise IRPAPIError(f"Failed to submit MRI import: {e}")

    def import_from_files(
        self,
        edm_name: str,
        portfolio_id: int,
        accounts_file: str,
        locations_file: str,
        mapping_file: str,
        working_dir: Optional[str] = None,
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
            working_dir: Directory containing files (optional)
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
        validate_positive_int(portfolio_id, "portfolio_id")
        validate_non_empty_string(accounts_file, "accounts_file")
        validate_non_empty_string(locations_file, "locations_file")
        validate_non_empty_string(mapping_file, "mapping_file")

        # Resolve file paths
        if working_dir:
            accounts_file_path = os.path.join(working_dir, accounts_file)
            locations_file_path = os.path.join(working_dir, locations_file)
            mapping_file_path = os.path.join(working_dir, mapping_file)
        else:
            accounts_file_path = accounts_file
            locations_file_path = locations_file
            mapping_file_path = mapping_file

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
        bucket_response = self.create_aws_bucket()
        bucket_url = get_location_header(bucket_response, "AWS bucket creation response")
        bucket_id = extract_id_from_location_header(bucket_response, "AWS bucket creation response")

        # Upload accounts file
        accounts_credentials = self.get_file_credentials(
            bucket_url,
            os.path.basename(accounts_file),
            accounts_size_kb,
            "account"
        )
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

        # Submit MRI import
        return self.execute_mri_import(
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
