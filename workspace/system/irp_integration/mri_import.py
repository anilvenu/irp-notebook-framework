from .client import Client
import boto3, base64, requests, json

class MRIImportManager:
    def __init__(self, client: Client):
        self.client = client

    def create_aws_bucket(self) -> requests.Response:
        return self.client.request('POST', '/riskmodeler/v1/storage')
    
    def get_file_credentials(self, bucket_url: str, filename: str, filesize: int, type: str) -> dict:
        data = {
            "fileName": filename,
            "fileSize": filesize,
            "fileType": type
        }
        response = self.client.request('POST', 'path', base_url=bucket_url, json=data)
        response_json = response.json()

        credentials = {
            'filename': filename,
            'file_id': response.headers['location'].split('/')[-1],
            'aws_access_key_id': base64.b64decode(response_json['accessKeyId']).decode("utf-8"),
            'aws_secret_access_key': base64.b64decode(response_json['secretAccessKey']).decode("utf-8"),
            'aws_session_token': base64.b64decode(response_json['sessionToken']).decode("utf-8"),
            's3_path': base64.b64decode(response_json['s3Path']).decode("utf-8"),
            's3_region': base64.b64decode(response_json['s3Region']).decode("utf-8")
        }

        return credentials
    
    def upload_file_to_s3(self, credentials: dict, file_path: str) -> None:
        session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            aws_session_token=credentials['aws_session_token'],
            region_name=credentials['s3_region']
        )
        s3 = session.client("s3")

        try:
            with open(file_path, 'rb') as file:
                s3.put_object(
                    Bucket=credentials['s3_path'].split('/')[0],
                    Key=credentials['s3_path'].split('/', 1)[1] + f"/{credentials['file_id']}-{credentials['filename']}",
                    Body=file,
                    ContentType='text/csv'
                )
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def upload_mapping_file(self, file_path: str, bucket_id: str) -> requests.Response:
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from '{file_path}'. Ensure it's a valid JSON format.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        return self.client.request('POST', f"/riskmodeler/v1/imports/createmapping/{bucket_id}", json=data)    
    
    def execute_mri_import(self, edm_name: str, portfolio_id: int, bucket_id: int, accounts_file_id: int, locations_file_id: int, mapping_file_id: int):
        data = {
            "importType": "MRI",
            "bucketId": bucket_id,
            "dataSourceName": edm_name,
            "accountsFileId": accounts_file_id,
            "locationsFileId": locations_file_id,
            "mappingFileId": mapping_file_id,
            "delimiter": "COMMA",
            "skipLines": 1,
            "currency": "USD",
            "portfolioId": portfolio_id,
            "appendLocations": False
        }
        
        response = self.client.execute_workflow('POST', '/riskmodeler/v1/imports', json=data)
        return response.json()