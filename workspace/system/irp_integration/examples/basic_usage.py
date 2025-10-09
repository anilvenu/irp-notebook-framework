from system.irp_integration import IRPClient
import json, os

def get_file_size_kb(file_path: str):
    """
    Returns the size of a file in kilobytes.

    Args:
        file_path (str): The path to the file.

    Returns:
        float: The file size in kilobytes, or -1 if the file does not exist.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return -1

    file_size_bytes = os.path.getsize(file_path)
    file_size_kb = int(file_size_bytes / 1024)  # Convert bytes to kilobytes
    return file_size_kb

irp_client = IRPClient()

# Create EDM
edm_name = "basic_usage_edm"
print(json.dumps(irp_client.edm.create_edm(edm_name, "databridge-1"), indent=2))

# Create Portfolio
portfolio_name = "basic_usage_portfolio"
print(json.dumps(irp_client.portfolio.create_portfolio(edm_name, portfolio_name), indent=2))

# MRI Import
accounts_file_path = "workspace/system/irp_integration/examples/accounts.csv"
accounts_file_name = accounts_file_path.split("/")[-1]
accounts_bucket_response = irp_client.mri_import.create_aws_bucket()
accounts_credentials = irp_client.mri_import.get_file_credentials(accounts_bucket_response.headers['file_path'], accounts_file_name, get_file_size_kb(accounts_file_path), "account")
irp_client.mri_import.upload_file_to_s3(accounts_credentials, accounts_file_path)

locations_file_path = "workspace/system/irp_integration/examples/locations.csv"
locations_file_name = locations_file_path.split("/")[-1]
locations_bucket_response = irp_client.mri_import.create_aws_bucket()
locations_credentials = irp_client.mri_import.get_file_credentials(locations_bucket_response.headers['file_path'], locations_file_name, get_file_size_kb(locations_file_path), "account")
irp_client.mri_import.upload_file_to_s3(locations_credentials, locations_file_path)



print(json.dumps(irp_client.edm.delete_edm(edm_name), indent=2))