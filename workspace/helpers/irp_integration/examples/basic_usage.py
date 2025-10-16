from helpers.irp_integration import IRPClient
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
portfolio_response = irp_client.portfolio.create_portfolio(edm_name, portfolio_name)
portfolio_id = portfolio_response['id'].split('/')[-1]
print(json.dumps(portfolio_response, indent=2))


# MRI Import
current_dir = os.path.dirname(os.path.abspath(__file__))
working_files_dir = os.path.join(current_dir, "working_files")

bucket_response = irp_client.mri_import.create_aws_bucket()
bucket_url = bucket_response.headers['location']
bucket_id = bucket_url.split('/')[-1]

accounts_file_name = "accounts.csv"
accounts_file_path = os.path.join(working_files_dir, accounts_file_name)
accounts_credentials = irp_client.mri_import.get_file_credentials(bucket_url, accounts_file_name, get_file_size_kb(accounts_file_path), "account")
irp_client.mri_import.upload_file_to_s3(accounts_credentials, accounts_file_path)

locations_file_name = "locations.csv"
locations_file_path = os.path.join(working_files_dir, locations_file_name)
locations_credentials = irp_client.mri_import.get_file_credentials(bucket_url, locations_file_name, get_file_size_kb(locations_file_path), "location")
irp_client.mri_import.upload_file_to_s3(locations_credentials, locations_file_path)

mapping_file_name = "mapping.json"
mapping_file_path = os.path.join(working_files_dir, mapping_file_name)
mapping_file_id = irp_client.mri_import.upload_mapping_file(mapping_file_path, bucket_id).json()

import_response = irp_client.mri_import.execute_mri_import(
    edm_name,
    int(portfolio_id),
    int(bucket_id),
    accounts_credentials['file_id'],
    locations_credentials['file_id'],
    mapping_file_id
)
print(json.dumps(import_response, indent=2))

# Geocode
geocode_response = irp_client.portfolio.geohaz_portfolio(edm_name, portfolio_id)
print(json.dumps(geocode_response, indent=2))

# Analysis
model_profile_name = 'North Atlantic Hurricane 18.0 EP Distributed Wind Only (Tutorial)'
model_profile_response = irp_client.analysis.get_model_profiles_by_name([model_profile_name])
output_profile_name = 'Default'
output_profile_response = irp_client.analysis.get_output_profile_by_name(output_profile_name)
event_rate_scheme_name = 'RMS 2023 Stochastic Event Rates'
event_rate_scheme_response = irp_client.analysis.get_event_rate_scheme_by_name(event_rate_scheme_name)
if model_profile_response['count'] > 0 and len(output_profile_response) > 0 and event_rate_scheme_response['count'] > 0:
    analysis_response = irp_client.analysis.analyze_portfolio("basic_job",
                                                            'basic_usage_edm',
                                                            1,
                                                            model_profile_response['items'][0]['id'],
                                                            output_profile_response[0]['id'],
                                                            event_rate_scheme_response['items'][0]['eventRateSchemeId'])
    print(json.dumps(analysis_response, indent=2))



# print(json.dumps(irp_client.edm.delete_edm(edm_name), indent=2))