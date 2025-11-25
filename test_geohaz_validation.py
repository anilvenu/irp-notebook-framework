"""
Test script for GeoHaz threshold validation logic.

This script tests the validate_geohaz_thresholds function using example data
from the IRP_Integration_Demo.ipynb and example_config_json.json.
"""

import sys
import json
import pandas as pd
from pathlib import Path

# Add workspace to path
workspace_path = Path(__file__).parent / "workspace"
sys.path.insert(0, str(workspace_path))

from helpers.control_totals import validate_geohaz_thresholds, get_import_file_mapping_from_config


# Example geocoding results from IRP_Integration_Demo.ipynb
# This is the output from 3e_GeocodingSummary.sql
geocoding_data = [
    {
        "DBname": "RM_EDM_202511_Quarterly_USEQ_DamQ",
        "PORTNAME": "USEQ",
        "GeoResolutionCode": 0,
        "GeocodeDescription": "Null",
        "RiskCount": 1,
        "TIV": 121940.00,
        "TRV": 121940.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USEQ_DamQ",
        "PORTNAME": "USEQ",
        "GeoResolutionCode": 1,
        "GeocodeDescription": "Coordinate",
        "RiskCount": 482615,
        "TIV": 57104105908.00,
        "TRV": 57307871863.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USEQ_DamQ",
        "PORTNAME": "USEQ",
        "GeoResolutionCode": 2,
        "GeocodeDescription": "Street address",
        "RiskCount": 13617,
        "TIV": 1720289855.00,
        "TRV": 1724018076.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USEQ_DamQ",
        "PORTNAME": "USEQ",
        "GeoResolutionCode": 4,
        "GeocodeDescription": "Street Name",
        "RiskCount": 4198,
        "TIV": 557073685.00,
        "TRV": 557109685.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USEQ_DamQ",
        "PORTNAME": "USEQ",
        "GeoResolutionCode": 5,
        "GeocodeDescription": "Postcode",
        "RiskCount": 16328,
        "TIV": 1976786566.00,
        "TRV": 1977427426.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USEQ_DamQ",
        "PORTNAME": "USEQ",
        "GeoResolutionCode": 7,
        "GeocodeDescription": "City/Town",
        "RiskCount": 15,
        "TIV": 1609494.00,
        "TRV": 1609494.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Commercial",
        "GeoResolutionCode": 1,
        "GeocodeDescription": "Coordinate",
        "RiskCount": 10056,
        "TIV": 6209416082.00,
        "TRV": 30434768993.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Commercial",
        "GeoResolutionCode": 2,
        "GeocodeDescription": "Street address",
        "RiskCount": 3,
        "TIV": 3554300.00,
        "TRV": 2611502.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Excess",
        "GeoResolutionCode": 1,
        "GeocodeDescription": "Coordinate",
        "RiskCount": 190,
        "TIV": 481644628.00,
        "TRV": 2793669869.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Excess",
        "GeoResolutionCode": 2,
        "GeocodeDescription": "Street address",
        "RiskCount": 1,
        "TIV": 4900000.00,
        "TRV": 9200191.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Other",
        "GeoResolutionCode": 1,
        "GeocodeDescription": "Coordinate",
        "RiskCount": 222907,
        "TIV": 30210964756.00,
        "TRV": 31291609921.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Other",
        "GeoResolutionCode": 2,
        "GeocodeDescription": "Street address",
        "RiskCount": 6435,
        "TIV": 880300177.00,
        "TRV": 904828250.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Other",
        "GeoResolutionCode": 4,
        "GeocodeDescription": "Street Name",
        "RiskCount": 1853,
        "TIV": 255056729.00,
        "TRV": 256402499.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Other",
        "GeoResolutionCode": 5,
        "GeocodeDescription": "Postcode",
        "RiskCount": 7929,
        "TIV": 985256990.00,
        "TRV": 992606661.00
    },
    {
        "DBname": "RM_EDM_202511_Quarterly_USFL_LIjg",
        "PORTNAME": "USFL_Other",
        "GeoResolutionCode": 7,
        "GeocodeDescription": "City/Town",
        "RiskCount": 10,
        "TIV": 2146808.00,
        "TRV": 2146808.00
    }
]

# Load example configuration
config_file = Path(__file__).parent / "workspace" / "example_config_json.json"
with open(config_file, 'r') as f:
    configuration_data = json.load(f)

# Extract thresholds
geohaz_thresholds = configuration_data.get('GeoHaz Thresholds', [])

# Get mapping
import_file_mapping = get_import_file_mapping_from_config(configuration_data)

# Convert geocoding data to DataFrame
geocoding_df = pd.DataFrame(geocoding_data)

# Run validation
print("=" * 80)
print("GeoHaz Threshold Validation Test")
print("=" * 80)
print()

validation_results, all_passed = validate_geohaz_thresholds(
    geocoding_results=geocoding_df,
    geohaz_thresholds=geohaz_thresholds,
    import_file_mapping=import_file_mapping
)

# Display results
print(f"Total Checks: {len(validation_results)}")
print(f"Passed: {len(validation_results[validation_results['Status'] == 'PASS'])}")
print(f"Failed: {len(validation_results[validation_results['Status'] == 'FAIL'])}")
print()

# Show detailed results
print("Validation Results:")
print("=" * 80)
print(validation_results.to_string(index=False))
print()

# Show summary
if all_passed:
    print("ALL THRESHOLDS PASSED!")
else:
    print("SOME THRESHOLDS FAILED")
    print()
    failures = validation_results[validation_results['Status'] == 'FAIL']
    print(f"Failed Checks ({len(failures)}):")
    print(failures.to_string(index=False))

print()
print("=" * 80)

# Calculate some example percentages manually to verify
print("\nManual Verification for USEQ:")
print("-" * 40)
useq_results = geocoding_df[geocoding_df['PORTNAME'] == 'USEQ']
grand_total = useq_results['RiskCount'].sum()
print(f"Grand Total: {grand_total}")

for _, row in useq_results.iterrows():
    level = row['GeocodeDescription']
    count = row['RiskCount']
    pct = (count / grand_total) * 100
    print(f"{level:30s}: {count:8d} ({pct:6.2f}%)")

print()
print("Expected from config (USEQ):")
print("-" * 40)
useq_thresholds = [t for t in geohaz_thresholds if t['Import File'] == 'USEQ']
for threshold in useq_thresholds[:6]:  # Show first few
    level = threshold['Geocode Level']
    expected = threshold['% of Grand Total']
    tolerance = threshold['Threshold %']
    print(f"{level:30s}: {expected:6.2f}% ± {tolerance:4.1f}%")
