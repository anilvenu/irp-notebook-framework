#!/usr/bin/env python3
"""
Generate set_02 demo data from real Excel configuration file.

This script uses the actual Excel configuration validators and transformers
to generate realistic demo data with all batch types and successful statuses.
"""

import sys
from pathlib import Path
import csv
import json
from datetime import datetime, timedelta

# Add workspace to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'workspace'))

from helpers.configuration import (
    validate_configuration_file,
    preview_transformer_jobs,
    get_transformer_list
)

# Constants
EXCEL_PATH = 'workspace/tests/files/valid_excel_configuration.xlsx'
OUTPUT_DIR = Path(__file__).parent / 'set_02' / 'csv_data'


def generate_configurations_csv():
    """Generate configurations.csv with real Excel config data"""
    print("Generating configurations.csv...")

    # Validate and load Excel configuration
    result = validate_configuration_file(EXCEL_PATH, cycle_id=None)
    config_data = result['configuration_data']

    # Write to CSV
    with open(OUTPUT_DIR / 'configurations.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'cycle_id', 'configuration_file_name', 'configuration_data', 'status', 'file_last_updated_ts', 'created_ts'])

        # Compact JSON for configuration_data
        config_json = json.dumps(config_data, separators=(',', ':'))
        writer.writerow([
            2,
            1,
            '/configs/2025-Q1-Full-Config.xlsx',
            config_json,
            'VALID',
            '2025-10-13 00:00:00',
            '2025-10-13 00:00:00'
        ])

    print(f"  Generated configurations.csv with config size: {len(config_json)} chars")
    return config_data


def generate_batches_and_configs(config_data):
    """Generate batches.csv, job_configurations.csv, and jobs.csv"""
    print("\nGenerating batch data...")

    # Get all transformers
    transformers = get_transformer_list(include_test=False)

    # Track data
    batches = []
    job_configs = []
    jobs = []

    batch_id = 201
    job_config_id = 1
    job_id = 1

    # Starting timestamp
    base_time = datetime(2025, 10, 14, 0, 0, 0)

    for transformer in transformers:
        print(f"\n  Processing {transformer}...")

        try:
            # Generate job configurations using the transformer
            transformer_jobs = preview_transformer_jobs(transformer, config_data)
            print(f"    Generated {len(transformer_jobs)} job configurations")

            # Create batch
            batch_created = base_time
            batch_submitted = batch_created + timedelta(hours=1)

            # Calculate completion time based on job count
            hours_to_complete = max(4, len(transformer_jobs) // 10)
            batch_completed = batch_submitted + timedelta(hours=hours_to_complete)

            batches.append({
                'id': batch_id,
                'step_id': 1,
                'configuration_id': 2,
                'batch_type': transformer,
                'status': 'COMPLETED',
                'created_ts': batch_created.strftime('%Y-%m-%d %H:%M:%S'),
                'submitted_ts': batch_submitted.strftime('%Y-%m-%d %H:%M:%S'),
                'completed_ts': batch_completed.strftime('%Y-%m-%d %H:%M:%S')
            })

            # Create job configurations and jobs
            for i, job_config_data in enumerate(transformer_jobs, 1):
                # Job configuration
                job_config_json = json.dumps(job_config_data, separators=(',', ':'))
                job_configs.append({
                    'id': job_config_id,
                    'batch_id': batch_id,
                    'configuration_id': 2,
                    'job_configuration_data': job_config_json,
                    'skipped': 'FALSE',
                    'overridden': 'FALSE',
                    'override_reason_txt': 'NULL',
                    'parent_job_configuration_id': 'NULL',
                    'skipped_reason_txt': 'NULL',
                    'override_job_configuration_id': 'NULL'
                })

                # Job
                job_created = batch_created + timedelta(minutes=i*2)
                job_submitted = batch_submitted
                job_completed = batch_submitted + timedelta(hours=(i % hours_to_complete))

                # Generate workflow ID
                prefix_map = {
                    'EDM Creation': 'EDM',
                    'Portfolio Creation': 'PF',
                    'MRI Import': 'MRI',
                    'Create Reinsurance Treaties': 'RT',
                    'EDM DB Upgrade': 'UPG',
                    'GeoHaz': 'GEO',
                    'Portfolio Mapping': 'MAP',
                    'Analysis': 'AN',
                    'Grouping': 'GRP',
                    'Export to RDM': 'RDM',
                    'Staging ETL': 'ETL'
                }
                prefix = prefix_map.get(transformer, 'MW')
                workflow_id = f"MW-{prefix}-{batch_id:03d}{i:03d}"

                jobs.append({
                    'id': job_id,
                    'batch_id': batch_id,
                    'job_configuration_id': job_config_id,
                    'moodys_workflow_id': workflow_id,
                    'status': 'FINISHED',
                    'skipped': 'FALSE',
                    'parent_job_id': 'NULL',
                    'created_ts': job_created.strftime('%Y-%m-%d %H:%M:%S'),
                    'submitted_ts': job_submitted.strftime('%Y-%m-%d %H:%M:%S'),
                    'completed_ts': job_completed.strftime('%Y-%m-%d %H:%M:%S')
                })

                job_config_id += 1
                job_id += 1

            # Move to next batch (advance time)
            base_time = batch_completed + timedelta(hours=1)
            batch_id += 1

        except Exception as e:
            print(f"    ERROR: {str(e)}")
            continue

    # Write CSVs
    print("\nWriting CSV files...")

    # Batches
    with open(OUTPUT_DIR / 'batches.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'step_id', 'configuration_id', 'batch_type', 'status', 'created_ts', 'submitted_ts', 'completed_ts'])
        writer.writeheader()
        writer.writerows(batches)
    print(f"  Written {len(batches)} batches")

    # Job Configurations
    with open(OUTPUT_DIR / 'job_configurations.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'batch_id', 'configuration_id', 'job_configuration_data', 'skipped', 'overridden', 'override_reason_txt', 'parent_job_configuration_id', 'skipped_reason_txt', 'override_job_configuration_id'])
        writer.writeheader()
        writer.writerows(job_configs)
    print(f"  Written {len(job_configs)} job configurations")

    # Jobs
    with open(OUTPUT_DIR / 'jobs.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'batch_id', 'job_configuration_id', 'moodys_workflow_id', 'status', 'skipped', 'parent_job_id', 'created_ts', 'submitted_ts', 'completed_ts'])
        writer.writeheader()
        writer.writerows(jobs)
    print(f"  Written {len(jobs)} jobs")


def main():
    """Main execution"""
    print("="*70)
    print("GENERATING SET_02 DATA FROM REAL EXCEL CONFIGURATION")
    print("="*70)
    print(f"Excel file: {EXCEL_PATH}")
    print(f"Output dir: {OUTPUT_DIR}")
    print("="*70)

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate configurations
    config_data = generate_configurations_csv()

    # Generate batches, job configs, and jobs
    generate_batches_and_configs(config_data)

    print("\n" + "="*70)
    print("SET_02 DATA GENERATION COMPLETE!")
    print("="*70)
    print("\nGenerated files:")
    for csv_file in OUTPUT_DIR.glob('*.csv'):
        if csv_file.name not in ['cycles.csv', 'stages.csv', 'steps.csv']:
            print(f"  - {csv_file.name}")


if __name__ == '__main__':
    main()
