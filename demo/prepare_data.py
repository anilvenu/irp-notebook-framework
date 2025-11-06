#!/usr/bin/env python3
"""
Data Preparation for Batch Viewer Demo

This script handles all database setup and test data loading:
1. Initializes demo schema with tables and views
2. Clears existing test data
3. Loads test data from CSV files

This must be run before generate_dashboards.py
"""

import sys
from pathlib import Path
import csv

# Add workspace to path so we can import helpers
workspace_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(workspace_path))

from helpers import database as db


SCHEMA = 'demo'


def initialize_schema():
    """Initialize demo schema with base tables and views"""
    print("\n" + "="*60)
    print("INITIALIZING DEMO SCHEMA")
    print("="*60)

    # Create schema
    try:
        db.execute_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        print(f"✓ Schema '{SCHEMA}' created/verified")
    except Exception as e:
        print(f"✗ Error creating schema: {e}")
        return False

    # Get paths to SQL files (use actual filesystem paths, not workspace_path from constants)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    init_sql = project_root / 'workspace' / 'helpers' / 'db' / 'init_database.sql'
    views_sql = project_root / 'workspace' / 'helpers' / 'db' / 'reporting_views.sql'

    # Execute initialization script
    try:
        print(f"Executing {init_sql}...")
        with open(init_sql, 'r') as f:
            sql_script = f.read()
        db.execute_command(sql_script, schema=SCHEMA)
        print(f"✓ Database tables created")
    except Exception as e:
        print(f"✗ Error executing init script: {e}")
        return False

    # Execute views script
    try:
        print(f"Executing {views_sql}...")
        with open(views_sql, 'r') as f:
            sql_script = f.read()
        db.execute_command(sql_script, schema=SCHEMA)
        print(f"✓ Reporting views created")
    except Exception as e:
        print(f"✗ Error executing views script: {e}")
        return False

    print("✓ Schema initialization complete")
    return True


def load_csv_data(csv_file, table_name, set_name='files'):
    """Load data from CSV file into table"""
    csv_path = Path(__file__).parent / set_name / csv_file

    if not csv_path.exists():
        print(f"✗ CSV file not found: {csv_file} in {set_name}")
        return False

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            print(f"⚠ No data in {csv_file}")
            return True

        # Get column names from first row
        columns = list(rows[0].keys())

        # Build INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # Prepare data tuples
        data_tuples = []
        jsonb_columns = []

        for row in rows:
            values = []
            for i, col in enumerate(columns):
                val = row[col]

                # Handle NULL values
                if val == '' or val.lower() == 'null':
                    values.append(None)
                # Handle boolean values
                elif val.lower() in ('true', 'false'):
                    values.append(val.lower() == 'true')
                # Handle JSONB columns (detect by column name or content)
                elif col.endswith('_data') or (val.startswith('{') and val.endswith('}')):
                    values.append(val)  # Keep as string, will be converted
                    if i not in jsonb_columns:
                        jsonb_columns.append(i)
                else:
                    values.append(val)

            data_tuples.append(tuple(values))

        # Bulk insert
        ids = db.bulk_insert(query, data_tuples, jsonb_columns=jsonb_columns if jsonb_columns else None, schema=SCHEMA)
        print(f"✓ Loaded {len(ids)} rows into {table_name}")
        return True

    except Exception as e:
        print(f"✗ Error loading {csv_file}: {e}")
        import traceback
        traceback.print_exc()
        return False


def clear_existing_data():
    """Clear existing test data"""
    print("\n" + "="*60)
    print("CLEARING EXISTING DATA")
    print("="*60)

    try:
        db.execute_command(
            "TRUNCATE irp_job, irp_job_configuration, irp_batch, irp_step, irp_stage, irp_configuration, irp_cycle CASCADE",
            schema=SCHEMA
        )
        print("✓ Existing data cleared")
        return True
    except Exception as e:
        print(f"✗ Error clearing data: {e}")
        return False


def load_test_data(set_name='files'):
    """Load all test data from CSV files"""
    print("\n" + "="*60)
    print(f"LOADING TEST DATA FROM {set_name.upper()}")
    print("="*60)

    # Load in dependency order
    tables = [
        ('csv_data/cycles.csv', 'irp_cycle'),
        ('csv_data/stages.csv', 'irp_stage'),
        ('csv_data/steps.csv', 'irp_step'),
        ('csv_data/configurations.csv', 'irp_configuration'),
        ('csv_data/batches.csv', 'irp_batch'),
        ('csv_data/job_configurations.csv', 'irp_job_configuration'),
        ('csv_data/jobs.csv', 'irp_job'),
    ]

    for csv_file, table_name in tables:
        if not load_csv_data(csv_file, table_name, set_name):
            return False

    print("\n✓ All test data loaded successfully")
    return True


def main():
    """Main execution"""
    # Get set name from command line argument (default to 'files' for backward compatibility)
    set_name = sys.argv[1] if len(sys.argv) > 1 else 'files'

    print("="*60)
    print(f"DATA PREPARATION FOR BATCH VIEWER DEMO - {set_name.upper()}")
    print("="*60)

    # Initialize schema
    if not initialize_schema():
        print("\n✗ Failed to initialize schema")
        return 1

    # Clear existing data
    if not clear_existing_data():
        print("\n✗ Failed to clear existing data")
        return 1

    # Load test data from CSV
    if not load_test_data(set_name):
        print("\n✗ Failed to load test data")
        return 1

    print("\n" + "="*60)
    print(f"DATA PREPARATION COMPLETE - {set_name.upper()}")
    print("="*60)
    print("✓ Schema initialized")
    print("✓ Test data loaded")
    print(f"\nYou can now run generate_dashboards.py {set_name}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
