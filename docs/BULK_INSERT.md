# Bulk Insert Function - Usage Guide

This guide shows how to use the `bulk_insert()` function for efficient database insertions with JSONB support.

## Overview

The `bulk_insert()` function allows you to insert multiple records in a single database transaction. It's significantly faster than individual inserts and includes automatic JSONB conversion.

### Key Features

- **Transactional**: All inserts succeed or all fail (atomic operation)
- **JSONB Support**: Automatically converts Python dicts to JSON
- **ID Tracking**: Returns list of inserted IDs in order
- **Schema Aware**: Supports testing in separate schemas
- **Error Handling**: Proper rollback on failures

## Basic Usage

### Simple Bulk Insert

```python
from helpers.database import bulk_insert

# Prepare your data
query = """
    INSERT INTO irp_cycle (cycle_name, status, created_by)
    VALUES (%s, %s, %s)
"""

params_list = [
    ('Analysis-2025-Q1', 'ACTIVE', 'analyst1'),
    ('Analysis-2025-Q2', 'ACTIVE', 'analyst1'),
    ('Analysis-2025-Q3', 'ACTIVE', 'analyst2'),
]

# Insert all records
ids = bulk_insert(query, params_list)

print(f"Inserted {len(ids)} records with IDs: {ids}")
# Output: Inserted 3 records with IDs: [1, 2, 3]
```

## JSONB Support

### Inserting with Metadata (JSONB)

```python
from helpers.database import bulk_insert

query = """
    INSERT INTO irp_cycle (cycle_name, status, created_by, metadata)
    VALUES (%s, %s, %s, %s)
"""

params_list = [
    ('Analysis-2025-Q1', 'ACTIVE', 'analyst1', {
        'environment': 'production',
        'priority': 'high',
        'tags': ['quarterly', 'risk-analysis']
    }),
    ('Analysis-2025-Q2', 'ACTIVE', 'analyst1', {
        'environment': 'production',
        'priority': 'medium',
        'settings': {'auto_archive': True, 'notify': ['team@example.com']}
    }),
]

# Specify which column index contains JSONB data (0-based)
# Column 3 (4th parameter) is metadata
ids = bulk_insert(query, params_list, jsonb_columns=[3])

print(f"Inserted {len(ids)} cycles with metadata")
```

### Step Runs with Output Data

```python
from helpers.database import bulk_insert

# Assume you have step_id from a previous query
step_id = 42

query = """
    INSERT INTO irp_step_run (step_id, run_number, status, started_by, output_data)
    VALUES (%s, %s, %s, %s, %s)
"""

params_list = [
    (step_id, 1, 'COMPLETED', 'analyst1', {
        'records_processed': 10000,
        'execution_time': 125.5,
        'summary': {
            'success': 9950,
            'failed': 50,
            'warnings': ['Missing data in 50 rows']
        }
    }),
    (step_id, 2, 'COMPLETED', 'analyst1', {
        'records_processed': 10000,
        'execution_time': 118.2,
        'summary': {
            'success': 10000,
            'failed': 0,
            'warnings': []
        }
    }),
]

# Column 4 (5th parameter) contains output_data JSONB
ids = bulk_insert(query, params_list, jsonb_columns=[4])
```

### Bulk Insert Configurations

```python
from helpers.database import bulk_insert

# Assume you have batch_id from a previous query
batch_id = 10

query = """
    INSERT INTO irp_configuration (batch_id, config_name, config_data, skip)
    VALUES (%s, %s, %s, %s)
"""

params_list = [
    (batch_id, 'portfolio_A_config', {
        'portfolio': 'Portfolio_A',
        'start_date': '2024-01-01',
        'end_date': '2024-12-31',
        'parameters': {
            'risk_threshold': 0.95,
            'confidence_level': 0.99
        }
    }, False),
    (batch_id, 'portfolio_B_config', {
        'portfolio': 'Portfolio_B',
        'start_date': '2024-01-01',
        'end_date': '2024-12-31',
        'parameters': {
            'risk_threshold': 0.85,
            'confidence_level': 0.95
        }
    }, False),
]

# Column 2 (3rd parameter) contains config_data JSONB
ids = bulk_insert(query, params_list, jsonb_columns=[2])
```

## Multiple JSONB Columns

If you have multiple JSONB columns, specify all their indices:

```python
query = """
    INSERT INTO some_table (name, data1, data2, data3)
    VALUES (%s, %s, %s, %s)
"""

params_list = [
    ('record1', {'key': 'value1'}, 'text_field', {'nested': {'data': 123}}),
    ('record2', {'key': 'value2'}, 'text_field', {'nested': {'data': 456}}),
]

# Columns 1 and 3 (2nd and 4th parameters) are JSONB
ids = bulk_insert(query, params_list, jsonb_columns=[1, 3])
```

## Testing with Separate Schema

For testing, use the `schema` parameter to avoid affecting production data:

```python
from helpers.database import bulk_insert, init_database

# Initialize test schema
init_database(schema='test')

# Insert into test schema
query = "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)"
params_list = [('test_cycle_1', 'ACTIVE'), ('test_cycle_2', 'ACTIVE')]

# All inserts go to 'test' schema
ids = bulk_insert(query, params_list, schema='test')

# Production data remains untouched!
```

## Error Handling

### Transaction Rollback

All inserts are part of a single transaction. If any insert fails, **none** are committed:

```python
from helpers.database import bulk_insert, DatabaseError

query = "INSERT INTO irp_cycle (cycle_name, status) VALUES (%s, %s)"

params_list = [
    ('cycle_1', 'ACTIVE'),
    ('cycle_2', 'ACTIVE'),
    ('cycle_1', 'ACTIVE'),  # Duplicate! Will cause unique constraint violation
]

try:
    ids = bulk_insert(query, params_list)
except DatabaseError as e:
    print(f"Insert failed: {e}")
    # No records were inserted - transaction was rolled back
```

### Handling Errors

```python
from helpers.database import bulk_insert, DatabaseError

def safe_bulk_insert(query, params_list, **kwargs):
    """Wrapper with error handling"""
    try:
        ids = bulk_insert(query, params_list, **kwargs)
        print(f"✓ Successfully inserted {len(ids)} records")
        return ids
    except DatabaseError as e:
        print(f"✗ Bulk insert failed: {e}")
        # Handle error - maybe log, retry, or raise
        return []

# Usage
ids = safe_bulk_insert(query, params_list, jsonb_columns=[2])
```

## Performance Benefits

Bulk insert is significantly faster than individual inserts:

```python
import time
from helpers.database import bulk_insert, execute_insert

# Prepare 100 records
records = [(f'cycle_{i}', 'ACTIVE', 'user') for i in range(100)]

# Method 1: Individual inserts
start = time.time()
for cycle_name, status, user in records:
    execute_insert(
        "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
        (cycle_name, status, user)
    )
individual_time = time.time() - start

# Method 2: Bulk insert
start = time.time()
ids = bulk_insert(
    "INSERT INTO irp_cycle (cycle_name, status, created_by) VALUES (%s, %s, %s)",
    records
)
bulk_time = time.time() - start

print(f"Individual: {individual_time:.2f}s")
print(f"Bulk:       {bulk_time:.2f}s")
print(f"Speedup:    {individual_time/bulk_time:.1f}x faster")

# Typical output:
# Individual: 2.45s
# Bulk:       0.12s
# Speedup:    20.4x faster
```

## Complete Example: Creating a Batch with Configurations

```python
from helpers.database import bulk_insert, execute_insert

# Create batch
batch_id = execute_insert(
    "INSERT INTO irp_batch (step_id, batch_name, status) VALUES (%s, %s, %s)",
    (step_id, 'Q1_2025_Analysis', 'PENDING')
)

# Bulk insert configurations
config_query = """
    INSERT INTO irp_configuration (batch_id, config_name, config_data, skip)
    VALUES (%s, %s, %s, %s)
"""

portfolios = ['Portfolio_A', 'Portfolio_B', 'Portfolio_C', 'Portfolio_D']
configs = []

for portfolio in portfolios:
    config_data = {
        'portfolio': portfolio,
        'start_date': '2025-01-01',
        'end_date': '2025-03-31',
        'parameters': {
            'risk_threshold': 0.95,
            'analysis_type': 'quarterly'
        }
    }
    configs.append((batch_id, f'{portfolio}_Q1_2025', config_data, False))

# Insert all configurations at once
config_ids = bulk_insert(config_query, configs, jsonb_columns=[2])

print(f"Created batch {batch_id} with {len(config_ids)} configurations")
```

## Best Practices

1. **Use for Multiple Records**: Bulk insert is most beneficial with 10+ records
2. **Specify JSONB Columns**: Always specify `jsonb_columns` for JSONB fields
3. **Handle Errors**: Wrap in try/except to handle constraint violations
4. **Test First**: Use `schema='test'` to test without affecting production
5. **Batch Size**: For very large datasets (1000+ records), consider batching
6. **Validate Data**: Validate your data before bulk insert to avoid rollbacks

## API Reference

```python
def bulk_insert(
    query: str,
    params_list: List[tuple],
    jsonb_columns: List[int] = None,
    schema: str = 'public'
) -> List[int]:
    """
    Execute bulk INSERT and return list of new record IDs

    Args:
        query: SQL INSERT query with %s placeholders
        params_list: List of tuples, each containing parameters for one insert
        jsonb_columns: List of column indices (0-based) that contain JSONB data
        schema: Database schema to use (default: 'public')

    Returns:
        List of IDs for newly inserted records (in order)

    Raises:
        DatabaseError: If insert fails (entire transaction is rolled back)
    """
```

## Running Tests

To test the bulk_insert functionality:

```bash
# From project root
./run_tests.sh
```

See [workspace/tests/README.md](workspace/tests/README.md) for detailed test documentation.
