# Configuration Transformers

## Overview

The `ConfigurationTransformer` class provides a flexible, extensible system for transforming configuration data into job configurations based on batch type. It uses a registry pattern that allows you to easily add custom transformation logic for different types of batch processing workflows.

## Architecture

### Registry Pattern

The transformer uses a class-level registry that maps batch type strings to transformation functions:

```python
ConfigurationTransformer._transformers = {
    'type': transform_function,
    'another_type': another_transform_function
    ...
}
```

This design allows for:
- **Decoupling**: Transformation logic is separate from the caller
- **Extensibility**: New types can be added without modifying existing code
- **Discoverability**: All registered types can be listed via `list_types()`

## Core API

### `get_job_configurations(batch_type, configuration)`

Transform a configuration dictionary into a list of job configurations.

**Parameters:**
- `batch_type` (str): The type of batch processing (must be registered)
- `configuration` (Dict[str, Any]): The input configuration dictionary

**Returns:**
- `List[Dict[str, Any]]`: List of job configuration dictionaries

**Raises:**
- `ConfigurationError`: If the batch type is not registered

**Example:**
```python
from helpers.configuration import ConfigurationTransformer

config = {
    'portfolio': 'PortfolioA',
    'start_date': '2024-01-01',
    'parameters': {'risk_level': 'high'}
}

jobs = ConfigurationTransformer.get_job_configurations('default', config)
# Returns: [{'portfolio': 'PortfolioA', 'start_date': '2024-01-01', ...}]
```

### `list_types()`

Get a list of all registered batch types.

**Returns:**
- `List[str]`: List of registered batch type names

**Example:**
```python
types = ConfigurationTransformer.list_types()
print(types)  # ['default', 'passthrough', 'multi_job', ...]
```

## Built-in Transformers

### `default`

Creates a single job configuration by copying the input config as-is.

**Use Case:** Simple batch jobs where the entire configuration applies to one job.

**Input:**
```python
config = {'param1': 'value1', 'param2': 100}
```

**Output:**
```python
[{'param1': 'value1', 'param2': 100}]
```

**Characteristics:**
- Returns a **copy** of the config (modifications won't affect the original)
- Always returns a list with one element

---

### `passthrough`

Returns the configuration unchanged (no copy).

**Use Case:** When you need the original config object (performance optimization, or when you want to preserve object identity).

**Input:**
```python
config = {'data': 'test'}
```

**Output:**
```python
[config]  # Same object, not a copy
```

**Characteristics:**
- Returns the **same object** (not a copy)
- Slightly faster than `default` (no deep copy overhead)

---

### `multi_job`

Creates multiple job configurations from a list, or falls back to a single job.

**Use Case:** Batch processing where one configuration describes multiple jobs.

**Input with jobs list:**
```python
config = {
    'batch_type': 'portfolio_batch',
    'jobs': [
        {'portfolio': 'A', 'param': 'x'},
        {'portfolio': 'B', 'param': 'y'},
        {'portfolio': 'C', 'param': 'z'}
    ]
}
```

**Output:**
```python
[
    {'portfolio': 'A', 'param': 'x'},
    {'portfolio': 'B', 'param': 'y'},
    {'portfolio': 'C', 'param': 'z'}
]
```

**Input without jobs list (fallback):**
```python
config = {'single_job': 'data'}
```

**Output:**
```python
[{'single_job': 'data'}]
```

**Characteristics:**
- Extracts and returns the `jobs` list if present
- Falls back to single job if no `jobs` key exists
- Flexible for both single and multi-job scenarios

## Creating Custom Transformers

### Using the `@register` Decorator

The recommended way to add a custom transformer is to use the `@register` decorator:

```python
from helpers.configuration import ConfigurationTransformer
from typing import Dict, Any, List

@ConfigurationTransformer.register('portfolio_batch')
def transform_portfolio_batch(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Create a job for each portfolio in the configuration.

    Expected config format:
    {
        'portfolios': ['PortfolioA', 'PortfolioB', ...],
        'start_date': '2024-01-01',
        'end_date': '2024-12-31',
        'parameters': {...}
    }
    """
    portfolios = config.get('portfolios', [])
    base_config = {k: v for k, v in config.items() if k != 'portfolios'}

    jobs = []
    for portfolio in portfolios:
        job_config = base_config.copy()
        job_config['portfolio'] = portfolio
        jobs.append(job_config)

    return jobs
```

### Usage of Custom Transformer

```python
config = {
    'portfolios': ['PortfolioA', 'PortfolioB', 'PortfolioC'],
    'start_date': '2024-01-01',
    'end_date': '2024-12-31',
    'parameters': {'risk_level': 'medium'}
}

jobs = ConfigurationTransformer.get_job_configurations('portfolio_batch', config)

# Result:
# [
#     {'portfolio': 'PortfolioA', 'start_date': '2024-01-01', ...},
#     {'portfolio': 'PortfolioB', 'start_date': '2024-01-01', ...},
#     {'portfolio': 'PortfolioC', 'start_date': '2024-01-01', ...}
# ]
```

## Advanced Examples

### Example 1: Date Range Splitting

Create a job for each month in a date range:

```python
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

@ConfigurationTransformer.register('monthly_split')
def transform_monthly_split(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Split a date range into monthly jobs.

    Expected config:
    {
        'start_date': '2024-01-01',
        'end_date': '2024-12-31',
        'other_params': {...}
    }
    """
    start = datetime.strptime(config['start_date'], '%Y-%m-%d')
    end = datetime.strptime(config['end_date'], '%Y-%m-%d')

    jobs = []
    current = start

    while current <= end:
        month_end = min(
            current + relativedelta(months=1) - timedelta(days=1),
            end
        )

        job_config = config.copy()
        job_config['start_date'] = current.strftime('%Y-%m-%d')
        job_config['end_date'] = month_end.strftime('%Y-%m-%d')
        job_config['month'] = current.strftime('%Y-%m')

        jobs.append(job_config)
        current = current + relativedelta(months=1)

    return jobs
```

### Example 2: Cartesian Product

Create jobs for all combinations of parameters:

```python
import itertools

@ConfigurationTransformer.register('cartesian_product')
def transform_cartesian_product(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Create jobs for all combinations of parameter values.

    Expected config:
    {
        'parameter_sets': {
            'portfolio': ['A', 'B', 'C'],
            'scenario': ['base', 'stress'],
            'model': ['model1', 'model2']
        },
        'common_config': {...}
    }
    """
    param_sets = config.get('parameter_sets', {})
    common_config = config.get('common_config', {})

    # Get all combinations
    keys = list(param_sets.keys())
    values = [param_sets[k] for k in keys]
    combinations = list(itertools.product(*values))

    jobs = []
    for combo in combinations:
        job_config = common_config.copy()
        for key, value in zip(keys, combo):
            job_config[key] = value
        jobs.append(job_config)

    return jobs
```

**Usage:**
```python
config = {
    'parameter_sets': {
        'portfolio': ['A', 'B'],
        'scenario': ['base', 'stress']
    },
    'common_config': {'threshold': 0.95}
}

jobs = ConfigurationTransformer.get_job_configurations('cartesian_product', config)

# Result: 4 jobs (2 portfolios × 2 scenarios)
# [
#     {'portfolio': 'A', 'scenario': 'base', 'threshold': 0.95},
#     {'portfolio': 'A', 'scenario': 'stress', 'threshold': 0.95},
#     {'portfolio': 'B', 'scenario': 'base', 'threshold': 0.95},
#     {'portfolio': 'B', 'scenario': 'stress', 'threshold': 0.95}
# ]
```

### Example 3: Configuration Table Split

Split rows from an Excel configuration table into individual jobs:

```python
@ConfigurationTransformer.register('table_split')
def transform_table_split(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Split configuration table rows into individual jobs.

    Expected config:
    {
        'tab_data': [
            {'col1': 'val1', 'col2': 'val2'},
            {'col1': 'val3', 'col2': 'val4'},
            ...
        ],
        'common_params': {...}
    }
    """
    tab_data = config.get('tab_data', [])
    common_params = config.get('common_params', {})

    jobs = []
    for row in tab_data:
        job_config = common_params.copy()
        job_config.update(row)
        jobs.append(job_config)

    return jobs
```

## Best Practices

### 1. **Naming Conventions**

Use descriptive, lowercase names with underscores:
- GOOD `portfolio_batch`, `monthly_split`, `table_split`
- BAD  `type1`, `MyTransformer`, `PORTFOLIO`

### 2. **Documentation**

Always include a docstring explaining:
- What the transformer does
- Expected input format
- Output format
- Any special requirements or edge cases

### 3. **Error Handling**

Validate input and provide helpful error messages:

```python
@ConfigurationTransformer.register('portfolio_batch')
def transform_portfolio_batch(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Create a job for each portfolio"""

    if 'portfolios' not in config:
        raise ConfigurationError(
            "portfolio_batch requires 'portfolios' key in configuration"
        )

    portfolios = config['portfolios']
    if not isinstance(portfolios, list) or len(portfolios) == 0:
        raise ConfigurationError(
            "portfolios must be a non-empty list"
        )

    # ... transformation logic ...
```

### 4. **Immutability**

Avoid modifying the input configuration:

```python
# Bad: Modifies input
def bad_transformer(config):
    config['processed'] = True  # Modifies input!
    return [config]

# Good: Creates copies
def good_transformer(config):
    job = config.copy()
    job['processed'] = True
    return [job]
```

### 5. **Testing**

Add tests for each custom transformer:

```python
def test_portfolio_batch_transformer():
    config = {
        'portfolios': ['A', 'B'],
        'param': 'value'
    }

    jobs = ConfigurationTransformer.get_job_configurations(
        'portfolio_batch',
        config
    )

    assert len(jobs) == 2
    assert jobs[0]['portfolio'] == 'A'
    assert jobs[1]['portfolio'] == 'B'
    assert all(j['param'] == 'value' for j in jobs)
```

## Integration with Batch Processing

### Typical Workflow

```python
from helpers.configuration import (
    ConfigurationTransformer,
    read_configuration
)
from helpers.database import bulk_insert

# 1. Read configuration from database
config_data = read_configuration(config_id)

# 2. Transform into job configurations
batch_type = 'portfolio_batch'  # Determined by batch metadata
job_configs = ConfigurationTransformer.get_job_configurations(
    batch_type,
    config_data['configuration_data']
)

# 3. Insert job configurations into database
query = """
    INSERT INTO irp_job_configuration
    (batch_id, configuration_id, job_configuration_data)
    VALUES (%s, %s, %s)
"""

params_list = [
    (batch_id, config_id, job_config)
    for job_config in job_configs
]

job_config_ids = bulk_insert(query, params_list, jsonb_columns=[2])

print(f"Created {len(job_config_ids)} job configurations")
```

### Storing Batch Type

The batch type can be stored in the `irp_batch.metadata` field:

```python
import json

# Create batch with type metadata
batch_metadata = {
    'batch_type': 'portfolio_batch',
    'description': 'Monthly portfolio analysis'
}

batch_id = execute_insert(
    """INSERT INTO irp_batch
       (step_id, batch_type, status, metadata)
       VALUES (%s, %s, %s, %s)""",
    (step_id, 'edmcreation', 'INITIATED', json.dumps(batch_metadata))
)

# Later, retrieve and use the batch type
batch = execute_query(
    "SELECT metadata FROM irp_batch WHERE id = %s",
    (batch_id,)
).iloc[0]

batch_type = batch['metadata'].get('batch_type', 'default')
```

## Troubleshooting

### Error: "No transformer registered for batch type 'xyz'"

**Cause:** The batch type hasn't been registered.

**Solutions:**
1. Check for typos in the batch type name
2. Ensure the transformer module has been imported
3. Verify the `@register` decorator was used correctly
4. List available types: `ConfigurationTransformer.list_types()`

### Empty Job List Returned

**Cause:** The transformer returned an empty list.

**Solutions:**
1. Check input configuration format matches transformer expectations
2. Add logging/debugging to your transformer function
3. Verify the configuration data is valid

### TypeError: 'X' object is not iterable

**Cause:** Transformer didn't return a list.

**Solution:** Ensure your transformer always returns a list:

```python
# Wrong
def bad_transformer(config):
    return config  # Should be [config]

# Correct
def good_transformer(config):
    return [config]
```

## Reference

### Module Location
`workspace/helpers/configuration.py`

### Class Definition
```python
class ConfigurationTransformer:
    _transformers = {}  # Registry

    @classmethod
    def register(cls, batch_type: str) -> Callable

    @classmethod
    def get_job_configurations(
        cls,
        batch_type: str,
        configuration: Dict[str, Any]
    ) -> List[Dict[str, Any]]

    @classmethod
    def list_types(cls) -> List[str]
```

### Test Location
`workspace/tests/test_configuration.py`

Tests 7-12 cover transformer functionality.

---

**Last Updated:** 2025-10-15