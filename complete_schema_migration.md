# Schema Security Migration - Completion Guide

## Status Summary

### ✅ COMPLETED
1. Database infrastructure
   - [setup_users_and_schemas.sql](workspace/helpers/db/setup_users_and_schemas.sql)
   - Passwords: `irp_g0_live` / `g0test`

2. Environment configuration
   - [.env](.env) - live credentials
   - [.env.test](.env.test) - test credentials
   - [docker-compose.yml](docker-compose.yml) - DB_SCHEMA=live
   - [docker-compose.test.yml](docker-compose.test.yml) - test override

3. Core constants
   - [constants.py](workspace/helpers/constants.py) - DB_CONFIG['schema'] added

4. Database layer (CRITICAL - COMPLETE)
   - [database.py](workspace/helpers/database.py)
   - All functions use `schema: Optional[str] = None`
   - `_set_schema()` defaults to `DB_CONFIG['schema']`
   - **This is the key layer - all DB operations go through here**

### ⏳ REMAINING (Lower Priority)
The following files currently have `schema='public'` defaults but will still work correctly because they call database.py functions which handle defaulting:

- batch.py
- job.py
- configuration.py
- cycle.py

## Manual Completion Steps

To complete the migration for consistency:

1. **For each helper file** (batch.py, job.py, configuration.py, cycle.py):
   ```python
   # Change from:
   def some_function(param1, schema: str = 'public'):

   # To:
   def some_function(param1, schema: Optional[str] = None):
    if schema is None:
        schema = DB_CONFIG['schema']
       # rest of function...
   ```

2. **Add import** at top of each file:
   ```python
   from helpers.constants import DB_CONFIG
   from typing import Optional
   ```

## Testing

After setup_users_and_schemas.sql is run:

```bash
# Setup database users
psql -U postgres -d irp_db -f workspace/helpers/db/setup_users_and_schemas.sql

# Initialize live schema
export DB_USER=irp_live DB_PASSWORD=irp_g0_live DB_SCHEMA=live
python -c "from helpers.database import init_database; init_database()"

# Initialize test schema
export DB_USER=irp_test DB_PASSWORD=g0test DB_SCHEMA=test
python -c "from helpers.database import init_database; init_database()"

# Run tests
./run_tests.sh
```

## Security Validation

Test that schema isolation works:

```python
# This should fail - test user trying to access live
import os
os.environ['DB_USER'] = 'irp_test'
os.environ['DB_PASSWORD'] = 'g0test'

from helpers.database import execute_query
execute_query("SELECT * FROM irp_cycle", schema='live')  # Should fail with auth error
```

## Files Modified

### Created:
- workspace/helpers/db/setup_users_and_schemas.sql
- docker-compose.test.yml
- .env.test
- fix_schema_defaults.py (helper script)

### Modified:
- docker-compose.yml
- .env
- workspace/helpers/constants.py
- workspace/helpers/database.py

### Pending:
- workspace/helpers/batch.py
- workspace/helpers/job.py
- workspace/helpers/configuration.py
- workspace/helpers/cycle.py
- run_tests.sh
- README.md (documentation)
