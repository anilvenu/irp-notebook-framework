#!/usr/bin/env python3
"""
Test script to verify the API setup
Run this before starting the server to check if everything is configured correctly
"""

import sys
import os
from pathlib import Path

print("=" * 60)
print("IRP Dashboard API - Setup Verification")
print("=" * 60)
print()

# Track if all checks pass
all_checks_passed = True

# Check 1: Python version
print("1. Checking Python version...")
if sys.version_info >= (3, 8):
    print(f"   ✓ Python {sys.version_info.major}.{sys.version_info.minor} (OK)")
else:
    print(f"   ✗ Python {sys.version_info.major}.{sys.version_info.minor} (Need 3.8+)")
    all_checks_passed = False

# Check 2: Required packages
print("\n2. Checking required packages...")
required_packages = {
    'fastapi': 'FastAPI',
    'uvicorn': 'Uvicorn',
    'jinja2': 'Jinja2',
    'psycopg2': 'psycopg2',
    'pandas': 'Pandas'
}

for package, name in required_packages.items():
    try:
        __import__(package)
        print(f"   ✓ {name}")
    except ImportError:
        print(f"   ✗ {name} (Not installed)")
        all_checks_passed = False

# Check 3: Database helper
print("\n3. Checking database helper...")
try:
    workspace_path = Path(__file__).parent.parent
    sys.path.insert(0, str(workspace_path))
    from helpers import database as db
    print("   ✓ Database helper imported")
except ImportError as e:
    print(f"   ✗ Database helper import failed: {e}")
    all_checks_passed = False

# Check 4: Environment variables
print("\n4. Checking environment variables...")
env_vars = ['DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
for var in env_vars:
    value = os.getenv(var)
    if value:
        # Mask password
        display_value = '****' if var == 'DB_PASSWORD' else value
        print(f"   ✓ {var}={display_value}")
    else:
        print(f"   ⚠ {var} not set (will use default)")

# Check 5: File structure
print("\n5. Checking file structure...")
demo_dir = Path(__file__).parent
files_to_check = [
    'app/app.py',
    'app/templates/base.html',
    'app/templates/home.html',
    'app/templates/cycle_dashboard.html',
    'app/templates/batch_detail.html',
    'app/static/css/style.css',
    'app/static/js/scripts.js',
    'requirements-api.txt',
    'run_api.sh'
]

for file_path in files_to_check:
    full_path = demo_dir / file_path
    if full_path.exists():
        print(f"   ✓ {file_path}")
    else:
        print(f"   ✗ {file_path} (Missing)")
        all_checks_passed = False

# Check 6: Database connection (optional)
print("\n6. Testing database connection...")
try:
    from helpers import database as db
    result = db.execute_query("SELECT 1 as test")
    if not result.empty and result.iloc[0]['test'] == 1:
        print("   ✓ Database connection successful")
    else:
        print("   ⚠ Database query returned unexpected result")
except Exception as e:
    print(f"   ⚠ Database connection failed: {e}")
    print("   Note: This is OK if database isn't running yet")

# Summary
print("\n" + "=" * 60)
if all_checks_passed:
    print("✓ All checks passed! Ready to start the API server.")
    print("\nTo start the server, run:")
    print("  ./demo/run_api.sh")
    print("\nOr directly:")
    print("  cd demo/app && python3 -m uvicorn app:app --reload")
else:
    print("✗ Some checks failed. Please fix the issues above.")
    print("\nTo install missing packages:")
    print("  pip install -r demo/requirements-api.txt")

print("=" * 60)

sys.exit(0 if all_checks_passed else 1)
