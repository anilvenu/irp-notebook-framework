#!/usr/bin/env python3
"""
Script to add schema defaulting logic to database helper functions.
This adds 'if schema is None: schema = DB_CONFIG["schema"]' after function docstrings.
"""

import re
from pathlib import Path

def add_schema_default(file_path):
    """Add schema defaulting to functions in file"""
    with open(file_path, 'r') as f:
        content = f.read()

    # Pattern: function definition with schema param, followed by docstring, then function body
    # We want to insert the default check right after the docstring
    pattern = r'(def \w+\([^)]*schema: Optional\[str\] = None[^)]*\)[^:]*:\s*"""[\s\S]*?""")\s*(\n    )'

    def replacer(match):
        """Add schema default check after docstring"""
        docstring_part = match.group(1)
        indent = match.group(2)

        # Don't add if it already has the check
        if 'if schema is None:' in content[match.end():match.end()+200]:
            return match.group(0)

        return docstring_part + indent + 'if schema is None:\n        schema = DB_CONFIG[\'schema\']\n' + indent

    new_content = re.sub(pattern, replacer, content)

    # Write back
    with open(file_path, 'w') as f:
        f.write(new_content)

    print(f"Updated {file_path}")

if __name__ == '__main__':
    # Process all helper files
    files = [
        '/home/avenugopal/irp-notebook-framework/workspace/helpers/database.py',
        '/home/avenugopal/irp-notebook-framework/workspace/helpers/batch.py',
        '/home/avenugopal/irp-notebook-framework/workspace/helpers/job.py',
        '/home/avenugopal/irp-notebook-framework/workspace/helpers/configuration.py',
        '/home/avenugopal/irp-notebook-framework/workspace/helpers/cycle.py',
    ]

    for file in files:
        if Path(file).exists():
            add_schema_default(file)
        else:
            print(f"Skipping {file} - not found")
