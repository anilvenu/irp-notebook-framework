"""
Check if subPeril pattern affects regionPerilSimulationSet requirement.
"""

import json
from pathlib import Path

# Load all manual request files and check pattern
MANUAL_REQUESTS_DIR = Path('workspace/helpers/irp_integration')

def main():
    pattern = '*_manual_request.json'
    files = list(MANUAL_REQUESTS_DIR.glob(pattern))

    print("Checking all manual request files for subPeril pattern:\n")
    print(f"{'File':<40} {'Has Entries':<15} {'Count'}")
    print("-" * 70)

    for f in sorted(files):
        with open(f, 'r') as file:
            data = json.load(file)

        entries = data.get('regionPerilSimulationSet', [])
        count = len(entries)
        has_entries = "Yes" if count > 0 else "No"

        print(f"{f.name:<40} {has_entries:<15} {count}")


if __name__ == '__main__':
    main()
