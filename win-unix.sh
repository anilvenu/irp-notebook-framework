#!/bin/bash
# Convert Windows line endings to Unix and make executable
# Usage: ./win-unix.sh <file1> <file2> ...

if [ $# -eq 0 ]; then
    echo "Usage: $0 <file1> [file2] [file3] ..."
    echo "Converts Windows line endings (CRLF) to Unix (LF) and makes files executable"
    exit 1
fi

for file in "$@"; do
    if [ ! -f "$file" ]; then
        echo "✗ File not found: $file"
        continue
    fi

    # Convert line endings
    dos2unix "$file" 2>/dev/null || sed -i 's/\r$//' "$file"

    # Make executable
    chmod +x "$file"

    echo "✓ Converted and made executable: $file"
done

echo "Done!"
