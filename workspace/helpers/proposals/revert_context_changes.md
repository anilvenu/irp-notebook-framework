# Proposal: Revert Changes to `context.py` from PR #32

## Overview

This proposal recommends reverting the changes made to [workspace/helpers/context.py](../context.py) as part of PR #32 (https://github.com/anilvenu/irp-notebook-framework/pull/32/files).

## Changes Made in PR #32

The following modifications were introduced:

### 1. Auto-Detection of Notebook Path
- **Changed**: `WorkContext.__init__()` now auto-detects notebook path using `os.getcwd()` when no path is provided
- **Impact**: Relies on JupyterLab setting the current working directory to the notebook's directory

### 2. Directory-Based Path Resolution
- **Changed**: `_parse_path()` method now handles both directory and file paths
- **Impact**: When given a directory, searches for `Step_*.ipynb` files to extract step information

### 3. Added `cycle_directory` Property
- **Changed**: New property that navigates up the directory tree to find `Active_{cycle_name}` directory
- **Impact**: Provides access to cycle root directory from any notebook location

### 4. Enhanced Error Messages
- **Changed**: More detailed error messages with user guidance
- **Impact**: Better UX but couples the framework to specific directory structure assumptions

### 5. Windows Path Compatibility
- **Changed**: Handles both `/` and `\` path separators when extracting cycle name
- **Impact**: Better cross-platform support

## Reason for Reversion

**We need to revert these changes because they introduce fragility and assumptions that may not hold in all execution environments:**

1. **JupyterLab Dependency**: The auto-detection relies on JupyterLab setting `os.getcwd()` to the notebook directory, which is not guaranteed across all notebook execution contexts (e.g., VS Code, command-line execution, automated workflows)

2. **Loss of Explicit Contract**: The original design required an explicit `notebook_path` parameter, making the dependency clear and testable

3. **Directory Scanning Overhead**: Searching for `Step_*.ipynb` files in directories adds unnecessary file I/O operations

4. **Multiple Step Files Ambiguity**: If a directory contains multiple `Step_*.ipynb` files, the code arbitrarily picks the first one, which could lead to incorrect context detection

5. **Tight Coupling to Directory Structure**: The validation logic is now tightly coupled to the `Active_` naming convention, reducing flexibility

## Recommended Action

**Revert to the original implementation** where:
- `notebook_path` is a required parameter
- Callers must explicitly pass the notebook path (e.g., using `__file__` in notebooks)
- Path parsing works only on file paths, not directories
- No assumptions about `os.getcwd()` behavior

## Migration Path

If auto-detection is desired, it should be:
1. Implemented as a separate helper function (not in `__init__`)
2. Clearly documented with caveats about environment requirements
3. Optional, with explicit path passing as the primary/recommended approach

---

**Related PR**: https://github.com/anilvenu/irp-notebook-framework/pull/32/files#diff-6b9628b6856bc73660b27ae93b5e13a6e9777d88474caf976f546def6058317f
