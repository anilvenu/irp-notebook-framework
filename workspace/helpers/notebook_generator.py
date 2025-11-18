"""
IRP Notebook Framework - Notebook Generator

This module provides functionality to dynamically generate Jupyter notebooks
and folder structures from CSV or Excel files containing workflow stages and steps.

Usage:
    from helpers.notebook_generator import generate_notebooks_from_file

    # Works with CSV
    generate_notebooks_from_file(
        file_path='path/to/stages_and_steps.csv',
        output_dir='path/to/output'
    )

    # Works with Excel
    generate_notebooks_from_file(
        file_path='path/to/stages_and_steps.xlsx',
        output_dir='path/to/output',
        sheet_name='Sheet1'  # Optional, defaults to first sheet
    )
"""

import os
import csv
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

try:
    import nbformat
    from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
except ImportError:
    raise ImportError(
        "nbformat is required for notebook generation. "
        "Install it with: pip install nbformat"
    )

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class NotebookGeneratorError(Exception):
    """Custom exception for notebook generation errors"""
    pass


def _detect_file_type(file_path: str) -> str:
    """
    Detect file type based on extension.

    Args:
        file_path: Path to the file

    Returns:
        File type: 'csv' or 'excel'

    Raises:
        NotebookGeneratorError: If file type is not supported
    """
    extension = Path(file_path).suffix.lower()

    if extension == '.csv':
        return 'csv'
    elif extension in ['.xlsx', '.xls', '.xlsm']:
        return 'excel'
    else:
        raise NotebookGeneratorError(
            f"Unsupported file type: {extension}. "
            f"Supported types: .csv, .xlsx, .xls, .xlsm"
        )


def parse_csv_to_structure(csv_path: str) -> Dict[str, Dict[str, List[str]]]:
    """
    Parse CSV file and organize data into nested structure.

    Structure:
        {
            'Stage_Name': {
                'Step_Name': ['Activity1', 'Activity2', ...],
                ...
            },
            ...
        }

    Args:
        csv_path: Path to the CSV file with Stage, Step, Activities columns

    Returns:
        Nested dictionary of stages -> steps -> activities

    Raises:
        NotebookGeneratorError: If CSV file is invalid or missing required columns
    """
    if not os.path.exists(csv_path):
        raise NotebookGeneratorError(f"CSV file not found: {csv_path}")

    # Structure to hold: stage -> step -> [activities]
    structure = defaultdict(lambda: defaultdict(list))

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Validate required columns
            required_columns = {'Stage', 'Step', 'Activities'}
            if not required_columns.issubset(set(reader.fieldnames or [])):
                raise NotebookGeneratorError(
                    f"CSV must contain columns: {', '.join(required_columns)}"
                )

            # Parse rows
            for row in reader:
                stage = row['Stage'].strip()
                step = row['Step'].strip()
                activity = row['Activities'].strip()

                # Skip empty rows
                if not stage or not step or not activity:
                    continue

                # Add activity to the appropriate stage/step
                structure[stage][step].append(activity)

    except csv.Error as e:
        raise NotebookGeneratorError(f"Error reading CSV file: {str(e)}")
    except Exception as e:
        raise NotebookGeneratorError(f"Unexpected error parsing CSV: {str(e)}")

    if not structure:
        raise NotebookGeneratorError("No valid data found in CSV file")

    return dict(structure)


def parse_excel_to_structure(
    excel_path: str,
    sheet_name: Optional[str] = None
) -> Dict[str, Dict[str, List[str]]]:
    """
    Parse Excel file and organize data into nested structure.

    Structure:
        {
            'Stage_Name': {
                'Step_Name': ['Activity1', 'Activity2', ...],
                ...
            },
            ...
        }

    Args:
        excel_path: Path to the Excel file with Stage, Step, Activities columns
        sheet_name: Name of the sheet to read (None = first sheet)

    Returns:
        Nested dictionary of stages -> steps -> activities

    Raises:
        NotebookGeneratorError: If Excel file is invalid or missing required columns
    """
    if not PANDAS_AVAILABLE:
        raise NotebookGeneratorError(
            "pandas is required to read Excel files. "
            "Install it with: pip install pandas openpyxl"
        )

    if not os.path.exists(excel_path):
        raise NotebookGeneratorError(f"Excel file not found: {excel_path}")

    # Structure to hold: stage -> step -> [activities]
    structure = defaultdict(lambda: defaultdict(list))

    try:
        # Read Excel file
        if sheet_name:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(excel_path)

        # Validate required columns
        required_columns = {'Stage', 'Step', 'Activities'}
        if not required_columns.issubset(set(df.columns)):
            raise NotebookGeneratorError(
                f"Excel file must contain columns: {', '.join(required_columns)}"
            )

        # Parse rows
        for _, row in df.iterrows():
            # Convert to string and strip whitespace
            stage = str(row['Stage']).strip() if pd.notna(row['Stage']) else ''
            step = str(row['Step']).strip() if pd.notna(row['Step']) else ''
            activity = str(row['Activities']).strip() if pd.notna(row['Activities']) else ''

            # Skip empty rows
            if not stage or not step or not activity or stage == 'nan' or step == 'nan' or activity == 'nan':
                continue

            # Add activity to the appropriate stage/step
            structure[stage][step].append(activity)

    except FileNotFoundError:
        raise NotebookGeneratorError(f"Excel file not found: {excel_path}")
    except Exception as e:
        raise NotebookGeneratorError(f"Error reading Excel file: {str(e)}")

    if not structure:
        raise NotebookGeneratorError("No valid data found in Excel file")

    return dict(structure)


def parse_file_to_structure(
    file_path: str,
    sheet_name: Optional[str] = None
) -> Dict[str, Dict[str, List[str]]]:
    """
    Parse CSV or Excel file and organize data into nested structure.

    Automatically detects file type based on extension.

    Args:
        file_path: Path to the CSV or Excel file
        sheet_name: For Excel files, name of sheet to read (None = first sheet)

    Returns:
        Nested dictionary of stages -> steps -> activities

    Raises:
        NotebookGeneratorError: If file is invalid or missing required columns
    """
    file_type = _detect_file_type(file_path)

    if file_type == 'csv':
        return parse_csv_to_structure(file_path)
    else:  # excel
        return parse_excel_to_structure(file_path, sheet_name)


def create_notebook_from_activities(activities: List[str]) -> nbformat.NotebookNode:
    """
    Create a Jupyter notebook with markdown headers and code cells for activities.

    Each activity becomes:
    - A markdown cell with the activity text as a header
    - An empty code cell for implementation

    Args:
        activities: List of activity descriptions

    Returns:
        nbformat.NotebookNode: The created notebook object
    """
    nb = new_notebook()
    cells = []

    for activity in activities:
        # Create markdown cell with activity as header
        markdown_cell = new_markdown_cell(f"## {activity}")
        cells.append(markdown_cell)

        # Create empty code cell
        code_cell = new_code_cell("")
        cells.append(code_cell)

    nb['cells'] = cells
    return nb


def generate_notebooks_from_file(
    file_path: str,
    output_dir: str,
    sheet_name: Optional[str] = None,
    overwrite: bool = False,
    verbose: bool = True
) -> Dict[str, any]:
    """
    Generate folder structure and Jupyter notebooks from CSV or Excel file.

    Automatically detects file type based on extension (.csv, .xlsx, .xls, .xlsm).

    Creates:
    - One folder per unique Stage
    - One notebook per unique Step (within its Stage folder)
    - Each notebook contains markdown headers and code cells for Activities

    Args:
        file_path: Path to CSV or Excel file with Stage, Step, Activities columns
        output_dir: Directory where folder structure will be created
        sheet_name: For Excel files, name of sheet to read (None = first sheet)
        overwrite: If True, overwrite existing notebooks. If False, skip existing files
        verbose: If True, print progress messages

    Returns:
        Dictionary with generation statistics:
        {
            'stages_created': int,
            'notebooks_created': int,
            'notebooks_skipped': int,
            'total_activities': int
        }

    Raises:
        NotebookGeneratorError: If there are errors during generation
    """
    # Detect file type
    file_type = _detect_file_type(file_path)

    # Parse file into structure
    if verbose:
        if file_type == 'csv':
            print(f"Parsing CSV file: {file_path}")
        else:
            sheet_info = f" (sheet: {sheet_name})" if sheet_name else " (first sheet)"
            print(f"Parsing Excel file: {file_path}{sheet_info}")

    structure = parse_file_to_structure(file_path, sheet_name)

    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Output directory: {output_path.absolute()}")
        print(f"Found {len(structure)} stages\n")

    # Track statistics
    stats = {
        'stages_created': 0,
        'notebooks_created': 0,
        'notebooks_skipped': 0,
        'total_activities': 0
    }

    # Generate folder structure and notebooks
    for stage_name, steps in structure.items():
        # Create stage folder
        stage_path = output_path / f"Stage_{stage_name}"
        stage_existed = stage_path.exists()
        stage_path.mkdir(exist_ok=True)

        if not stage_existed:
            stats['stages_created'] += 1

        if verbose:
            print(f"Stage: Stage_{stage_name}")

        # Create notebooks for each step
        for step_name, activities in steps.items():
            notebook_path = stage_path / f"Step_{step_name}.ipynb"

            # Check if notebook exists
            if notebook_path.exists() and not overwrite:
                if verbose:
                    print(f"  - Step_{step_name}.ipynb (skipped - already exists)")
                stats['notebooks_skipped'] += 1
                continue

            # Create notebook
            nb = create_notebook_from_activities(activities)

            # Write notebook to file
            try:
                with open(notebook_path, 'w', encoding='utf-8') as f:
                    nbformat.write(nb, f)

                stats['notebooks_created'] += 1
                stats['total_activities'] += len(activities)

                if verbose:
                    print(f"  - Step_{step_name}.ipynb ({len(activities)} activities)")

            except Exception as e:
                raise NotebookGeneratorError(
                    f"Failed to write notebook {notebook_path}: {str(e)}"
                )

        if verbose:
            print()  # Blank line between stages

    # Print summary
    if verbose:
        print("=" * 60)
        print("GENERATION COMPLETE")
        print("=" * 60)
        print(f"Stages created: {stats['stages_created']}")
        print(f"Notebooks created: {stats['notebooks_created']}")
        print(f"Notebooks skipped: {stats['notebooks_skipped']}")
        print(f"Total activities: {stats['total_activities']}")
        print("=" * 60)

    return stats


def generate_notebooks_from_csv(
    csv_path: str,
    output_dir: str,
    overwrite: bool = False,
    verbose: bool = True
) -> Dict[str, any]:
    """
    Generate folder structure and Jupyter notebooks from CSV file.

    DEPRECATED: Use generate_notebooks_from_file() instead for better flexibility.
    This function is kept for backward compatibility.

    Args:
        csv_path: Path to CSV file with Stage, Step, Activities columns
        output_dir: Directory where folder structure will be created
        overwrite: If True, overwrite existing notebooks
        verbose: If True, print progress messages

    Returns:
        Dictionary with generation statistics
    """
    return generate_notebooks_from_file(
        file_path=csv_path,
        output_dir=output_dir,
        overwrite=overwrite,
        verbose=verbose
    )


def preview_structure(file_path: str, sheet_name: Optional[str] = None) -> str:
    """
    Preview the folder structure that would be generated from CSV or Excel file.

    Useful for validating the file before generating notebooks.

    Args:
        file_path: Path to CSV or Excel file
        sheet_name: For Excel files, name of sheet to read (None = first sheet)

    Returns:
        String representation of the folder structure
    """
    structure = parse_file_to_structure(file_path, sheet_name)

    file_type = _detect_file_type(file_path)
    lines = ["Folder Structure Preview:", "=" * 60]

    if file_type == 'excel' and sheet_name:
        lines.append(f"Source: Excel file (sheet: {sheet_name})")
    elif file_type == 'excel':
        lines.append(f"Source: Excel file (first sheet)")
    else:
        lines.append(f"Source: CSV file")

    lines.append("=" * 60)

    for stage_name, steps in structure.items():
        lines.append(f"Stage_{stage_name}/")
        for step_name, activities in steps.items():
            lines.append(f"  - Step_{step_name}.ipynb ({len(activities)} activities)")

    lines.append("=" * 60)

    return "\n".join(lines)


def clear_generated_folder(
    output_dir: str,
    confirm: bool = True,
    verbose: bool = True
) -> Dict[str, any]:
    """
    Clear all contents from the generated notebooks folder.

    WARNING: This will delete all files and folders in the specified directory!

    Args:
        output_dir: Path to the directory to clear
        confirm: If True, requires the directory name to contain 'generated' as a safety check
        verbose: If True, print progress messages

    Returns:
        Dictionary with deletion statistics:
        {
            'deleted': bool,
            'folders_removed': int,
            'files_removed': int,
            'path': str
        }

    Raises:
        NotebookGeneratorError: If safety checks fail or deletion errors occur
    """
    output_path = Path(output_dir)

    # Safety check: only allow deletion if 'generated' is in the path
    if confirm and 'generated' not in str(output_path).lower():
        raise NotebookGeneratorError(
            f"Safety check failed: Directory path must contain 'generated' to use clear function. "
            f"Path provided: {output_path}\n"
            f"To bypass this check, use confirm=False (NOT RECOMMENDED)"
        )

    # Check if directory exists
    if not output_path.exists():
        if verbose:
            print(f"Directory does not exist: {output_path}")
            print("Nothing to clear.")
        return {
            'deleted': False,
            'folders_removed': 0,
            'files_removed': 0,
            'path': str(output_path)
        }

    # Count items before deletion
    folders_count = 0
    files_count = 0

    try:
        # Count all items
        for item in output_path.rglob('*'):
            if item.is_file():
                files_count += 1
            elif item.is_dir():
                folders_count += 1

        if verbose:
            print(f"Clearing directory: {output_path.absolute()}")
            print(f"Found {files_count} files and {folders_count} folders")

        # Remove all contents
        for item in output_path.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)

        if verbose:
            print("=" * 60)
            print("CLEAR COMPLETE")
            print("=" * 60)
            print(f"Removed {files_count} files and {folders_count} folders")
            print("=" * 60)

        return {
            'deleted': True,
            'folders_removed': folders_count,
            'files_removed': files_count,
            'path': str(output_path.absolute())
        }

    except Exception as e:
        raise NotebookGeneratorError(f"Failed to clear directory: {str(e)}")


# Example usage
if __name__ == "__main__":
    # Example 1: Generate notebooks from CSV
    csv_file = "../workflows/_Tools/files/stages_and_steps.csv"
    output_directory = "../workflows/_Template/generated"

    # Preview structure first
    print("CSV Example:")
    print(preview_structure(csv_file))
    print("\n")

    # Generate notebooks
    generate_notebooks_from_file(
        file_path=csv_file,
        output_dir=output_directory,
        overwrite=False,
        verbose=True
    )

    # Example 2: Generate notebooks from Excel (if you have an Excel file)
    # excel_file = "../workflows/_Tools/files/stages_and_steps.xlsx"
    # print("\nExcel Example:")
    # print(preview_structure(excel_file, sheet_name="Sheet1"))
    # print("\n")
    # generate_notebooks_from_file(
    #     file_path=excel_file,
    #     output_dir=output_directory,
    #     sheet_name="Sheet1",  # Optional: specify sheet name
    #     overwrite=False,
    #     verbose=True
    # )
