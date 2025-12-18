"""
Excel Export Helper Module

Provides functionality to export pandas DataFrames to Excel files with formatting.
"""

from pathlib import Path
from typing import Optional, Union
import pandas as pd
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter


def _get_peril_from_import_file(import_file: str) -> str:
    """
    Extract peril from Import File name.

    Peril is the first part before any underscore. For example:
    - "USEQ" -> "USEQ"
    - "USIF_Commercial" -> "USIF"
    - "USIF_Excess" -> "USIF"
    - "USFL_Other" -> "USFL"

    Args:
        import_file: Import File name

    Returns:
        Peril name (first part before underscore)
    """
    return import_file.split('_')[0]


def save_geohaz_validation_to_excel(
    validation_results: pd.DataFrame,
    date_value: str,
    cycle_type: str,
    output_dir: Union[str, Path]
) -> Optional[Path]:
    """
    Save GeoHaz validation results to an Excel file with one sheet per Peril.

    Creates a formatted Excel workbook where each sheet contains validation results
    for a single Peril (e.g., USEQ, USIF, USFL). Import Files are grouped by peril,
    which is the first part of the Import File name before any underscore.

    Includes formatting:
    - Bold headers
    - Conditional coloring on Status cell (green for PASS, red for FAIL)
    - Auto-fit column widths

    Args:
        validation_results: DataFrame from validate_geohaz_thresholds() with columns:
            Import File, Portfolio, Geocode Level, Expected %, Threshold %,
            Min %, Max %, Actual %, Risk Count, Status
        date_value: Date value for filename (e.g., '202511')
        cycle_type: Cycle type for filename (e.g., 'Quarterly')
        output_dir: Directory to save the Excel file

    Returns:
        Path to created Excel file, or None if validation_results is empty

    Example:
        ```python
        validation_results, all_passed = validate_geohaz_thresholds(
            geocoding_results, geohaz_thresholds, import_file_mapping
        )

        excel_path = save_geohaz_validation_to_excel(
            validation_results=validation_results,
            date_value='202511',
            cycle_type='Quarterly',
            output_dir=Path('/path/to/notebook/directory')
        )
        ```
    """
    # Don't create file if no results
    if validation_results.empty:
        return None

    # Ensure output_dir is a Path
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build filename
    filename = f"GeoHaz_Validation_{date_value}_{cycle_type}.xlsx"
    file_path = output_dir / filename

    # Add Peril column based on Import File
    results_with_peril = validation_results.copy()
    results_with_peril['Peril'] = results_with_peril['Import File'].apply(_get_peril_from_import_file)

    # Get unique Perils
    perils = results_with_peril['Peril'].unique()

    # Create Excel writer
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        for peril in perils:
            # Filter data for this Peril
            sheet_data = results_with_peril[
                results_with_peril['Peril'] == peril
            ].copy()

            # Drop the Peril column (redundant - it's the sheet name)
            sheet_data = sheet_data.drop(columns=['Peril'])

            # Write DataFrame to sheet
            sheet_data.to_excel(writer, sheet_name=peril, index=False)

            # Get the worksheet for formatting
            worksheet = writer.sheets[peril]

            # Apply formatting
            _format_validation_sheet(worksheet, sheet_data)

    return file_path


def _format_validation_sheet(worksheet, data: pd.DataFrame) -> None:
    """
    Apply formatting to a GeoHaz validation worksheet.

    Args:
        worksheet: openpyxl worksheet object
        data: DataFrame that was written to the sheet (for column info)
    """
    # Define styles
    header_font = Font(bold=True)
    pass_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
    fail_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
    center_align = Alignment(horizontal='center')

    # Get column count and find Status column index
    num_cols = len(data.columns)
    status_col_idx = list(data.columns).index('Status') + 1  # 1-indexed for openpyxl

    # Format header row (row 1)
    for col in range(1, num_cols + 1):
        cell = worksheet.cell(row=1, column=col)
        cell.font = header_font
        cell.alignment = center_align

    # Apply center alignment to data cells and conditional formatting to Status cell only
    for row in range(2, len(data) + 2):  # Start from row 2 (after header)
        # Center align all cells in the row
        for col in range(1, num_cols + 1):
            cell = worksheet.cell(row=row, column=col)
            cell.alignment = center_align

        # Apply conditional fill to Status cell only
        status_cell = worksheet.cell(row=row, column=status_col_idx)
        status_value = status_cell.value
        status_cell.fill = pass_fill if status_value == 'PASS' else fail_fill

    # Auto-fit column widths
    for col_idx, column in enumerate(data.columns, 1):
        # Calculate max width needed
        max_length = len(str(column))  # Header length
        for row in range(2, len(data) + 2):
            cell_value = worksheet.cell(row=row, column=col_idx).value
            if cell_value is not None:
                max_length = max(max_length, len(str(cell_value)))

        # Set column width with a little padding
        adjusted_width = max_length + 2
        worksheet.column_dimensions[get_column_letter(col_idx)].width = adjusted_width
