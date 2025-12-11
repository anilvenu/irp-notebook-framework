"""
Analysis Results Validator Module

Compares analysis outputs between a production run (baseline) and a test run
to validate that the test produced expected results.

Endpoints compared:
- Statistics (/stats)
- EP Metrics (/ep)
- Event Loss Table (/elt)
- Period Loss Table (/plt) - HD analyses only

Supports both single analysis validation and batch validation from CSV files.
"""

import csv
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

import pandas as pd

from helpers.irp_integration import IRPClient


# =============================================================================
# Field Definitions
# =============================================================================

# Fields to IGNORE when comparing (metadata, not analysis results)
IGNORED_FIELDS = {
    'analysisId',
    'jobId',
    'uri',
    'exposureResourceId',
    'exposureResourceType',
    'exposureResourceNumber',
    'perspectiveCode',
    'appAnalysisId',
    'createdAt',
    'modifiedAt',
    'createdBy',
    'modifiedBy',
}

# Meaningful fields to compare per endpoint (based on actual API responses)
ELT_FIELDS = {
    'eventId',       # Key field
    'sourceId',
    'positionValue', # Mean loss
    'stdDevI',       # Standard deviation (independent)
    'stdDevC',       # Standard deviation (correlated)
    'expValue',      # Exposure value
    'rate',          # Event rate
    'peril',
    'region',
    'oepWUC',
}

# EP returns nested structure with returnPeriods and positionValues arrays in 'value'
EP_FIELDS = {
    'epType',        # Key field (AEP, OEP, etc.)
    'value',         # Contains returnPeriods and positionValues arrays
}

STATS_FIELDS = {
    'epType',        # Key field (OEP, AEP, etc.)
    'purePremium',   # AAL / Pure Premium
    'totalStdDev',   # Total standard deviation
    'cv',            # Coefficient of variation
    'netPurePremium',
    'activation',
    'exhaustion',
    'totalLossRatio',
    'limit',
    'premium',
    'netStdDev',
    'exhaustAllReinstatements',
}

# PLT uses composite key (periodId + eventId) since same event can appear in multiple periods
PLT_FIELDS = {
    'periodId',      # Key field (composite with eventId)
    'eventId',       # Key field (composite with periodId)
    'weight',        # Period weight
    'eventDate',     # Event occurrence date
    'lossDate',      # Loss date
    'positionValue', # Loss value
    'peril',         # Peril code
    'region',        # Region code
}

# Analysis settings fields to compare (from search_analyses response)
# Excludes: IDs, dates, currency, GUIDs/URIs, user metadata
SETTINGS_FIELDS = {
    # Core analysis configuration
    'engineType',            # 'HD' or 'DLM'
    'engineVersion',         # e.g., 'RL23'
    'analysisType',          # e.g., 'Exceedance Probability'
    'analysisMode',          # e.g., 'Distributed'
    'analysisFramework',     # e.g., 'ELT'
    # Model configuration
    'modelProfile',          # Dict with id, code, name
    'outputProfile',         # Dict with id, code, name
    'eventRateSchemeNames',  # List of dicts with id, code, name
    # Peril/region configuration
    'peril',                 # e.g., 'Windstorm'
    'perilCode',             # e.g., 'WS'
    'subPeril',              # e.g., 'Surge Only'
    'region',                # e.g., 'North Atlantic (including Hawaii)'
    'regionCode',            # e.g., 'NA'
    # Loss configuration
    'lossAmplification',     # e.g., 'Building, Contents, BI'
    'insuranceType',         # e.g., 'Property'
    'vulnerabilityCurve',    # e.g., 'Vulnerability - Default'
    # Other settings
    'engineSubType',         # e.g., 'Not Applicable'
    'isMultiEvent',          # Boolean
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ComparisonResult:
    """Result of comparing two datasets."""
    endpoint: str
    passed: bool
    total_records_prod: int
    total_records_test: int
    differences: List[Dict[str, Any]] = field(default_factory=list)
    missing_in_test: List[Any] = field(default_factory=list)
    extra_in_test: List[Any] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class ValidationResult:
    """Overall validation result containing all endpoint comparisons."""
    production_app_analysis_id: int
    test_app_analysis_id: int
    perspective_code: str
    results: List[ComparisonResult] = field(default_factory=list)
    name: Optional[str] = None  # Optional label for this validation pair
    error: Optional[str] = None  # Error if validation couldn't run at all

    @property
    def passed(self) -> bool:
        """Returns True if all endpoint comparisons passed."""
        if self.error:
            return False
        return all(r.passed for r in self.results)

    def get_result(self, endpoint: str) -> Optional[ComparisonResult]:
        """Get result for a specific endpoint."""
        for r in self.results:
            if r.endpoint == endpoint:
                return r
        return None


@dataclass
class AnalysisPairResult:
    """Validation results for a single analysis pair across all perspectives."""
    production_app_analysis_id: int
    test_app_analysis_id: int
    name: Optional[str] = None
    perspective_results: Dict[str, ValidationResult] = field(default_factory=dict)
    error: Optional[str] = None  # Error if pair couldn't be validated at all

    @property
    def passed(self) -> bool:
        """Returns True if all perspectives passed."""
        if self.error:
            return False
        return all(r.passed for r in self.perspective_results.values())

    @property
    def perspectives_validated(self) -> List[str]:
        """List of perspectives that were validated."""
        return list(self.perspective_results.keys())

    def get_perspective_result(self, perspective: str) -> Optional[ValidationResult]:
        """Get result for a specific perspective."""
        return self.perspective_results.get(perspective)

    def get_failed_perspectives(self) -> List[str]:
        """Get list of perspectives that failed."""
        return [p for p, r in self.perspective_results.items() if not r.passed]


@dataclass
class BatchValidationResult:
    """Results from validating multiple analysis pairs across all perspectives."""
    results: List[AnalysisPairResult] = field(default_factory=list)
    perspectives: List[str] = field(default_factory=lambda: ['GR', 'GU', 'RL'])
    include_plt: Union[bool, str] = 'auto'  # 'auto', True, or False
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    skipped_pairs: List[Dict[str, Any]] = field(default_factory=list)  # Pairs that couldn't be resolved

    @property
    def passed(self) -> bool:
        """Returns True if all validations passed."""
        return all(r.passed for r in self.results)

    @property
    def total_count(self) -> int:
        """Total number of analysis pairs validated."""
        return len(self.results)

    @property
    def passed_count(self) -> int:
        """Number of analysis pairs that passed (all perspectives)."""
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        """Number of analysis pairs that failed (any perspective)."""
        return sum(1 for r in self.results if not r.passed)

    @property
    def pass_rate(self) -> float:
        """Pass rate as a percentage."""
        if self.total_count == 0:
            return 0.0
        return (self.passed_count / self.total_count) * 100

    def get_failed_results(self) -> List[AnalysisPairResult]:
        """Get only the failed analysis pair results."""
        return [r for r in self.results if not r.passed]

    def get_passed_results(self) -> List[AnalysisPairResult]:
        """Get only the passed analysis pair results."""
        return [r for r in self.results if r.passed]

    @property
    def skipped_count(self) -> int:
        """Number of analysis pairs that were skipped (couldn't be resolved)."""
        return len(self.skipped_pairs)

    @property
    def total_input_count(self) -> int:
        """Total number of analysis pairs in input (validated + skipped)."""
        return self.total_count + self.skipped_count


# =============================================================================
# Comparison Functions
# =============================================================================

def values_match(a: Any, b: Any, rel_tol: float = 1e-9) -> bool:
    """Compare two values with tolerance for floats."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if a == 0 and b == 0:
            return True
        return math.isclose(a, b, rel_tol=rel_tol)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        return all(values_match(x, y, rel_tol) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(values_match(a[k], b[k], rel_tol) for k in a.keys())
    return a == b


def compare_records(
    prod_record: Dict[str, Any],
    test_record: Dict[str, Any],
    key_fields: set,
    fields_to_compare: set = None,
    rel_tol: float = 1e-9
) -> List[Dict[str, Any]]:
    """Compare two records and return list of field differences.

    Args:
        prod_record: Production record
        test_record: Test record
        key_fields: Fields used as key (will be skipped in comparison)
        fields_to_compare: If provided, only compare these fields.
                          If None, compare all fields except IGNORED_FIELDS.
        rel_tol: Relative tolerance for float comparison
    """
    differences = []

    # Determine which fields to compare
    if fields_to_compare:
        # Use allowlist - only compare specified fields (excluding keys)
        all_keys = fields_to_compare - key_fields
    else:
        # Compare all fields except ignored ones and keys
        all_keys = (set(prod_record.keys()) | set(test_record.keys())) - IGNORED_FIELDS - key_fields

    for key in all_keys:
        prod_val = prod_record.get(key)
        test_val = test_record.get(key)

        if not values_match(prod_val, test_val, rel_tol):
            differences.append({
                'field': key,
                'prod_value': prod_val,
                'test_value': test_val
            })

    return differences


def compare_datasets(
    prod_data: List[Dict[str, Any]],
    test_data: List[Dict[str, Any]],
    key_field: str,
    endpoint_name: str,
    fields_to_compare: set = None,
    rel_tol: float = 1e-9
) -> ComparisonResult:
    """Compare two datasets by matching on key_field."""
    # Build lookup dictionaries
    prod_by_key = {r.get(key_field): r for r in prod_data}
    test_by_key = {r.get(key_field): r for r in test_data}

    prod_keys = set(prod_by_key.keys())
    test_keys = set(test_by_key.keys())

    # Find missing/extra records
    missing_in_test = list(prod_keys - test_keys)
    extra_in_test = list(test_keys - prod_keys)
    common_keys = prod_keys & test_keys

    # Compare common records
    all_differences = []
    for key in common_keys:
        diffs = compare_records(
            prod_by_key[key],
            test_by_key[key],
            {key_field},
            fields_to_compare,
            rel_tol
        )
        if diffs:
            all_differences.append({
                'key': key,
                'differences': diffs
            })

    passed = (len(missing_in_test) == 0 and
              len(extra_in_test) == 0 and
              len(all_differences) == 0)

    return ComparisonResult(
        endpoint=endpoint_name,
        passed=passed,
        total_records_prod=len(prod_data),
        total_records_test=len(test_data),
        differences=all_differences,
        missing_in_test=missing_in_test,
        extra_in_test=extra_in_test
    )


def compare_datasets_composite_key(
    prod_data: List[Dict[str, Any]],
    test_data: List[Dict[str, Any]],
    key_fields: List[str],
    endpoint_name: str,
    fields_to_compare: set = None,
    rel_tol: float = 1e-9
) -> ComparisonResult:
    """Compare two datasets using a composite key (multiple fields)."""
    def make_key(record: Dict[str, Any]) -> tuple:
        return tuple(record.get(k) for k in key_fields)

    # Build lookup dictionaries
    prod_by_key = {make_key(r): r for r in prod_data}
    test_by_key = {make_key(r): r for r in test_data}

    prod_keys = set(prod_by_key.keys())
    test_keys = set(test_by_key.keys())

    # Find missing/extra records
    missing_in_test = list(prod_keys - test_keys)
    extra_in_test = list(test_keys - prod_keys)
    common_keys = prod_keys & test_keys

    # Compare common records
    all_differences = []
    for key in common_keys:
        diffs = compare_records(
            prod_by_key[key],
            test_by_key[key],
            set(key_fields),
            fields_to_compare,
            rel_tol
        )
        if diffs:
            # Format composite key for display
            key_display = dict(zip(key_fields, key))
            all_differences.append({
                'key': key_display,
                'differences': diffs
            })

    passed = (len(missing_in_test) == 0 and
              len(extra_in_test) == 0 and
              len(all_differences) == 0)

    return ComparisonResult(
        endpoint=endpoint_name,
        passed=passed,
        total_records_prod=len(prod_data),
        total_records_test=len(test_data),
        differences=all_differences,
        missing_in_test=missing_in_test,
        extra_in_test=extra_in_test
    )


def compare_by_index(
    prod_data: List[Dict[str, Any]],
    test_data: List[Dict[str, Any]],
    endpoint_name: str,
    fields_to_compare: set = None,
    rel_tol: float = 1e-9
) -> ComparisonResult:
    """Compare data by index position (for stats/EP without unique keys)."""
    if len(prod_data) != len(test_data):
        return ComparisonResult(
            endpoint=endpoint_name,
            passed=False,
            total_records_prod=len(prod_data),
            total_records_test=len(test_data),
            differences=[],
            missing_in_test=[],
            extra_in_test=[],
            error=f"Record count mismatch: prod={len(prod_data)}, test={len(test_data)}"
        )

    all_differences = []
    for i, (prod_rec, test_rec) in enumerate(zip(prod_data, test_data)):
        diffs = compare_records(
            prod_rec,
            test_rec,
            key_fields={'_index_'},
            fields_to_compare=fields_to_compare,
            rel_tol=rel_tol
        )
        if diffs:
            all_differences.append({
                'key': f'record_{i}',
                'differences': diffs
            })

    return ComparisonResult(
        endpoint=endpoint_name,
        passed=len(all_differences) == 0,
        total_records_prod=len(prod_data),
        total_records_test=len(test_data),
        differences=all_differences,
        missing_in_test=[],
        extra_in_test=[]
    )


# =============================================================================
# File Input Parsing (CSV/XLSX)
# =============================================================================

# All perspective codes to validate
ALL_PERSPECTIVES = ['GR', 'GU', 'RL']

# Default output folder for validation results
VALIDATION_OUTPUTS_FOLDER = 'validation_outputs'


def load_analysis_pairs(
    file_path: Union[str, Path]
) -> List[Dict[str, Any]]:
    """Load analysis pairs from a CSV or XLSX file.

    Expected columns:
    - production_app_analysis_id (required): App analysis ID for production baseline
    - test_app_analysis_id (required): App analysis ID for test run
    - name (optional): Label/description for this analysis pair

    Args:
        file_path: Path to CSV or XLSX file

    Returns:
        List of dicts with keys: production_app_analysis_id, test_app_analysis_id, name
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == '.csv':
        return _load_pairs_from_csv(file_path)
    elif suffix in ('.xlsx', '.xls'):
        return _load_pairs_from_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}. Use .csv or .xlsx")


def _load_pairs_from_csv(file_path: Path) -> List[Dict[str, Any]]:
    """Load analysis pairs from a CSV file.

    Skips rows with missing or invalid production/test analysis IDs.
    """
    pairs = []
    skipped_rows = []

    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # Validate required columns
        required_cols = {'production_app_analysis_id', 'test_app_analysis_id'}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"File must have columns: {required_cols}. "
                f"Found: {reader.fieldnames}"
            )

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            prod_val = row.get('production_app_analysis_id', '').strip()
            test_val = row.get('test_app_analysis_id', '').strip()

            # Skip rows with missing values
            if not prod_val or not test_val:
                skipped_rows.append(row_num)
                continue

            try:
                prod_id = int(prod_val)
                test_id = int(test_val)
            except (ValueError, TypeError):
                # Skip rows with non-integer values
                skipped_rows.append(row_num)
                continue

            pairs.append({
                'production_app_analysis_id': prod_id,
                'test_app_analysis_id': test_id,
                'name': row.get('name', '').strip() or None
            })

    if skipped_rows:
        print(f"Skipped {len(skipped_rows)} incomplete row(s): {skipped_rows}")

    return pairs


def _load_pairs_from_excel(file_path: Path) -> List[Dict[str, Any]]:
    """Load analysis pairs from an Excel file.

    Supports two formats:
    1. Legacy format with columns: production_app_analysis_id, test_app_analysis_id, name (optional)
    2. New format with columns: Analysis Name, Prod Database, Prod Analysis ID, Test Database, Test Analysis ID

    For the new format, analysis IDs can be empty - they will be looked up by the validator.
    Skips rows with missing or invalid production/test analysis IDs in legacy format.
    """
    df = pd.read_excel(file_path)

    # Check if this is the new format (has 'Analysis Name' column)
    new_format_cols = {'Analysis Name', 'Prod Database', 'Test Database'}
    legacy_format_cols = {'production_app_analysis_id', 'test_app_analysis_id'}

    if new_format_cols.issubset(set(df.columns)):
        return _load_pairs_from_excel_new_format(df)
    elif legacy_format_cols.issubset(set(df.columns)):
        return _load_pairs_from_excel_legacy_format(df)
    else:
        raise ValueError(
            f"Excel file must have either:\n"
            f"  - New format columns: {new_format_cols}\n"
            f"  - Legacy format columns: {legacy_format_cols}\n"
            f"Found: {list(df.columns)}"
        )


def _load_pairs_from_excel_new_format(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Load analysis pairs from Excel using new format with EDM names.

    New format columns:
    - Analysis Name (required): Name of the analysis
    - Prod Database (required): Production EDM name
    - Prod Analysis ID (optional): Will be looked up if empty
    - Test Database (required): Test EDM name
    - Test Analysis ID (optional): Will be looked up if empty

    Returns pairs with lookup info for analyses that need ID resolution.
    """
    pairs = []
    skipped_rows = []

    for row_num, row in df.iterrows():
        analysis_name = row.get('Analysis Name')
        prod_edm = row.get('Prod Database')
        test_edm = row.get('Test Database')
        prod_id = row.get('Prod Analysis ID')
        test_id = row.get('Test Analysis ID')

        # Skip rows with missing required fields
        if pd.isna(analysis_name) or pd.isna(prod_edm) or pd.isna(test_edm):
            skipped_rows.append(row_num + 2)  # +2 for 1-indexed + header
            continue

        analysis_name = str(analysis_name).strip()
        prod_edm = str(prod_edm).strip()
        test_edm = str(test_edm).strip()

        if not analysis_name or not prod_edm or not test_edm:
            skipped_rows.append(row_num + 2)
            continue

        # Handle optional IDs - convert to int if present, None if missing
        try:
            prod_id = int(prod_id) if not pd.isna(prod_id) else None
        except (ValueError, TypeError):
            prod_id = None

        try:
            test_id = int(test_id) if not pd.isna(test_id) else None
        except (ValueError, TypeError):
            test_id = None

        pairs.append({
            'name': analysis_name,
            'analysis_name': analysis_name,
            'prod_edm_name': prod_edm,
            'test_edm_name': test_edm,
            'production_app_analysis_id': prod_id,
            'test_app_analysis_id': test_id,
            'needs_lookup': prod_id is None or test_id is None
        })

    if skipped_rows:
        print(f"Skipped {len(skipped_rows)} row(s) with missing required fields: {skipped_rows}")

    return pairs


def _load_pairs_from_excel_legacy_format(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Load analysis pairs from Excel using legacy format with direct IDs.

    Legacy format columns:
    - production_app_analysis_id (required)
    - test_app_analysis_id (required)
    - name (optional)

    Skips rows with missing or invalid production/test analysis IDs.
    """
    pairs = []
    skipped_rows = []

    for row_num, row in df.iterrows():
        prod_val = row['production_app_analysis_id']
        test_val = row['test_app_analysis_id']

        # Skip rows with missing values (NaN, None, empty)
        if pd.isna(prod_val) or pd.isna(test_val):
            skipped_rows.append(row_num + 2)  # +2 for 1-indexed + header
            continue

        try:
            prod_id = int(prod_val)
            test_id = int(test_val)
        except (ValueError, TypeError):
            # Skip rows with non-integer values
            skipped_rows.append(row_num + 2)
            continue

        name = row.get('name', '')
        if pd.isna(name):
            name = None
        elif isinstance(name, str):
            name = name.strip() or None

        pairs.append({
            'production_app_analysis_id': prod_id,
            'test_app_analysis_id': test_id,
            'name': name,
            'needs_lookup': False
        })

    if skipped_rows:
        print(f"Skipped {len(skipped_rows)} incomplete row(s): {skipped_rows}")

    return pairs


# Keep old function name for backwards compatibility
def load_analysis_pairs_from_csv(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Deprecated: Use load_analysis_pairs() instead."""
    return load_analysis_pairs(file_path)


# =============================================================================
# Main Validator Class
# =============================================================================

class AnalysisResultsValidator:
    """Validates analysis results by comparing production and test outputs."""

    def __init__(self, irp_client: IRPClient = None):
        """Initialize validator with optional IRP client.

        Args:
            irp_client: IRPClient instance. If None, creates a new one.
        """
        self.irp_client = irp_client or IRPClient()

    def lookup_analysis_id(
        self,
        analysis_name: str,
        edm_name: str
    ) -> Optional[int]:
        """Look up app_analysis_id by analysis name and EDM name.

        Uses LIKE clauses for flexible matching since EDM names may have
        slight variations (e.g., 'RM_' vs 'RMS_' prefixes).

        Args:
            analysis_name: Name of the analysis
            edm_name: Name of the EDM (exposure database)

        Returns:
            app_analysis_id if found, None if not found

        Raises:
            IRPAPIError: If API call fails (not including "not found")
        """
        try:
            # Use LIKE for flexible matching on EDM name
            filter_str = f'analysisName = "{analysis_name}" AND exposureName LIKE "%{edm_name}%"'
            analyses = self.irp_client.analysis.search_analyses(filter=filter_str)

            if len(analyses) == 0:
                # Try with just LIKE on analysis name too in case of minor differences
                filter_str = f'analysisName LIKE "%{analysis_name}%" AND exposureName LIKE "%{edm_name}%"'
                analyses = self.irp_client.analysis.search_analyses(filter=filter_str)

            if len(analyses) == 0:
                return None
            if len(analyses) > 1:
                # Multiple matches - try to find exact match on analysis name
                exact_matches = [a for a in analyses if a.get('analysisName') == analysis_name]
                if len(exact_matches) == 1:
                    return exact_matches[0].get('appAnalysisId')
                # Still multiple - return first match but log warning
                print(f"  Warning: Multiple analyses found for '{analysis_name}' in EDM like '{edm_name}', using first match")

            return analyses[0].get('appAnalysisId')
        except Exception as e:
            # Check if it's a "not found" error - return None in that case
            error_msg = str(e).lower()
            if 'not found' in error_msg or 'no analysis' in error_msg:
                return None
            # Re-raise other errors
            raise

    def validate(
        self,
        production_app_analysis_id: int,
        test_app_analysis_id: int,
        perspective_code: str = 'GR',
        include_plt: Union[bool, str] = 'auto',
        relative_tolerance: float = 1e-9
    ) -> ValidationResult:
        """Validate test analysis against production analysis.

        Args:
            production_app_analysis_id: App analysis ID for production (baseline)
            test_app_analysis_id: App analysis ID for test run
            perspective_code: Perspective code ('GR', 'GU', 'RL')
            include_plt: Whether to include PLT comparison.
                - 'auto' (default): Include PLT only for HD analyses (inferred from engineType)
                - True: Always include PLT
                - False: Never include PLT
            relative_tolerance: Tolerance for floating-point comparison

        Returns:
            ValidationResult containing all comparison results
        """
        result = ValidationResult(
            production_app_analysis_id=production_app_analysis_id,
            test_app_analysis_id=test_app_analysis_id,
            perspective_code=perspective_code
        )

        # Fetch analysis metadata
        prod_analysis = self.irp_client.analysis.get_analysis_by_app_analysis_id(
            production_app_analysis_id
        )
        test_analysis = self.irp_client.analysis.get_analysis_by_app_analysis_id(
            test_app_analysis_id
        )

        prod_analysis_id = prod_analysis['analysisId']
        test_analysis_id = test_analysis['analysisId']
        prod_exposure_resource_id = prod_analysis['exposureResourceId']
        test_exposure_resource_id = test_analysis['exposureResourceId']

        # Determine whether to include PLT based on engine type
        if include_plt == 'auto':
            engine_type = prod_analysis.get('engineType')
            should_include_plt = (engine_type == 'HD')
        else:
            should_include_plt = bool(include_plt)

        # Compare Analysis Settings (configuration, not results)
        result.results.append(self._compare_settings(prod_analysis, test_analysis))

        # Compare Statistics
        result.results.append(self._compare_stats(
            prod_analysis_id, test_analysis_id,
            perspective_code,
            prod_exposure_resource_id, test_exposure_resource_id,
            relative_tolerance
        ))

        # Compare EP Metrics
        result.results.append(self._compare_ep(
            prod_analysis_id, test_analysis_id,
            perspective_code,
            prod_exposure_resource_id, test_exposure_resource_id,
            relative_tolerance
        ))

        # Compare ELT
        result.results.append(self._compare_elt(
            prod_analysis_id, test_analysis_id,
            perspective_code,
            prod_exposure_resource_id, test_exposure_resource_id,
            relative_tolerance
        ))

        # Compare PLT (HD analyses only, or when explicitly requested)
        if should_include_plt:
            result.results.append(self._compare_plt(
                prod_analysis_id, test_analysis_id,
                perspective_code,
                prod_exposure_resource_id, test_exposure_resource_id,
                relative_tolerance
            ))

        return result

    def resolve_analysis_pairs(
        self,
        analysis_pairs: List[Dict[str, Any]],
        progress_callback: callable = None
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Resolve analysis pairs that need ID lookup.

        For pairs with 'needs_lookup' flag, looks up production and test analysis IDs
        using the analysis name and EDM names. Populates the IDs in the pair dict.

        Args:
            analysis_pairs: List of analysis pair dicts (may have 'needs_lookup' flag)
            progress_callback: Optional callback(current, total, name, status) for progress

        Returns:
            Tuple of (resolved_pairs, skipped_pairs):
            - resolved_pairs: Pairs with both IDs resolved (ready for validation)
            - skipped_pairs: Pairs where one or both analyses were not found
        """
        resolved_pairs = []
        skipped_pairs = []

        total = len(analysis_pairs)
        for i, pair in enumerate(analysis_pairs):
            name = pair.get('name') or pair.get('analysis_name', 'Unknown')

            # Check if this pair needs lookup
            if not pair.get('needs_lookup', False):
                # Already has IDs, just validate they exist
                if pair.get('production_app_analysis_id') and pair.get('test_app_analysis_id'):
                    resolved_pairs.append(pair)
                else:
                    skipped_pairs.append({
                        **pair,
                        'skip_reason': 'Missing analysis IDs'
                    })
                continue

            if progress_callback:
                progress_callback(i + 1, total, name, 'Looking up')

            # Need to look up IDs
            analysis_name = pair.get('analysis_name')
            prod_edm = pair.get('prod_edm_name')
            test_edm = pair.get('test_edm_name')

            if not analysis_name or not prod_edm or not test_edm:
                skipped_pairs.append({
                    **pair,
                    'skip_reason': 'Missing analysis_name, prod_edm_name, or test_edm_name'
                })
                continue

            # Look up production analysis ID if needed
            prod_id = pair.get('production_app_analysis_id')
            if prod_id is None:
                try:
                    prod_id = self.lookup_analysis_id(analysis_name, prod_edm)
                except Exception as e:
                    skipped_pairs.append({
                        **pair,
                        'skip_reason': f'Error looking up prod analysis: {e}'
                    })
                    continue

            if prod_id is None:
                skipped_pairs.append({
                    **pair,
                    'skip_reason': f'Production analysis not found: {analysis_name} in {prod_edm}'
                })
                continue

            # Look up test analysis ID if needed
            test_id = pair.get('test_app_analysis_id')
            if test_id is None:
                try:
                    test_id = self.lookup_analysis_id(analysis_name, test_edm)
                except Exception as e:
                    skipped_pairs.append({
                        **pair,
                        'skip_reason': f'Error looking up test analysis: {e}'
                    })
                    continue

            if test_id is None:
                skipped_pairs.append({
                    **pair,
                    'production_app_analysis_id': prod_id,  # Save the prod ID we found
                    'skip_reason': f'Test analysis not found: {analysis_name} in {test_edm}'
                })
                continue

            # Both IDs resolved successfully
            resolved_pair = {
                **pair,
                'production_app_analysis_id': prod_id,
                'test_app_analysis_id': test_id,
                'needs_lookup': False
            }
            resolved_pairs.append(resolved_pair)

        return resolved_pairs, skipped_pairs

    def validate_batch(
        self,
        analysis_pairs: List[Dict[str, Any]],
        perspectives: List[str] = None,
        include_plt: Union[bool, str] = 'auto',
        relative_tolerance: float = 1e-9,
        progress_callback: callable = None
    ) -> BatchValidationResult:
        """Validate multiple analysis pairs across all perspectives.

        Args:
            analysis_pairs: List of dicts with keys:
                - production_app_analysis_id (required, or use needs_lookup)
                - test_app_analysis_id (required, or use needs_lookup)
                - name (optional)
                For new format with lookup:
                - needs_lookup: True if IDs need to be resolved
                - analysis_name: Analysis name for lookup
                - prod_edm_name: Production EDM name
                - test_edm_name: Test EDM name
            perspectives: List of perspective codes to validate (default: ['GR', 'GU', 'RL'])
            include_plt: Whether to include PLT comparison.
                - 'auto' (default): Include PLT only for HD analyses (inferred from engineType)
                - True: Always include PLT
                - False: Never include PLT
            relative_tolerance: Tolerance for floating-point comparison
            progress_callback: Optional callback(current, total, name, perspective) for progress

        Returns:
            BatchValidationResult containing all validation results
        """
        if perspectives is None:
            perspectives = ALL_PERSPECTIVES

        batch_result = BatchValidationResult(
            perspectives=perspectives,
            include_plt=include_plt
        )

        total = len(analysis_pairs)
        for i, pair in enumerate(analysis_pairs):
            prod_id = pair['production_app_analysis_id']
            test_id = pair['test_app_analysis_id']
            name = pair.get('name')

            pair_result = AnalysisPairResult(
                production_app_analysis_id=prod_id,
                test_app_analysis_id=test_id,
                name=name
            )

            # Validate each perspective
            for perspective in perspectives:
                if progress_callback:
                    progress_callback(
                        i + 1, total,
                        name or f"{prod_id} vs {test_id}",
                        perspective
                    )

                try:
                    result = self.validate(
                        production_app_analysis_id=prod_id,
                        test_app_analysis_id=test_id,
                        perspective_code=perspective,
                        include_plt=include_plt,
                        relative_tolerance=relative_tolerance
                    )
                    result.name = name
                    pair_result.perspective_results[perspective] = result
                except Exception as e:
                    # If validation fails for this perspective
                    result = ValidationResult(
                        production_app_analysis_id=prod_id,
                        test_app_analysis_id=test_id,
                        perspective_code=perspective,
                        name=name,
                        error=str(e)
                    )
                    pair_result.perspective_results[perspective] = result

            batch_result.results.append(pair_result)

        return batch_result

    def validate_batch_from_file(
        self,
        file_path: Union[str, Path],
        perspectives: List[str] = None,
        include_plt: Union[bool, str] = 'auto',
        relative_tolerance: float = 1e-9,
        progress_callback: callable = None,
        output_file: Union[str, Path] = None
    ) -> BatchValidationResult:
        """Validate multiple analysis pairs from a CSV or XLSX file.

        Supports two file formats:
        1. Legacy format with columns: production_app_analysis_id, test_app_analysis_id, name
        2. New format with columns: Analysis Name, Prod Database, Prod Analysis ID, Test Database, Test Analysis ID

        For the new format, analysis IDs can be empty - they will be looked up automatically
        using the analysis name and EDM names. Analyses that cannot be found are skipped.

        Args:
            file_path: Path to CSV or XLSX file with analysis pairs
            perspectives: List of perspective codes to validate (default: ['GR', 'GU', 'RL'])
            include_plt: Whether to include PLT comparison.
                - 'auto' (default): Include PLT only for HD analyses (inferred from engineType)
                - True: Always include PLT
                - False: Never include PLT
            relative_tolerance: Tolerance for floating-point comparison
            progress_callback: Optional callback(current, total, name, status) for progress
            output_file: Optional path to write updated Excel file with resolved IDs

        Returns:
            BatchValidationResult containing all validation results.
            The result also includes 'skipped_pairs' attribute with pairs that couldn't be resolved.
        """
        pairs = load_analysis_pairs(file_path)

        # Check if any pairs need lookup
        needs_lookup = any(pair.get('needs_lookup', False) for pair in pairs)

        if needs_lookup:
            print(f"Resolving analysis IDs for {len(pairs)} analysis pairs...")
            resolved_pairs, skipped_pairs = self.resolve_analysis_pairs(
                pairs, progress_callback=progress_callback
            )

            if skipped_pairs:
                print(f"\nSkipped {len(skipped_pairs)} analysis pair(s):")
                for pair in skipped_pairs:
                    name = pair.get('name') or pair.get('analysis_name', 'Unknown')
                    reason = pair.get('skip_reason', 'Unknown reason')
                    print(f"  - {name}: {reason}")
                print()

            if not resolved_pairs:
                print("No analysis pairs could be resolved. Nothing to validate.")
                result = BatchValidationResult(
                    perspectives=perspectives or ALL_PERSPECTIVES,
                    include_plt=include_plt
                )
                result.skipped_pairs = skipped_pairs
                return result

            print(f"Found {len(resolved_pairs)} analysis pair(s) to validate.")

            # Optionally write updated file with resolved IDs
            if output_file:
                self._write_resolved_pairs_to_excel(
                    file_path, resolved_pairs, skipped_pairs, output_file
                )
        else:
            resolved_pairs = pairs
            skipped_pairs = []

        result = self.validate_batch(
            analysis_pairs=resolved_pairs,
            perspectives=perspectives,
            include_plt=include_plt,
            relative_tolerance=relative_tolerance,
            progress_callback=progress_callback
        )

        # Attach skipped pairs to result for reference
        result.skipped_pairs = skipped_pairs
        return result

    def _write_resolved_pairs_to_excel(
        self,
        input_file: Union[str, Path],
        resolved_pairs: List[Dict[str, Any]],
        skipped_pairs: List[Dict[str, Any]],
        output_file: Union[str, Path]
    ) -> None:
        """Write resolved analysis pairs back to an Excel file.

        Updates the original Excel with resolved analysis IDs and adds a status column.
        """
        # Read original file
        df = pd.read_excel(input_file)

        # Create lookup for resolved and skipped pairs
        resolved_lookup = {
            pair.get('analysis_name'): pair for pair in resolved_pairs
        }
        skipped_lookup = {
            pair.get('analysis_name'): pair for pair in skipped_pairs
        }

        # Update IDs and add status column
        statuses = []
        for _, row in df.iterrows():
            analysis_name = row.get('Analysis Name')
            if analysis_name in resolved_lookup:
                pair = resolved_lookup[analysis_name]
                df.loc[df['Analysis Name'] == analysis_name, 'Prod Analysis ID'] = pair['production_app_analysis_id']
                df.loc[df['Analysis Name'] == analysis_name, 'Test Analysis ID'] = pair['test_app_analysis_id']
                statuses.append('Resolved')
            elif analysis_name in skipped_lookup:
                pair = skipped_lookup[analysis_name]
                # Update prod ID if we found it
                if pair.get('production_app_analysis_id'):
                    df.loc[df['Analysis Name'] == analysis_name, 'Prod Analysis ID'] = pair['production_app_analysis_id']
                statuses.append(f"Skipped: {pair.get('skip_reason', 'Unknown')}")
            else:
                statuses.append('Unknown')

        df['Lookup Status'] = statuses

        # Write to output file
        df.to_excel(output_file, index=False)
        print(f"Updated Excel file written to: {output_file}")

    # Keep old method name for backwards compatibility
    def validate_batch_from_csv(
        self,
        csv_path: Union[str, Path],
        perspectives: List[str] = None,
        include_plt: Union[bool, str] = 'auto',
        relative_tolerance: float = 1e-9,
        progress_callback: callable = None
    ) -> BatchValidationResult:
        """Deprecated: Use validate_batch_from_file() instead."""
        return self.validate_batch_from_file(
            file_path=csv_path,
            perspectives=perspectives,
            include_plt=include_plt,
            relative_tolerance=relative_tolerance,
            progress_callback=progress_callback
        )

    def _compare_stats(
        self,
        prod_analysis_id: int,
        test_analysis_id: int,
        perspective_code: str,
        prod_exposure_resource_id: int,
        test_exposure_resource_id: int,
        rel_tol: float
    ) -> ComparisonResult:
        """Compare Statistics endpoint."""
        try:
            prod_data = self.irp_client.analysis.get_stats(
                prod_analysis_id, perspective_code, prod_exposure_resource_id
            )
            test_data = self.irp_client.analysis.get_stats(
                test_analysis_id, perspective_code, test_exposure_resource_id
            )
            return compare_by_index(prod_data, test_data, 'Statistics', STATS_FIELDS, rel_tol)
        except Exception as e:
            return ComparisonResult(
                endpoint='Statistics',
                passed=False,
                total_records_prod=0,
                total_records_test=0,
                error=str(e)
            )

    def _compare_ep(
        self,
        prod_analysis_id: int,
        test_analysis_id: int,
        perspective_code: str,
        prod_exposure_resource_id: int,
        test_exposure_resource_id: int,
        rel_tol: float
    ) -> ComparisonResult:
        """Compare EP Metrics endpoint."""
        try:
            prod_data = self.irp_client.analysis.get_ep(
                prod_analysis_id, perspective_code, prod_exposure_resource_id
            )
            test_data = self.irp_client.analysis.get_ep(
                test_analysis_id, perspective_code, test_exposure_resource_id
            )
            return compare_by_index(prod_data, test_data, 'EP Metrics', EP_FIELDS, rel_tol)
        except Exception as e:
            return ComparisonResult(
                endpoint='EP Metrics',
                passed=False,
                total_records_prod=0,
                total_records_test=0,
                error=str(e)
            )

    def _compare_elt(
        self,
        prod_analysis_id: int,
        test_analysis_id: int,
        perspective_code: str,
        prod_exposure_resource_id: int,
        test_exposure_resource_id: int,
        rel_tol: float
    ) -> ComparisonResult:
        """Compare ELT endpoint."""
        try:
            prod_data = self.irp_client.analysis.get_elt(
                prod_analysis_id, perspective_code, prod_exposure_resource_id
            )
            test_data = self.irp_client.analysis.get_elt(
                test_analysis_id, perspective_code, test_exposure_resource_id
            )
            return compare_datasets(prod_data, test_data, 'eventId', 'ELT', ELT_FIELDS, rel_tol)
        except Exception as e:
            return ComparisonResult(
                endpoint='ELT',
                passed=False,
                total_records_prod=0,
                total_records_test=0,
                error=str(e)
            )

    def _compare_plt(
        self,
        prod_analysis_id: int,
        test_analysis_id: int,
        perspective_code: str,
        prod_exposure_resource_id: int,
        test_exposure_resource_id: int,
        rel_tol: float
    ) -> ComparisonResult:
        """Compare PLT endpoint."""
        try:
            prod_data = self.irp_client.analysis.get_plt(
                prod_analysis_id, perspective_code, prod_exposure_resource_id
            )
            test_data = self.irp_client.analysis.get_plt(
                test_analysis_id, perspective_code, test_exposure_resource_id
            )
            return compare_datasets_composite_key(
                prod_data, test_data,
                key_fields=['periodId', 'eventId'],
                endpoint_name='PLT',
                fields_to_compare=PLT_FIELDS,
                rel_tol=rel_tol
            )
        except Exception as e:
            return ComparisonResult(
                endpoint='PLT',
                passed=False,
                total_records_prod=0,
                total_records_test=0,
                error=str(e)
            )

    def _compare_settings(
        self,
        prod_analysis: Dict[str, Any],
        test_analysis: Dict[str, Any]
    ) -> ComparisonResult:
        """Compare analysis settings between production and test.

        Compares configuration fields from the search_analyses response,
        excluding IDs, dates, currency, and metadata.
        """
        differences = []

        # Get the raw analysis data (full response from search_analyses)
        prod_raw = prod_analysis.get('raw', prod_analysis)
        test_raw = test_analysis.get('raw', test_analysis)

        for field in SETTINGS_FIELDS:
            prod_val = prod_raw.get(field)
            test_val = test_raw.get(field)

            # For nested objects like modelProfile, compare by name only (ignore IDs)
            if field in ('modelProfile', 'outputProfile'):
                prod_val = prod_val.get('name') if isinstance(prod_val, dict) else prod_val
                test_val = test_val.get('name') if isinstance(test_val, dict) else test_val
            elif field == 'eventRateSchemeNames':
                # Compare list of scheme names (ignore IDs)
                prod_val = [s.get('name') for s in prod_val] if isinstance(prod_val, list) else prod_val
                test_val = [s.get('name') for s in test_val] if isinstance(test_val, list) else test_val

            if not values_match(prod_val, test_val):
                differences.append({
                    'key': field,
                    'differences': [{
                        'field': field,
                        'prod_value': prod_val,
                        'test_value': test_val
                    }]
                })

        return ComparisonResult(
            endpoint='Settings',
            passed=len(differences) == 0,
            total_records_prod=1,
            total_records_test=1,
            differences=differences
        )


# =============================================================================
# Output Formatting
# =============================================================================

def _truncate_value(value: Any, max_items: int = 5) -> str:
    """Truncate long lists/arrays for display.

    Args:
        value: Value to format (may be list, dict, or scalar)
        max_items: Maximum number of items to show for lists

    Returns:
        String representation, truncated if necessary
    """
    if isinstance(value, list):
        if len(value) <= max_items:
            return str(value)
        shown = value[:max_items]
        return f"{shown} ... ({len(value)} items total)"
    if isinstance(value, dict):
        # For nested dicts like EP 'value' field, truncate inner arrays
        truncated = {}
        for k, v in value.items():
            if isinstance(v, list) and len(v) > max_items:
                truncated[k] = f"[{len(v)} items]"
            else:
                truncated[k] = v
        return str(truncated)
    return str(value)


def print_validation_summary(result: ValidationResult, max_differences: int = 50) -> None:
    """Print a formatted summary of validation results.

    Args:
        result: ValidationResult from AnalysisResultsValidator.validate()
        max_differences: Maximum number of differences to show per endpoint
    """
    print("=" * 60)
    print("ANALYSIS VALIDATION RESULTS")
    print("=" * 60)
    print()
    print(f"Production Analysis ID: {result.production_app_analysis_id}")
    print(f"Test Analysis ID:       {result.test_app_analysis_id}")
    print(f"Perspective:            {result.perspective_code}")
    print()
    print("-" * 60)
    print("Endpoint Results:")
    print("-" * 60)

    for r in result.results:
        status = "PASS" if r.passed else "FAIL"
        icon = "[OK]" if r.passed else "[X]"

        details = ""
        if r.error:
            details = f" (Error: {r.error})"
        elif not r.passed:
            issues = []
            if r.differences:
                issues.append(f"{len(r.differences)} value differences")
            if r.missing_in_test:
                issues.append(f"{len(r.missing_in_test)} missing in test")
            if r.extra_in_test:
                issues.append(f"{len(r.extra_in_test)} extra in test")
            details = f" ({', '.join(issues)})"

        print(f"  {icon} {r.endpoint}: {status}{details}")

    print()
    print("=" * 60)
    overall_status = "PASS" if result.passed else "FAIL"
    print(f"OVERALL: {overall_status}")
    print("=" * 60)


def print_validation_details(result: ValidationResult, max_differences: int = 50) -> None:
    """Print detailed differences for failed endpoints.

    Args:
        result: ValidationResult from AnalysisResultsValidator.validate()
        max_differences: Maximum number of differences to show per endpoint
    """
    if result.passed:
        print("\nNo differences found - all endpoints match!")
        return

    for r in result.results:
        if r.passed:
            continue

        print()
        print("=" * 60)
        print(f"{r.endpoint} DIFFERENCES")
        print("=" * 60)

        if r.error:
            print(f"\nError: {r.error}")
            continue

        # Missing records
        if r.missing_in_test:
            print(f"\nRecords in PRODUCTION but missing in TEST ({len(r.missing_in_test)} total):")
            shown = r.missing_in_test[:max_differences]
            for key in shown:
                print(f"  - {key}")
            if len(r.missing_in_test) > max_differences:
                print(f"  ... and {len(r.missing_in_test) - max_differences} more")

        # Extra records
        if r.extra_in_test:
            print(f"\nRecords in TEST but not in PRODUCTION ({len(r.extra_in_test)} total):")
            shown = r.extra_in_test[:max_differences]
            for key in shown:
                print(f"  - {key}")
            if len(r.extra_in_test) > max_differences:
                print(f"  ... and {len(r.extra_in_test) - max_differences} more")

        # Value differences
        if r.differences:
            print(f"\nValue differences ({len(r.differences)} records with differences):")
            shown = r.differences[:max_differences]
            for diff in shown:
                print(f"\n  Key: {diff['key']}")
                for field_diff in diff['differences']:
                    print(f"    {field_diff['field']}:")
                    print(f"      prod: {_truncate_value(field_diff['prod_value'])}")
                    print(f"      test: {_truncate_value(field_diff['test_value'])}")
            if len(r.differences) > max_differences:
                print(f"\n  ... and {len(r.differences) - max_differences} more records with differences")


# =============================================================================
# Batch Output Formatting
# =============================================================================

def batch_results_to_dataframe(result: BatchValidationResult) -> pd.DataFrame:
    """Convert batch validation results to a summary DataFrame.

    Args:
        result: BatchValidationResult from validate_batch()

    Returns:
        DataFrame with columns for each perspective and endpoint combination
    """
    rows = []
    for pair in result.results:
        row = {
            'name': pair.name or '',
            'prod_id': pair.production_app_analysis_id,
            'test_id': pair.test_app_analysis_id,
            'status': 'PASS' if pair.passed else 'FAIL',
        }

        if pair.error:
            # Error at pair level
            row['settings'] = 'ERROR'
            for perspective in result.perspectives:
                row[f'{perspective}_stats'] = 'ERROR'
                row[f'{perspective}_ep'] = 'ERROR'
                row[f'{perspective}_elt'] = 'ERROR'
                if result.include_plt:
                    row[f'{perspective}_plt'] = 'ERROR'
            row['error'] = pair.error
        else:
            # Settings is the same across all perspectives, get from first perspective
            first_persp = result.perspectives[0] if result.perspectives else None
            first_result = pair.get_perspective_result(first_persp) if first_persp else None
            if first_result and not first_result.error:
                row['settings'] = _endpoint_status(first_result.get_result('Settings'))
            else:
                row['settings'] = 'N/A'

            # Add per-perspective, per-endpoint status
            for perspective in result.perspectives:
                persp_result = pair.get_perspective_result(perspective)
                if persp_result is None:
                    row[f'{perspective}_stats'] = 'N/A'
                    row[f'{perspective}_ep'] = 'N/A'
                    row[f'{perspective}_elt'] = 'N/A'
                    if result.include_plt:
                        row[f'{perspective}_plt'] = 'N/A'
                elif persp_result.error:
                    row[f'{perspective}_stats'] = 'ERROR'
                    row[f'{perspective}_ep'] = 'ERROR'
                    row[f'{perspective}_elt'] = 'ERROR'
                    if result.include_plt:
                        row[f'{perspective}_plt'] = 'ERROR'
                else:
                    row[f'{perspective}_stats'] = _endpoint_status(persp_result.get_result('Statistics'))
                    row[f'{perspective}_ep'] = _endpoint_status(persp_result.get_result('EP Metrics'))
                    row[f'{perspective}_elt'] = _endpoint_status(persp_result.get_result('ELT'))
                    if result.include_plt:
                        row[f'{perspective}_plt'] = _endpoint_status(persp_result.get_result('PLT'))
            row['error'] = ''

        rows.append(row)

    return pd.DataFrame(rows)


def _endpoint_status(result: Optional[ComparisonResult]) -> str:
    """Get status string for an endpoint result."""
    if result is None:
        return 'N/A'
    if result.error:
        return 'ERROR'
    if result.passed:
        return 'PASS'
    # Build failure summary
    issues = []
    if result.differences:
        issues.append(f"{len(result.differences)} diff")
    if result.missing_in_test:
        issues.append(f"{len(result.missing_in_test)} miss")
    if result.extra_in_test:
        issues.append(f"{len(result.extra_in_test)} extra")
    return f"FAIL ({', '.join(issues)})" if issues else 'FAIL'


def print_batch_summary(result: BatchValidationResult) -> None:
    """Print a summary of batch validation results.

    Args:
        result: BatchValidationResult from validate_batch()
    """
    print("=" * 70)
    print("BATCH VALIDATION SUMMARY")
    print("=" * 70)
    print()
    if result.skipped_count > 0:
        print(f"Total Input:     {result.total_input_count}")
        print(f"Skipped:         {result.skipped_count}")
    print(f"Validated:       {result.total_count}")
    print(f"Passed:          {result.passed_count}")
    print(f"Failed:          {result.failed_count}")
    print(f"Pass Rate:       {result.pass_rate:.1f}%")
    print(f"Perspectives:    {', '.join(result.perspectives)}")
    print(f"Include PLT:     {result.include_plt}")
    print()

    # Show skipped pairs if any
    if result.skipped_pairs:
        print(f"Skipped analyses ({result.skipped_count}):")
        print("-" * 70)
        for pair in result.skipped_pairs:
            name = pair.get('name') or pair.get('analysis_name', 'Unknown')
            reason = pair.get('skip_reason', 'Unknown reason')
            print(f"  [SKIP]  {name}: {reason}")
        print()

    if result.passed and result.total_count > 0:
        print("All validated analyses passed!")
    elif result.total_count == 0:
        print("No analyses were validated.")
    else:
        print(f"Failed analyses ({result.failed_count}):")
        print("-" * 70)
        for pair in result.get_failed_results():
            name = pair.name or f"{pair.production_app_analysis_id} vs {pair.test_app_analysis_id}"
            if pair.error:
                print(f"  [ERROR] {name}: {pair.error}")
            else:
                # List failed perspectives and their failed endpoints
                failed_details = []
                for perspective in pair.get_failed_perspectives():
                    persp_result = pair.get_perspective_result(perspective)
                    if persp_result and not persp_result.error:
                        failed_endpoints = [
                            ep.endpoint for ep in persp_result.results if not ep.passed
                        ]
                        if failed_endpoints:
                            failed_details.append(f"{perspective}({', '.join(failed_endpoints)})")
                        else:
                            failed_details.append(perspective)
                    else:
                        failed_details.append(f"{perspective}(ERROR)")
                print(f"  [FAIL]  {name}: {', '.join(failed_details)}")

    print()
    print("=" * 70)
    overall = "PASS" if result.passed else "FAIL"
    print(f"OVERALL: {overall}")
    print("=" * 70)


def export_batch_failures_to_json(
    result: BatchValidationResult,
    filename: str = 'validation_failures.json',
    output_dir: Union[str, Path] = None,
    max_differences: int = 10
) -> Path:
    """Export detailed failure information to a JSON file.

    Args:
        result: BatchValidationResult from validate_batch()
        filename: Name of output JSON file (default: 'validation_failures.json')
        output_dir: Directory for output file. If None, uses 'validation_outputs' folder.
        max_differences: Maximum number of differences to include per endpoint

    Returns:
        Path to created file
    """
    if output_dir is None:
        output_dir = Path(VALIDATION_OUTPUTS_FOLDER)
    else:
        output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    failures = []
    for pair in result.get_failed_results():
        failure_data = {
            'name': pair.name,
            'production_app_analysis_id': pair.production_app_analysis_id,
            'test_app_analysis_id': pair.test_app_analysis_id,
        }

        if pair.error:
            failure_data['error'] = pair.error
            failure_data['perspectives'] = {}
        else:
            failure_data['error'] = None
            failure_data['perspectives'] = {}

            # Export each failed perspective
            for perspective in pair.get_failed_perspectives():
                persp_result = pair.get_perspective_result(perspective)
                if persp_result is None:
                    continue

                persp_data = {'error': persp_result.error, 'endpoints': []}

                if not persp_result.error:
                    for ep in persp_result.results:
                        if ep.passed:
                            continue

                        ep_data = {
                            'endpoint': ep.endpoint,
                            'total_records_prod': ep.total_records_prod,
                            'total_records_test': ep.total_records_test,
                        }

                        if ep.error:
                            ep_data['error'] = ep.error
                        else:
                            ep_data['error'] = None
                            ep_data['missing_in_test_count'] = len(ep.missing_in_test)
                            ep_data['extra_in_test_count'] = len(ep.extra_in_test)
                            ep_data['value_differences_count'] = len(ep.differences)

                            # Include sample of differences
                            ep_data['missing_in_test_sample'] = ep.missing_in_test[:max_differences]
                            ep_data['extra_in_test_sample'] = ep.extra_in_test[:max_differences]
                            ep_data['value_differences_sample'] = ep.differences[:max_differences]

                        persp_data['endpoints'].append(ep_data)

                failure_data['perspectives'][perspective] = persp_data

        failures.append(failure_data)

    output = {
        'timestamp': result.timestamp,
        'perspectives': result.perspectives,
        'include_plt': result.include_plt,
        'total_count': result.total_count,
        'passed_count': result.passed_count,
        'failed_count': result.failed_count,
        'failures': failures
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, default=str)

    return output_path


def export_batch_summary_to_csv(
    result: BatchValidationResult,
    filename: str = 'validation_summary.csv',
    output_dir: Union[str, Path] = None
) -> Path:
    """Export batch validation summary to a CSV file.

    Args:
        result: BatchValidationResult from validate_batch()
        filename: Name of output CSV file (default: 'validation_summary.csv')
        output_dir: Directory for output file. If None, uses 'validation_outputs' folder.

    Returns:
        Path to created file
    """
    if output_dir is None:
        output_dir = Path(VALIDATION_OUTPUTS_FOLDER)
    else:
        output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    df = batch_results_to_dataframe(result)
    df.to_csv(output_path, index=False)
    return output_path