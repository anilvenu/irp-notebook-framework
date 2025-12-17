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

# PLT uses composite key (eventId, periodId, eventDate, lossDate) to uniquely identify records
PLT_FIELDS = {
    'eventId',       # Key field
    'periodId',      # Key field
    'eventDate',     # Key field (event occurrence date)
    'lossDate',      # Key field (loss date)
    'weight',        # Period weight
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


def compare_ep_curves(
    prod_data: List[Dict[str, Any]],
    test_data: List[Dict[str, Any]],
    rel_tol: float = 1e-9,
    max_point_diffs: int = 5
) -> ComparisonResult:
    """Compare EP curves with detailed return period differences.

    EP data structure:
    [
        {
            "epType": "OEP",
            "value": {
                "returnPeriods": [1, 2, 5, 10, ...],
                "positionValues": [100.0, 200.0, 500.0, 1000.0, ...]
            }
        },
        ...
    ]

    Args:
        prod_data: Production EP data
        test_data: Test EP data
        rel_tol: Relative tolerance for float comparison
        max_point_diffs: Maximum number of return period differences to show per curve

    Returns:
        ComparisonResult with detailed differences showing specific return periods
    """
    # Build lookup by epType
    prod_by_type = {r.get('epType'): r for r in prod_data}
    test_by_type = {r.get('epType'): r for r in test_data}

    prod_types = set(prod_by_type.keys())
    test_types = set(test_by_type.keys())

    missing_in_test = list(prod_types - test_types)
    extra_in_test = list(test_types - prod_types)
    common_types = prod_types & test_types

    all_differences = []

    for ep_type in sorted(t for t in common_types if t is not None):
        prod_rec = prod_by_type[ep_type]
        test_rec = test_by_type[ep_type]

        prod_value = prod_rec.get('value', {})
        test_value = test_rec.get('value', {})

        prod_rps = prod_value.get('returnPeriods', [])
        prod_vals = prod_value.get('positionValues', [])
        test_rps = test_value.get('returnPeriods', [])
        test_vals = test_value.get('positionValues', [])

        # Check if return periods match
        if prod_rps != test_rps:
            all_differences.append({
                'key': ep_type,
                'differences': [{
                    'field': 'returnPeriods',
                    'prod_value': f"{len(prod_rps)} periods: {prod_rps[:5]}{'...' if len(prod_rps) > 5 else ''}",
                    'test_value': f"{len(test_rps)} periods: {test_rps[:5]}{'...' if len(test_rps) > 5 else ''}"
                }]
            })
            continue

        # Compare values at each return period
        point_diffs = []
        for rp, prod_val, test_val in zip(prod_rps, prod_vals, test_vals):
            if not values_match(prod_val, test_val, rel_tol):
                point_diffs.append({
                    'return_period': rp,
                    'prod_value': prod_val,
                    'test_value': test_val
                })

        if point_diffs:
            # Format differences to show specific return periods
            diff_details = []
            shown_diffs = point_diffs[:max_point_diffs]
            for pd in shown_diffs:
                diff_details.append({
                    'field': f"Return Period {pd['return_period']}",
                    'prod_value': pd['prod_value'],
                    'test_value': pd['test_value']
                })

            # Add summary if there are more differences
            if len(point_diffs) > max_point_diffs:
                diff_details.append({
                    'field': '(summary)',
                    'prod_value': f"{len(point_diffs)} return periods differ",
                    'test_value': f"showing first {max_point_diffs}"
                })

            all_differences.append({
                'key': ep_type,
                'differences': diff_details
            })

    passed = (len(missing_in_test) == 0 and
              len(extra_in_test) == 0 and
              len(all_differences) == 0)

    return ComparisonResult(
        endpoint='EP Metrics',
        passed=passed,
        total_records_prod=len(prod_data),
        total_records_test=len(test_data),
        differences=all_differences,
        missing_in_test=missing_in_test,
        extra_in_test=extra_in_test
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

    Skips rows with missing or invalid production/test analysis IDs.
    """
    df = pd.read_excel(file_path)

    # Validate required columns
    required_cols = {'production_app_analysis_id', 'test_app_analysis_id'}
    if not required_cols.issubset(set(df.columns)):
        raise ValueError(
            f"File must have columns: {required_cols}. "
            f"Found: {list(df.columns)}"
        )

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
            'name': name
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
                - production_app_analysis_id (required)
                - test_app_analysis_id (required)
                - name (optional)
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
        progress_callback: callable = None
    ) -> BatchValidationResult:
        """Validate multiple analysis pairs from a CSV or XLSX file.

        Expected columns:
        - production_app_analysis_id (required)
        - test_app_analysis_id (required)
        - name (optional)

        Args:
            file_path: Path to CSV or XLSX file with analysis pairs
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
        pairs = load_analysis_pairs(file_path)
        return self.validate_batch(
            analysis_pairs=pairs,
            perspectives=perspectives,
            include_plt=include_plt,
            relative_tolerance=relative_tolerance,
            progress_callback=progress_callback
        )

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
        """Compare EP Metrics endpoint with detailed return period differences."""
        try:
            prod_data = self.irp_client.analysis.get_ep(
                prod_analysis_id, perspective_code, prod_exposure_resource_id
            )
            test_data = self.irp_client.analysis.get_ep(
                test_analysis_id, perspective_code, test_exposure_resource_id
            )
            return compare_ep_curves(prod_data, test_data, rel_tol)
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
        rel_tol: float,
        sample_size: int = 500
    ) -> ComparisonResult:
        """Compare ELT endpoint using sampled comparison.

        Instead of fetching all events (which can be 200k+), this method:
        1. Fetches a sample of events from the production ELT
        2. Fetches those specific events from test using filter
        3. Compares only the sampled events

        This handles out-of-order API responses and is efficient for large ELTs.

        Args:
            prod_analysis_id: Production analysis ID
            test_analysis_id: Test analysis ID
            perspective_code: Perspective code (GR, GU, RL)
            prod_exposure_resource_id: Production exposure resource ID
            test_exposure_resource_id: Test exposure resource ID
            rel_tol: Relative tolerance for float comparison
            sample_size: Number of events to sample for comparison (default: 100)
        """
        try:
            # Step 1: Fetch a sample of events from production
            prod_data = self.irp_client.analysis.get_elt(
                prod_analysis_id,
                perspective_code,
                prod_exposure_resource_id,
                limit=sample_size
            )

            if not prod_data:
                # No events in production ELT - check if test also has none
                test_data = self.irp_client.analysis.get_elt(
                    test_analysis_id,
                    perspective_code,
                    test_exposure_resource_id,
                    limit=sample_size
                )
                if test_data:
                    return ComparisonResult(
                        endpoint='ELT',
                        passed=False,
                        total_records_prod=0,
                        total_records_test=len(test_data),
                        extra_in_test=[r.get('eventId') for r in test_data]
                    )
                return ComparisonResult(
                    endpoint='ELT',
                    passed=True,
                    total_records_prod=0,
                    total_records_test=0
                )

            # Step 2: Extract event IDs from the production sample
            sample_event_ids = [r.get('eventId') for r in prod_data if r.get('eventId') is not None]

            if not sample_event_ids:
                return ComparisonResult(
                    endpoint='ELT',
                    passed=False,
                    total_records_prod=len(prod_data),
                    total_records_test=0,
                    error="No valid eventId values found in production ELT sample"
                )

            # Step 3: Build filter for specific event IDs and fetch from test
            event_ids_str = ", ".join(str(eid) for eid in sample_event_ids)
            event_filter = f"eventId IN ({event_ids_str})"

            test_data = self.irp_client.analysis.get_elt(
                test_analysis_id,
                perspective_code,
                test_exposure_resource_id,
                filter=event_filter,
                limit=sample_size
            )

            # Step 4: Compare the sampled events by eventId key
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
        rel_tol: float,
        sample_size: int = 500
    ) -> ComparisonResult:
        """Compare PLT endpoint using sampled comparison.

        Instead of fetching all records (which can be 100k+), this method:
        1. Fetches a sample of records from the production PLT
        2. Extracts unique event IDs from that sample
        3. Fetches records with those event IDs from test using filter
        4. Compares records by composite key (eventId, periodId)

        Note: Filtering by eventId returns multiple records (one per periodId),
        so we must match by both eventId and periodId to compare correctly.

        Args:
            prod_analysis_id: Production analysis ID
            test_analysis_id: Test analysis ID
            perspective_code: Perspective code (GR, GU, RL)
            prod_exposure_resource_id: Production exposure resource ID
            test_exposure_resource_id: Test exposure resource ID
            rel_tol: Relative tolerance for float comparison
            sample_size: Number of records to sample for comparison (default: 500)
        """
        try:
            # Step 1: Fetch a sample of records from production
            prod_data = self.irp_client.analysis.get_plt(
                prod_analysis_id,
                perspective_code,
                prod_exposure_resource_id,
                limit=sample_size
            )

            if not prod_data:
                # No records in production PLT - check if test also has none
                test_data = self.irp_client.analysis.get_plt(
                    test_analysis_id,
                    perspective_code,
                    test_exposure_resource_id
                )
                if test_data:
                    return ComparisonResult(
                        endpoint='PLT',
                        passed=False,
                        total_records_prod=0,
                        total_records_test=len(test_data),
                        extra_in_test=[r.get('eventId') for r in test_data]
                    )
                return ComparisonResult(
                    endpoint='PLT',
                    passed=True,
                    total_records_prod=0,
                    total_records_test=0
                )

            # Step 2: Extract unique event IDs from the production sample
            sample_event_ids = list(set(
                r.get('eventId') for r in prod_data if r.get('eventId') is not None
            ))

            if not sample_event_ids:
                return ComparisonResult(
                    endpoint='PLT',
                    passed=False,
                    total_records_prod=len(prod_data),
                    total_records_test=0,
                    error="No valid eventId values found in production PLT sample"
                )

            # Step 3: Build composite keys from production sample
            # PLT records are uniquely identified by (eventId, periodId, eventDate, lossDate)
            def make_plt_key(r: Dict[str, Any]) -> tuple:
                return (r.get('eventId'), r.get('periodId'), r.get('eventDate'), r.get('lossDate'))

            prod_keys = set(make_plt_key(r) for r in prod_data)

            # Step 4: Fetch from test using eventId filter
            event_ids_str = ", ".join(str(eid) for eid in sample_event_ids)
            event_filter = f"eventId IN ({event_ids_str})"

            test_data_all = self.irp_client.analysis.get_plt(
                test_analysis_id,
                perspective_code,
                test_exposure_resource_id,
                filter=event_filter
            )

            # Step 5: Filter test data to only include records matching prod's composite keys
            # This is necessary because we can only filter by eventId in the API
            test_data = [
                r for r in test_data_all
                if make_plt_key(r) in prod_keys
            ]

            # Step 6: Compare by composite key (eventId, periodId, eventDate, lossDate)
            return compare_datasets_composite_key(
                prod_data, test_data,
                key_fields=['eventId', 'periodId', 'eventDate', 'lossDate'],
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
    print(f"Total Analyses:  {result.total_count}")
    print(f"Passed:          {result.passed_count}")
    print(f"Failed:          {result.failed_count}")
    print(f"Pass Rate:       {result.pass_rate:.1f}%")
    print(f"Perspectives:    {', '.join(result.perspectives)}")
    print(f"Include PLT:     {result.include_plt}")
    print()

    if result.passed:
        print("All analyses passed validation!")
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