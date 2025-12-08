"""
Analysis Results Validator Module

Compares analysis outputs between a production run (baseline) and a test run
to validate that the test produced expected results.

Endpoints compared:
- Statistics (/stats)
- EP Metrics (/ep)
- Event Loss Table (/elt)
- Period Loss Table (/plt) - HD analyses only
"""

import math
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

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

    @property
    def passed(self) -> bool:
        """Returns True if all endpoint comparisons passed."""
        return all(r.passed for r in self.results)

    def get_result(self, endpoint: str) -> Optional[ComparisonResult]:
        """Get result for a specific endpoint."""
        for r in self.results:
            if r.endpoint == endpoint:
                return r
        return None


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
        include_plt: bool = False,
        relative_tolerance: float = 1e-9
    ) -> ValidationResult:
        """Validate test analysis against production analysis.

        Args:
            production_app_analysis_id: App analysis ID for production (baseline)
            test_app_analysis_id: App analysis ID for test run
            perspective_code: Perspective code ('GR', 'GU', 'RL')
            include_plt: Whether to include PLT comparison (HD analyses only)
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

        # Compare PLT (optional)
        if include_plt:
            result.results.append(self._compare_plt(
                prod_analysis_id, test_analysis_id,
                perspective_code,
                prod_exposure_resource_id, test_exposure_resource_id,
                relative_tolerance
            ))

        return result

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


# =============================================================================
# Output Formatting
# =============================================================================

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
                    print(f"      prod: {field_diff['prod_value']}")
                    print(f"      test: {field_diff['test_value']}")
            if len(r.differences) > max_differences:
                print(f"\n  ... and {len(r.differences) - max_differences} more records with differences")