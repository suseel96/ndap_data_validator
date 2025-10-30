from __future__ import annotations

from typing import Dict, List, Tuple, Optional
import re
import json
import os

import pandas as pd


DATATYPE_OPTIONS: List[str] = [
    "string",
    "integer",
    "float",
    "boolean",
    "date",
    "datetime",
    "category",
]


def guess_datatype_option_for_series(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "string"


def _coerce_to_string(series: pd.Series) -> Tuple[pd.Series, int]:
    converted = series.astype("string")
    # Strings do not create conversion errors per se
    return converted, 0


def _coerce_to_integer(series: pd.Series) -> Tuple[pd.Series, int]:
    numeric = pd.to_numeric(series, errors="coerce")
    # Count values that are fractional (e.g., 1.2) as conversion errors for integer type
    fractionals = numeric.notna() & (numeric % 1 != 0)
    # Set fractional values to NA so they appear as errors in the cleaned data
    if fractionals.any():
        numeric = numeric.mask(fractionals, other=pd.NA)
    errors = int(numeric.isna().sum()) - int(series.isna().sum())
    # Use pandas nullable integer dtype so NaNs are supported
    converted = numeric.astype("Int64")
    return converted, max(errors, 0)


def _coerce_to_float(series: pd.Series) -> Tuple[pd.Series, int]:
    numeric = pd.to_numeric(series, errors="coerce")
    errors = int(numeric.isna().sum()) - int(series.isna().sum())
    converted = numeric.astype("Float64")
    return converted, max(errors, 0)


def _coerce_to_boolean(series: pd.Series) -> Tuple[pd.Series, int]:
    # Normalize to string and strip
    as_str = series.astype("string").str.strip().str.lower()
    true_vals = {"true", "t", "yes", "y", "1"}
    false_vals = {"false", "f", "no", "n", "0"}

    def parse_bool(val: str):
        if val in true_vals:
            return True
        if val in false_vals:
            return False
        return pd.NA

    converted = as_str.map(parse_bool)
    errors = int(converted.isna().sum()) - int(series.isna().sum())
    converted = converted.astype("boolean")
    return converted, max(errors, 0)


def _coerce_to_datetime(series: pd.Series, date_only: bool) -> Tuple[pd.Series, int]:
    converted = pd.to_datetime(series, errors="coerce", utc=False, infer_datetime_format=True)
    errors = int(converted.isna().sum()) - int(series.isna().sum())
    if date_only:
        # Normalize to date (midnight)
        converted = converted.dt.normalize()
    return converted, max(errors, 0)


def _coerce_to_category(series: pd.Series) -> Tuple[pd.Series, int]:
    converted = series.astype("string").astype("category")
    return converted, 0


def coerce_dataframe_to_selected_types(
    df: pd.DataFrame, dtype_selection: Dict[str, str]
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, int]]]:
    coerced = {}
    report: Dict[str, Dict[str, int]] = {}

    for col in df.columns:
        desired = dtype_selection.get(col, "string")
        s = df[col]
        if desired == "string":
            converted, errs = _coerce_to_string(s)
        elif desired == "integer":
            converted, errs = _coerce_to_integer(s)
        elif desired == "float":
            converted, errs = _coerce_to_float(s)
        elif desired == "boolean":
            converted, errs = _coerce_to_boolean(s)
        elif desired == "date":
            converted, errs = _coerce_to_datetime(s, date_only=True)
        elif desired == "datetime":
            converted, errs = _coerce_to_datetime(s, date_only=False)
        elif desired == "category":
            converted, errs = _coerce_to_category(s)
        else:
            converted, errs = _coerce_to_string(s)

        coerced[col] = converted
        report[col] = {"conversion_errors": int(errs)}

    coerced_df = pd.DataFrame(coerced)
    return coerced_df, report


def validate_dataframe(df: pd.DataFrame) -> Dict[str, object]:
    per_col_nulls = {col: int(df[col].isna().sum()) for col in df.columns}
    total_nulls = int(sum(per_col_nulls.values()))
    return {
        "per_column_nulls": per_col_nulls,
        "total_nulls": total_nulls,
        "passed": total_nulls == 0,
    }


# Role-based validation
ROLE_OPTIONS: List[str] = [
    "Location",
    "Time",
    "Measures",
    "Others",
]

MEASURE_TYPES: List[str] = ["integer", "float"]


def _count_time_format_errors(series: pd.Series) -> int:
    month = r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
    patterns: List[Tuple[str, re.Pattern]] = [
        ("YYYY", re.compile(r"^\d{4}$")),
        ("YYYY-YY", re.compile(r"^\d{4}-\d{2}$")),
        ("MMM-YYYY", re.compile(rf"^(?:{month})-\d{{4}}$", re.IGNORECASE)),
        (
            "MMM-MMM, YYYY",
            re.compile(rf"^(?:{month})-(?:{month}),\s*\d{{4}}$", re.IGNORECASE),
        ),
        (
            "MMM - MMM, YYYY",
            re.compile(rf"^(?:{month}) - (?:{month}),\s*\d{{4}}$", re.IGNORECASE),
        ),
    ]
    vals = series[~series.isna()].astype("string").dropna()
    invalid = 0
    valid_labels: List[str] = []
    for val in vals:
        v = str(val).strip()
        if v == "":
            invalid += 1
            continue
        matched = None
        for label, pat in patterns:
            if pat.match(v):
                matched = label
                break
        if matched is None:
            invalid += 1
        else:
            valid_labels.append(matched)
    # Count rows not matching the majority valid format as extra errors
    extra_inconsistent = 0
    if valid_labels:
        counts: Dict[str, int] = {}
        for lab in valid_labels:
            counts[lab] = counts.get(lab, 0) + 1
        majority = max(counts, key=counts.get)
        extra_inconsistent = sum(1 for lab in valid_labels if lab != majority)
    return int(invalid + extra_inconsistent)
def guess_role_for_series(series: pd.Series, column_name: Optional[str] = None) -> Tuple[str, Optional[str]]:
    name = (column_name or series.name or "").lower()
    if pd.api.types.is_datetime64_any_dtype(series) or any(tok in name for tok in ["date", "time", "year", "month"]):
        return "Time", None
    if pd.api.types.is_integer_dtype(series):
        return "Measures", "integer"
    if pd.api.types.is_float_dtype(series):
        return "Measures", "float"
    return "Others", None


def coerce_dataframe_by_roles(
    df: pd.DataFrame,
    role_selection: Dict[str, str],
    measure_type_selection: Dict[str, str],
    time_date_only: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, Dict[str, int]]]:
    coerced = {}
    report: Dict[str, Dict[str, int]] = {}

    for col in df.columns:
        role = role_selection.get(col, "Others")
        s = df[col]
        if role == "Location":
            converted, errs = _coerce_to_string(s)
        elif role == "Time":
            # Keep as strings; count invalid time format rows as conversion errors
            converted, _ = _coerce_to_string(s)
            errs = _count_time_format_errors(s)
        elif role == "Measures":
            mtype = measure_type_selection.get(col, "float")
            if mtype == "integer":
                converted, errs = _coerce_to_integer(s)
            else:
                converted, errs = _coerce_to_float(s)
        else:  # Others -> categorical
            converted, errs = _coerce_to_category(s)

        coerced[col] = converted
        report[col] = {"conversion_errors": int(max(errs, 0))}

    coerced_df = pd.DataFrame(coerced)
    return coerced_df, report


def _load_rules() -> Dict[str, object]:
    path = os.path.join(os.path.dirname(__file__), "validation_rules.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_dataframe_by_roles(
    df: pd.DataFrame,
    role_selection: Dict[str, str],
    coercion_report: Dict[str, Dict[str, int]],
    schema_name: str | None = None,
) -> Dict[str, object]:
    per_column: Dict[str, Dict[str, object]] = {}
    failed_columns: List[str] = []

    rules = _load_rules()
    schemas = rules.get("schemas", {}) if isinstance(rules, dict) else {}
    schema_key = schema_name or rules.get("defaultSchema") or "National"
    schema = schemas.get(schema_key, {}) if isinstance(schemas, dict) else {}

    # Precompile time format patterns
    month = r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
    time_patterns: List[Tuple[str, re.Pattern]] = [
        ("YYYY", re.compile(r"^\d{4}$")),
        ("YYYY-YY", re.compile(r"^\d{4}-\d{2}$")),
        ("MMM-YYYY", re.compile(rf"^(?:{month})-\d{{4}}$", re.IGNORECASE)),
        (
            "MMM-MMM, YYYY",
            re.compile(rf"^(?:{month})-(?:{month}),\s*\d{{4}}$", re.IGNORECASE),
        ),
        (
            "MMM - MMM, YYYY",
            re.compile(rf"^(?:{month}) - (?:{month}),\s*\d{{4}}$", re.IGNORECASE),
        ),
    ]

    for col in df.columns:
        role = role_selection.get(col, "Others")
        nulls = int(df[col].isna().sum())
        conv_errs = int(coercion_report.get(col, {}).get("conversion_errors", 0))
        passed = True
        reasons: List[str] = []

        # Apply rule for this role, with special case: Global schema makes Location optional
        role_rule = schema.get(role, {}) if isinstance(schema, dict) else {}
        not_null_req = bool(role_rule.get("notNull", False))
        mandatory = bool(role_rule.get("mandatory", False))
        numeric_only = bool(role_rule.get("numericOnly", False))

        # If schema is Global and role is Location, it's optional; if column not present then skip.
        # We are iterating only present columns; to enforce mandatory columns we would check later at summary level.

        if numeric_only and conv_errs > 0:
            passed = False
            reasons.append("Non-numeric values detected")
        if not_null_req and nulls > 0:
            passed = False
            reasons.append("No Nulls allowed")

        # Extra rule: Time columns must match one of the allowed formats and be consistent
        if role == "Time":
            series = df[col]
            # Consider only non-null string values for format checking
            non_null_vals = series[~series.isna()].astype("string").dropna()
            formats_seen: List[str] = []
            invalid_count = 0
            for val in non_null_vals:
                v = str(val).strip()
                if v == "":
                    # empty strings count as invalid
                    invalid_count += 1
                    continue
                fmt_matched: Optional[str] = None
                for label, pat in time_patterns:
                    if pat.match(v):
                        fmt_matched = label
                        break
                if fmt_matched is None:
                    invalid_count += 1
                else:
                    formats_seen.append(fmt_matched)

            if invalid_count > 0:
                passed = False
                reasons.append(
                    "Invalid time format; allowed: 'YYYY', 'YYYY-YY', 'MMM-YYYY', 'MMM-MMM, YYYY', 'MMM - MMM, YYYY' "
                )
            elif len(formats_seen) > 0:
                first_fmt = formats_seen[0]
                # If more than one distinct format observed, mark inconsistent
                if any(f != first_fmt for f in formats_seen[1:]):
                    passed = False
                    reasons.append("Inconsistent time formats across rows")

        if not passed:
            failed_columns.append(col)

        per_column[col] = {
            "role": role,
            "nulls": nulls,
            "conversion_errors": conv_errs,
            "passed": passed,
            "reasons": reasons,
        }

    # Enforce mandatory roles: if any required role has zero assigned columns, mark failure
    assigned_by_role: Dict[str, int] = {r: 0 for r in ["Location", "Time", "Measures", "Others"]}
    for c, r in role_selection.items():
        assigned_by_role[r] = assigned_by_role.get(r, 0) + 1
    schema_mandatory_roles = [k for k, v in schema.items() if isinstance(v, dict) and v.get("mandatory")]
    # Always require at least one Time and one Measures column for all schemas
    for hard_required in ("Time", "Measures"):
        if hard_required not in schema_mandatory_roles:
            schema_mandatory_roles.append(hard_required)
    missing_roles = [r for r in schema_mandatory_roles if assigned_by_role.get(r, 0) == 0]
    overall_passed = len(failed_columns) == 0 and len(missing_roles) == 0

    return {
        "per_column": per_column,
        "failed_columns": failed_columns,
        "missing_roles": missing_roles,
        "passed": overall_passed,
    }
