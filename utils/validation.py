from __future__ import annotations

from typing import Dict, List, Tuple, Optional

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
            converted, errs = _coerce_to_datetime(s, date_only=time_date_only)
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


def validate_dataframe_by_roles(
    df: pd.DataFrame,
    role_selection: Dict[str, str],
    coercion_report: Dict[str, Dict[str, int]],
) -> Dict[str, object]:
    per_column: Dict[str, Dict[str, object]] = {}
    failed_columns: List[str] = []

    for col in df.columns:
        role = role_selection.get(col, "Others")
        nulls = int(df[col].isna().sum())
        conv_errs = int(coercion_report.get(col, {}).get("conversion_errors", 0))
        passed = True
        reasons: List[str] = []

        if role in ("Location", "Time"):
            if nulls > 0:
                passed = False
                reasons.append("No Null required")
        elif role == "Measures":
            # Numeric only and no nulls
            if nulls > 0:
                passed = False
                reasons.append("Measures must be non-null")
            if conv_errs > 0:
                passed = False
                reasons.append("Non-numeric values detected")
        # Others: no strict constraints

        if not passed:
            failed_columns.append(col)

        per_column[col] = {
            "role": role,
            "nulls": nulls,
            "conversion_errors": conv_errs,
            "passed": passed,
            "reasons": reasons,
        }

    return {
        "per_column": per_column,
        "failed_columns": failed_columns,
        "passed": len(failed_columns) == 0,
    }


