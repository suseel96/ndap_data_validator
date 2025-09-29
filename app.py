import io
from datetime import datetime

import pandas as pd
import streamlit as st

from utils.validation import (
    DATATYPE_OPTIONS,
    ROLE_OPTIONS,
    MEASURE_TYPES,
    coerce_dataframe_to_selected_types,
    guess_datatype_option_for_series,
    validate_dataframe,
    guess_role_for_series,
    coerce_dataframe_by_roles,
    validate_dataframe_by_roles,
)
from utils.s3_uploader import S3Uploader, S3CredentialsMode


st.set_page_config(page_title="CSV Validation and S3 Uploader", layout="wide")


def ensure_session_state_defaults() -> None:
    if "dtype_selection" not in st.session_state:
        st.session_state.dtype_selection = {}
    if "coercion_report" not in st.session_state:
        st.session_state.coercion_report = {}
    if "validation_report" not in st.session_state:
        st.session_state.validation_report = {}
    if "cleaned_df" not in st.session_state:
        st.session_state.cleaned_df = None
    if "original_bytes" not in st.session_state:
        st.session_state.original_bytes = None
    if "original_filename" not in st.session_state:
        st.session_state.original_filename = None


ensure_session_state_defaults()

st.title("CSV Validation and S3 Uploader")
st.caption("Upload a CSV, assign roles (Location, Time, Measures, Others), validate per checklist, then upload to S3 if valid.")


with st.sidebar:
    st.header("Instructions")
    st.markdown(
        "- Upload a CSV file.\n"
        "- Select the intended data type for each column.\n"
        "- Run validation to find nulls and conversion issues.\n"
        "- If validation passes, upload to S3."
    )


uploaded_file = st.file_uploader("Upload CSV", type=["csv"]) 

if uploaded_file is not None:
    st.session_state.original_filename = uploaded_file.name
    st.session_state.original_bytes = uploaded_file.getvalue()

    try:
        df = pd.read_csv(io.BytesIO(st.session_state.original_bytes))
    except Exception as read_err:
        st.error(f"Failed to read CSV: {read_err}")
        df = None

    if df is not None:
        st.subheader("Preview")
        with st.expander("Show CSV preview", expanded=True):
            st.dataframe(df.head(200), use_container_width=True)

        st.subheader("Assign Column Roles")
        st.caption("Choose a role per column. Measures require numeric values; Location/Time require no nulls.")

        cols = df.columns.tolist()
        selection_cols = st.columns(min(3, max(1, len(cols)))) if cols else []

        # Track role selection and measure sub-types
        if "role_selection" not in st.session_state:
            st.session_state.role_selection = {}
        if "measure_type_selection" not in st.session_state:
            st.session_state.measure_type_selection = {}

        for idx, col in enumerate(cols):
            guessed_role, guessed_measure_type = guess_role_for_series(df[col], col)
            role_default = st.session_state.role_selection.get(col, guessed_role)
            with selection_cols[idx % len(selection_cols)]:
                role = st.selectbox(
                    f"{col}",
                    options=ROLE_OPTIONS,
                    index=ROLE_OPTIONS.index(role_default) if role_default in ROLE_OPTIONS else 0,
                    key=f"role_{col}",
                    help="Assign the role for this column",
                )
                st.session_state.role_selection[col] = role
                if role == "Measures":
                    mt_default = st.session_state.measure_type_selection.get(col, guessed_measure_type or "float")
                    st.session_state.measure_type_selection[col] = st.selectbox(
                        f"{col} measure type",
                        options=MEASURE_TYPES,
                        index=MEASURE_TYPES.index(mt_default) if mt_default in MEASURE_TYPES else 1,
                        key=f"measure_type_{col}",
                        help="Select numeric type for measures",
                    )

        st.divider()
        left, right = st.columns([1, 1])
        with left:
            time_date_only = st.checkbox(
                "Treat Time as date only (drop time of day)",
                value=False,
            )
        with right:
            st.write("")

        run_validation = st.button("Run validation", type="primary")

        if run_validation:
            cleaned_df, coercion_report = coerce_dataframe_by_roles(
                df,
                st.session_state.role_selection,
                st.session_state.measure_type_selection,
                time_date_only=time_date_only,
            )
            validation_report = validate_dataframe_by_roles(
                cleaned_df,
                st.session_state.role_selection,
                coercion_report,
            )

            st.session_state.cleaned_df = cleaned_df
            st.session_state.coercion_report = coercion_report
            st.session_state.validation_report = validation_report

        if st.session_state.validation_report:
            st.subheader("Validation Results")
            # Summaries
            total_nulls = int(sum(v.get("nulls", 0) for v in st.session_state.validation_report["per_column"].values()))
            total_conversion_errors = int(sum(item.get("conversion_errors", 0) for item in st.session_state.coercion_report.values()))

            met1, met2, met3 = st.columns(3)
            with met1:
                st.metric("Rows", value=len(st.session_state.cleaned_df) if st.session_state.cleaned_df is not None else len(df))
            with met2:
                st.metric("Total nulls", value=total_nulls)
            with met3:
                st.metric("Total conversion errors", value=total_conversion_errors)

            # Per-column breakdown
            breakdown = []
            for col in df.columns:
                col_report = st.session_state.validation_report["per_column"].get(col, {})
                nulls = int(col_report.get("nulls", 0))
                conv_errs = int(st.session_state.coercion_report.get(col, {}).get("conversion_errors", 0))
                role = col_report.get("role")
                passed = col_report.get("passed")
                reasons = ", ".join(col_report.get("reasons", []))
                breakdown.append({"column": col, "role": role, "nulls": nulls, "conversion_errors": conv_errs, "passed": passed, "reasons": reasons})

            if breakdown:
                st.dataframe(pd.DataFrame(breakdown), use_container_width=True)

            failed_columns = st.session_state.validation_report.get("failed_columns", [])
            if failed_columns:
                st.error("Validation failed. Fix the following columns: " + ", ".join(failed_columns))
            else:
                st.success("Validation passed. You can upload to S3.")

                st.subheader("Upload to S3")
                creds_mode = st.radio(
                    "Credentials mode",
                    options=[S3CredentialsMode.Environment.value, S3CredentialsMode.Manual.value],
                    horizontal=True,
                )

                bucket = st.text_input("S3 bucket name", placeholder="my-bucket")
                prefix = st.text_input("Key prefix (optional)", placeholder="uploads/")
                upload_cleaned = st.checkbox("Upload cleaned CSV (instead of original)", value=True)

                default_key_name = (
                    f"{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_cleaned.csv" if upload_cleaned else st.session_state.original_filename or "upload.csv"
                )
                object_key_name = st.text_input("Object key name", value=default_key_name)

                access_key = secret_key = region_name = None
                if creds_mode == S3CredentialsMode.Manual.value:
                    access_key = st.text_input("AWS Access Key ID", type="default")
                    secret_key = st.text_input("AWS Secret Access Key", type="password")
                    region_name = st.text_input("AWS Region", value="us-east-1")

                do_upload = st.button("Upload to S3", type="primary", disabled=not bucket or not object_key_name)

                if do_upload and bucket and object_key_name:
                    try:
                        uploader = S3Uploader(
                            mode=S3CredentialsMode(creds_mode),
                            access_key_id=access_key,
                            secret_access_key=secret_key,
                            region_name=region_name,
                        )

                        if upload_cleaned and st.session_state.cleaned_df is not None:
                            csv_bytes = st.session_state.cleaned_df.to_csv(index=False).encode("utf-8")
                        else:
                            csv_bytes = st.session_state.original_bytes

                        key = f"{prefix}{object_key_name}" if prefix else object_key_name
                        s3_uri = uploader.upload_bytes(
                            bucket=bucket,
                            key=key,
                            data_bytes=csv_bytes,
                            content_type="text/csv",
                        )
                        st.success(f"Uploaded to {s3_uri}")
                    except Exception as ex:
                        st.error(f"Upload failed: {ex}")


