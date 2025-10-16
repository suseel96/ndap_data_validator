from __future__ import annotations

import io
import json
import traceback
from typing import Dict, List, Tuple
import os
from uuid import uuid4
import base64
import json as _json
from urllib.parse import urlparse, urlunparse, quote, urlencode
import re
import urllib.request
import urllib.error

import pandas as pd
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from utils.validation import (
    ROLE_OPTIONS,
    MEASURE_TYPES,
    coerce_dataframe_by_roles,
    validate_dataframe_by_roles,
)
from utils.s3_uploader import S3Uploader, S3CredentialsMode
from utils.db import Database


app = FastAPI(title="NDAP Data Validator")
app.add_middleware(SessionMiddleware, secret_key="change-this-secret")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]) 
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# Simple in-memory store for uploaded bytes by token
DATA_STORE: Dict[str, bytes] = {}
# Persist last validation state for navigation
VALIDATION_STATE: Dict[str, Dict[str, object]] = {}
# Track step completion per token
STEP_COMPLETION: Dict[str, set] = {}
DB = Database()
DB.init()
DB.ensure_default_admin()

def get_username(request: Request) -> str | None:
    return request.session.get("username")

def require_login(request: Request) -> bool:
    return bool(get_username(request))


def _read_df_from_bytes(data: bytes, nrows=None) -> pd.DataFrame:
    return (
        pd.read_csv(io.BytesIO(data), nrows=nrows)
        if nrows
        else pd.read_csv(io.BytesIO(data))
    )


def _get_data_bytes(token: str) -> bytes | None:
    # Prefer memory; fallback to DB if present
    data = DATA_STORE.get(token)
    if data:
        return data
    try:
        return DB.get_upload_bytes(token)
    except Exception:
        return None

def _get_meta(token: str) -> Dict[str, object]:
    state = VALIDATION_STATE.get(token, {}) or {}
    file_name = state.get("file_name", "")
    record_count = int(state.get("record_count", 0) or 0)
    if (not file_name) or record_count == 0:
        try:
            meta = DB.get_upload_meta(token)
            if meta:
                if not file_name:
                    file_name = meta.get("filename") or ""
                if record_count == 0:
                    record_count = int(meta.get("record_count") or 0)
        except Exception:
            pass
    return {"file_name": file_name, "record_count": record_count}

def _ensure_url_scheme(url: str, default: str = "http") -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.scheme:
        return url
    return f"{default}://{url}"

def _add_basic_auth_to_url(url: str, username: str | None, password: str | None) -> str:
    if not url or not username:
        return url
    parsed = urlparse(url)
    netloc = parsed.netloc
    if not netloc:
        return url
    userinfo = quote(username)
    if password:
        userinfo += ":" + quote(password)
    if "@" in netloc:
        netloc = netloc.split("@", 1)[-1]
    parsed = parsed._replace(netloc=f"{userinfo}@{netloc}")
    return urlunparse(parsed)


def _get_airflow_config() -> tuple[str, str | None, str | None]:
    base = (os.environ.get("NDAP_AIRFLOW_URL") or (DB.get_setting("NDAP_AIRFLOW_URL") or "")).strip()
    base = _ensure_url_scheme(base)
    username = (os.environ.get("NDAP_AIRFLOW_USER") or (DB.get_setting("NDAP_AIRFLOW_USER") or "")).strip() or None
    password = (os.environ.get("NDAP_AIRFLOW_PASSWORD") or (DB.get_setting("NDAP_AIRFLOW_PASSWORD") or "")).strip() or None
    return base, username, password


def _airflow_api_get_json(base_url: str, path: str, username: str | None, password: str | None, timeout: int = 10) -> tuple[bool, dict | str]:
    if not base_url:
        return False, "Airflow base URL is not configured"
    url = f"{base_url.rstrip('/')}{path}"
    req = urllib.request.Request(url=url, method="GET")
    req.add_header("Accept", "application/json")
    if username:
        token = base64.b64encode(f"{username}:{password or ''}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            try:
                return True, _json.loads(body)
            except Exception:
                return True, {"raw": body}
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = str(e)
        return False, f"HTTP {e.code}: {detail}"
    except Exception as exc:
        return False, str(exc)


AIRFLOW_TERMINAL_STATES = {"success", "failed", "error", "upstream_failed"}
TASK_LOG_SNIPPET_LIMIT = 4000


def trigger_airflow_dag(base_url: str, dag_id: str, username: str | None = None, password: str | None = None, conf: dict | None = None) -> tuple[bool, dict]:
    try:
        import urllib.request
        import urllib.error
    except Exception:
        return False, {"error": "urllib not available"}
    if not base_url or not dag_id:
        return False, {"error": "Base URL and DAG ID are required"}
    url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    payload = {"conf": conf or {}}
    data = _json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url=url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if username:
        token = base64.b64encode(f"{username}:{password or ''}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            try:
                j = _json.loads(body)
            except Exception:
                j = {"raw": body}
            return True, j
    except Exception as e:
        return False, {"error": str(e)}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("upload.html", {
        "request": request, 
        "active_step": 1, 
        "title": "Upload",
        "completed_steps": set(),
        "token": None
    })


@app.post("/preview", response_class=HTMLResponse)
async def preview(request: Request, file: UploadFile = File(...), schema: str = Form("National")):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    try:
        content = await file.read()
        token = uuid4().hex
        DATA_STORE[token] = content
        # Persist to database as well to survive reloads
        # (we call DB.save_upload below after computing record_count)

        df = _read_df_from_bytes(content, nrows=10)
        columns: List[str] = list(df.columns)
        preview_html = df.head(10).to_html(index=False, classes="table table-sm table-striped", na_rep="")
        file_name = file.filename or ""
        try:
            record_count = int(_read_df_from_bytes(content).shape[0])
        except Exception:
            record_count = 0
        try:
            DB.save_upload(token, get_username(request), file_name, content, record_count)
        except Exception:
            pass

        # Persist selected schema and mark step 1 as completed
        VALIDATION_STATE[token] = {"schema": schema, "file_name": file_name, "record_count": record_count}
        STEP_COMPLETION[token] = {1}
        return templates.TemplateResponse(
            "preview.html",
            {
                "request": request,
                "token": token,
                "columns": columns,
                "preview_html": preview_html,
                "role_options": ROLE_OPTIONS,
                "measure_types": MEASURE_TYPES,
                "schema": schema,
                "file_name": file_name,
                "record_count": record_count,
                "active_step": 2,
                "title": "Select types",
                "completed_steps": STEP_COMPLETION.get(token, set()),
            },
        )
    except Exception as ex:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": f"Failed to read CSV: {ex}"},
        )


@app.get("/preview", response_class=HTMLResponse)
async def preview_get(request: Request, token: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not token:
        return RedirectResponse(url="/", status_code=302)
    # Re-render preview/selection by reading back from stored bytes
    content = _get_data_bytes(token)
    if not content:
        return RedirectResponse(url="/", status_code=302)

    df = _read_df_from_bytes(content, nrows=10)
    columns: List[str] = list(df.columns)
    preview_html = df.head(10).to_html(index=False, classes="table table-sm table-striped", na_rep="")

    state = VALIDATION_STATE.get(token, {})
    schema = state.get("schema", "National")
    meta = _get_meta(token)
    return templates.TemplateResponse(
        "preview.html",
        {
            "request": request,
            "token": token,
            "columns": columns,
            "preview_html": preview_html,
            "role_options": ROLE_OPTIONS,
            "measure_types": MEASURE_TYPES,
            "schema": schema,
            "file_name": meta.get("file_name", ""),
            "record_count": meta.get("record_count", 0),
            "active_step": 2,
            "title": "Select types",
            "completed_steps": STEP_COMPLETION.get(token, set()),
        },
    )


@app.post("/validate", response_class=HTMLResponse)
async def validate(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    form = await request.form()
    token = str(form.get("token"))
    columns_serialized = str(form.get("columns", ""))
    columns: List[str] = [c for c in columns_serialized.split("|||") if c]

    # role selections
    role_selection: Dict[str, str] = {}
    measure_type_selection: Dict[str, str] = {}
    for col in columns:
        role_selection[col] = str(form.get(f"role_{col}", "Others"))
        mt = str(form.get(f"measure_type_{col}", "float"))
        measure_type_selection[col] = mt

    time_date_only = False

    try:
        if not _get_data_bytes(token):
            return templates.TemplateResponse(
                "upload.html",
                {"request": request, "error": "Session expired or file not found. Please re-upload your CSV."},
            )
        data = _get_data_bytes(token)
        df = _read_df_from_bytes(data)

        cleaned_df, coercion_report = coerce_dataframe_by_roles(
            df,
            role_selection,
            measure_type_selection,
            time_date_only=time_date_only,
        )
        validation_report = validate_dataframe_by_roles(
            cleaned_df,
            role_selection,
            coercion_report,
            schema_name=VALIDATION_STATE.get(token, {}).get("schema", "National"),
        )

        per_column_rows: List[Dict[str, object]] = []
        for col in df.columns:
            col_report = validation_report["per_column"].get(col, {})
            per_column_rows.append(
                {
                    "column": col,
                    "role": col_report.get("role"),
                    "nulls": int(col_report.get("nulls", 0)),
                    "conversion_errors": int(coercion_report.get(col, {}).get("conversion_errors", 0)),
                    "passed": bool(col_report.get("passed", False)),
                    "reasons": ", ".join(col_report.get("reasons", [])),
                }
            )

        passed = bool(validation_report.get("passed", False))
        failed_columns = validation_report.get("failed_columns", [])

        # persist validation state for navigation
        meta = _get_meta(token)
        VALIDATION_STATE[token] = {
            "columns": columns,
            "role_selection": role_selection,
            "measure_type_selection": measure_type_selection,
            "schema": VALIDATION_STATE.get(token, {}).get("schema", "National"),
            "rows": per_column_rows,
            "passed": passed,
            "failed_columns": failed_columns,
            "file_name": meta.get("file_name", ""),
            "record_count": meta.get("record_count", 0),
        }
        # Mark step 2 as completed
        if token not in STEP_COMPLETION:
            STEP_COMPLETION[token] = set()
        STEP_COMPLETION[token].add(2)
        # If validation passed, mark step 3 as completed so stepper turns green and step 4 unlocks
        if passed:
            STEP_COMPLETION[token].add(3)

        resp = templates.TemplateResponse(
            "validate.html",
            {
                "request": request,
                "token": token,
                "columns": columns,
                "role_options": ROLE_OPTIONS,
                "measure_types": MEASURE_TYPES,
                "role_selection": role_selection,
                "measure_type_selection": measure_type_selection,
                "schema": VALIDATION_STATE.get(token, {}).get("schema", "National"),
                "rows": per_column_rows,
                "passed": passed,
                "failed_columns": failed_columns,
                "file_name": meta.get("file_name", ""),
                "record_count": meta.get("record_count", 0),
                "active_step": 3,
                "title": "Validate",
                "completed_steps": STEP_COMPLETION.get(token, set()),
            },
        )
        # Persist validation snapshot to DB for resilience across reloads
        try:
            DB.save_validation_snapshot(
                token,
                json.dumps(columns),
                json.dumps(role_selection),
                False,
                passed,
            )
        except Exception:
            pass
        try:
            DB.log_validation(token, get_username(request), passed, ",".join(failed_columns))
        except Exception:
            pass
        return resp
    except Exception as ex:
        trace = traceback.format_exc()
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": f"Validation failed: {ex}", "trace": trace},
        )


@app.get("/validate", response_class=HTMLResponse)
async def validate_get(request: Request, token: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not token:
        return RedirectResponse(url="/", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state:
        # If we do not have validation state, send user to selection step
        return RedirectResponse(url=f"/preview?token={token}", status_code=302)

    # If validation already passed, ensure step 3 is marked completed
    if state.get("passed", False):
        if token not in STEP_COMPLETION:
            STEP_COMPLETION[token] = set()
        STEP_COMPLETION[token].add(3)

    return templates.TemplateResponse(
        "validate.html",
        {
            "request": request,
            "token": token,
            "columns": state.get("columns", []),
            "role_options": ROLE_OPTIONS,
            "measure_types": MEASURE_TYPES,
            "role_selection": state.get("role_selection", {}),
            "measure_type_selection": state.get("measure_type_selection", {}),
            "schema": state.get("schema", "National"),
            "rows": state.get("rows", []),
            "passed": state.get("passed", False),
            "failed_columns": state.get("failed_columns", []),
            "file_name": _get_meta(token).get("file_name", ""),
            "record_count": _get_meta(token).get("record_count", 0),
            "active_step": 3,
            "title": "Validate",
            "completed_steps": STEP_COMPLETION.get(token, set()),
        },
    )


@app.get("/upload", response_class=HTMLResponse)
async def upload_get(request: Request, token: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not token:
        return RedirectResponse(url="/", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state or not state.get("passed", False):
        # Fallback to DB snapshot if available
        try:
            snap = DB.get_validation_snapshot(token)
        except Exception:
            snap = None
        if snap and snap.get("validated_passed"):
            try:
                cols = json.loads(snap.get("columns_json") or "[]")
            except Exception:
                cols = []
            try:
                roles = json.loads(snap.get("role_selection_json") or "{}")
            except Exception:
                roles = {}
            state = {
                "columns": cols,
                "role_selection": roles,
                "measure_type_selection": {},
                "schema": VALIDATION_STATE.get(token, {}).get("schema", "National"),
                "rows": [],
                "passed": True,
                "failed_columns": [],
            }
            VALIDATION_STATE[token] = state
            if token not in STEP_COMPLETION:
                STEP_COMPLETION[token] = set()
            STEP_COMPLETION[token].add(2)
            STEP_COMPLETION[token].add(3)
        else:
            # No snapshot/passed info, route back appropriately
            if not state:
                return RedirectResponse(url=f"/preview?token={token}", status_code=302)
            return RedirectResponse(url=f"/validate?token={token}", status_code=302)

    # Ensure step 3 is recorded as completed when reaching step 4
    if token not in STEP_COMPLETION:
        STEP_COMPLETION[token] = set()
    STEP_COMPLETION[token].add(3)

    # Check required settings for Step 4 (S3 only)
    s3_bucket_cfg = (DB.get_setting("S3_BUCKET") or os.environ.get("S3_BUCKET") or "").strip()
    missing_settings = []
    if not s3_bucket_cfg:
        missing_settings.append("S3_BUCKET")
    settings_ready = len(missing_settings) == 0

    s3_form_defaults = state.get("s3_form") or state.get("s3_upload") or {}
    object_key_value = s3_form_defaults.get("object_key", "")
    s3_folder_value = s3_form_defaults.get("folder", "")

    return templates.TemplateResponse(
        "s3_upload.html",
        {
            "request": request,
            "token": token,
            "columns": state.get("columns", []),
            "role_selection": state.get("role_selection", {}),
            "measure_type_selection": state.get("measure_type_selection", {}),
            "file_name": _get_meta(token).get("file_name", ""),
            "record_count": _get_meta(token).get("record_count", 0),
            "settings_ready": settings_ready,
            "missing_settings": missing_settings,
            "object_key_value": object_key_value,
            "s3_folder_value": s3_folder_value,
            "active_step": 4,
            "title": "Upload",
            "completed_steps": STEP_COMPLETION.get(token, set()),
        },
    )


@app.post("/upload", response_class=HTMLResponse)
async def upload(
    request: Request,
    token: str = Form(...),
    columns: str = Form(...),
    object_key_name: str = Form(...),
    s3_folder_name: str | None = Form(None),
):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    form = await request.form()
    columns_list: List[str] = [c for c in columns.split("|||") if c]
    role_selection: Dict[str, str] = {}
    measure_type_selection: Dict[str, str] = {}
    for col in columns_list:
        role_selection[col] = str(form.get(f"role_{col}", "Others"))
        measure_type_selection[col] = str(form.get(f"measure_type_{col}", "float"))

    state = VALIDATION_STATE.setdefault(token, {})

    s3_bucket = (DB.get_setting("S3_BUCKET") or os.environ.get("S3_BUCKET") or "").strip()
    s3_access_key_id = (DB.get_setting("S3_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    s3_secret_key = (DB.get_setting("S3_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    s3_region = (os.environ.get("AWS_REGION") or "us-east-1").strip()

    def _render_upload_with_error(message: str, trace: str | None = None):
        missing_settings = []
        if not s3_bucket:
            missing_settings.append("S3_BUCKET")
        form_defaults = state.get("s3_form") or {}
        context = {
            "request": request,
            "token": token,
            "columns": state.get("columns", columns_list),
            "role_selection": state.get("role_selection", role_selection),
            "measure_type_selection": state.get("measure_type_selection", measure_type_selection),
            "file_name": _get_meta(token).get("file_name", ""),
            "record_count": _get_meta(token).get("record_count", 0),
            "settings_ready": len(missing_settings) == 0,
            "missing_settings": missing_settings,
            "active_step": 4,
            "title": "Upload",
            "completed_steps": STEP_COMPLETION.get(token, set()),
            "error": message,
            "trace": trace,
            "object_key_value": form_defaults.get("object_key", ""),
            "s3_folder_value": form_defaults.get("folder", ""),
        }
        return templates.TemplateResponse("s3_upload.html", context)

    try:
        if not _get_data_bytes(token):
            return templates.TemplateResponse(
                "upload.html",
                {"request": request, "error": "Session expired or file not found. Please re-upload your CSV."},
            )
        data = _get_data_bytes(token)
        df = _read_df_from_bytes(data)

        cleaned_df, _ = coerce_dataframe_by_roles(
            df,
            role_selection,
            measure_type_selection,
            time_date_only=False,
        )

        if not s3_bucket:
            return _render_upload_with_error("S3 bucket is not configured. Please set it in Admin → Settings.")
        if s3_access_key_id and s3_secret_key:
            uploader = S3Uploader(
                mode=S3CredentialsMode.Manual,
                access_key_id=s3_access_key_id,
                secret_access_key=s3_secret_key,
                region_name=s3_region,
            )
        else:
            uploader = S3Uploader(
                mode=S3CredentialsMode.Environment,
                region_name=s3_region,
            )

        csv_bytes = cleaned_df.to_csv(index=False).encode("utf-8")

        folder_value = (s3_folder_name or "").strip()
        state["s3_form"] = {
            "object_key": object_key_name,
            "folder": folder_value,
        }
        folder_prefix = folder_value
        if folder_prefix:
            if not folder_prefix.endswith("/"):
                folder_prefix = folder_prefix + "/"
            folder_prefix = folder_prefix + "pending/"
        key = f"{folder_prefix}{object_key_name}" if folder_prefix else object_key_name
        s3_uri = uploader.upload_bytes(
            bucket=s3_bucket,
            key=key,
            data_bytes=csv_bytes,
            content_type="text/csv",
        )

        if token not in STEP_COMPLETION:
            STEP_COMPLETION[token] = set()
        STEP_COMPLETION[token].add(3)
        STEP_COMPLETION[token].add(4)

        state["s3_upload"] = {
            "bucket": s3_bucket,
            "object_key": key,
            "folder": folder_value,
            "s3_uri": s3_uri,
        }

        try:
            DB.log_upload(token, get_username(request), s3_bucket, key, s3_uri, True)
        except Exception:
            pass

        return RedirectResponse(url=f"/airflow-trigger?token={token}&uploaded=1", status_code=303)
    except Exception as ex:
        trace = traceback.format_exc()
        return _render_upload_with_error(str(ex), trace)


@app.get("/airflow-trigger", response_class=HTMLResponse)
async def airflow_trigger_get(request: Request, token: str | None = None, uploaded: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not token:
        return RedirectResponse(url="/", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state or "s3_upload" not in state:
        return RedirectResponse(url=f"/upload?token={token}", status_code=302)
    if token not in STEP_COMPLETION:
        STEP_COMPLETION[token] = set()
    STEP_COMPLETION[token].add(3)
    STEP_COMPLETION[token].add(4)

    s3_info = state["s3_upload"]
    s3_bucket = s3_info.get("bucket", "")
    s3_object_key = s3_info.get("object_key", "")
    s3_uri = s3_info.get("s3_uri", "")
    s3_folder = s3_info.get("folder", "")

    airflow_base, username, password = _get_airflow_config()
    airflow_dag_id_val = (DB.get_setting("loading_dag_id") or DB.get_setting("NDAP_AIRFLOW_DAG_ID") or "").strip()
    missing_settings = []
    if not airflow_base:
        missing_settings.append("NDAP_AIRFLOW_URL")
    if not airflow_dag_id_val:
        missing_settings.append("loading_dag_id/NDAP_AIRFLOW_DAG_ID")
    settings_ready = len(missing_settings) == 0

    airflow_params = state.get("airflow_params", {})
    is_incremental = bool(airflow_params.get("is_incremental"))
    schema_exists = bool(airflow_params.get("schema_exists"))

    meta = _get_meta(token)
    context = {
        "request": request,
        "token": token,
        "s3_bucket": s3_bucket,
        "s3_object_key": s3_object_key,
        "s3_uri": s3_uri,
        "s3_folder": s3_folder,
        "is_incremental": is_incremental,
        "schema_exists": schema_exists,
        "airflow_ready": settings_ready,
        "missing_settings": missing_settings,
        "uploaded_recently": uploaded == "1",
        "file_name": meta.get("file_name", ""),
        "record_count": meta.get("record_count", 0),
        "active_step": 5,
        "title": "Trigger Airflow",
        "completed_steps": STEP_COMPLETION.get(token, set()),
        "error": None,
        "trace": None,
    }
    return templates.TemplateResponse("airflow_trigger.html", context)


@app.post("/airflow-trigger", response_class=HTMLResponse)
async def airflow_trigger_post(
    request: Request,
    token: str = Form(...),
    is_incremental: str | None = Form(None),
    schema_exists: str | None = Form(None),
):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state or "s3_upload" not in state:
        return RedirectResponse(url=f"/upload?token={token}", status_code=302)
    if token not in STEP_COMPLETION:
        STEP_COMPLETION[token] = set()
    s3_info = state["s3_upload"]
    s3_bucket = s3_info.get("bucket")
    s3_object_key = s3_info.get("object_key")
    s3_uri = s3_info.get("s3_uri")
    folder_value = s3_info.get("folder", "")
    if not s3_bucket or not s3_object_key or not s3_uri:
        return RedirectResponse(url=f"/upload?token={token}", status_code=302)

    inc_bool = is_incremental == "on"
    schema_bool = schema_exists == "on"
    state["airflow_params"] = {"is_incremental": inc_bool, "schema_exists": schema_bool}

    airflow_base, username, password = _get_airflow_config()
    airflow_dag_id_val = (DB.get_setting("loading_dag_id") or DB.get_setting("NDAP_AIRFLOW_DAG_ID") or "").strip()
    missing_settings = []
    if not airflow_base:
        missing_settings.append("NDAP_AIRFLOW_URL")
    if not airflow_dag_id_val:
        missing_settings.append("loading_dag_id/NDAP_AIRFLOW_DAG_ID")
    settings_ready = len(missing_settings) == 0

    def _render_trigger_with_error(message: str, trace: str | None = None):
        meta = _get_meta(token)
        context = {
            "request": request,
            "token": token,
            "s3_bucket": s3_bucket or "",
            "s3_object_key": s3_object_key or "",
            "s3_uri": s3_uri or "",
            "s3_folder": folder_value,
            "is_incremental": inc_bool,
            "schema_exists": schema_bool,
            "airflow_ready": settings_ready,
            "missing_settings": missing_settings,
            "uploaded_recently": False,
            "file_name": meta.get("file_name", ""),
            "record_count": meta.get("record_count", 0),
            "active_step": 5,
            "title": "Trigger Airflow",
            "completed_steps": STEP_COMPLETION.get(token, set()),
            "error": message,
            "trace": trace,
        }
        return templates.TemplateResponse("airflow_trigger.html", context)

    if not settings_ready:
        return _render_trigger_with_error("Airflow settings are incomplete. Please update the Admin settings.")

    dag = airflow_dag_id_val
    usr = username
    pwd = password

    conf_obj = {
        "source_code": folder_value,
        "arg1": "yes" if inc_bool else "no",
        "arg2": "yes" if schema_bool else "no",
        "s3_uri": s3_uri,
        "token": token,
    }
    ok, info = trigger_airflow_dag(airflow_base, dag, username=usr or None, password=pwd or None, conf=conf_obj)
    airflow_dag_run_id = None
    airflow_run_url = None
    airflow_embed_url = None
    airflow_error = None

    if ok:
        airflow_dag_run_id = info.get("dag_run_id") if isinstance(info, dict) else None
        v = "grid"
        query_params: List[Tuple[str, str]] = [("tab", "graph")]
        if airflow_dag_run_id:
            query_params.append(("dag_run_id", airflow_dag_run_id))
        query = urlencode(query_params, quote_via=quote)
        run_path = f"/dags/{dag}/{v}"
        if query:
            run_path = f"{run_path}?{query}"
        embed_base = _add_basic_auth_to_url(airflow_base, usr or None, pwd or None)
        airflow_run_url = f"{airflow_base.rstrip('/')}{run_path}"
        airflow_embed_url = f"{embed_base.rstrip('/')}{run_path}"
        STEP_COMPLETION[token].add(5)
    else:
        if isinstance(info, dict):
            airflow_error = info.get("error") or info.get("message") or info.get("raw")
        else:
            airflow_error = str(info)
        return _render_trigger_with_error(airflow_error or "Failed to trigger Airflow DAG.")

    meta = _get_meta(token)
    columns_state = state.get("columns", [])

    return templates.TemplateResponse(
        "result.html",
        {
            "request": request,
            "success": True,
            "s3_uri": s3_uri,
            "airflow_triggered": ok,
            "airflow_dag_id": airflow_dag_id_val,
            "airflow_dag_run_id": airflow_dag_run_id,
            "airflow_embed_url": airflow_embed_url,
            "airflow_run_url": airflow_run_url,
            "airflow_error": airflow_error,
            "file_name": meta.get("file_name", ""),
            "record_count": meta.get("record_count", 0),
            "active_step": 5,
            "title": "Trigger Airflow",
            "token": token,
            "columns": columns_state,
            "completed_steps": STEP_COMPLETION.get(token, set()),
        },
    )


@app.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "title": "Login", "show_stepper": False})


@app.post("/login")
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    ok, user = DB.verify_user(username, password)
    if ok and user:
        request.session["username"] = username
        request.session["is_admin"] = bool(user.get("is_admin"))
        request.session["user_id"] = int(user.get("id"))
        if user.get("force_reset"):
            return RedirectResponse(url="/reset-password", status_code=302)
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "title": "Login", "error": "Invalid credentials", "show_stepper": False})


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


def require_admin(request: Request) -> bool:
    return bool(request.session.get("is_admin"))


@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not require_admin(request):
        return RedirectResponse(url="/", status_code=302)
    users = DB.list_users()
    return templates.TemplateResponse("admin_users.html", {"request": request, "title": "Manage Users", "users": users, "show_stepper": False})
@app.get("/admin/logs", response_class=HTMLResponse)
async def admin_logs(request: Request, username: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not require_admin(request):
        return RedirectResponse(url="/", status_code=302)
    validation_logs = DB.list_validation_logs(username=username or None)
    upload_logs = DB.list_upload_logs(username=username or None)
    return templates.TemplateResponse(
        "admin_logs.html", {"request": request, "title": "Logs", "validation_logs": validation_logs, "upload_logs": upload_logs, "filter_username": username or "", "show_stepper": False},
    )


@app.get("/logs", response_class=HTMLResponse)
async def user_logs(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    username = get_username(request)
    validation_logs = DB.list_validation_logs(username=username)
    upload_logs = DB.list_upload_logs(username=username)
    return templates.TemplateResponse(
        "user_logs.html", {"request": request, "title": "My Logs", "validation_logs": validation_logs, "upload_logs": upload_logs, "show_stepper": False},
    )


@app.post("/admin/users/create")
async def admin_create_user(request: Request, username: str = Form(...), email: str = Form(...), first_name: str = Form(...), last_name: str = Form(...), is_admin: str = Form("false")):
    if not require_login(request) or not require_admin(request):
        return RedirectResponse(url="/login", status_code=302)
    DB.create_user(username=username, email=email, first_name=first_name, last_name=last_name, is_admin=(is_admin == "true"))
    return RedirectResponse(url="/admin/users", status_code=302)


@app.post("/admin/users/delete")
async def admin_delete_user(request: Request, user_id: int = Form(...)):
    if not require_login(request) or not require_admin(request):
        return RedirectResponse(url="/login", status_code=302)
    DB.delete_user(user_id)
    return RedirectResponse(url="/admin/users", status_code=302)


@app.post("/admin/users/reset")
async def admin_require_reset(request: Request, user_id: int = Form(...)):
    if not require_login(request) or not require_admin(request):
        return RedirectResponse(url="/login", status_code=302)
    DB.require_password_reset(user_id)
    return RedirectResponse(url="/admin/users", status_code=302)


@app.post("/admin/users/update")
async def admin_update_user(
    request: Request,
    user_id: int = Form(...),
    email: str = Form(...),
    first_name: str = Form(...),
    last_name: str = Form(...),
    is_admin: str = Form("false"),
):
    if not require_login(request) or not require_admin(request):
        return RedirectResponse(url="/login", status_code=302)
    DB.update_user(user_id=int(user_id), email=email, first_name=first_name, last_name=last_name, is_admin=(is_admin == "true"))
    return RedirectResponse(url="/admin/users", status_code=302)


@app.get("/reset-password", response_class=HTMLResponse)
async def reset_password_get(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("password_reset.html", {"request": request, "title": "Reset Password", "show_stepper": False})


@app.post("/reset-password")
async def reset_password_post(request: Request, password1: str = Form(...), password2: str = Form(...)):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if password1 != password2:
        return templates.TemplateResponse("password_reset.html", {"request": request, "title": "Reset Password", "error": "Passwords do not match", "show_stepper": False})
    user_id = request.session.get("user_id")
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)
    DB.set_password(int(user_id), password1, clear_force_reset=True)
    return RedirectResponse(url="/", status_code=302)


@app.get("/airflow", response_class=HTMLResponse)
async def airflow(request: Request, url: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    airflow_url = url or os.environ.get("NDAP_AIRFLOW_URL", "")
    return templates.TemplateResponse(
        "airflow.html",
        {
            "request": request,
            "title": "Airflow",
            "airflow_url": airflow_url,
            "show_stepper": False,
        },
    )


@app.get("/airflow/run-status")
async def airflow_run_status(request: Request, dag_id: str, dag_run_id: str):
    if not require_login(request):
        return JSONResponse({"error": "Not authenticated"}, status_code=401)
    dag_id = (dag_id or "").strip()
    dag_run_id = (dag_run_id or "").strip()
    if not dag_id or not dag_run_id:
        return JSONResponse({"error": "dag_id and dag_run_id are required"}, status_code=400)
    base, username, password = _get_airflow_config()
    if not base:
        return JSONResponse({"error": "Airflow base URL is not configured"}, status_code=400)
    success, dag_info = _airflow_api_get_json(base, f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}", username, password)
    if not success:
        return JSONResponse({"error": dag_info}, status_code=502)
    state = dag_info.get("state")
    payload: Dict[str, object] = {
        "state": state,
        "dag_run": dag_info,
        "complete": str(state or "").lower() in AIRFLOW_TERMINAL_STATES,
    }
    tasks_success, tasks_info = _airflow_api_get_json(
        base, f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", username, password
    )
    tasks: List[Dict[str, object]] = []
    source_code: str | None = None
    if tasks_success and isinstance(tasks_info, dict):
        for task in tasks_info.get("task_instances", [])[:15]:
            task_id = task.get("task_id")
            log_snippet = ""
            if task_id:
                log_success, log_info = _airflow_api_get_json(
                    base,
                    f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/1",
                    username,
                    password,
                    timeout=20,
                )
                if log_success and isinstance(log_info, dict):
                    content = log_info.get("content") or ""
                    log_snippet = content[-TASK_LOG_SNIPPET_LIMIT:]
                elif log_success and isinstance(log_info, str):
                    log_snippet = log_info[-TASK_LOG_SNIPPET_LIMIT:]
                else:
                    log_snippet = f"(log unavailable: {log_info})" if log_info else "(log unavailable)"
                if log_snippet and source_code is None:
                    match = re.search(r"New source code generated:\s*(\d+)", log_snippet)
                    if match:
                        source_code = match.group(1)
            tasks.append(
                {
                    "task_id": task.get("task_id"),
                    "state": task.get("state"),
                    "start_date": task.get("start_date"),
                    "end_date": task.get("end_date"),
                    "log": log_snippet,
                }
            )
    else:
        if not tasks_success:
            payload["tasks_error"] = tasks_info
    payload["tasks"] = tasks
    if source_code:
        payload["source_code"] = source_code
    return JSONResponse(payload)


@app.get("/airflow/dag/{dag_id}", response_class=HTMLResponse)
async def airflow_dag(
    request: Request,
    dag_id: str,
    view: str = "grid",
    legacy: str | None = None,
    base_url: str | None = None,
):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    airflow_base = base_url or os.environ.get("NDAP_AIRFLOW_URL", "")
    airflow_url = ""
    if airflow_base:
        v = (view or "grid").lower()
        if legacy in ("1", "true", "yes"):
            # Older Airflow URL pattern
            path = f"/{v}?dag_id={dag_id}"
        else:
            # Airflow 2.x modern UI
            path = f"/dags/{dag_id}/{v}"
        airflow_url = airflow_base.rstrip("/") + path
    return templates.TemplateResponse(
        "airflow.html",
        {
            "request": request,
            "title": f"Airflow - {dag_id}",
            "airflow_url": airflow_url,
            "show_stepper": False,
        },
    )


@app.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings_get(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not require_admin(request):
        return RedirectResponse(url="/", status_code=302)
    keys = ["NDAP_AIRFLOW_URL","NDAP_AIRFLOW_USER","NDAP_AIRFLOW_PASSWORD","NDAP_AIRFLOW_DAG_ID","loading_dag_id","S3_BUCKET","S3_ACCESS_KEY_ID","S3_SECRET_ACCESS_KEY"]
    try:
        settings = DB.get_settings(keys)
    except Exception:
        settings = {k: "" for k in keys}
    saved = True if request.query_params.get("saved") == "1" else False
    return templates.TemplateResponse("admin_settings.html", {"request": request, "title": "Settings", "settings": settings, "show_stepper": False, "saved": saved})


@app.post("/admin/settings/save")
async def admin_settings_save(
    request: Request,
    NDAP_AIRFLOW_URL: str = Form(""),
    NDAP_AIRFLOW_USER: str = Form(""),
    NDAP_AIRFLOW_PASSWORD: str = Form(""),
    NDAP_AIRFLOW_DAG_ID: str = Form(""),
    loading_dag_id: str = Form(""),
    S3_BUCKET: str = Form(""),
    S3_ACCESS_KEY_ID: str = Form(""),
    S3_SECRET_ACCESS_KEY: str = Form(""),
):
    if not require_login(request) or not require_admin(request):
        return RedirectResponse(url="/login", status_code=302)
    try:
        DB.set_setting("NDAP_AIRFLOW_URL", NDAP_AIRFLOW_URL)
        DB.set_setting("NDAP_AIRFLOW_USER", NDAP_AIRFLOW_USER)
        DB.set_setting("NDAP_AIRFLOW_PASSWORD", NDAP_AIRFLOW_PASSWORD)
        DB.set_setting("NDAP_AIRFLOW_DAG_ID", NDAP_AIRFLOW_DAG_ID)
        DB.set_setting("loading_dag_id", loading_dag_id)
        DB.set_setting("S3_BUCKET", S3_BUCKET)
        DB.set_setting("S3_ACCESS_KEY_ID", S3_ACCESS_KEY_ID)
        DB.set_setting("S3_SECRET_ACCESS_KEY", S3_SECRET_ACCESS_KEY)
    except Exception:
        pass
    return RedirectResponse(url="/admin/settings?saved=1", status_code=303)
