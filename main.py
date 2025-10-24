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


def _get_session_steps(request: Request) -> set[int]:
    raw = request.session.get("step_progress")
    if not raw:
        return set()
    steps: set[int] = set()
    for value in raw:
        try:
            steps.add(int(value))
        except Exception:
            continue
    return steps


def _set_session_steps(request: Request, steps: set[int]) -> None:
    request.session["step_progress"] = sorted(int(s) for s in steps)


def _add_session_step(request: Request, step: int) -> None:
    steps = _get_session_steps(request)
    steps.add(int(step))
    _set_session_steps(request, steps)


def _set_active_token(request: Request, token: str) -> None:
    if token:
        request.session["active_token"] = token


def _get_active_token(request: Request) -> str | None:
    return request.session.get("active_token")


def _clear_active_run(request: Request) -> None:
    request.session.pop("active_token", None)
    request.session.pop("step_progress", None)


def _combined_steps(request: Request, token: str | None) -> set[int]:
    steps = _get_session_steps(request)
    if token and token in STEP_COMPLETION:
        steps |= STEP_COMPLETION[token]
    return steps

def get_username(request: Request) -> str | None:
    return request.session.get("username")

def require_login(request: Request) -> bool:
    return bool(get_username(request))


def _generate_run_id() -> str:
    return f"RUN-{uuid4().hex[:8].upper()}"


def _reset_run_id(request: Request) -> str:
    run_id = _generate_run_id()
    request.session["current_run_id"] = run_id
    return run_id


def _get_active_run_id(request: Request) -> str:
    run_id = request.session.get("current_run_id")
    if not run_id:
        run_id = _reset_run_id(request)
    return run_id


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
    run_id = state.get("run_id", "")
    if (not file_name) or record_count == 0 or not run_id:
        try:
            meta = DB.get_upload_meta(token)
            if meta:
                if not file_name:
                    file_name = meta.get("filename") or ""
                if record_count == 0:
                    record_count = int(meta.get("record_count") or 0)
                if not run_id:
                    run_id = str(meta.get("run_id") or "")
                # Update cached state so future lookups have metadata
                cached = VALIDATION_STATE.setdefault(token, {})
                if file_name and not cached.get("file_name"):
                    cached["file_name"] = file_name
                if record_count and not cached.get("record_count"):
                    cached["record_count"] = record_count
                if run_id and not cached.get("run_id"):
                    cached["run_id"] = run_id
        except Exception:
            pass
    return {"file_name": file_name, "record_count": record_count, "run_id": run_id}


@app.post("/load-mode")
async def set_load_mode(
    request: Request,
    token: str = Form(...),
    load_mode: str = Form("new"),
    next: str | None = Form(None),
):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    state = VALIDATION_STATE.setdefault(token, {})
    mode = (load_mode or "").strip().lower()
    inc_bool = False
    schema_bool = False
    delta_struct_bool = False
    if mode == "full_reload":
        inc_bool = False; schema_bool = True; delta_struct_bool = False
    elif mode == "delta":
        inc_bool = True; schema_bool = True; delta_struct_bool = False
    elif mode == "structure_change":
        inc_bool = False; schema_bool = False; delta_struct_bool = True
    else:
        inc_bool = False; schema_bool = False; delta_struct_bool = False
    state["airflow_params"] = {
        "is_incremental": inc_bool,
        "schema_exists": schema_bool,
        "delta_with_structure_change": delta_struct_bool,
    }
    _set_active_token(request, token)
    if next == "airflow":
        return RedirectResponse(url=f"/airflow-trigger?token={token}", status_code=303)
    elif next == "upload":
        return RedirectResponse(url=f"/upload?token={token}", status_code=303)
    else:
        return RedirectResponse(url=f"/validate?token={token}", status_code=303)

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
# Allow large logs; UI will handle collapse/expand
TASK_LOG_SNIPPET_LIMIT = 200000


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
async def index(request: Request, reset: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    run_id = request.session.get("current_run_id")
    active_token = request.session.get("active_token")
    if reset and reset.lower() in {"1", "true", "yes"}:
        _clear_active_run(request)
        run_id = _reset_run_id(request)
    elif not run_id:
        run_id = _reset_run_id(request)
    completed_steps = _get_session_steps(request)
    # If we already have a token (load type chosen), begin the stepper at Upload step
    if active_token and VALIDATION_STATE.get(active_token):
        # derive load mode for banner
        st = VALIDATION_STATE.get(active_token) or {}
        params = st.get("airflow_params", {}) if isinstance(st, dict) else {}
        inc = bool(params.get("is_incremental"))
        sch = bool(params.get("schema_exists"))
        dsc = bool(params.get("delta_with_structure_change"))
        if dsc:
            load_mode = "structure_change"
        elif inc and sch:
            load_mode = "delta"
        elif (not inc) and sch:
            load_mode = "full_reload"
        else:
            load_mode = "new"
        return templates.TemplateResponse("upload.html", {
            "request": request,
            "active_step": 1,
            "title": "Upload",
            "completed_steps": completed_steps,
            "token": active_token,
            "run_id": run_id,
            "s3_folder_mode": False,
            "load_mode": load_mode,
        })
    # Otherwise render load type selection outside of stepper
    return templates.TemplateResponse("load_type.html", {
        "request": request,
        "active_step": 0,
        "title": "Select Load Type",
        "completed_steps": [],
        "token": None,
        "run_id": run_id,
        "show_stepper": False,
    })


@app.get("/load-type", response_class=HTMLResponse)
async def load_type_get(request: Request, reset: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if reset and reset.lower() in {"1","true","yes"}:
        _clear_active_run(request)
    run_id = request.session.get("current_run_id") or _reset_run_id(request)
    active_token = None  # force selection screen outside stepper when reset
    # Preselect from active token if present
    load_mode = "new"
    if active_token and active_token in VALIDATION_STATE:
        st = VALIDATION_STATE.get(active_token) or {}
        params = st.get("airflow_params", {}) if isinstance(st, dict) else {}
        inc = bool(params.get("is_incremental"))
        sch = bool(params.get("schema_exists"))
        dsc = bool(params.get("delta_with_structure_change"))
        if dsc:
            load_mode = "structure_change"
        elif inc and sch:
            load_mode = "delta"
        elif (not inc) and sch:
            load_mode = "full_reload"
        else:
            load_mode = "new"
    return templates.TemplateResponse("load_type.html", {
        "request": request,
        "active_step": 0,
        "title": "Select Load Type",
        "completed_steps": [],
        "token": None,
        "run_id": run_id,
        "load_mode": load_mode,
        "show_stepper": False,
    })


@app.post("/load-type/save")
async def load_type_save(request: Request, load_mode: str = Form("new")):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    # Start a new lifecycle token
    token = uuid4().hex
    run_id = _reset_run_id(request)
    # Map mode to flags
    mode = (load_mode or "").strip().lower()
    inc = False; sch = False; dsc = False
    if mode == "full_reload":
        inc = False; sch = True; dsc = False
    elif mode == "delta":
        inc = True; sch = True; dsc = False
    elif mode == "structure_change":
        inc = False; sch = False; dsc = True
    else:
        inc = False; sch = False; dsc = False
    airflow_params = {"is_incremental": inc, "schema_exists": sch, "delta_with_structure_change": dsc}
    VALIDATION_STATE[token] = {
        "airflow_params": airflow_params,
        "run_id": run_id,
    }
    # Persist selection in session so multi-worker environments can restore it
    request.session["airflow_params"] = airflow_params
    request.session["load_mode"] = load_mode
    _set_active_token(request, token)
    # Proceed to stepper start (Upload / preview page)
    return RedirectResponse(url="/", status_code=303)


@app.post("/preview", response_class=HTMLResponse)
async def preview(
    request: Request,
    file: UploadFile | None = File(None),
    schema: str = Form("National"),
    run_id: str | None = Form(None),
    s3_folder: str | None = Form(None),
):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    try:
        run_id = (run_id or "").strip() or _generate_run_id()
        request.session["current_run_id"] = run_id
        token = uuid4().hex

        # Determine if S3 folder mode or file upload mode
        s3_input = (s3_folder or "").strip()
        # If both given, prefer S3 folder. If none, show error.
        if not s3_input and (not file or not (file.filename or "").strip()):
            _clear_active_run(request)
            run_id = _reset_run_id(request)
            return templates.TemplateResponse(
                "upload.html",
                {
                    "request": request,
                    "error": "Please upload a CSV or provide an S3 folder path.",
                    "run_id": run_id,
                    "active_step": 1,
                    "title": "Upload",
                    "completed_steps": _get_session_steps(request),
                    "token": _get_active_token(request),
                },
            )

        s3_mode_flag = False
        if s3_input:
            # Build S3 client
            s3_bucket_cfg = (DB.get_setting("S3_BUCKET") or os.environ.get("S3_BUCKET") or "").strip()
            s3_access_key_id = (DB.get_setting("S3_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
            s3_secret_key = (DB.get_setting("S3_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
            s3_region = (os.environ.get("AWS_REGION") or "us-east-1").strip()
            if s3_access_key_id and s3_secret_key:
                uploader = S3Uploader(
                    mode=S3CredentialsMode.Manual,
                    access_key_id=s3_access_key_id,
                    secret_access_key=s3_secret_key,
                    region_name=s3_region,
                )
            else:
                uploader = S3Uploader(mode=S3CredentialsMode.Environment, region_name=s3_region)

            # Parse input: allow full s3://bucket/prefix or just prefix
            in_bucket = s3_bucket_cfg
            in_prefix = s3_input
            if s3_input.lower().startswith("s3://"):
                # s3://bucket/prefix[/]
                rest = s3_input[len("s3://"):]
                parts = rest.split("/", 1)
                in_bucket = parts[0]
                in_prefix = parts[1] if len(parts) > 1 else ""
            in_prefix = (in_prefix or "").lstrip("/")
            if in_prefix and not in_prefix.endswith("/"):
                in_prefix = in_prefix + "/"

            if not in_bucket:
                raise ValueError("S3 bucket is not configured. Please set S3_BUCKET in Settings.")

            all_keys = uploader.list_objects(in_bucket, in_prefix)
            # pick first CSV-like key
            csv_keys = [k for k in all_keys if k.lower().endswith(".csv")]
            if not csv_keys:
                raise ValueError("No CSV files found in the provided S3 folder.")
            sample_key = csv_keys[0]
            content = uploader.get_object_bytes(in_bucket, sample_key)

            DATA_STORE[token] = content
            df = _read_df_from_bytes(content, nrows=10)
            columns: List[str] = list(df.columns)
            preview_html = df.head(10).to_html(index=False, classes="table table-sm table-striped", na_rep="")
            try:
                record_count = int(_read_df_from_bytes(content).shape[0])
            except Exception:
                record_count = 0
            file_name = sample_key

            # Persist selected schema and S3 folder info
            VALIDATION_STATE[token] = {
                "schema": schema,
                "file_name": file_name,
                "record_count": record_count,
                "run_id": run_id,
                "s3_folder_info": {
                    "bucket": in_bucket,
                    "prefix": in_prefix,
                    "files": csv_keys,
                    "sample_key": sample_key,
                },
            }
            try:
                DB.save_upload(token, get_username(request), file_name, content, record_count, run_id=run_id)
            except Exception:
                pass
            s3_mode_flag = True
        else:
            # Backward-compatible file upload path
            content = await file.read()
            # Enforce 300MB max for direct upload
            if len(content) > 300 * 1024 * 1024:
                _clear_active_run(request)
                run_id = _reset_run_id(request)
                return templates.TemplateResponse(
                    "upload.html",
                    {
                        "request": request,
                        "error": f"File is larger than 300MB ({len(content) // (1024*1024)} MB). Please split the file and use the S3 folder option.",
                        "run_id": run_id,
                        "active_step": 1,
                        "title": "Upload",
                        "completed_steps": _get_session_steps(request),
                        "token": _get_active_token(request),
                    },
                )
            DATA_STORE[token] = content
            df = _read_df_from_bytes(content, nrows=10)
            columns: List[str] = list(df.columns)
            preview_html = df.head(10).to_html(index=False, classes="table table-sm table-striped", na_rep="")
            file_name = file.filename or ""
            try:
                record_count = int(_read_df_from_bytes(content).shape[0])
            except Exception:
                record_count = 0
            try:
                DB.save_upload(token, get_username(request), file_name, content, record_count, run_id=run_id)
            except Exception:
                pass
            VALIDATION_STATE[token] = {
                "schema": schema,
                "file_name": file_name,
                "record_count": record_count,
                "run_id": run_id,
            }

        # Mark step 1 completed
        STEP_COMPLETION[token] = {1}
        _set_active_token(request, token)
        _set_session_steps(request, {1})
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
                "completed_steps": _combined_steps(request, token),
                "run_id": run_id,
                "s3_folder_mode": s3_mode_flag,
            },
        )
    except Exception as ex:
        _clear_active_run(request)
        run_id = _reset_run_id(request)
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "error": f"Failed to read CSV: {ex}",
                "run_id": run_id,
                "active_step": 1,
                "title": "Upload",
                "completed_steps": _get_session_steps(request),
                "token": _get_active_token(request),
            },
        )


@app.get("/preview", response_class=HTMLResponse)
async def preview_get(request: Request, token: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not token:
        token = _get_active_token(request)
        if token:
            return RedirectResponse(url=f"/preview?token={token}", status_code=302)
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
    # Compute source code to display for non-new modes
    source_code_display = None
    try:
        if load_mode != "new":
            if s3_folder_mode:
                pref = (s3_folder or "").strip("/")
                if "/pending" in pref:
                    pref = pref.split("/pending", 1)[0]
                source_code_display = (pref.rsplit("/", 1)[-1] if pref else None)
            else:
                source_code_display = ((state.get("s3_form", {}) or {}).get("folder") or None)
    except Exception:
        source_code_display = None
    _set_active_token(request, token)
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
            "completed_steps": _combined_steps(request, token),
            "run_id": meta.get("run_id", ""),
            "s3_folder_mode": bool(VALIDATION_STATE.get(token, {}).get("s3_folder_info")),
        },
    )


@app.post("/validate", response_class=HTMLResponse)
async def validate(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    form = await request.form()
    token = str(form.get("token") or (_get_active_token(request) or ""))
    columns_serialized = str(form.get("columns", ""))
    columns: List[str] = [c for c in columns_serialized.split("|||") if c]
    run_id_raw = str(form.get("run_id") or "").strip()

    # role selections
    role_selection: Dict[str, str] = {}
    measure_type_selection: Dict[str, str] = {}
    for col in columns:
        role_selection[col] = str(form.get(f"role_{col}", "Others"))
        mt = str(form.get(f"measure_type_{col}", "float"))
        measure_type_selection[col] = mt

    time_date_only = False

    try:
        state0 = VALIDATION_STATE.get(token, {})
        s3_info = state0.get("s3_folder_info") if isinstance(state0, dict) else None
        per_column_rows: List[Dict[str, object]] = []
        file_results: List[Dict[str, object]] = []
        passed_all = True

        if s3_info:
            # Validate all CSV files in the S3 folder with the selected roles
            s3_bucket = s3_info.get("bucket", "")
            s3_prefix = s3_info.get("prefix", "")
            csv_files: List[str] = list(s3_info.get("files", []) or [])
            s3_access_key_id = (DB.get_setting("S3_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
            s3_secret_key = (DB.get_setting("S3_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
            s3_region = (os.environ.get("AWS_REGION") or "us-east-1").strip()
            if s3_access_key_id and s3_secret_key:
                uploader = S3Uploader(
                    mode=S3CredentialsMode.Manual,
                    access_key_id=s3_access_key_id,
                    secret_access_key=s3_secret_key,
                    region_name=s3_region,
                )
            else:
                uploader = S3Uploader(mode=S3CredentialsMode.Environment, region_name=s3_region)

            for key in csv_files:
                try:
                    data = uploader.get_object_bytes(s3_bucket, key)
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
                        schema_name=state0.get("schema", "National"),
                    )
                    passed = bool(validation_report.get("passed", False))
                    failed_columns = validation_report.get("failed_columns", [])
                    # Build per-column details similar to single-file view
                    per_col_details: List[Dict[str, object]] = []
                    for col in df.columns:
                        col_report = validation_report["per_column"].get(col, {})
                        per_col_details.append(
                            {
                                "column": col,
                                "role": col_report.get("role"),
                                "nulls": int(col_report.get("nulls", 0)),
                                "conversion_errors": int(coercion_report.get(col, {}).get("conversion_errors", 0)),
                                "passed": bool(col_report.get("passed", False)),
                                "reasons": ", ".join(col_report.get("reasons", [])),
                            }
                        )

                    file_results.append({
                        "file": key,
                        "passed": passed,
                        "failed_columns": failed_columns,
                        "failed_count": len(failed_columns or []),
                        "rows": df.shape[0],
                        "details": per_col_details,
                    })
                    # Log per-file result
                    try:
                        DB.log_validation(token, get_username(request), passed, ",".join(failed_columns), run_id=state0.get("run_id"))
                    except Exception:
                        pass
                    if not passed:
                        passed_all = False
                except Exception as ex_file:
                    file_results.append({
                        "file": key,
                        "passed": False,
                        "failed_columns": [f"error: {ex_file}"],
                        "failed_count": 1,
                        "rows": 0,
                    })
                    passed_all = False

            # set meta to show prefix context
            meta = _get_meta(token)
            prior_state = VALIDATION_STATE.get(token, {})
            schema_name = prior_state.get("schema", "National")
            run_id = run_id_raw or prior_state.get("run_id") or meta.get("run_id", "")
            VALIDATION_STATE[token] = {
                "columns": columns,
                "role_selection": role_selection,
                "measure_type_selection": measure_type_selection,
                "schema": schema_name,
                "rows": per_column_rows,
                "file_results": file_results,
                "passed": passed_all,
                "failed_columns": [],
                "file_name": f"{s3_bucket}/{s3_prefix}",
                "record_count": meta.get("record_count", 0),
                "run_id": run_id,
                "s3_folder_info": s3_info,
            }

            # steps: complete 2, and if all pass, 3 and 4 (skip upload)
            if token not in STEP_COMPLETION:
                STEP_COMPLETION[token] = set()
            STEP_COMPLETION[token].add(2)
            if passed_all:
                STEP_COMPLETION[token].add(3)
                STEP_COMPLETION[token].add(4)
            _set_active_token(request, token)
            _add_session_step(request, 2)
            if passed_all:
                _add_session_step(request, 3)
                _add_session_step(request, 4)

            return templates.TemplateResponse(
                "validate.html",
                {
                    "request": request,
                    "token": token,
                    "columns": columns,
                    "role_options": ROLE_OPTIONS,
                    "measure_types": MEASURE_TYPES,
                    "role_selection": role_selection,
                    "measure_type_selection": measure_type_selection,
                    "schema": schema_name,
                    "rows": per_column_rows,
                    "file_results": file_results,
                    "passed": passed_all,
                    "failed_columns": [],
                    "file_name": f"{s3_bucket}/{s3_prefix}",
                    "record_count": meta.get("record_count", 0),
                    "active_step": 3,
                    "title": "Validate",
                    "completed_steps": _combined_steps(request, token),
                    "run_id": run_id,
                    "s3_folder_mode": True,
                },
            )
        else:
            # Original single-file validation path
            if not _get_data_bytes(token):
                _clear_active_run(request)
                run_id = _reset_run_id(request)
                return templates.TemplateResponse(
                    "upload.html",
                    {
                        "request": request,
                        "error": "Session expired or file not found. Please re-upload your CSV.",
                        "run_id": run_id,
                        "active_step": 1,
                        "title": "Upload",
                        "completed_steps": _get_session_steps(request),
                        "token": None,
                    },
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
            prior_state = VALIDATION_STATE.get(token, {})
            schema_name = prior_state.get("schema", "National")
            run_id = run_id_raw or prior_state.get("run_id") or meta.get("run_id", "")
            VALIDATION_STATE[token] = {
                "columns": columns,
                "role_selection": role_selection,
                "measure_type_selection": measure_type_selection,
                "schema": schema_name,
                "rows": per_column_rows,
                "passed": passed,
                "failed_columns": failed_columns,
                "file_name": meta.get("file_name", ""),
                "record_count": meta.get("record_count", 0),
                "run_id": run_id,
            }
            # Mark step 2 as completed
            if token not in STEP_COMPLETION:
                STEP_COMPLETION[token] = set()
            STEP_COMPLETION[token].add(2)
            # If validation passed, mark step 3 as completed so stepper turns green and step 4 unlocks
            if passed:
                STEP_COMPLETION[token].add(3)

            _set_active_token(request, token)
            _add_session_step(request, 2)
            if passed:
                _add_session_step(request, 3)

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
                    "completed_steps": _combined_steps(request, token),
                    "run_id": run_id,
                    "s3_folder_mode": False,
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
                    run_id=run_id,
                )
            except Exception:
                pass
            try:
                DB.log_validation(token, get_username(request), passed, ",".join(failed_columns), run_id=run_id)
            except Exception:
                pass
            return resp
    except Exception as ex:
        trace = traceback.format_exc()
        _clear_active_run(request)
        run_id = _reset_run_id(request)
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "error": f"Validation failed: {ex}",
                "trace": trace,
                "run_id": run_id,
                "active_step": 1,
                "title": "Upload",
                "completed_steps": _get_session_steps(request),
                "token": None,
            },
        )


@app.get("/validate", response_class=HTMLResponse)
async def validate_get(request: Request, token: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not token:
        token = _get_active_token(request)
        if token:
            return RedirectResponse(url=f"/validate?token={token}", status_code=302)
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

    meta = _get_meta(token)
    if meta.get("run_id") and not state.get("run_id"):
        state["run_id"] = meta.get("run_id")
    _set_active_token(request, token)
    # derive current load mode selection
    airflow_params = state.get("airflow_params", {}) if isinstance(state, dict) else {}
    is_incremental = bool(airflow_params.get("is_incremental"))
    schema_exists = bool(airflow_params.get("schema_exists"))
    delta_with_structure_change = bool(airflow_params.get("delta_with_structure_change"))
    if delta_with_structure_change:
        load_mode = "structure_change"
    elif is_incremental and schema_exists:
        load_mode = "delta"
    elif (not is_incremental) and schema_exists:
        load_mode = "full_reload"
    else:
        load_mode = "new"
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
            "file_results": state.get("file_results", []),
            "passed": state.get("passed", False),
            "failed_columns": state.get("failed_columns", []),
            "file_name": meta.get("file_name", ""),
            "record_count": meta.get("record_count", 0),
            "active_step": 3,
            "title": "Validate",
            "completed_steps": _combined_steps(request, token),
            "run_id": state.get("run_id") or meta.get("run_id", ""),
            "s3_folder_mode": bool(state.get("s3_folder_info")),
            "load_mode": load_mode,
        },
    )


@app.get("/upload", response_class=HTMLResponse)
async def upload_get(request: Request, token: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not token:
        token = _get_active_token(request)
        if token:
            return RedirectResponse(url=f"/upload?token={token}", status_code=302)
        return RedirectResponse(url="/", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state or not state.get("passed", False):
        # Fallback to DB snapshot if available
        try:
            snap = DB.get_validation_snapshot(token)
        except Exception:
            snap = None
        if snap and snap.get("validated_passed"):
            prev = VALIDATION_STATE.get(token, {}) or {}
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
                "run_id": (snap.get("run_id") or ""),
            }
            # Preserve previously selected airflow params (load type)
            if isinstance(prev, dict) and prev.get("airflow_params"):
                state["airflow_params"] = prev.get("airflow_params")
            elif request.session.get("airflow_params"):
                state["airflow_params"] = request.session.get("airflow_params")
            VALIDATION_STATE[token] = state
            if token not in STEP_COMPLETION:
                STEP_COMPLETION[token] = set()
            STEP_COMPLETION[token].add(2)
            STEP_COMPLETION[token].add(3)
            _add_session_step(request, 2)
            _add_session_step(request, 3)
        else:
            # No snapshot/passed info, route back appropriately
            if not state:
                return RedirectResponse(url=f"/preview?token={token}", status_code=302)
            return RedirectResponse(url=f"/validate?token={token}", status_code=302)

    # Ensure step 3 is recorded as completed when reaching step 4
    if token not in STEP_COMPLETION:
        STEP_COMPLETION[token] = set()
    STEP_COMPLETION[token].add(3)
    _set_active_token(request, token)
    _add_session_step(request, 3)

    # Check required settings for Step 4 (S3 only)
    s3_bucket_cfg = (DB.get_setting("S3_BUCKET") or os.environ.get("S3_BUCKET") or "").strip()
    missing_settings = []
    if not s3_bucket_cfg:
        missing_settings.append("S3_BUCKET")
    settings_ready = len(missing_settings) == 0

    s3_form_defaults = state.get("s3_form") or state.get("s3_upload") or {}
    object_key_value = s3_form_defaults.get("object_key", "")
    s3_folder_value = s3_form_defaults.get("folder", "")

    meta = _get_meta(token)
    if meta.get("run_id") and not state.get("run_id"):
        state["run_id"] = meta.get("run_id")
    # Ensure airflow params available from session if missing
    if not state.get("airflow_params") and request.session.get("airflow_params"):
        state["airflow_params"] = request.session.get("airflow_params")
    # derive current load mode selection
    airflow_params = state.get("airflow_params", {}) if isinstance(state, dict) else {}
    is_incremental = bool(airflow_params.get("is_incremental"))
    schema_exists = bool(airflow_params.get("schema_exists"))
    delta_with_structure_change = bool(airflow_params.get("delta_with_structure_change"))
    if delta_with_structure_change:
        load_mode = "structure_change"
    elif is_incremental and schema_exists:
        load_mode = "delta"
    elif (not is_incremental) and schema_exists:
        load_mode = "full_reload"
    else:
        load_mode = "new"
    return templates.TemplateResponse(
        "s3_upload.html",
        {
            "request": request,
            "token": token,
            "columns": state.get("columns", []),
            "role_selection": state.get("role_selection", {}),
            "measure_type_selection": state.get("measure_type_selection", {}),
            "file_name": meta.get("file_name", ""),
            "record_count": meta.get("record_count", 0),
            "settings_ready": settings_ready,
            "missing_settings": missing_settings,
            "object_key_value": object_key_value,
            "s3_folder_value": s3_folder_value,
            "active_step": 4,
            "title": "Upload",
            "completed_steps": _combined_steps(request, token),
            "run_id": state.get("run_id") or meta.get("run_id", ""),
            "load_mode": load_mode,
        },
    )


@app.post("/upload", response_class=HTMLResponse)
async def upload(
    request: Request,
    token: str = Form(...),
    columns: str = Form(...),
    object_key_name: str = Form(""),
    s3_folder_name: str | None = Form(None),
    source_code_value: str | None = Form(None),
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
    meta = _get_meta(token)
    run_id_form = str(form.get("run_id") or "").strip()
    run_id = run_id_form or state.get("run_id") or meta.get("run_id", "")
    if run_id and not state.get("run_id"):
        state["run_id"] = run_id
    _set_active_token(request, token)

    existing_upload = state.get("s3_upload")
    if existing_upload and existing_upload.get("s3_uri"):
        if token not in STEP_COMPLETION:
            STEP_COMPLETION[token] = set()
        STEP_COMPLETION[token].add(3)
        STEP_COMPLETION[token].add(4)
        _add_session_step(request, 3)
        _add_session_step(request, 4)
        return RedirectResponse(url=f"/airflow-trigger?token={token}", status_code=303)

    s3_bucket = (DB.get_setting("S3_BUCKET") or os.environ.get("S3_BUCKET") or "").strip()
    s3_access_key_id = (DB.get_setting("S3_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    s3_secret_key = (DB.get_setting("S3_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    s3_region = (os.environ.get("AWS_REGION") or "us-east-1").strip()

    def _render_upload_with_error(message: str, trace: str | None = None):
        missing_settings = []
        if not s3_bucket:
            missing_settings.append("S3_BUCKET")
        form_defaults = state.get("s3_form") or {}
        meta = _get_meta(token)
        context = {
            "request": request,
            "token": token,
            "columns": state.get("columns", columns_list),
            "role_selection": state.get("role_selection", role_selection),
            "measure_type_selection": state.get("measure_type_selection", measure_type_selection),
            "file_name": meta.get("file_name", ""),
            "record_count": meta.get("record_count", 0),
            "settings_ready": len(missing_settings) == 0,
            "missing_settings": missing_settings,
            "active_step": 4,
            "title": "Upload",
            "completed_steps": _combined_steps(request, token),
            "error": message,
            "trace": trace,
            "object_key_value": form_defaults.get("object_key", ""),
            "s3_folder_value": form_defaults.get("folder", ""),
            "run_id": state.get("run_id") or meta.get("run_id", ""),
        }
        return templates.TemplateResponse("s3_upload.html", context)

    try:
        if not _get_data_bytes(token):
            _clear_active_run(request)
            run_id = _reset_run_id(request)
            return templates.TemplateResponse(
                "upload.html",
                {
                    "request": request,
                    "error": "Session expired or file not found. Please re-upload your CSV.",
                    "run_id": run_id,
                    "active_step": 1,
                    "title": "Upload",
                    "completed_steps": _get_session_steps(request),
                    "token": None,
                },
            )
        data = _get_data_bytes(token)
        # Keep original CSV intact for upload; do not coerce types here to avoid altering integers (e.g., 9 -> 9.0)
        df = _read_df_from_bytes(data)

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

        # Upload the original bytes exactly as provided by user
        csv_bytes = data

        # Determine effective object key and folder based on load mode selection
        airflow_params = state.get("airflow_params", {}) if isinstance(state, dict) else {}
        is_incremental = bool(airflow_params.get("is_incremental"))
        schema_exists = bool(airflow_params.get("schema_exists"))
        delta_with_structure_change = bool(airflow_params.get("delta_with_structure_change"))
        if delta_with_structure_change:
            eff_mode = "structure_change"
        elif is_incremental and schema_exists:
            eff_mode = "delta"
        elif (not is_incremental) and schema_exists:
            eff_mode = "full_reload"
        else:
            eff_mode = "new"

        folder_value = (s3_folder_name or "").strip()
        eff_object_key = (object_key_name or "").strip()
        if eff_mode != "new":
            src = (source_code_value or "").strip()
            if not src:
                return _render_upload_with_error("Please provide Source Code for the selected load type.")
            eff_object_key = f"{src}.csv"
            folder_value = src

        state["s3_form"] = {
            "object_key": eff_object_key,
            "folder": folder_value,
        }
        folder_prefix = folder_value
        if folder_prefix:
            if not folder_prefix.endswith("/"):
                folder_prefix = folder_prefix + "/"
            folder_prefix = folder_prefix + "pending/"
        key = f"{folder_prefix}{eff_object_key}" if folder_prefix else eff_object_key
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
        _set_active_token(request, token)
        _add_session_step(request, 3)
        _add_session_step(request, 4)

        state["s3_upload"] = {
            "bucket": s3_bucket,
            "object_key": key,
            "folder": folder_value,
            "s3_uri": s3_uri,
        }

        try:
            # Determine current load_mode for logging
            airflow_params = state.get("airflow_params", {}) if isinstance(state, dict) else {}
            inc = bool(airflow_params.get("is_incremental")); sch = bool(airflow_params.get("schema_exists")); dsc = bool(airflow_params.get("delta_with_structure_change"))
            if dsc: eff_mode = "structure_change"
            elif inc and sch: eff_mode = "delta"
            elif (not inc) and sch: eff_mode = "full_reload"
            else: eff_mode = "new"
            src_code = None
            if eff_mode != "new":
                sc = (state.get("s3_form", {}) or {}).get("folder") or ""
                src_code = sc or None
            DB.log_upload(
                token,
                get_username(request),
                s3_bucket,
                key,
                s3_uri,
                status="Success",
                comments="Upload completed",
                dag_status="Not triggered",
                dag_run_id=None,
                source_code=src_code,
                run_id=run_id,
                load_mode=eff_mode,
            )
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
        token = _get_active_token(request)
        if token:
            return RedirectResponse(url=f"/airflow-trigger?token={token}", status_code=302)
        return RedirectResponse(url="/", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state or ("s3_upload" not in state and "s3_folder_info" not in state):
        # Try to reconstruct S3 upload state from DB to avoid repeated prompts on multi-worker servers
        rebuilt = False
        try:
            ul = DB.get_latest_upload_log(token)
            if ul and (ul.get("s3_uri") or ul.get("object_key")):
                if not state:
                    state = VALIDATION_STATE.setdefault(token, {})
                state["s3_upload"] = {
                    "bucket": ul.get("bucket", ""),
                    "object_key": ul.get("object_key", ""),
                    "s3_uri": ul.get("s3_uri", ""),
                    # folder not stored; best-effort derive from key's leading path
                    "folder": (ul.get("object_key", "").rpartition('/')[0] if ul.get("object_key") else ""),
                }
                rebuilt = True
                # ensure steps 3 and 4 marked
                if token not in STEP_COMPLETION:
                    STEP_COMPLETION[token] = set()
                STEP_COMPLETION[token].add(3)
                STEP_COMPLETION[token].add(4)
        except Exception:
            pass
        if not rebuilt:
            return RedirectResponse(url=f"/upload?token={token}", status_code=302)
    if token not in STEP_COMPLETION:
        STEP_COMPLETION[token] = set()
    STEP_COMPLETION[token].add(3)
    STEP_COMPLETION[token].add(4)
    _set_active_token(request, token)
    _add_session_step(request, 3)
    _add_session_step(request, 4)

    s3_upload_info = state.get("s3_upload")
    s3_folder_info = state.get("s3_folder_info")
    if s3_upload_info:
        s3_bucket = s3_upload_info.get("bucket", "")
        s3_object_key = s3_upload_info.get("object_key", "")
        s3_uri = s3_upload_info.get("s3_uri", "")
        s3_folder = s3_upload_info.get("folder", "")
        s3_folder_mode = False
    else:
        s3_bucket = (s3_folder_info or {}).get("bucket", "")
        s3_object_key = ""
        s3_prefix = (s3_folder_info or {}).get("prefix", "")
        s3_uri = f"s3://{s3_bucket}/{s3_prefix}"
        s3_folder = s3_prefix
        s3_folder_mode = True

    airflow_base, username, password = _get_airflow_config()
    airflow_dag_id_val = (DB.get_setting("loading_dag_id") or DB.get_setting("NDAP_AIRFLOW_DAG_ID") or "").strip()
    missing_settings = []
    if not airflow_base:
        missing_settings.append("NDAP_AIRFLOW_URL")
    if not airflow_dag_id_val:
        missing_settings.append("loading_dag_id/NDAP_AIRFLOW_DAG_ID")
    settings_ready = len(missing_settings) == 0

    # Ensure airflow params present from session if missing
    if (not isinstance(state, dict)) or ("airflow_params" not in state or not state.get("airflow_params")):
        sess_params = request.session.get("airflow_params")
        if sess_params and isinstance(state, dict):
            state["airflow_params"] = sess_params
    airflow_params = state.get("airflow_params", {})
    is_incremental = bool(airflow_params.get("is_incremental"))
    schema_exists = bool(airflow_params.get("schema_exists"))
    delta_with_structure_change = bool(airflow_params.get("delta_with_structure_change"))

    # Derive load mode
    if delta_with_structure_change:
        load_mode = "structure_change"
    elif is_incremental and schema_exists:
        load_mode = "delta"
    elif (not is_incremental) and schema_exists:
        load_mode = "full_reload"
    else:
        load_mode = "new"

    meta = _get_meta(token)
    # Compute source code display for non-new modes
    source_code_display = None
    try:
        if load_mode != "new":
            if s3_folder_mode:
                pref = (s3_folder or "").strip("/")
                if "/pending" in pref:
                    pref = pref.split("/pending", 1)[0]
                source_code_display = (pref.rsplit("/", 1)[-1] if pref else None)
            else:
                source_code_display = ((state.get("s3_form", {}) or {}).get("folder") or None)
    except Exception:
        source_code_display = None
    # Include existing run info if present
    run_state = state.get("airflow_run", {}) if isinstance(state, dict) else {}
    airflow_dag_run_id = run_state.get("dag_run_id") or None
    airflow_embed_url = run_state.get("airflow_embed_url") or None
    airflow_run_url = run_state.get("airflow_run_url") or None
    # Allow query param to signal a fresh trigger (in case state is missing between workers)
    trig_q = request.query_params.get("triggered")
    forced_trigger = True if trig_q in ("1","true","yes") else False
    drid_q = request.query_params.get("dag_run_id") or None
    context = {
        "request": request,
        "token": token,
        "s3_bucket": s3_bucket,
        "s3_object_key": s3_object_key,
        "s3_uri": s3_uri,
        "s3_folder": s3_folder,
        "load_mode": load_mode,
        "airflow_ready": settings_ready,
        "missing_settings": missing_settings,
        "uploaded_recently": uploaded == "1",
        "s3_folder_mode": s3_folder_mode,
        "file_name": meta.get("file_name", ""),
        "record_count": meta.get("record_count", 0),
        "active_step": 5,
        "title": "Trigger Airflow",
        "completed_steps": _combined_steps(request, token),
        "error": state.get("airflow_error"),
        "trace": None,
        "run_id": meta.get("run_id", ""),
        "airflow_triggered": bool(airflow_dag_run_id) or forced_trigger,
        "source_code_display": source_code_display,
        "airflow_dag_run_id": airflow_dag_run_id or drid_q,
        "airflow_embed_url": airflow_embed_url,
        "airflow_run_url": airflow_run_url,
        "airflow_dag_id": airflow_dag_id_val,
    }
    return templates.TemplateResponse("airflow_trigger.html", context)


@app.post("/airflow-trigger", response_class=HTMLResponse)
async def airflow_trigger_post(
    request: Request,
    token: str = Form(...),
    load_mode: str | None = Form(None),
    is_incremental: str | None = Form(None),
    schema_exists: str | None = Form(None),
    run_id: str | None = Form(None),
):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state or ("s3_upload" not in state and "s3_folder_info" not in state):
        # Attempt to reconstruct minimal context to avoid bouncing back to Upload
        rebuilt = False
        try:
            # If we have an upload log entry, rebuild s3_upload (handled earlier in upload_get)
            # For S3-folder mode, try deriving from saved upload meta (sample filename as key)
            meta_guess = _get_meta(token)
            s3_bucket_cfg = (DB.get_setting("S3_BUCKET") or os.environ.get("S3_BUCKET") or "").strip()
            sample_name = (meta_guess or {}).get("file_name") or ""
            if s3_bucket_cfg and sample_name and "/" in sample_name:
                prefix = sample_name.rpartition("/")[0]
                if not state:
                    state = VALIDATION_STATE.setdefault(token, {})
                state["s3_folder_info"] = {"bucket": s3_bucket_cfg, "prefix": prefix}
                rebuilt = True
        except Exception:
            pass
        if not rebuilt:
            return RedirectResponse(url=f"/upload?token={token}", status_code=302)
    if token not in STEP_COMPLETION:
        STEP_COMPLETION[token] = set()
    _set_active_token(request, token)
    _add_session_step(request, 4)
    s3_upload_info = state.get("s3_upload")
    s3_folder_info = state.get("s3_folder_info")
    s3_bucket = None
    s3_object_key = None
    s3_uri = None
    folder_value = ""
    s3_folder_mode = False
    if s3_upload_info:
        s3_bucket = s3_upload_info.get("bucket")
        s3_object_key = s3_upload_info.get("object_key")
        s3_uri = s3_upload_info.get("s3_uri")
        folder_value = s3_upload_info.get("folder", "")
    elif s3_folder_info:
        s3_bucket = s3_folder_info.get("bucket")
        s3_object_key = ""
        folder_value = s3_folder_info.get("prefix", "")
        s3_uri = f"s3://{s3_bucket}/{folder_value}"
        s3_folder_mode = True
    if not s3_bucket or not s3_uri:
        return RedirectResponse(url=f"/upload?token={token}", status_code=302)

    existing_run = state.get("airflow_run")
    if existing_run and existing_run.get("dag_run_id"):
        STEP_COMPLETION[token].add(5)
        _add_session_step(request, 5)
        return RedirectResponse(url=f"/airflow-trigger?token={token}", status_code=303)

    # Map UI selection
    inc_bool = False
    schema_bool = False
    delta_struct_bool = False
    mode = (load_mode or "").strip().lower()
    # 1) If mode is explicitly posted, use it
    if mode in {"new", "full_reload", "delta", "structure_change"}:
        if mode == "new":
            inc_bool = False; schema_bool = False; delta_struct_bool = False
        elif mode == "full_reload":
            inc_bool = False; schema_bool = True; delta_struct_bool = False
        elif mode == "delta":
            inc_bool = True; schema_bool = True; delta_struct_bool = False
        elif mode == "structure_change":
            inc_bool = False; schema_bool = False; delta_struct_bool = True
    else:
        # 2) Prefer saved selection from validation/start
        params_saved = state.get("airflow_params", {}) if isinstance(state, dict) else {}
        if params_saved:
            inc_bool = bool(params_saved.get("is_incremental"))
            schema_bool = bool(params_saved.get("schema_exists"))
            delta_struct_bool = bool(params_saved.get("delta_with_structure_change"))
        else:
            # 3) Fallback to legacy form toggles if posted
            inc_bool = (is_incremental == "on")
            schema_bool = True if inc_bool else (schema_exists == "on")
            delta_struct_bool = False
    state["airflow_params"] = {"is_incremental": inc_bool, "schema_exists": schema_bool, "delta_with_structure_change": delta_struct_bool}

    airflow_base, username, password = _get_airflow_config()
    airflow_dag_id_val = (DB.get_setting("loading_dag_id") or DB.get_setting("NDAP_AIRFLOW_DAG_ID") or "").strip()
    missing_settings = []
    if not airflow_base:
        missing_settings.append("NDAP_AIRFLOW_URL")
    if not airflow_dag_id_val:
        missing_settings.append("loading_dag_id/NDAP_AIRFLOW_DAG_ID")
    settings_ready = len(missing_settings) == 0

    meta = _get_meta(token)
    run_id_form = (run_id or "").strip()
    resolved_run_id = run_id_form or state.get("run_id") or meta.get("run_id", "")
    if resolved_run_id and not state.get("run_id"):
        state["run_id"] = resolved_run_id
    if resolved_run_id and not meta.get("run_id"):
        meta["run_id"] = resolved_run_id

    def _render_trigger_with_error(message: str, trace: str | None = None):
        refreshed_meta = _get_meta(token)
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
            "file_name": refreshed_meta.get("file_name", ""),
            "record_count": refreshed_meta.get("record_count", 0),
            "active_step": 5,
            "title": "Trigger Airflow",
            "completed_steps": _combined_steps(request, token),
            "error": message,
            "trace": trace,
            "run_id": resolved_run_id or refreshed_meta.get("run_id", ""),
            "s3_folder_mode": s3_folder_mode,
        }
        return templates.TemplateResponse("airflow_trigger.html", context)

    if not settings_ready:
        return _render_trigger_with_error("Airflow settings are incomplete. Please update the Admin settings.")

    dag = airflow_dag_id_val
    usr = username
    pwd = password

    # Build conf (include delta_with_structure_change)
    if s3_folder_mode:
        # Prefer override
        src_override = (state.get("source_code_override") or "").strip() if isinstance(state, dict) else ""
        source_code_value = src_override or folder_value
        conf_obj = {
            "source_code": source_code_value,
            "is_incremental": "yes" if inc_bool else "no",
            "schema_exist": "yes" if schema_bool else "no",
            "delta_with_structure_change": "yes" if delta_struct_bool else "no",
        }
    else:
        conf_obj = {
            "source_code": folder_value,
            "is_incremental": "yes" if inc_bool else "no",
            "schema_exist": "yes" if schema_bool else "no",
            "delta_with_structure_change": "yes" if delta_struct_bool else "no",
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
        _add_session_step(request, 5)
        # Determine source code for logging (non-new modes)
        source_code_for_log = None
        eff_mode = (mode if (load_mode and isinstance(load_mode, str)) else (load_mode or "")).lower() if (load_mode is not None) else (
            "structure_change" if delta_struct_bool else ("delta" if (inc_bool and schema_bool) else ("full_reload" if (not inc_bool and schema_bool) else "new"))
        )
        if eff_mode != "new":
            if s3_folder_mode:
                pref = (folder_value or "").strip("/")
                if "/pending" in pref:
                    pref = pref.split("/pending", 1)[0]
                source_code_for_log = (pref.rsplit("/", 1)[-1] if pref else None)
            else:
                source_code_for_log = ((state.get("s3_form", {}) or {}).get("folder") or None)
        try:
            DB.update_upload_airflow(
                token,
                dag_status="Triggered",
                dag_run_id=airflow_dag_run_id,
                comments="",
                bucket=s3_bucket,
                object_key=s3_object_key or None,
                s3_uri=s3_uri,
                status="Success",
                run_id=resolved_run_id,
                load_mode=(load_mode if 'load_mode' in locals() and load_mode else (
                    "structure_change" if delta_struct_bool else ("delta" if (inc_bool and schema_bool) else ("full_reload" if (not inc_bool and schema_bool) else "new"))
                )),
            )
        except Exception:
            pass
        state["airflow_run"] = {
            "dag_run_id": airflow_dag_run_id or "",
            "status": "Triggered",
        }
    else:
        if isinstance(info, dict):
            airflow_error = info.get("error") or info.get("message") or info.get("raw")
        else:
            airflow_error = str(info)
        try:
            DB.update_upload_airflow(
                token,
                dag_status="Failed",
                comments=airflow_error or "",
                bucket=s3_bucket,
                object_key=s3_object_key,
                s3_uri=s3_uri,
                run_id=resolved_run_id,
            )
        except Exception:
            pass
        return _render_trigger_with_error(airflow_error or "Failed to trigger Airflow DAG.")

    # Redirect to GET view to avoid stale Step 4 header and enable live status polling
    q = f"?token={token}"
    if airflow_dag_run_id:
        from urllib.parse import quote as _q
        q += f"&triggered=1&dag_run_id={_q(str(airflow_dag_run_id))}"
    else:
        q += "&triggered=1"
    return RedirectResponse(url=f"/airflow-trigger{q}", status_code=303)


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
    pipeline_logs = DB.list_pipeline_logs(username=username or None)
    airflow_dag_id = (DB.get_setting("loading_dag_id") or DB.get_setting("NDAP_AIRFLOW_DAG_ID") or "").strip()
    return templates.TemplateResponse(
        "admin_logs.html",
        {
            "request": request,
            "title": "Logs",
            "pipeline_logs": pipeline_logs,
            "filter_username": username or "",
            "show_stepper": False,
            "airflow_dag_id": airflow_dag_id,
        },
    )


@app.get("/logs", response_class=HTMLResponse)
async def user_logs(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    username = get_username(request)
    pipeline_logs = DB.list_pipeline_logs(username=username)
    airflow_dag_id = (DB.get_setting("loading_dag_id") or DB.get_setting("NDAP_AIRFLOW_DAG_ID") or "").strip()
    return templates.TemplateResponse(
        "user_logs.html",
        {
            "request": request,
            "title": "My Logs",
            "pipeline_logs": pipeline_logs,
            "show_stepper": False,
            "airflow_dag_id": airflow_dag_id,
        },
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
    token_param = request.query_params.get("token")
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
                # Try to use full_content=true to fetch complete log content for the latest try
                try_number = task.get("try_number") or 1
                log_path = f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}?full_content=true"
                log_success, log_info = _airflow_api_get_json(
                    base,
                    log_path,
                    username,
                    password,
                    timeout=30,
                )
                if log_success:
                    if isinstance(log_info, dict):
                        content = log_info.get("content") or ""
                        log_snippet = content[-TASK_LOG_SNIPPET_LIMIT:]
                    elif isinstance(log_info, list):
                        # Some Airflow versions return list of segments with 'content'
                        parts = []
                        for seg in log_info:
                            if isinstance(seg, dict) and seg.get("content"):
                                parts.append(str(seg.get("content")))
                            elif isinstance(seg, str):
                                parts.append(seg)
                        content = "\n".join(parts)
                        log_snippet = content[-TASK_LOG_SNIPPET_LIMIT:]
                    elif isinstance(log_info, str):
                        log_snippet = log_info[-TASK_LOG_SNIPPET_LIMIT:]
                    else:
                        log_snippet = "(log format unrecognized)"
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
    payload["dag_run_id"] = dag_run_id
    if source_code:
        payload["source_code"] = source_code

    normalized_state = (state or "").strip()
    status_label = normalized_state.replace("_", " ").title() if normalized_state else None
    if token_param:
        try:
            meta = _get_meta(token_param)
            DB.update_upload_airflow(
                token_param,
                dag_status=status_label,
                dag_run_id=dag_run_id if dag_run_id else None,
                source_code=source_code if source_code else None,
                status="Success" if normalized_state.lower() == "success" else None,
                run_id=meta.get("run_id", ""),
            )
        except Exception:
            pass
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













