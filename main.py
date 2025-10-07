from __future__ import annotations

import io
import traceback
from typing import Dict, List, Tuple
import os
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
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



def _read_df_from_bytes(data: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(data), nrows=10)


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

        df = _read_df_from_bytes(content)
        columns: List[str] = list(df.columns)
        preview_html = df.head(10).to_html(index=False, classes="table table-sm table-striped")

        # Persist selected schema and mark step 1 as completed
        VALIDATION_STATE[token] = {"schema": schema}
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
async def preview_get(request: Request, token: str):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    # Re-render preview/selection by reading back from stored bytes
    content = DATA_STORE.get(token)
    if not content:
        return RedirectResponse(url="/", status_code=302)

    df = _read_df_from_bytes(content)
    columns: List[str] = list(df.columns)
    preview_html = df.head(10).to_html(index=False, classes="table table-sm table-striped")

    schema = VALIDATION_STATE.get(token, {}).get("schema", "National")
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

    time_date_only = form.get("time_date_only") == "on"

    try:
        if token not in DATA_STORE:
            return templates.TemplateResponse(
                "upload.html",
                {"request": request, "error": "Session expired or file not found. Please re-upload your CSV."},
            )
        data = DATA_STORE[token]
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
        VALIDATION_STATE[token] = {
            "columns": columns,
            "role_selection": role_selection,
            "measure_type_selection": measure_type_selection,
            "time_date_only": time_date_only,
            "schema": VALIDATION_STATE.get(token, {}).get("schema", "National"),
            "rows": per_column_rows,
            "passed": passed,
            "failed_columns": failed_columns,
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
                "time_date_only": time_date_only,
                "schema": VALIDATION_STATE.get(token, {}).get("schema", "National"),
                "rows": per_column_rows,
                "passed": passed,
                "failed_columns": failed_columns,
                "active_step": 3,
                "title": "Validate",
                "completed_steps": STEP_COMPLETION.get(token, set()),
            },
        )
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
async def validate_get(request: Request, token: str):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
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
            "time_date_only": state.get("time_date_only", False),
            "schema": state.get("schema", "National"),
            "rows": state.get("rows", []),
            "passed": state.get("passed", False),
            "failed_columns": state.get("failed_columns", []),
            "active_step": 3,
            "title": "Validate",
            "completed_steps": STEP_COMPLETION.get(token, set()),
        },
    )


@app.get("/upload", response_class=HTMLResponse)
async def upload_get(request: Request, token: str):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    state = VALIDATION_STATE.get(token)
    if not state:
        return RedirectResponse(url=f"/preview?token={token}", status_code=302)
    if not state.get("passed", False):
        return RedirectResponse(url=f"/validate?token={token}", status_code=302)

    # Ensure step 3 is recorded as completed when reaching step 4
    if token not in STEP_COMPLETION:
        STEP_COMPLETION[token] = set()
    STEP_COMPLETION[token].add(3)

    return templates.TemplateResponse(
        "s3_upload.html",
        {
            "request": request,
            "token": token,
            "columns": state.get("columns", []),
            "role_selection": state.get("role_selection", {}),
            "measure_type_selection": state.get("measure_type_selection", {}),
            "time_date_only": state.get("time_date_only", False),
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
    upload_cleaned: str | None = Form(None),
    bucket: str = Form(...),
    object_key_name: str = Form(...),
    prefix: str = Form(""),
    creds_mode: str = Form("Use environment"),
    access_key: str | None = Form(None),
    secret_key: str | None = Form(None),
    region_name: str | None = Form("us-east-1"),
    time_date_only: str | None = Form(None),
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

    try:
        if token not in DATA_STORE:
            return templates.TemplateResponse(
                "upload.html",
                {"request": request, "error": "Session expired or file not found. Please re-upload your CSV."},
            )
        data = DATA_STORE[token]
        df = _read_df_from_bytes(data)

        cleaned_df, _ = coerce_dataframe_by_roles(
            df,
            role_selection,
            measure_type_selection,
            time_date_only=(time_date_only == "on"),
        )

        uploader = S3Uploader(
            mode=S3CredentialsMode(creds_mode),
            access_key_id=access_key or None,
            secret_access_key=secret_key or None,
            region_name=region_name or None,
        )

        if upload_cleaned == "on":
            csv_bytes = cleaned_df.to_csv(index=False).encode("utf-8")
        else:
            csv_bytes = data

        key = f"{prefix}{object_key_name}" if prefix else object_key_name
        s3_uri = uploader.upload_bytes(
            bucket=bucket,
            key=key,
            data_bytes=csv_bytes,
            content_type="text/csv",
        )

        # Mark step 3 as completed
        if token not in STEP_COMPLETION:
            STEP_COMPLETION[token] = set()
        STEP_COMPLETION[token].add(3)

        resp = templates.TemplateResponse(
            "result.html",
            {
                "request": request,
                "success": True,
                "s3_uri": s3_uri,
                "active_step": 4,
                "title": "Upload",
                "token": token,
                "columns": columns_list,
                "completed_steps": STEP_COMPLETION.get(token, set()),
            },
        )
        try:
            DB.log_upload(token, get_username(request), bucket, key, s3_uri, upload_cleaned == "on")
        except Exception:
            pass
        return resp
    except Exception as ex:
        trace = traceback.format_exc()
        return templates.TemplateResponse(
            "result.html",
            {
                "request": request,
                "success": False,
                "error": str(ex),
                "trace": trace,
                "active_step": 4,
                "title": "Upload",
                "token": token,
                "columns": columns_list,
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
    return templates.TemplateResponse("admin_users.html", {"request": request, "title": "Manage Users", "users": users})
@app.get("/admin/logs", response_class=HTMLResponse)
async def admin_logs(request: Request, username: str | None = None):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    if not require_admin(request):
        return RedirectResponse(url="/", status_code=302)
    validation_logs = DB.list_validation_logs(username=username or None)
    upload_logs = DB.list_upload_logs(username=username or None)
    return templates.TemplateResponse(
        "admin_logs.html",
        {"request": request, "title": "Logs", "validation_logs": validation_logs, "upload_logs": upload_logs, "filter_username": username or ""},
    )


@app.get("/logs", response_class=HTMLResponse)
async def user_logs(request: Request):
    if not require_login(request):
        return RedirectResponse(url="/login", status_code=302)
    username = get_username(request)
    validation_logs = DB.list_validation_logs(username=username)
    upload_logs = DB.list_upload_logs(username=username)
    return templates.TemplateResponse(
        "user_logs.html",
        {"request": request, "title": "My Logs", "validation_logs": validation_logs, "upload_logs": upload_logs},
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
