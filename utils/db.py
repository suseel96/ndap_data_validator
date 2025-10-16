from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any

import duckdb
from passlib.context import CryptContext


PWD_CONTEXT = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


@dataclass
class Database:
    path: str = os.environ.get("NDAP_APP_DB", "app_data.duckdb")

    def connect(self):
        return duckdb.connect(self.path)

    def init(self) -> None:
        con = self.connect()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                  id INTEGER PRIMARY KEY,
                  username TEXT UNIQUE NOT NULL,
                  password_hash TEXT NOT NULL,
                  email TEXT,
                  first_name TEXT,
                  last_name TEXT,
                  is_admin BOOLEAN DEFAULT FALSE,
                  force_reset BOOLEAN DEFAULT FALSE,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS uploads (
                  token TEXT,
                  username TEXT,
                  filename TEXT,
                  data_bytes BLOB,
                  size_bytes BIGINT,
                  record_count BIGINT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            # Backfill columns for uploads table (validation snapshot persistence)
            con.execute("ALTER TABLE uploads ADD COLUMN IF NOT EXISTS columns_json TEXT;")
            con.execute("ALTER TABLE uploads ADD COLUMN IF NOT EXISTS role_selection_json TEXT;")
            con.execute("ALTER TABLE uploads ADD COLUMN IF NOT EXISTS time_date_only BOOLEAN;")
            con.execute("ALTER TABLE uploads ADD COLUMN IF NOT EXISTS validated_passed BOOLEAN;")
            # Backfill columns if database exists from older version
            con.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;")
            con.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;")
            con.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT;")
            con.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;")
            con.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS force_reset BOOLEAN DEFAULT FALSE;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS validation_logs (
                  id BIGINT PRIMARY KEY,
                  token TEXT,
                  username TEXT,
                  passed BOOLEAN,
                  failed_columns TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_logs (
                  id BIGINT PRIMARY KEY,
                  token TEXT,
                  username TEXT,
                  bucket TEXT,
                  object_key TEXT,
                  s3_uri TEXT,
                  cleaned BOOLEAN,
                  status TEXT,
                  comments TEXT,
                  dag_status TEXT,
                  dag_run_id TEXT,
                  source_code TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            con.execute("ALTER TABLE upload_logs ADD COLUMN IF NOT EXISTS status TEXT;")
            con.execute("ALTER TABLE upload_logs ADD COLUMN IF NOT EXISTS comments TEXT;")
            con.execute("ALTER TABLE upload_logs ADD COLUMN IF NOT EXISTS dag_status TEXT;")
            con.execute("ALTER TABLE upload_logs ADD COLUMN IF NOT EXISTS dag_run_id TEXT;")
            con.execute("ALTER TABLE upload_logs ADD COLUMN IF NOT EXISTS source_code TEXT;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS app_settings (
                  key TEXT PRIMARY KEY,
                  value TEXT
                );
                """
            )
        finally:
            con.close()

    def ensure_default_admin(self) -> None:
        username = os.environ.get("NDAP_ADMIN_USER", "admin")
        password = os.environ.get("NDAP_ADMIN_PASSWORD", "admin")
        con = self.connect()
        try:
            exists = con.execute("SELECT 1 FROM users WHERE username = ?", [username]).fetchone()
            if not exists:
                ph = PWD_CONTEXT.hash(password)
                con.execute(
                    "INSERT INTO users (id, username, password_hash, email, first_name, last_name, is_admin, force_reset) VALUES (?, ?, ?, ?, ?, ?, TRUE, FALSE)",
                    [1, username, ph, f"{username}@example.com", "Admin", "User"],
                )
        finally:
            con.close()

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        con = self.connect()
        try:
            row = con.execute(
                "SELECT id, username, password_hash, email, first_name, last_name, is_admin, force_reset FROM users WHERE username = ?",
                [username],
            ).fetchone()
            if not row:
                return None
            cols = ["id", "username", "password_hash", "email", "first_name", "last_name", "is_admin", "force_reset"]
            return dict(zip(cols, row))
        finally:
            con.close()

    def list_users(self) -> list[Dict[str, Any]]:
        con = self.connect()
        try:
            rows = con.execute(
                "SELECT id, username, email, first_name, last_name, is_admin, force_reset, created_at FROM users ORDER BY id"
            ).fetchall()
            result = []
            cols = ["id", "username", "email", "first_name", "last_name", "is_admin", "force_reset", "created_at"]
            for r in rows:
                result.append(dict(zip(cols, r)))
            return result
        finally:
            con.close()

    def create_user(self, username: str, email: str, first_name: str, last_name: str, is_admin: bool, default_password: str = "ChangeMe123!") -> None:
        ph = PWD_CONTEXT.hash(default_password)
        con = self.connect()
        try:
            # simple id generation: max(id)+1 or 1
            row = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM users").fetchone()
            new_id = int(row[0]) if row else 1
            con.execute(
                "INSERT INTO users (id, username, password_hash, email, first_name, last_name, is_admin, force_reset) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)",
                [new_id, username, ph, email, first_name, last_name, bool(is_admin)],
            )
        finally:
            con.close()

    def update_user(self, user_id: int, email: str, first_name: str, last_name: str, is_admin: bool) -> None:
        con = self.connect()
        try:
            con.execute(
                "UPDATE users SET email = ?, first_name = ?, last_name = ?, is_admin = ? WHERE id = ?",
                [email, first_name, last_name, bool(is_admin), int(user_id)],
            )
        finally:
            con.close()

    def delete_user(self, user_id: int) -> None:
        con = self.connect()
        try:
            con.execute("DELETE FROM users WHERE id = ?", [int(user_id)])
        finally:
            con.close()

    def set_password(self, user_id: int, new_password: str, clear_force_reset: bool = True) -> None:
        ph = PWD_CONTEXT.hash(new_password)
        con = self.connect()
        try:
            if clear_force_reset:
                con.execute("UPDATE users SET password_hash = ?, force_reset = FALSE WHERE id = ?", [ph, int(user_id)])
            else:
                con.execute("UPDATE users SET password_hash = ? WHERE id = ?", [ph, int(user_id)])
        finally:
            con.close()

    def require_password_reset(self, user_id: int) -> None:
        con = self.connect()
        try:
            con.execute("UPDATE users SET force_reset = TRUE WHERE id = ?", [int(user_id)])
        finally:
            con.close()

    def verify_user(self, username: str, password: str) -> tuple[bool, Optional[Dict[str, Any]]]:
        con = self.connect()
        try:
            row = con.execute("SELECT password_hash FROM users WHERE username = ?", [username]).fetchone()
            if not row:
                return False, None
            ok = PWD_CONTEXT.verify(password, row[0])
            if not ok:
                return False, None
            return True, self.get_user(username)
        finally:
            con.close()

    def log_validation(self, token: str, username: Optional[str], passed: bool, failed_columns_csv: str) -> None:
        con = self.connect()
        try:
            con.execute(
                "INSERT INTO validation_logs (id, token, username, passed, failed_columns) VALUES (hash(now()), ?, ?, ?, ?)",
                [token, username or "", passed, failed_columns_csv],
            )
        finally:
            con.close()

    def log_upload(
        self,
        token: str,
        username: Optional[str],
        bucket: str,
        object_key: str,
        s3_uri: str,
        status: str,
        comments: Optional[str] = None,
        dag_status: Optional[str] = None,
        dag_run_id: Optional[str] = None,
        source_code: Optional[str] = None,
    ) -> None:
        con = self.connect()
        try:
            con.execute(
                """
                INSERT INTO upload_logs (
                  id, token, username, bucket, object_key, s3_uri, cleaned, status, comments, dag_status, dag_run_id, source_code
                )
                VALUES (hash(now()), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    token,
                    username or "",
                    bucket,
                    object_key,
                    s3_uri,
                    False,
                    status,
                    comments or "",
                    dag_status or "",
                    dag_run_id or "",
                    source_code or "",
                ],
            )
        finally:
            con.close()

    # Upload persistence for CSV bytes
    def save_upload(self, token: str, username: Optional[str], filename: str, data_bytes: bytes, record_count: int) -> None:
        con = self.connect()
        try:
            con.execute("DELETE FROM uploads WHERE token = ?", [token])
            con.execute(
                "INSERT INTO uploads (token, username, filename, data_bytes, size_bytes, record_count) VALUES (?, ?, ?, ?, ?, ?)",
                [token, username or "", filename, data_bytes, int(len(data_bytes)), int(record_count)],
            )
        finally:
            con.close()

    def get_upload_bytes(self, token: str) -> Optional[bytes]:
        con = self.connect()
        try:
            row = con.execute("SELECT data_bytes FROM uploads WHERE token = ?", [token]).fetchone()
            if not row:
                return None
            return row[0]
        finally:
            con.close()

    def get_upload_meta(self, token: str) -> Optional[Dict[str, Any]]:
        con = self.connect()
        try:
            row = con.execute("SELECT filename, record_count, size_bytes FROM uploads WHERE token = ?", [token]).fetchone()
            if not row:
                return None
            return {"filename": row[0], "record_count": int(row[1] or 0), "size_bytes": int(row[2] or 0)}
        finally:
            con.close()

    def save_validation_snapshot(self, token: str, columns_json: str, role_selection_json: str, time_date_only: bool, passed: bool) -> None:
        con = self.connect()
        try:
            con.execute(
                "UPDATE uploads SET columns_json = ?, role_selection_json = ?, time_date_only = ?, validated_passed = ? WHERE token = ?",
                [columns_json, role_selection_json, bool(time_date_only), bool(passed), token],
            )
        finally:
            con.close()

    def get_validation_snapshot(self, token: str) -> Optional[Dict[str, Any]]:
        con = self.connect()
        try:
            row = con.execute(
                "SELECT columns_json, role_selection_json, time_date_only, validated_passed FROM uploads WHERE token = ?",
                [token],
            ).fetchone()
            if not row:
                return None
            return {
                "columns_json": row[0],
                "role_selection_json": row[1],
                "time_date_only": bool(row[2]) if row[2] is not None else False,
                "validated_passed": bool(row[3]) if row[3] is not None else False,
            }
        finally:
            con.close()

    def list_validation_logs(self, username: Optional[str] = None) -> list[Dict[str, Any]]:
        con = self.connect()
        try:
            if username:
                rows = con.execute(
                    (
                        "SELECT v.id, v.token, v.username, v.passed, v.failed_columns, v.created_at, u.filename "
                        "FROM validation_logs v LEFT JOIN uploads u ON v.token = u.token "
                        "WHERE v.username = ? ORDER BY v.created_at DESC"
                    ),
                    [username],
                ).fetchall()
            else:
                rows = con.execute(
                    (
                        "SELECT v.id, v.token, v.username, v.passed, v.failed_columns, v.created_at, u.filename "
                        "FROM validation_logs v LEFT JOIN uploads u ON v.token = u.token "
                        "ORDER BY v.created_at DESC"
                    )
                ).fetchall()
            cols = ["id", "token", "username", "passed", "failed_columns", "created_at", "filename"]
            return [dict(zip(cols, r)) for r in rows]
        finally:
            con.close()

    def update_upload_airflow(
        self,
        token: str,
        dag_status: Optional[str] = None,
        dag_run_id: Optional[str] = None,
        source_code: Optional[str] = None,
        comments: Optional[str] = None,
    ) -> None:
        con = self.connect()
        try:
            updates = []
            params: list[object] = []
            if dag_status is not None:
                updates.append("dag_status = ?")
                params.append(dag_status)
            if dag_run_id is not None:
                updates.append("dag_run_id = ?")
                params.append(dag_run_id)
            if source_code is not None:
                updates.append("source_code = ?")
                params.append(source_code)
            if comments is not None:
                updates.append("comments = ?")
                params.append(comments)
            if not updates:
                return
            params.append(token)
            con.execute(f"UPDATE upload_logs SET {', '.join(updates)} WHERE token = ?", params)
        finally:
            con.close()

    def list_pipeline_logs(self, username: Optional[str] = None) -> list[Dict[str, Any]]:
        con = self.connect()
        try:
            query = """
                WITH latest_upload AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY token ORDER BY created_at DESC) AS rn
                    FROM upload_logs
                ),
                latest_validation AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY token ORDER BY created_at DESC) AS rn
                    FROM validation_logs
                ),
                tokens AS (
                    SELECT token FROM latest_upload
                    UNION
                    SELECT token FROM latest_validation
                )
                SELECT
                    COALESCE(u.created_at, v.created_at, up.created_at) AS event_time,
                    COALESCE(u.username, v.username, '') AS username,
                    up.filename,
                    CASE
                        WHEN v.token IS NULL THEN 'Not run'
                        WHEN v.passed THEN 'Success'
                        ELSE 'Failed'
                    END AS validation_status,
                    COALESCE(v.failed_columns, '') AS validation_comments,
                    CASE
                        WHEN u.status IS NULL OR u.status = '' THEN 'Not uploaded'
                        ELSE u.status
                    END AS upload_status,
                    COALESCE(u.object_key, '') AS upload_key,
                    COALESCE(u.comments, '') AS upload_comments,
                    CASE
                        WHEN u.dag_status IS NULL OR u.dag_status = '' THEN 'Not triggered'
                        ELSE u.dag_status
                    END AS airflow_status,
                    CASE
                        WHEN u.source_code IS NULL OR u.source_code = '' THEN 'N/A'
                        ELSE u.source_code
                    END AS source_code,
                    COALESCE(u.dag_run_id, '') AS dag_run_id
                FROM tokens t
                LEFT JOIN latest_upload u ON u.token = t.token AND u.rn = 1
                LEFT JOIN latest_validation v ON v.token = t.token AND v.rn = 1
                LEFT JOIN uploads up ON up.token = t.token
                {where_clause}
                ORDER BY event_time DESC NULLS LAST
            """
            where_clause = ""
            params: list[object] = []
            if username:
                where_clause = "WHERE COALESCE(u.username, v.username, '') = ?"
                params.append(username)
            rows = con.execute(query.format(where_clause=where_clause), params).fetchall()
            cols = [
                "event_time",
                "username",
                "filename",
                "validation_status",
                "validation_comments",
                "upload_status",
                "upload_key",
                "upload_comments",
                "airflow_status",
                "source_code",
                "dag_run_id",
            ]
            results = []
            for row in rows:
                record = dict(zip(cols, row))
                if record["event_time"]:
                    record["event_time"] = str(record["event_time"])
                results.append(record)
            return results
        finally:
            con.close()



    # Settings helpers
    def set_setting(self, key: str, value: Optional[str]) -> None:
        con = self.connect()
        try:
            con.execute("DELETE FROM app_settings WHERE key = ?", [key])
            con.execute("INSERT INTO app_settings (key, value) VALUES (?, ?)", [key, value or ""])
        finally:
            con.close()

    def get_setting(self, key: str) -> Optional[str]:
        con = self.connect()
        try:
            row = con.execute("SELECT value FROM app_settings WHERE key = ?", [key]).fetchone()
            return row[0] if row else None
        finally:
            con.close()

    def get_settings(self, keys: list[str]) -> Dict[str, Optional[str]]:
        result: Dict[str, Optional[str]] = {}
        con = self.connect()
        try:
            for k in keys:
                row = con.execute("SELECT value FROM app_settings WHERE key = ?", [k]).fetchone()
                result[k] = row[0] if row else None
            return result
        finally:
            con.close()


