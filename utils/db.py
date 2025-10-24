from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any
import uuid

import duckdb
from passlib.context import CryptContext


PWD_CONTEXT = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


@dataclass
class Database:
    path: str = os.environ.get("NDAP_APP_DB", "app_data.duckdb")

    def connect(self):
        return duckdb.connect(self.path)

    @staticmethod
    def _new_id() -> int:
        return uuid.uuid4().int & ((1 << 63) - 1)

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
                  run_id TEXT,
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
            con.execute("ALTER TABLE uploads ADD COLUMN IF NOT EXISTS run_id TEXT;")
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
                  run_id TEXT,
                  username TEXT,
                  passed BOOLEAN,
                  failed_columns TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            con.execute("ALTER TABLE validation_logs ADD COLUMN IF NOT EXISTS run_id TEXT;")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_logs (
                  id BIGINT PRIMARY KEY,
                  token TEXT,
                  run_id TEXT,
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
            con.execute("ALTER TABLE upload_logs ADD COLUMN IF NOT EXISTS run_id TEXT;")
            con.execute("ALTER TABLE upload_logs ADD COLUMN IF NOT EXISTS load_mode TEXT;")
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

    def log_validation(
        self,
        token: str,
        username: Optional[str],
        passed: bool,
        failed_columns_csv: str,
        run_id: Optional[str] = None,
    ) -> None:
        con = self.connect()
        try:
            con.execute(
                "INSERT INTO validation_logs (id, token, run_id, username, passed, failed_columns) VALUES (?, ?, ?, ?, ?, ?)",
                [self._new_id(), token, run_id or "", username or "", passed, failed_columns_csv],
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
        dag_status: Optional[str] = "Not triggered",
        dag_run_id: Optional[str] = None,
        source_code: Optional[str] = None,
        run_id: Optional[str] = None,
        load_mode: Optional[str] = None,
    ) -> None:
        con = self.connect()
        try:
            con.execute(
                """
                INSERT INTO upload_logs (
                  id, token, run_id, username, bucket, object_key, s3_uri, cleaned, status, comments, dag_status, dag_run_id, source_code, load_mode
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self._new_id(),
                    token,
                    run_id or "",
                    username or "",
                    bucket,
                    object_key,
                    s3_uri,
                    True,
                    status,
                    comments or "",
                    dag_status or "",
                    dag_run_id or "",
                    source_code or "",
                    load_mode or "",
                ],
            )
        finally:
            con.close()

    # Upload persistence for CSV bytes
    def save_upload(
        self,
        token: str,
        username: Optional[str],
        filename: str,
        data_bytes: bytes,
        record_count: int,
        run_id: Optional[str] = None,
    ) -> None:
        con = self.connect()
        try:
            con.execute("DELETE FROM uploads WHERE token = ?", [token])
            con.execute(
                "INSERT INTO uploads (token, run_id, username, filename, data_bytes, size_bytes, record_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    token,
                    run_id or "",
                    username or "",
                    filename,
                    data_bytes,
                    int(len(data_bytes)),
                    int(record_count),
                ],
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
            row = con.execute(
                "SELECT filename, record_count, size_bytes, run_id FROM uploads WHERE token = ?",
                [token],
            ).fetchone()
            if not row:
                return None
            return {
                "filename": row[0],
                "record_count": int(row[1] or 0),
                "size_bytes": int(row[2] or 0),
                "run_id": row[3] or "",
            }
        finally:
            con.close()

    def save_validation_snapshot(
        self,
        token: str,
        columns_json: str,
        role_selection_json: str,
        time_date_only: bool,
        passed: bool,
        run_id: Optional[str] = None,
    ) -> None:
        con = self.connect()
        try:
            con.execute(
                "UPDATE uploads SET columns_json = ?, role_selection_json = ?, time_date_only = ?, validated_passed = ?, run_id = COALESCE(NULLIF(?, ''), run_id) WHERE token = ?",
                [columns_json, role_selection_json, bool(time_date_only), bool(passed), run_id or "", token],
            )
        finally:
            con.close()

    def get_validation_snapshot(self, token: str) -> Optional[Dict[str, Any]]:
        con = self.connect()
        try:
            row = con.execute(
                "SELECT columns_json, role_selection_json, time_date_only, validated_passed, run_id FROM uploads WHERE token = ?",
                [token],
            ).fetchone()
            if not row:
                return None
            return {
                "columns_json": row[0],
                "role_selection_json": row[1],
                "time_date_only": bool(row[2]) if row[2] is not None else False,
                "validated_passed": bool(row[3]) if row[3] is not None else False,
                "run_id": row[4] or "",
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
        bucket: Optional[str] = None,
        object_key: Optional[str] = None,
        s3_uri: Optional[str] = None,
        status: Optional[str] = None,
        run_id: Optional[str] = None,
        load_mode: Optional[str] = None,
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
            if status is not None:
                updates.append("status = ?")
                params.append(status)
            if run_id is not None:
                updates.append("run_id = ?")
                params.append(run_id)
            if load_mode is not None:
                updates.append("load_mode = ?")
                params.append(load_mode)
            if not updates:
                updates.append("created_at = created_at")
            params.append(token)
            con.execute(f"UPDATE upload_logs SET {', '.join(updates)} WHERE token = ?", params)
            existing = con.execute("SELECT 1 FROM upload_logs WHERE token = ?", [token]).fetchone()
            if not existing and (bucket or object_key or s3_uri):
                # Try to resolve a username from related tables so we don't insert blanks
                uname_row = con.execute(
                    (
                        "SELECT COALESCE(NULLIF(up.username,''), NULLIF(v.username,''), '') AS u "
                        "FROM (SELECT ? AS token) t "
                        "LEFT JOIN uploads up ON up.token = t.token "
                        "LEFT JOIN validation_logs v ON v.token = t.token "
                        "ORDER BY v.created_at DESC NULLS LAST LIMIT 1"
                    ),
                    [token],
                ).fetchone()
                resolved_user = (uname_row[0] if uname_row else "") or ""
                con.execute(
                    """
                    INSERT INTO upload_logs (
                      id, token, run_id, username, bucket, object_key, s3_uri, cleaned, status, comments, dag_status, dag_run_id, source_code, load_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        self._new_id(),
                        token,
                        run_id or "",
                        resolved_user,
                        bucket or "",
                        object_key or "",
                        s3_uri or "",
                        True,
                        status or (dag_status or ""),
                        comments or "",
                        dag_status or "",
                        dag_run_id or "",
                        source_code or "",
                        load_mode or "",
                    ],
                )
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
                    UNION
                    SELECT token FROM uploads
                )
                SELECT
                    CASE
                        WHEN u.created_at IS NOT NULL OR v.created_at IS NOT NULL OR up.created_at IS NOT NULL THEN
                            GREATEST(
                                COALESCE(u.created_at, TIMESTAMP '1970-01-01 00:00:00'),
                                COALESCE(v.created_at, TIMESTAMP '1970-01-01 00:00:00'),
                                COALESCE(up.created_at, TIMESTAMP '1970-01-01 00:00:00')
                            )
                        ELSE NULL
                    END AS event_time,
                    COALESCE(NULLIF(u.username, ''), NULLIF(v.username, ''), NULLIF(up.username, ''), '') AS username,
                    COALESCE(NULLIF(up.filename, ''), u.object_key, '') AS filename,
                    CASE
                        WHEN v.token IS NOT NULL THEN
                            CASE WHEN v.passed THEN 'Success' ELSE 'Failed' END
                        WHEN up.validated_passed IS NOT NULL THEN
                            CASE WHEN up.validated_passed THEN 'Success' ELSE 'Failed' END
                        ELSE 'Not run'
                    END AS validation_status,
                    COALESCE(NULLIF(v.failed_columns, ''), '') AS validation_comments,
                    CASE
                        WHEN u.token IS NULL THEN 'Not uploaded'
                        WHEN COALESCE(u.status, '') <> '' THEN u.status
                        WHEN COALESCE(u.object_key, '') <> '' THEN 'Success'
                        ELSE 'Not uploaded'
                    END AS upload_status,
                    COALESCE(NULLIF(u.object_key, ''), '') AS upload_key,
                    COALESCE(NULLIF(u.comments, ''), '') AS upload_comments,
                    CASE
                        WHEN u.token IS NULL THEN 'Not triggered'
                        WHEN COALESCE(u.dag_status, '') <> '' THEN u.dag_status
                        ELSE 'Not triggered'
                    END AS airflow_status,
                    CASE WHEN NULLIF(u.source_code, '') IS NULL THEN 'N/A' ELSE u.source_code END AS source_code,
                    COALESCE(NULLIF(u.dag_run_id, ''), '') AS dag_run_id,
                    COALESCE(NULLIF(up.run_id, ''), NULLIF(u.run_id, ''), NULLIF(v.run_id, ''), '') AS run_id,
                    t.token AS token,
                    COALESCE(NULLIF(u.load_mode, ''), '') AS load_mode
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
                where_clause = "WHERE COALESCE(u.username, v.username, up.username, '') = ?"
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
                "run_id",
                "token",
                "load_mode",
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

    def get_latest_upload_log(self, token: str) -> Optional[Dict[str, Any]]:
        con = self.connect()
        try:
            row = con.execute(
                (
                    "SELECT bucket, object_key, s3_uri, status, comments, dag_status, dag_run_id, run_id, created_at "
                    "FROM upload_logs WHERE token = ? ORDER BY created_at DESC LIMIT 1"
                ),
                [token],
            ).fetchone()
            if not row:
                return None
            return {
                "bucket": row[0] or "",
                "object_key": row[1] or "",
                "s3_uri": row[2] or "",
                "status": row[3] or "",
                "comments": row[4] or "",
                "dag_status": row[5] or "",
                "dag_run_id": row[6] or "",
                "run_id": row[7] or "",
                "created_at": str(row[8]) if row[8] else "",
            }
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




