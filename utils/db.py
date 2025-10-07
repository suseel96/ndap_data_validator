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
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    def log_upload(self, token: str, username: Optional[str], bucket: str, object_key: str, s3_uri: str, cleaned: bool) -> None:
        con = self.connect()
        try:
            con.execute(
                "INSERT INTO upload_logs (id, token, username, bucket, object_key, s3_uri, cleaned) VALUES (hash(now()), ?, ?, ?, ?, ?, ?)",
                [token, username or "", bucket, object_key, s3_uri, cleaned],
            )
        finally:
            con.close()

    def list_validation_logs(self, username: Optional[str] = None) -> list[Dict[str, Any]]:
        con = self.connect()
        try:
            if username:
                rows = con.execute(
                    "SELECT id, token, username, passed, failed_columns, created_at FROM validation_logs WHERE username = ? ORDER BY created_at DESC",
                    [username],
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT id, token, username, passed, failed_columns, created_at FROM validation_logs ORDER BY created_at DESC"
                ).fetchall()
            cols = ["id", "token", "username", "passed", "failed_columns", "created_at"]
            return [dict(zip(cols, r)) for r in rows]
        finally:
            con.close()

    def list_upload_logs(self, username: Optional[str] = None) -> list[Dict[str, Any]]:
        con = self.connect()
        try:
            if username:
                rows = con.execute(
                    "SELECT id, token, username, bucket, object_key, s3_uri, cleaned, created_at FROM upload_logs WHERE username = ? ORDER BY created_at DESC",
                    [username],
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT id, token, username, bucket, object_key, s3_uri, cleaned, created_at FROM upload_logs ORDER BY created_at DESC"
                ).fetchall()
            cols = ["id", "token", "username", "bucket", "object_key", "s3_uri", "cleaned", "created_at"]
            return [dict(zip(cols, r)) for r in rows]
        finally:
            con.close()


