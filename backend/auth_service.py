from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from backend.db import get_conn, init_db

init_db()

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PASSWORD_MIN_LEN = 6
_SESSION_DAYS = 30


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def _hash_password(password: str, *, salt: Optional[str] = None, rounds: int = 120_000) -> str:
    pwd = str(password or "")
    s = salt or secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), s.encode("utf-8"), int(rounds))
    digest = base64.b64encode(dk).decode("ascii")
    return f"pbkdf2_sha256${int(rounds)}${s}${digest}"


def _verify_password(password: str, stored: str) -> bool:
    txt = str(stored or "").strip()
    if not txt:
        return False
    try:
        algo, rounds_txt, salt, digest = txt.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        computed = _hash_password(password, salt=salt, rounds=int(rounds_txt))
        return hmac.compare_digest(computed, txt)
    except Exception:
        return False


def _validate_register_input(email: str, password: str) -> tuple[str, str]:
    em = _normalize_email(email)
    pwd = str(password or "")
    if not em:
        raise ValueError("email required")
    if not _EMAIL_RE.match(em):
        raise ValueError("invalid email format")
    if len(pwd) < _PASSWORD_MIN_LEN:
        raise ValueError(f"password too short, min {_PASSWORD_MIN_LEN} chars")
    return em, pwd


def _user_from_row(row: Any) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "email": str(row["email"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    uid = int(user_id)
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, email, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (uid,),
        ).fetchone()
    return _user_from_row(row) if row else None


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    em = _normalize_email(email)
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, email, created_at, updated_at
            FROM users
            WHERE email = ?
            """,
            (em,),
        ).fetchone()
    return _user_from_row(row) if row else None


def register_user(email: str, password: str) -> Dict[str, Any]:
    em, pwd = _validate_register_input(email, password)
    password_hash = _hash_password(pwd)
    with get_conn() as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO users (email, password_hash, created_at, updated_at)
                VALUES (?, ?, datetime('now','localtime'), datetime('now','localtime'))
                """,
                (em, password_hash),
            )
        except Exception as e:
            raise ValueError("email already registered") from e
        user_id = int(cur.lastrowid)

    # New user gets one default account.
    from backend import portfolio_service as ps

    ps.ensure_default_account_for_user(user_id)
    user = get_user_by_id(user_id)
    if not user:
        raise ValueError("create user failed")
    return user


def _get_user_with_password(email: str) -> Optional[Dict[str, Any]]:
    em = _normalize_email(email)
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, email, password_hash, created_at, updated_at
            FROM users
            WHERE email = ?
            """,
            (em,),
        ).fetchone()
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "email": str(row["email"] or ""),
        "password_hash": str(row["password_hash"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def create_session(user_id: int, days: int = _SESSION_DAYS) -> Dict[str, Any]:
    uid = int(user_id)
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now() + timedelta(days=max(1, int(days)))).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO auth_sessions (token, user_id, created_at, expires_at, revoked_at)
            VALUES (?, ?, datetime('now','localtime'), ?, NULL)
            """,
            (token, uid, expires_at),
        )
    return {"token": token, "expires_at": expires_at}


def login_user(email: str, password: str) -> Dict[str, Any]:
    em = _normalize_email(email)
    pwd = str(password or "")
    if not em:
        raise ValueError("email required")
    if not pwd:
        raise ValueError("password required")

    user = _get_user_with_password(em)
    if not user:
        raise ValueError("invalid email or password")
    if not _verify_password(pwd, user.get("password_hash", "")):
        raise ValueError("invalid email or password")

    session = create_session(int(user["id"]))
    return {
        "token": session["token"],
        "expires_at": session["expires_at"],
        "user": {
            "id": int(user["id"]),
            "email": str(user["email"] or ""),
            "created_at": str(user["created_at"] or ""),
            "updated_at": str(user["updated_at"] or ""),
        },
    }


def get_user_by_token(token: str) -> Optional[Dict[str, Any]]:
    tk = str(token or "").strip()
    if not tk:
        return None
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT u.id, u.email, u.created_at, u.updated_at
            FROM auth_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ?
              AND (s.revoked_at IS NULL OR s.revoked_at = '')
              AND (s.expires_at IS NULL OR s.expires_at > datetime('now','localtime'))
            """,
            (tk,),
        ).fetchone()
    return _user_from_row(row) if row else None


def revoke_session(token: str) -> None:
    tk = str(token or "").strip()
    if not tk:
        return
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE auth_sessions
            SET revoked_at = datetime('now','localtime')
            WHERE token = ? AND (revoked_at IS NULL OR revoked_at = '')
            """,
            (tk,),
        )

