"""Optional Supabase persistence mirror for Scord.

SQLite remains the low-latency local cache. When the Render service is configured
with a Supabase service-role key, every committed write is coalesced into an
atomic remote snapshot. A fresh/ephemeral instance restores that snapshot before
the bootstrap account and template rooms are created.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


log = logging.getLogger("shercord.supabase")
SUPABASE_URL = (os.environ.get("SCORD_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SCORD_SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
SNAPSHOT_ID = os.environ.get("SCORD_SUPABASE_SNAPSHOT_ID") or "primary"
_timer: threading.Timer | None = None
_timer_lock = threading.Lock()


def enabled() -> bool:
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


def _request(method: str, path: str, payload: Any | None = None) -> Any:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/{path}",
        data=body,
        method=method,
        headers={
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"Supabase HTTP {error.code}: {detail}") from error


def _table_rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in conn.execute(f"SELECT * FROM {table}").fetchall()]
    except sqlite3.OperationalError:
        return []


def build_snapshot(database_file: str) -> dict[str, Any]:
    conn = sqlite3.connect(database_file)
    conn.row_factory = sqlite3.Row
    try:
        return {
            "schema_version": 2,
            "captured_at": time.time(),
            "accounts": _table_rows(conn, "accounts"),
            "friendships": _table_rows(conn, "friendships"),
            "servers": _table_rows(conn, "servers"),
            "server_members": _table_rows(conn, "server_members"),
            "deleted_servers": _table_rows(conn, "deleted_servers"),
        }
    finally:
        conn.close()


def push_snapshot(database_file: str) -> bool:
    if not enabled() or not Path(database_file).exists():
        return False
    payload = build_snapshot(database_file)
    _request(
        "POST",
        "scord_state_snapshots?on_conflict=id",
        [{"id": SNAPSHOT_ID, "payload": payload, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}],
    )
    log.info("Supabase snapshot synced (%d accounts, %d servers)", len(payload["accounts"]), len(payload["servers"]))
    return True


def schedule_push(database_file: str, delay: float = 1.25) -> None:
    if not enabled():
        return

    def run() -> None:
        global _timer
        try:
            push_snapshot(database_file)
        except Exception as error:  # remote persistence must never crash realtime traffic
            log.warning("Supabase snapshot sync failed: %s", error)
        finally:
            with _timer_lock:
                _timer = None

    global _timer
    with _timer_lock:
        if _timer:
            _timer.cancel()
        _timer = threading.Timer(delay, run)
        _timer.daemon = True
        _timer.start()


def restore_if_empty(database_file: str) -> bool:
    if not enabled() or not Path(database_file).exists():
        return False
    local = sqlite3.connect(database_file)
    try:
        account_count = local.execute("SELECT count(*) FROM accounts").fetchone()[0]
        server_count = local.execute("SELECT count(*) FROM servers").fetchone()[0]
    finally:
        local.close()
    if account_count or server_count:
        return False

    rows = _request("GET", f"scord_state_snapshots?id=eq.{SNAPSHOT_ID}&select=payload&limit=1") or []
    if not rows or not isinstance(rows[0].get("payload"), dict):
        return False
    snapshot = rows[0]["payload"]
    table_order = ("accounts", "friendships", "servers", "server_members", "deleted_servers")
    conn = sqlite3.connect(database_file)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        for table in table_order:
            for row in snapshot.get(table, []):
                if not isinstance(row, dict) or not row:
                    continue
                columns = list(row)
                placeholders = ",".join("?" for _ in columns)
                conn.execute(
                    f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({placeholders})",
                    [row[column] for column in columns],
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info("Supabase snapshot restored (%d accounts, %d servers)", len(snapshot.get("accounts", [])), len(snapshot.get("servers", [])))
    return True
