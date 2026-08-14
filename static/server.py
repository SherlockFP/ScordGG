"""
Shercord Signaling Server
=========================
Tiny FastAPI WebSocket relay for WebRTC peer discovery.
All actual chat/voice data flows directly peer-to-peer; this server
only exchanges SDP offers/answers and ICE candidates.
"""

import os
import json
import time
import uuid
import asyncio
import logging
import threading
import urllib.request
import urllib.parse
import re
import sqlite3
import hashlib
import secrets
import importlib.util
from contextlib import contextmanager
from typing import Dict, Set, Optional, Any
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi import Response
from starlette.websockets import WebSocketState

try:
    from supabase_store import restore_if_empty as restore_supabase_if_empty, schedule_push as schedule_supabase_push
except ImportError:  # package-style imports in some runners
    _supabase_spec = importlib.util.spec_from_file_location("scord_supabase_store", Path(__file__).with_name("supabase_store.py"))
    if _supabase_spec is None or _supabase_spec.loader is None:
        raise
    _supabase_module = importlib.util.module_from_spec(_supabase_spec)
    _supabase_spec.loader.exec_module(_supabase_module)
    restore_supabase_if_empty = _supabase_module.restore_if_empty
    schedule_supabase_push = _supabase_module.schedule_push

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("shercord")

# Do not upload a newly-created empty SQLite file before startup has had a
# chance to restore the last durable Supabase snapshot.
_SUPABASE_SYNC_READY = False

app = FastAPI(title="SCORD Signaling Server")

# ΓöÇΓöÇ Global State ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

rooms: Dict[str, "Room"] = {}
# Kal─▒c─▒ olarak silinen oda id'leri. Sunucu yeniden ba┼ƒlasa bile bu odalar geri
# gelmemeli; istemciler sync s─▒ras─▒nda bunlar─▒ g├╢r├╝p yerel kopyay─▒ temizler.
deleted_room_ids: Set[str] = set()
DELETED_TOMBSTONE_CAP = 500

# Basit in-memory login rate-limit: ip -> ba┼ƒar─▒s─▒z deneme zaman damgalar─▒.
# (ponytail: tek dict + time.time yeter, redis yok — tek instance varsay─▒m─▒.)
LOGIN_RATE_WINDOW = 60.0
LOGIN_RATE_MAX = 10
_login_attempts: Dict[str, list] = {}
_login_attempts_lock = threading.Lock()
# Kal─▒c─▒ disk deste─ƒi: Render'da SCORD_DATA_DIR ile persistent disk yolunu ver
# (├╢rn. /var/data). Verilmezse repo k├╢k├╝ kullan─▒l─▒r ΓÇö free tier'da her
# restart'ta s─▒f─▒rlan─▒r; istemci taraf─▒ndaki auto-reregister bunu telafi eder.
_DATA_DIR = Path(os.environ.get("SCORD_DATA_DIR") or Path(__file__).parent.parent)
_DATA_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_FILE = str(_DATA_DIR / "rooms.json")  # one-time legacy import
# Accounts, sessions, friendships, servers and memberships live in one durable
# SQLite file. Keeping the historical filename preserves existing deployments.
ACCOUNTS_DB_FILE = str(_DATA_DIR / "scord_accounts.db")
PLATFORM_ADMIN_USERNAMES = {
    name.strip().lower()
    for name in os.environ.get("SCORD_PLATFORM_ADMIN_USERNAMES", "sherlock").split(",")
    if name.strip()
}
PLATFORM_ADMIN_IDS: Set[str] = set()

DEFAULT_ROLE_PERMISSIONS = {
    "owner": {
        "manage_server", "manage_roles", "manage_channels", "kick_members",
        "move_members", "force_disconnect", "join_voice", "speak",
        "screen_share", "camera", "music_control", "send_messages",
        "ban_members", "timeout_members", "manage_invites", "view_audit_log", "manage_reports",
    },
    "admin": {
        "manage_server", "manage_roles", "manage_channels", "kick_members",
        "move_members", "force_disconnect", "join_voice", "speak",
        "screen_share", "camera", "music_control", "send_messages",
        "ban_members", "timeout_members", "manage_invites", "view_audit_log", "manage_reports",
    },
    "mod": {
        "kick_members", "move_members", "force_disconnect", "join_voice",
        "speak", "screen_share", "camera", "music_control", "send_messages",
        "ban_members", "timeout_members", "manage_reports",
    },
    "member": {"join_voice", "speak", "screen_share", "camera", "send_messages"},
}


# ΓöÇΓöÇ Accounts / ├╝yelik sistemi ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ
# SQLite ├╝zerinde ger├ºek kay─▒t: kullan─▒c─▒ ad─▒ + ┼ƒifre (pbkdf2 ile hash'lenmi┼ƒ),
# profil (avatar/bio/banner) buraya ba─ƒl─▒ ΓÇö art─▒k sadece o an a├º─▒k olan
# taray─▒c─▒n─▒n localStorage'─▒nda de─ƒil, giri┼ƒ yapan her cihazda ayn─▒ hesap.

@contextmanager
def _db():
    conn = sqlite3.connect(ACCOUNTS_DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    changed = False
    starting_changes = conn.total_changes
    try:
        yield conn
        conn.commit()
        changed = conn.total_changes > starting_changes
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        if changed and _SUPABASE_SYNC_READY:
            schedule_supabase_push(ACCOUNTS_DB_FILE)


def _account_name(row: sqlite3.Row | dict | None) -> str:
    if not row:
        return ""
    keys = row.keys() if hasattr(row, "keys") else row
    display = row["display_name"] if "display_name" in keys else ""
    return (display or row["username"] or "").strip()


def _allocate_discriminator(conn: sqlite3.Connection, display_name: str) -> str:
    """Return an unused four-digit tag for a display name."""
    start = secrets.randbelow(9999) + 1
    for offset in range(9999):
        tag = f"{((start + offset - 1) % 9999) + 1:04d}"
        exists = conn.execute(
            "SELECT 1 FROM accounts WHERE lower(COALESCE(NULLIF(display_name, ''), username)) = lower(?) AND discriminator = ?",
            (display_name, tag),
        ).fetchone()
        if not exists:
            return tag
    raise RuntimeError("No discriminator available for display name")


def init_accounts_db():
    with _db() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                peer_id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                avatar_image TEXT NOT NULL DEFAULT '',
                avatar_color TEXT NOT NULL DEFAULT '#7c3aed',
                bio TEXT NOT NULL DEFAULT '',
                banner_url TEXT NOT NULL DEFAULT '',
                banner_color TEXT NOT NULL DEFAULT '#5865f2',
                platform_admin INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            )
        """)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
        if "banner_color" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN banner_color TEXT NOT NULL DEFAULT '#5865f2'")
        if "platform_admin" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN platform_admin INTEGER NOT NULL DEFAULT 0")
        if "display_name" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN display_name TEXT NOT NULL DEFAULT ''")
        if "discriminator" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN discriminator TEXT NOT NULL DEFAULT ''")
        if "email" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN email TEXT NOT NULL DEFAULT ''")
        conn.execute("UPDATE accounts SET display_name = username WHERE display_name = ''")
        for account in conn.execute("SELECT peer_id, username, display_name, discriminator FROM accounts").fetchall():
            if not account["discriminator"]:
                conn.execute(
                    "UPDATE accounts SET discriminator = ? WHERE peer_id = ?",
                    (_allocate_discriminator(conn, account["display_name"] or account["username"]), account["peer_id"]),
                )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_display_tag ON accounts(lower(display_name), discriminator)"
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_email ON accounts(lower(email)) WHERE email <> ''")
        PLATFORM_ADMIN_IDS.update(
            row[0] for row in conn.execute("SELECT peer_id FROM accounts WHERE platform_admin = 1").fetchall()
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS friendships (
                peer_id TEXT NOT NULL,
                friend_peer_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'accepted',
                created_at REAL NOT NULL,
                PRIMARY KEY (peer_id, friend_peer_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS servers (
                room_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                owner_username TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                invite_code TEXT NOT NULL,
                icon_url TEXT,
                is_public INTEGER NOT NULL DEFAULT 1,
                state_json TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_servers_public_updated ON servers(is_public, updated_at DESC)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_servers_invite_code ON servers(invite_code)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS server_members (
                room_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT 'member',
                joined_at REAL NOT NULL,
                last_seen REAL NOT NULL,
                PRIMARY KEY (room_id, peer_id),
                FOREIGN KEY (room_id) REFERENCES servers(room_id) ON DELETE CASCADE
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_server_members_role ON server_members(room_id, role)")
        # New social/moderation state is additive. Existing friendship rows and
        # deployments remain valid while newer clients can opt into these flows.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS friend_requests (
                requester_peer_id TEXT NOT NULL,
                target_peer_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (requester_peer_id, target_peer_id),
                CHECK (requester_peer_id <> target_peer_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_friend_requests_target ON friend_requests(target_peer_id, status, updated_at DESC)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS friend_blocks (
                blocker_peer_id TEXT NOT NULL,
                blocked_peer_id TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (blocker_peer_id, blocked_peer_id),
                CHECK (blocker_peer_id <> blocked_peer_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS server_invites (
                invite_code TEXT PRIMARY KEY,
                room_id TEXT NOT NULL,
                created_by TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                expires_at REAL,
                max_uses INTEGER,
                use_count INTEGER NOT NULL DEFAULT 0,
                revoked_at REAL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_server_invites_room ON server_invites(room_id, revoked_at)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS server_bans (
                room_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                actor_peer_id TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                PRIMARY KEY (room_id, peer_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS server_timeouts (
                room_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                actor_peer_id TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                expires_at REAL NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (room_id, peer_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_server_timeouts_expiry ON server_timeouts(expires_at)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS server_message_throttle (
                room_id TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                last_sent_at REAL NOT NULL,
                PRIMARY KEY (room_id, channel_id, peer_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS server_audit_log (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id TEXT NOT NULL,
                actor_peer_id TEXT NOT NULL,
                target_peer_id TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_server_audit_log_room ON server_audit_log(room_id, created_at DESC)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS server_message_reports (
                report_id TEXT PRIMARY KEY,
                room_id TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                reporter_peer_id TEXT NOT NULL,
                author_peer_id TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'open',
                moderator_peer_id TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                resolved_at REAL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_server_message_reports_room ON server_message_reports(room_id, status, created_at DESC)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS deleted_servers (
                room_id TEXT PRIMARY KEY,
                deleted_at REAL NOT NULL
            )
        """)
    log.info(f"SCORD database ready at {ACCOUNTS_DB_FILE}")


def _to_int32(n: int) -> int:
    n &= 0xFFFFFFFF
    return n - 0x100000000 if n >= 0x80000000 else n


def _js_shl5(h: int) -> int:
    u = h & 0xFFFFFFFF
    shifted = (u << 5) & 0xFFFFFFFF
    return shifted - 0x100000000 if shifted >= 0x80000000 else shifted


def legacy_peer_id(username: str, password: str) -> str:
    """
    Eski istemci taray─▒c─▒da kullan─▒c─▒ ad─▒+┼ƒifreden deterministik bir peer_id
    t├╝retiyordu (ger├ºek dogrulama yoktu). Var olan oda sahiplikleri
    (peer_roles/owner_id) o id'lere g├╢re kay─▒tl─▒ oldu─ƒu i├ºin, ger├ºek ├╝yelik
    sistemine ge├ºerken AYNI form├╝l├╝ koruyoruz ΓÇö aksi halde herkes kendi
    sunucusunun sahipli─ƒini kaybederdi.
    """
    h = 0
    s = (username or "").lower().strip() + ":" + (password or "")
    for ch in s:
        h = _to_int32(_js_shl5(h) - h + ord(ch))
    n = abs(h)
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    if n == 0:
        return "id_0"
    b36 = ""
    while n:
        n, r = divmod(n, 36)
        b36 = digits[r] + b36
    return "id_" + b36


def _hash_password(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000).hex()


def ensure_bootstrap_platform_admin() -> None:
    """Create the first Scord operator account without storing a plaintext password.

    The password is required from the deployment secret. If the named account
    already exists, its hash is synchronized to that secret and it is promoted.
    """
    username = (os.environ.get("SCORD_BOOTSTRAP_ADMIN_USERNAME") or "sherlock").strip()
    password = (os.environ.get("SCORD_BOOTSTRAP_ADMIN_PASSWORD") or "").strip()
    email = (os.environ.get("SCORD_BOOTSTRAP_ADMIN_EMAIL") or "sherlock@scord.local").strip().lower()
    if not username or len(password) < 4:
        log.info("Bootstrap platform admin skipped: SCORD_BOOTSTRAP_ADMIN_PASSWORD is not configured")
        return
    with _db() as conn:
        row = conn.execute(
            "SELECT * FROM accounts WHERE lower(COALESCE(NULLIF(display_name, ''), username)) = lower(?) ORDER BY created_at LIMIT 1",
            (username,),
        ).fetchone()
        if row:
            salt = secrets.token_hex(16)
            conn.execute(
                "UPDATE accounts SET password_hash = ?, salt = ?, platform_admin = 1 WHERE peer_id = ?",
                (_hash_password(password, salt), salt, row["peer_id"]),
            )
            PLATFORM_ADMIN_IDS.add(row["peer_id"])
            return
        peer_id = legacy_peer_id(username, password)
        salt = secrets.token_hex(16)
        tag = _allocate_discriminator(conn, username)
        conn.execute(
            "INSERT INTO accounts (peer_id, username, display_name, discriminator, email, password_hash, salt, platform_admin, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
            (peer_id, username, username, tag, email, _hash_password(password, salt), salt, time.time()),
        )
        PLATFORM_ADMIN_IDS.add(peer_id)
        log.info("Bootstrap platform admin account created: %s", username)


def _account_public(row: sqlite3.Row) -> dict:
    return {
        "peer_id": row["peer_id"],
        "username": _account_name(row),
        "discriminator": row["discriminator"] if "discriminator" in row.keys() else "",
        "avatar_image": row["avatar_image"],
        "avatar_color": row["avatar_color"],
        "bio": row["bio"],
        "banner_url": row["banner_url"],
        "banner_color": row["banner_color"],
        "platform_admin": bool(row["platform_admin"]),
    }


def _account_private(row: sqlite3.Row) -> dict:
    return {**_account_public(row), "email": row["email"] if "email" in row.keys() else ""}


def _is_platform_admin(peer_id: str) -> bool:
    return bool(peer_id and peer_id in PLATFORM_ADMIN_IDS)


SESSION_MAX_AGE_SEC = 30 * 24 * 3600  # 30 g├╝n


def _account_by_token(token: str) -> Optional[sqlite3.Row]:
    if not token:
        return None
    with _db() as conn:
        sess = conn.execute(
            "SELECT peer_id, created_at FROM sessions WHERE token = ?", (token,)
        ).fetchone()
        if not sess:
            return None
        # S├╝resi dolmu┼ƒ session'─▒ g├Âsterimde de─ƒil, ger├ºekten sil.
        if time.time() - sess["created_at"] > SESSION_MAX_AGE_SEC:
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            log.info("Expired session token rejected and removed")
            return None
        return conn.execute("SELECT * FROM accounts WHERE peer_id = ?", (sess["peer_id"],)).fetchone()


def _check_login_rate_limit(request: Request):
    """Ayn─▒ IP'den pencere i├ºinde LOGIN_RATE_MAX'tan fazla ba┼ƒar─▒s─▒z deneme → 429."""
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    with _login_attempts_lock:
        stamps = [t for t in _login_attempts.get(ip, []) if now - t < LOGIN_RATE_WINDOW]
        if len(stamps) >= LOGIN_RATE_MAX:
            log.warning(f"Rate limit hit for {ip} (login/register)")
            raise HTTPException(status_code=429, detail="too many attempts")
        stamps.append(now)
        _login_attempts[ip] = stamps


def _clear_login_attempts(request: Request):
    ip = request.client.host if request.client else "unknown"
    with _login_attempts_lock:
        _login_attempts.pop(ip, None)


def _request_account(request: Request, body: dict | None = None) -> Optional[sqlite3.Row]:
    body = body or {}
    auth = request.headers.get("authorization") or ""
    token = body.get("token") or (auth[7:].strip() if auth.lower().startswith("bearer ") else "")
    return _account_by_token(token)


def _upsert_server_member(room_id: str, peer_id: str, username: str, role: str = "member"):
    if not room_id or not peer_id:
        return
    now = time.time()
    with _db() as conn:
        conn.execute("""
            INSERT INTO server_members (room_id, peer_id, username, role, joined_at, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(room_id, peer_id) DO UPDATE SET
                username = excluded.username,
                role = excluded.role,
                last_seen = excluded.last_seen
        """, (room_id, peer_id, username or "", role or "member", now, now))


def _server_member_count(room_id: str) -> int:
    try:
        with _db() as conn:
            row = conn.execute("SELECT COUNT(*) FROM server_members WHERE room_id = ?", (room_id,)).fetchone()
            return int(row[0]) if row else 0
    except sqlite3.Error:
        return 0


def _is_room_member(room_id: str, peer_id: str) -> bool:
    """Server-side membership check; platform admins retain global access."""
    if not peer_id:
        return False
    if _is_platform_admin(peer_id):
        return True
    room = rooms.get(room_id)
    if room and room.owner_id == peer_id:
        return True
    with _db() as conn:
        return bool(conn.execute(
            "SELECT 1 FROM server_members WHERE room_id = ? AND peer_id = ?",
            (room_id, peer_id),
        ).fetchone())


def _require_room_member(room_id: str, request: Request, body: dict | None = None) -> tuple["Room", sqlite3.Row]:
    room = rooms.get(room_id)
    if not room or room_id in deleted_room_ids:
        raise HTTPException(status_code=404, detail="not_found")
    account = _request_account(request, body)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    if not _is_room_member(room_id, account["peer_id"]):
        raise HTTPException(status_code=403, detail="not_a_member")
    if _is_banned(room_id, account["peer_id"]):
        raise HTTPException(status_code=403, detail="banned")
    return room, account


def _is_banned(room_id: str, peer_id: str) -> bool:
    with _db() as conn:
        return bool(conn.execute(
            "SELECT 1 FROM server_bans WHERE room_id = ? AND peer_id = ?",
            (room_id, peer_id),
        ).fetchone())


def _active_timeout(room_id: str, peer_id: str) -> Optional[sqlite3.Row]:
    now = time.time()
    with _db() as conn:
        conn.execute("DELETE FROM server_timeouts WHERE expires_at <= ?", (now,))
        return conn.execute(
            "SELECT * FROM server_timeouts WHERE room_id = ? AND peer_id = ? AND expires_at > ?",
            (room_id, peer_id, now),
        ).fetchone()


def _record_audit(room_id: str, actor_peer_id: str, action: str, target_peer_id: str = "", reason: str = "", metadata: dict | None = None) -> None:
    with _db() as conn:
        conn.execute(
            "INSERT INTO server_audit_log (room_id, actor_peer_id, target_peer_id, action, reason, metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (room_id, actor_peer_id, target_peer_id, action, reason[:500], json.dumps(metadata or {}, ensure_ascii=False), time.time()),
        )


def _can_moderate_target(room: "Room", actor_peer_id: str, target_peer_id: str) -> bool:
    """Platform admins may act globally; role peers cannot act sideways/upwards."""
    if actor_peer_id == target_peer_id:
        return False
    if _is_platform_admin(actor_peer_id):
        return True
    rank = {"member": 0, "mod": 1, "admin": 2, "owner": 3}
    return rank.get(room.role_for(actor_peer_id), 0) > rank.get(room.role_for(target_peer_id), 0)


def _invite_payload(row: sqlite3.Row) -> dict:
    return {
        "invite_code": row["invite_code"],
        "expires_at": row["expires_at"],
        "max_uses": row["max_uses"],
        "use_count": row["use_count"],
        "revoked": bool(row["revoked_at"]),
    }


def _ensure_room_invite(room: "Room", created_by: str = "") -> None:
    """Backfill legacy/current invite codes without changing their behavior."""
    if not room.invite_code:
        room.invite_code = str(uuid.uuid4())[:6].upper()
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO server_invites (invite_code, room_id, created_by, created_at) VALUES (?, ?, ?, ?)",
            (room.invite_code, room.room_id, created_by, time.time()),
        )


def _role_defaults(role_id: str) -> dict:
    return {p: True for p in DEFAULT_ROLE_PERMISSIONS.get(role_id, DEFAULT_ROLE_PERMISSIONS["member"])}

def _save_db_legacy():
    try:
        data: Dict[str, Any] = {rid: r.to_persist_dict() for rid, r in rooms.items()}
        # Tombstone listesi oda kay─▒tlar─▒yla ayn─▒ dosyada ya┼ƒ─▒yor ("_deleted"
        # anahtar─▒ oda id format─▒nda olmad─▒─ƒ─▒ i├ºin ├ºak─▒┼ƒmaz).
        data["_deleted"] = sorted(deleted_room_ids)[-DELETED_TOMBSTONE_CAP:]
        tmp_file = DATABASE_FILE + ".tmp"
        if os.path.exists(tmp_file):  # ├Ânceki yar─▒da kalm─▒┼ƒ yaz─▒m kal─▒nt─▒s─▒
            os.remove(tmp_file)
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        # Mevcut dosyay─▒ yede─ƒe al (os.replace atomik — her save'de tek .bak ├╝zerine yazar).
        if os.path.exists(DATABASE_FILE):
            os.replace(DATABASE_FILE, DATABASE_FILE + ".bak")
        # Atomik takas: tmp -> as─▒l. Windows'ta ayn─▒ dizin i├ºinde os.replace atomiktir;
        # yaz─▒m ortas─▒nda crash olsa bile rooms.json ya eski ya da yeni tam i├ºeriktir.
        os.replace(tmp_file, DATABASE_FILE)
        log.info(f"Database saved to {DATABASE_FILE}")
    except Exception as e:
        log.error(f"Failed to save db: {e}")


_db_save_timer: Optional[threading.Timer] = None
_db_save_lock = threading.Lock()


def schedule_save_db(delay_sec: float = 1.2):
    """Birle┼ƒik disk yaz─▒m─▒ ΓÇö pin/ikon/rol gibi s─▒k u├ºlar─▒ y─▒─ƒ─▒nda tek save."""
    global _db_save_timer

    def _flush():
        global _db_save_timer
        with _db_save_lock:
            save_db()
            _db_save_timer = None

    with _db_save_lock:
        if _db_save_timer:
            _db_save_timer.cancel()
        _db_save_timer = threading.Timer(delay_sec, _flush)
        _db_save_timer.daemon = True
        _db_save_timer.start()

def _load_db_legacy():
    if not os.path.exists(DATABASE_FILE):
        return
    try:
        with open(DATABASE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        deleted_room_ids.update(data.pop("_deleted", []))
        for rid, rdata in data.items():
            room = Room(rid, rdata["name"], rdata.get("owner_id", "unknown"), rdata.get("owner_username", ""))
            room.owner_key = rdata.get("owner_key")
            room.created_at = rdata.get("created_at", room.created_at)
            room.channels = rdata.get("channels", room.channels)
            room.roles = rdata.get("roles", room.roles)
            room.channel_permissions = rdata.get("channel_permissions", room.channel_permissions)
            room.peer_roles = rdata.get("peer_roles", room.peer_roles)
            room.pinned_messages = rdata.get("pinned_messages", [])
            room.messages = rdata.get("messages", {})
            room.channel_backgrounds = rdata.get("channel_backgrounds", {})
            room.icon_url = rdata.get("icon_url", None)
            room.description = rdata.get("description", "")
            room.invite_code = rdata.get("invite_code", str(uuid.uuid4())[:6].upper())
            room.normalize_permissions()
            rooms[rid] = room
        log.info(f"Database loaded from {DATABASE_FILE} ({len(rooms)} rooms)")
    except Exception as e:
        log.error(f"Failed to load db from {DATABASE_FILE}: {e}")
        bak_file = DATABASE_FILE + ".bak"
        if os.path.exists(bak_file):
            try:
                with open(bak_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for rid, rdata in data.items():
                    room = Room(rid, rdata["name"], rdata.get("owner_id", "unknown"), rdata.get("owner_username", ""))
                    room.owner_key = rdata.get("owner_key")
                    room.created_at = rdata.get("created_at", room.created_at)
                    room.channels = rdata.get("channels", room.channels)
                    room.roles = rdata.get("roles", room.roles)
                    room.channel_permissions = rdata.get("channel_permissions", room.channel_permissions)
                    room.peer_roles = rdata.get("peer_roles", room.peer_roles)
                    room.pinned_messages = rdata.get("pinned_messages", [])
                    room.messages = rdata.get("messages", {})
                    room.channel_backgrounds = rdata.get("channel_backgrounds", {})
                    room.icon_url = rdata.get("icon_url", None)
                    room.description = rdata.get("description", "")
                    room.invite_code = rdata.get("invite_code", str(uuid.uuid4())[:6].upper())
                    room.normalize_permissions()
                    rooms[rid] = room
                log.warning(f"Recovered {len(rooms)} rooms from backup {bak_file}")
            except Exception as be:
                log.error(f"Backup {bak_file} also corrupted — {len(rooms)} rooms lost. Fix or delete {DATABASE_FILE} and restart.")
        else:
            log.error(f"No backup found — rooms in {DATABASE_FILE} are lost. Fix or delete the file and restart.")


def _room_from_snapshot(rid: str, data: dict) -> "Room":
    room = Room(rid, data.get("name", "Unnamed Server"), data.get("owner_id", "unknown"), data.get("owner_username", ""))
    room.owner_key = data.get("owner_key")
    room.created_at = data.get("created_at", room.created_at)
    room.channels = data.get("channels", room.channels)
    room.roles = data.get("roles", room.roles)
    room.channel_permissions = data.get("channel_permissions", room.channel_permissions)
    room.peer_roles = data.get("peer_roles", room.peer_roles)
    room.pinned_messages = data.get("pinned_messages", [])
    room.messages = data.get("messages", {})
    room.channel_backgrounds = data.get("channel_backgrounds", {})
    room.icon_url = data.get("icon_url")
    room.description = data.get("description", "")
    room.invite_code = data.get("invite_code", str(uuid.uuid4())[:6].upper())
    room.is_public = bool(data.get("is_public", True))
    room.channel_slow_modes = {
        str(channel_id): max(0, min(int(seconds), 21600))
        for channel_id, seconds in (data.get("channel_slow_modes") or {}).items()
    }
    room.normalize_permissions()
    return room


def save_db():
    """Persist all durable SCORD server state in one SQLite transaction."""
    try:
        now = time.time()
        snapshots = [(rid, room, room.to_persist_dict()) for rid, room in rooms.items()]
        with _db() as conn:
            existing = {row[0] for row in conn.execute("SELECT room_id FROM servers").fetchall()}
            live_ids = {rid for rid, _, _ in snapshots}
            for stale_id in existing - live_ids:
                conn.execute("DELETE FROM servers WHERE room_id = ?", (stale_id,))
            for rid, room, payload in snapshots:
                conn.execute("""
                    INSERT INTO servers (
                        room_id, name, owner_id, owner_username, created_at, updated_at,
                        description, invite_code, icon_url, is_public, state_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(room_id) DO UPDATE SET
                        name = excluded.name,
                        owner_id = excluded.owner_id,
                        owner_username = excluded.owner_username,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at,
                        description = excluded.description,
                        invite_code = excluded.invite_code,
                        icon_url = excluded.icon_url,
                        is_public = excluded.is_public,
                        state_json = excluded.state_json
                """, (
                    rid, room.name, room.owner_id, room.owner_username, room.created_at, now,
                    room.description, room.invite_code, room.icon_url, int(room.is_public),
                    json.dumps(payload, ensure_ascii=False),
                ))
                if room.owner_id:
                    conn.execute("""
                        INSERT INTO server_members (room_id, peer_id, username, role, joined_at, last_seen)
                        VALUES (?, ?, ?, 'owner', ?, ?)
                        ON CONFLICT(room_id, peer_id) DO UPDATE SET
                            username = excluded.username,
                            role = 'owner',
                            last_seen = excluded.last_seen
                    """, (rid, room.owner_id, room.owner_username or "", room.created_at, now))
                # Each persisted room has at least one durable invite record.
                # INSERT OR IGNORE preserves usage/expiry metadata for a code.
                conn.execute(
                    "INSERT OR IGNORE INTO server_invites (invite_code, room_id, created_by, created_at) VALUES (?, ?, ?, ?)",
                    (room.invite_code, rid, room.owner_id or "", now),
                )
            for rid in sorted(deleted_room_ids)[-DELETED_TOMBSTONE_CAP:]:
                conn.execute(
                    "INSERT OR REPLACE INTO deleted_servers (room_id, deleted_at) VALUES (?, ?)",
                    (rid, now),
                )
            conn.execute("""
                DELETE FROM deleted_servers WHERE room_id NOT IN (
                    SELECT room_id FROM deleted_servers ORDER BY deleted_at DESC LIMIT ?
                )
            """, (DELETED_TOMBSTONE_CAP,))
        log.info(f"Server state saved to SQLite ({len(snapshots)} servers)")
    except Exception as exc:
        log.error(f"Failed to save server database: {exc}")


def load_db():
    """Load SQLite snapshots and import a legacy rooms.json once if needed."""
    loaded = 0
    try:
        with _db() as conn:
            rows = conn.execute("SELECT room_id, state_json FROM servers ORDER BY created_at").fetchall()
            deleted_room_ids.update(row[0] for row in conn.execute("SELECT room_id FROM deleted_servers").fetchall())
        for row in rows:
            rooms[row["room_id"]] = _room_from_snapshot(row["room_id"], json.loads(row["state_json"]))
            loaded += 1
    except Exception as exc:
        log.error(f"Failed to load SQLite server state: {exc}")

    if loaded or not os.path.exists(DATABASE_FILE):
        log.info(f"Loaded {loaded} servers from SQLite")
        return

    try:
        with open(DATABASE_FILE, "r", encoding="utf-8") as handle:
            legacy = json.load(handle)
        deleted_room_ids.update(legacy.pop("_deleted", []))
        for rid, payload in legacy.items():
            rooms[rid] = _room_from_snapshot(rid, payload)
        save_db()
        log.info(f"Migrated {len(rooms)} legacy rooms from rooms.json to SQLite")
    except Exception as exc:
        log.error(f"Legacy rooms.json migration failed: {exc}")
        rooms.clear()
        deleted_room_ids.clear()
        _load_db_legacy()
        if rooms:
            save_db()


def _template_channels(kind: str) -> list[dict]:
    templates = {
        "creative": [
            {"id": "rules", "name": "kurallar-ve-baslangic", "type": "text", "category": "GIRIS"},
            {"id": "announcements", "name": "duyurular", "type": "text", "category": "GIRIS"},
            {"id": "showcase", "name": "eser-vitrini", "type": "text", "category": "TOPLULUK"},
            {"id": "collab", "name": "ekip-bul", "type": "text", "category": "TOPLULUK"},
            {"id": "resources", "name": "kaynaklar", "type": "text", "category": "URETIM"},
            {"id": "feedback", "name": "geri-bildirim", "type": "text", "category": "URETIM"},
            {"id": "voice-lounge", "name": "studio-lounge", "type": "voice", "category": "SES"},
            {"id": "voice-focus", "name": "sessiz-calisma", "type": "voice", "category": "SES"},
            {"id": "voice-stage", "name": "sunum-sahnesi", "type": "voice", "category": "ETKINLIK"},
        ],
        "gaming": [
            {"id": "rules", "name": "kurallar", "type": "text", "category": "GIRIS"},
            {"id": "patch", "name": "yama-notlari", "type": "text", "category": "HABER"},
            {"id": "looking", "name": "takim-ara", "type": "text", "category": "OYUN"},
            {"id": "clips", "name": "klipler", "type": "text", "category": "OYUN"},
            {"id": "builds", "name": "rehber-ve-build", "type": "text", "category": "OYUN"},
            {"id": "voice-ranked", "name": "ranked-1", "type": "voice", "category": "PARTI"},
            {"id": "voice-casual", "name": "casual-sohbet", "type": "voice", "category": "PARTI"},
            {"id": "voice-music", "name": "muzik-odasi", "type": "voice", "category": "PARTI"},
        ],
        "music": [
            {"id": "rules", "name": "dinleme-kurallari", "type": "text", "category": "GIRIS"},
            {"id": "drops", "name": "yeni-cikanlar", "type": "text", "category": "MUZIK"},
            {"id": "queue", "name": "sarki-onerileri", "type": "text", "category": "MUZIK"},
            {"id": "playlists", "name": "playlist-paylas", "type": "text", "category": "MUZIK"},
            {"id": "production", "name": "produksiyon", "type": "text", "category": "STUDYO"},
            {"id": "voice-listen", "name": "senkron-dinleme", "type": "voice", "category": "CANLI"},
            {"id": "voice-dj", "name": "dj-kabini", "type": "voice", "category": "CANLI"},
            {"id": "voice-after", "name": "after-talk", "type": "voice", "category": "CANLI"},
        ],
        "dev": [
            {"id": "rules", "name": "katki-kurallari", "type": "text", "category": "GIRIS"},
            {"id": "roadmap", "name": "roadmap", "type": "text", "category": "PROJE"},
            {"id": "bugs", "name": "bug-raporlari", "type": "text", "category": "PROJE"},
            {"id": "prs", "name": "pull-request", "type": "text", "category": "PROJE"},
            {"id": "snippets", "name": "kod-parcalari", "type": "text", "category": "BILGI"},
            {"id": "voice-standup", "name": "daily-standup", "type": "voice", "category": "SES"},
            {"id": "voice-pair", "name": "pair-programming", "type": "voice", "category": "SES"},
            {"id": "voice-debug", "name": "debug-odasi", "type": "voice", "category": "SES"},
        ],
        "study": [
            {"id": "rules", "name": "topluluk-notlari", "type": "text", "category": "GIRIS"},
            {"id": "planner", "name": "haftalik-plan", "type": "text", "category": "CALISMA"},
            {"id": "notes", "name": "ders-notlari", "type": "text", "category": "CALISMA"},
            {"id": "questions", "name": "soru-cevap", "type": "text", "category": "CALISMA"},
            {"id": "wins", "name": "bugunun-kazanimi", "type": "text", "category": "MOTIVASYON"},
            {"id": "voice-pomodoro", "name": "pomodoro-50-10", "type": "voice", "category": "ODAK"},
            {"id": "voice-library", "name": "kutuphane-sessiz", "type": "voice", "category": "ODAK"},
            {"id": "voice-break", "name": "mola-sohbeti", "type": "voice", "category": "ODAK"},
        ],
    }
    return templates[kind]


def _rules_message(server_name: str, channel_id: str = "rules") -> dict:
    return {
        "id": f"seed-{channel_id}",
        "type": "chat",
        "channelId": channel_id,
        "author": "Shercord Guide",
        "authorId": "shercord-bot",
        "avatarColor": "#5865f2",
        "text": (
            f"{server_name} kurallari: saygili ol, spam yapma, izin almadan kayit/paylasim yapma, "
            "ses odalarinda sirayi bozma, muzik botunda baskalarinin dinleme deneyimini ezme. "
            "Burasi Discord hissi tasir ama Shercord'a ozgu daha sakin ve uretken bir topluluk alanidir."
        ),
        "time": "09:00",
    }


def ensure_template_rooms():
    specs = [
        ("tpl-creative-hub", "Creator Forge", "creative", "#8b5cf6"),
        ("tpl-gaming-lounge", "Arcade Lobby", "gaming", "#22c55e"),
        ("tpl-music-room", "Midnight Sessions", "music", "#f43f5e"),
        ("tpl-dev-lab", "Open Source Lab", "dev", "#38bdf8"),
        ("tpl-study-cafe", "Focus Cafe", "study", "#f59e0b"),
    ]
    changed = False
    for rid, name, kind, color in specs:
        if rid in rooms:
            continue
        room = Room(rid, name, "shercord-bot", "Scord")
        room.channels = _template_channels(kind)
        room.roles = {
            "admin": {"name": "Admin", "color": "#ef4444", "hoist": True, "permissions": _role_defaults("admin")},
            "mod": {"name": "Moderator", "color": "#22c55e", "hoist": True, "permissions": _role_defaults("mod")},
            "member": {"name": "Uye", "color": "#94a3b8", "hoist": False, "permissions": _role_defaults("member")},
            "bot": {"name": "Shercord Bot", "color": color, "hoist": True, "permissions": _role_defaults("mod")},
        }
        room.peer_roles = {"shercord-bot": "bot"}
        room.messages = {"rules": [_rules_message(name)]}
        room.pinned_messages = [room.messages["rules"][0]]
        rooms[rid] = room
        changed = True
    if changed:
        schedule_save_db(0.2)

class Room:
    def __init__(self, room_id: str, name: str, owner_id: str, owner_username: str = ""):
        self.room_id = room_id
        self.name = name
        self.owner_id = owner_id
        self.owner_username = owner_username or ""
        self.owner_key: Optional[str] = None  # gizli sahip anahtarı (sadece diskte; to_dict'e ASLA)
        self.created_at = time.time()
        self.last_seen: Dict[str, float] = {}
        self.voice_members: Dict[str, Dict[str, dict]] = {}
        self.music_session: Optional[dict] = None
        self.peers: Dict[str, WebSocket] = {}        # peer_id ΓåÆ ws
        self.peer_info: Dict[str, dict] = {}         # peer_id ΓåÆ {username, avatar_color}
        
        # Channels and Roles ΓÇö IDs must match client-side constants
        self.channels = [
            {"id": "ch-genel", "name": "genel", "type": "text"},
            {"id": "ch-duyurular", "name": "duyurular", "type": "text"},
            {"id": "ch-sesli", "name": "sesli-sohbet", "type": "voice"},
            {"id": "ch-muzik", "name": "müzik", "type": "voice"},
        ]
        self.roles = {
            "admin": {"name": "Admin", "color": "#ef4444", "hoist": True, "permissions": _role_defaults("admin")},
            "mod": {"name": "Moderator", "color": "#22c55e", "hoist": True, "permissions": _role_defaults("mod")},
            "member": {"name": "Üye", "color": "#94a3b8", "hoist": False}
        }
        self.channel_permissions = {}
        self.peer_roles = {owner_id: "admin"}
        self.pinned_messages = []
        self.messages = {}  # channel_id -> list[dict]
        self.channel_backgrounds = {}  # channel_id -> image url
        self.icon_url = None
        self.description = ""
        self.is_public = True
        self.invite_code = str(uuid.uuid4())[:6].upper()
        # Text-channel slow mode is durable room configuration; enforcement is
        # server-side in save_history_message.
        self.channel_slow_modes: Dict[str, int] = {}

    def to_persist_dict(self):
        """Data to be saved to disk (metadata only)."""
        self.normalize_permissions()
        return {
            "room_id": self.room_id,
            "name": self.name,
            "owner_id": self.owner_id,
            "owner_username": self.owner_username,
            "created_at": self.created_at,
            "owner_key": self.owner_key,
            "channels": self.channels,
            "roles": self.roles,
            "channel_permissions": self.channel_permissions,
            "peer_roles": self.peer_roles,
            "pinned_messages": self.pinned_messages,
            "messages": self.messages,
            "channel_backgrounds": self.channel_backgrounds,
            "icon_url": self.icon_url,
            "description": self.description,
            "is_public": self.is_public,
            "invite_code": self.invite_code,
            "channel_slow_modes": self.channel_slow_modes,
        }

    def to_discovery_dict(self):
        """Small public card payload; message history never leaks into discovery."""
        return {
            "room_id": self.room_id,
            "name": self.name,
            "description": self.description,
            "icon_url": self.icon_url,
            "invite_code": self.invite_code,
            "channel_slow_modes": self.channel_slow_modes,
            "owner_username": self.owner_username,
            "member_count": max(_server_member_count(self.room_id), len(self.peers), 1),
            "channel_count": len(self.channels),
            "is_public": self.is_public,
            "administrators": self.administrator_snapshot(),
        }

    def to_dict(self):
        """Data to be sent to frontend (includes transient state)."""
        self.normalize_permissions()
        return {
            "room_id": self.room_id,
            "name": self.name,
            "owner_id": self.owner_id,
            "owner_username": self.owner_username,
            "created_at": self.created_at,
            "administrators": self.administrator_snapshot(),
            "peer_count": max(_server_member_count(self.room_id), len(self.peers), 1),
            "channels": self.channels,
            "roles": self.roles,
            "channel_permissions": self.channel_permissions,
            "peer_roles": self.peer_roles,
            "pinned_messages": self.pinned_messages,
            "channel_backgrounds": self.channel_backgrounds,
            "icon_url": self.icon_url,
            "description": self.description,
            "is_public": self.is_public,
            "invite_code": self.invite_code,
            "messages": self.messages, # Sent for history sync
            "peers": [
                {
                    "peer_id": pid, 
                    "role": self.peer_roles.get(pid, "member"),
                    **info
                }
                for pid, info in self.peer_info.items()
            ],
            "voice_members": {
                ch: list(members.values())
                for ch, members in self.voice_members.items()
            },
            "music_session": self.music_session,
        }

    def normalize_permissions(self):
        for role_id, role in self.roles.items():
            perms = role.setdefault("permissions", {})
            for perm, enabled in _role_defaults(role_id).items():
                perms.setdefault(perm, enabled)
        if "member" not in self.roles:
            self.roles["member"] = {
                "name": "Uye",
                "color": "#94a3b8",
                "hoist": False,
                "permissions": _role_defaults("member"),
            }

    def role_for(self, peer_id: str) -> str:
        if _is_platform_admin(peer_id):
            return "owner"
        if peer_id == self.owner_id:
            return "owner"
        return self.peer_roles.get(peer_id, "member")

    def has_permission(self, peer_id: str, permission: str, channel_id: str | None = None) -> bool:
        if _is_platform_admin(peer_id) or peer_id == self.owner_id:
            return True
        self.normalize_permissions()
        role_id = self.role_for(peer_id)
        role = self.roles.get(role_id, self.roles.get("member", {}))
        allowed = bool(role.get("permissions", {}).get(permission, False))
        if channel_id:
            overrides = self.channel_permissions.get(channel_id, {})
            role_override = overrides.get(role_id) or overrides.get("member")
            if isinstance(role_override, dict):
                if permission in role_override.get("deny", []):
                    allowed = False
                if permission in role_override.get("allow", []):
                    allowed = True
        return allowed

    def administrator_snapshot(self) -> list[dict]:
        """Stable, non-secret server leadership metadata for the client UI."""
        leaders: list[dict] = []
        seen: set[str] = set()
        for peer_id, role in [(self.owner_id, "owner"), *self.peer_roles.items()]:
            effective = "owner" if _is_platform_admin(peer_id) or peer_id == self.owner_id else role
            if effective not in ("owner", "admin") or peer_id in seen:
                continue
            seen.add(peer_id)
            info = self.peer_info.get(peer_id, {})
            leaders.append({
                "peer_id": peer_id,
                "username": info.get("username") or (self.owner_username if peer_id == self.owner_id else ""),
                "role": effective,
            })
        for peer_id in sorted(PLATFORM_ADMIN_IDS):
            if peer_id in seen:
                continue
            leaders.append({"peer_id": peer_id, "username": "Sherlock", "role": "owner"})
        return leaders


# ΓöÇΓöÇ Helpers ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

async def broadcast_to_room(room: Room, message: dict, exclude: str | None = None):
    """Send a JSON message to every peer in a room except the excluded one."""
    dead: list[str] = []
    data = json.dumps(message)
    for peer_id, ws in list(room.peers.items()):
        if peer_id == exclude:
            continue
        try:
            if ws.client_state == WebSocketState.CONNECTED:
                await ws.send_text(data)
            else:
                dead.append(peer_id)
        except Exception:
            dead.append(peer_id)
    for pid in dead:
        room.peers.pop(pid, None)
        room.peer_info.pop(pid, None)


async def send_to_peer(room: Room, peer_id: str, message: dict):
    ws = room.peers.get(peer_id)
    if ws and ws.client_state == WebSocketState.CONNECTED:
        await ws.send_text(json.dumps(message))


def _voice_snapshot(room: Room, channel_id: str | None = None) -> dict:
    members = {
        ch: list(ch_members.values())
        for ch, ch_members in room.voice_members.items()
    }
    return {
        "type": "voice_state_snapshot",
        "room_id": room.room_id,
        "channelId": channel_id,
        "voiceMembers": members,
        "musicSession": room.music_session,
    }


async def broadcast_voice_snapshot(room: Room, channel_id: str | None = None):
    await broadcast_to_room(room, _voice_snapshot(room, channel_id))


def _remove_peer_from_voice(room: Room, peer_id: str):
    for members in room.voice_members.values():
        members.pop(peer_id, None)
    empty = [ch for ch, members in room.voice_members.items() if not members]
    for ch in empty:
        room.voice_members.pop(ch, None)


def _member_payload(room: Room, peer_id: str, username: str, avatar_color: str, data: dict) -> dict:
    existing = {}
    for members in room.voice_members.values():
        if peer_id in members:
            existing = members[peer_id]
            break
    return {
        "peer_id": peer_id,
        "username": data.get("username") or existing.get("username") or username,
        "avatar_color": data.get("avatarColor") or data.get("avatar_color") or existing.get("avatar_color") or avatar_color,
        "avatar_image": data.get("avatarImage") if "avatarImage" in data else existing.get("avatar_image"),
        "isSharingScreen": bool(data.get("isSharingScreen", existing.get("isSharingScreen", False))),
        "isSharingCamera": bool(data.get("isSharingCamera", existing.get("isSharingCamera", False))),
        "isSpeaking": bool(data.get("isSpeaking", existing.get("isSpeaking", False))),
    }


def _music_public_state(room: Room) -> dict:
    return {"type": "music_state", "room_id": room.room_id, "session": room.music_session}


def _can_control_music(room: Room, peer_id: str) -> bool:
    session = room.music_session or {}
    return (
        peer_id == session.get("controllerId")
        or room.has_permission(peer_id, "music_control", session.get("voiceChannelId"))
    )

# ΓöÇΓöÇ REST endpoints ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

@app.get("/api/rooms")
def list_rooms():
    return [r.to_discovery_dict() for r in rooms.values() if r.is_public]


@app.get("/api/rooms/{room_id}")
def get_room(room_id: str, request: Request):
    """Tek oda i├ºin taze otoriter durum ΓÇö istemci periyodik resync i├ºin kullan─▒r."""
    room, _ = _require_room_member(room_id, request)
    return room.to_dict()

@app.post("/api/auth/register")
def register_account(body: dict, request: Request):
    _check_login_rate_limit(request)
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    email = (body.get("email") or "").strip().lower()
    if len(username) < 2 or len(username) > 32 or "#" in username:
        return {"error": "invalid_username"}
    if len(password) < 4:
        return {"error": "invalid_password"}
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email) or len(email) > 254:
        return {"error": "invalid_email"}
    salt = secrets.token_hex(16)
    pw_hash = _hash_password(password, salt)
    with _db() as conn:
        if conn.execute("SELECT 1 FROM accounts WHERE lower(email) = lower(?)", (email,)).fetchone():
            return {"error": "email_taken"}
        discriminator = _allocate_discriminator(conn, username)
        internal_username = username
        if conn.execute("SELECT 1 FROM accounts WHERE lower(username) = lower(?)", (internal_username,)).fetchone():
            internal_username = f"{username}#{discriminator}"
        peer_id = legacy_peer_id(internal_username, password)
        try:
            conn.execute(
                "INSERT INTO accounts (peer_id, username, display_name, discriminator, email, password_hash, salt, platform_admin, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
                (peer_id, internal_username, username, discriminator, email, pw_hash, salt, time.time()),
            )
        except sqlite3.IntegrityError:
            return {"error": "already_registered"}
        token = secrets.token_urlsafe(32)
        conn.execute(
            "INSERT INTO sessions (token, peer_id, created_at) VALUES (?, ?, ?)",
            (token, peer_id, time.time()),
        )
        row = conn.execute("SELECT * FROM accounts WHERE peer_id = ?", (peer_id,)).fetchone()
    log.info(f"Account registered: {username!r} ({peer_id})")
    return {"success": True, "token": token, **_account_private(row)}


@app.post("/api/auth/login")
def login_account(body: dict, request: Request):
    _check_login_rate_limit(request)
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    with _db() as conn:
        match = re.fullmatch(r"(.+)#(\d{4})", username)
        if match:
            row = conn.execute(
                "SELECT * FROM accounts WHERE lower(COALESCE(NULLIF(display_name, ''), username)) = lower(?) AND discriminator = ?",
                (match.group(1).strip(), match.group(2)),
            ).fetchone()
        else:
            matches = conn.execute(
                "SELECT * FROM accounts WHERE lower(COALESCE(NULLIF(display_name, ''), username)) = lower(?) ORDER BY created_at",
                (username,),
            ).fetchall()
            if len(matches) > 1:
                return {"error": "ambiguous_username"}
            row = matches[0] if matches else None
        if not row:
            return {"error": "not_found"}
        if _hash_password(password, row["salt"]) != row["password_hash"]:
            return {"error": "wrong_password"}
        token = secrets.token_urlsafe(32)
        conn.execute(
            "INSERT INTO sessions (token, peer_id, created_at) VALUES (?, ?, ?)",
            (token, row["peer_id"], time.time()),
        )
    _clear_login_attempts(request)
    return {"success": True, "token": token, **_account_private(row)}


@app.post("/api/auth/logout")
def logout_account(body: dict):
    token = body.get("token") or ""
    with _db() as conn:
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
    return {"success": True}


@app.get("/api/account/me")
def get_account_me(request: Request, token: str = ""):
    # Token ├Âncelikle Authorization: Bearer <token> header'─▒ndan (URL loglar─▒na
    # s─▒zmaz); eski istemciler i├ºin query param fallback'i korunur.
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip() or token
    row = _account_by_token(token)
    if not row:
        return {"error": "unauthorized"}
    return {"success": True, **_account_private(row)}


@app.post("/api/account/update")
def update_account(body: dict):
    token = body.get("token") or ""
    row = _account_by_token(token)
    if not row:
        return {"error": "unauthorized"}
    fields = {}
    # avatar_image / banner_url base64 data-URL olabilir (PC'den y├╝kleme) ΓÇö
    # 128px JPEG ~10-20KB; 300K s─▒n─▒r─▒ bozulmadan saklamaya yeter.
    limits = {"avatar_image": 300_000, "banner_url": 300_000}
    for key in ("avatar_image", "avatar_color", "bio", "banner_url", "banner_color"):
        if key in body:
            fields[key] = str(body[key])[:limits.get(key, 8000)]
    if not fields:
        return {"success": True, **_account_private(row)}
    with _db() as conn:
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(
            f"UPDATE accounts SET {set_clause} WHERE peer_id = ?",
            (*fields.values(), row["peer_id"]),
        )
        updated = conn.execute("SELECT * FROM accounts WHERE peer_id = ?", (row["peer_id"],)).fetchone()
    return {"success": True, **_account_public(updated)}


@app.get("/api/friends")
def list_friends(request: Request):
    account = _request_account(request)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    with _db() as conn:
        rows = conn.execute(
            "SELECT a.*, f.status FROM friendships f JOIN accounts a ON a.peer_id = f.friend_peer_id "
            "WHERE f.peer_id = ? AND f.status = 'accepted' ORDER BY lower(a.username)",
            (account["peer_id"],),
        ).fetchall()
        incoming = conn.execute(
            "SELECT a.*, r.created_at FROM friend_requests r JOIN accounts a ON a.peer_id = r.requester_peer_id "
            "WHERE r.target_peer_id = ? AND r.status = 'pending' ORDER BY r.created_at DESC",
            (account["peer_id"],),
        ).fetchall()
        outgoing = conn.execute(
            "SELECT a.*, r.created_at FROM friend_requests r JOIN accounts a ON a.peer_id = r.target_peer_id "
            "WHERE r.requester_peer_id = ? AND r.status = 'pending' ORDER BY r.created_at DESC",
            (account["peer_id"],),
        ).fetchall()
        blocked = conn.execute(
            "SELECT a.*, b.created_at FROM friend_blocks b JOIN accounts a ON a.peer_id = b.blocked_peer_id "
            "WHERE b.blocker_peer_id = ? ORDER BY b.created_at DESC",
            (account["peer_id"],),
        ).fetchall()
    return {
        "friends": [{**_account_public(row), "status": row["status"]} for row in rows],
        "incoming_requests": [{**_account_public(row), "created_at": row["created_at"]} for row in incoming],
        "outgoing_requests": [{**_account_public(row), "created_at": row["created_at"]} for row in outgoing],
        "blocked": [{**_account_public(row), "created_at": row["created_at"]} for row in blocked],
    }


def _friend_target_from_body(conn: sqlite3.Connection, body: dict) -> Optional[sqlite3.Row]:
    target_id = str(body.get("target_peer_id") or "").strip()
    if target_id:
        return conn.execute("SELECT * FROM accounts WHERE peer_id = ?", (target_id,)).fetchone()
    identifier = str(body.get("identifier") or "").strip()
    match = re.fullmatch(r"(.{2,32})#(\d{4})", identifier)
    if not match:
        return None
    return conn.execute(
        "SELECT * FROM accounts WHERE lower(COALESCE(NULLIF(display_name, ''), username)) = lower(?) AND discriminator = ?",
        (match.group(1).strip(), match.group(2)),
    ).fetchone()


@app.post("/api/friends/requests")
def create_friend_request(body: dict, request: Request):
    account = _request_account(request, body)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    with _db() as conn:
        target = _friend_target_from_body(conn, body)
        if not target:
            raise HTTPException(status_code=404, detail="account_not_found")
        if target["peer_id"] == account["peer_id"]:
            raise HTTPException(status_code=400, detail="cannot_friend_self")
        blocked = conn.execute(
            "SELECT 1 FROM friend_blocks WHERE (blocker_peer_id = ? AND blocked_peer_id = ?) OR (blocker_peer_id = ? AND blocked_peer_id = ?)",
            (account["peer_id"], target["peer_id"], target["peer_id"], account["peer_id"]),
        ).fetchone()
        if blocked:
            raise HTTPException(status_code=403, detail="friend_request_unavailable")
        existing = conn.execute(
            "SELECT status FROM friendships WHERE peer_id = ? AND friend_peer_id = ?",
            (account["peer_id"], target["peer_id"]),
        ).fetchone()
        if existing and existing["status"] == "accepted":
            return {"success": True, "already_friends": True, "friend": _account_public(target)}
        now = time.time()
        # An opposite pending request becomes an accepted friendship directly.
        reverse = conn.execute(
            "SELECT 1 FROM friend_requests WHERE requester_peer_id = ? AND target_peer_id = ? AND status = 'pending'",
            (target["peer_id"], account["peer_id"]),
        ).fetchone()
        if reverse:
            conn.execute("DELETE FROM friend_requests WHERE (requester_peer_id = ? AND target_peer_id = ?) OR (requester_peer_id = ? AND target_peer_id = ?)",
                         (account["peer_id"], target["peer_id"], target["peer_id"], account["peer_id"]))
            conn.execute("INSERT OR REPLACE INTO friendships (peer_id, friend_peer_id, status, created_at) VALUES (?, ?, 'accepted', ?)",
                         (account["peer_id"], target["peer_id"], now))
            conn.execute("INSERT OR REPLACE INTO friendships (peer_id, friend_peer_id, status, created_at) VALUES (?, ?, 'accepted', ?)",
                         (target["peer_id"], account["peer_id"], now))
            return {"success": True, "accepted": True, "friend": _account_public(target)}
        conn.execute(
            "INSERT INTO friend_requests (requester_peer_id, target_peer_id, status, created_at, updated_at) VALUES (?, ?, 'pending', ?, ?) "
            "ON CONFLICT(requester_peer_id, target_peer_id) DO UPDATE SET status = 'pending', updated_at = excluded.updated_at",
            (account["peer_id"], target["peer_id"], now, now),
        )
    return {"success": True, "request": {"peer_id": target["peer_id"], "status": "pending"}}


@app.post("/api/friends/requests/{requester_peer_id}/accept")
def accept_friend_request(requester_peer_id: str, body: dict, request: Request):
    account = _request_account(request, body)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    with _db() as conn:
        pending = conn.execute(
            "SELECT 1 FROM friend_requests WHERE requester_peer_id = ? AND target_peer_id = ? AND status = 'pending'",
            (requester_peer_id, account["peer_id"]),
        ).fetchone()
        target = conn.execute("SELECT * FROM accounts WHERE peer_id = ?", (requester_peer_id,)).fetchone()
        if not pending or not target:
            raise HTTPException(status_code=404, detail="request_not_found")
        now = time.time()
        conn.execute("DELETE FROM friend_requests WHERE requester_peer_id = ? AND target_peer_id = ?", (requester_peer_id, account["peer_id"]))
        for left, right in ((account["peer_id"], requester_peer_id), (requester_peer_id, account["peer_id"])):
            conn.execute("INSERT OR REPLACE INTO friendships (peer_id, friend_peer_id, status, created_at) VALUES (?, ?, 'accepted', ?)", (left, right, now))
    return {"success": True, "friend": _account_public(target)}


@app.post("/api/friends/requests/{requester_peer_id}/decline")
def decline_friend_request(requester_peer_id: str, body: dict, request: Request):
    account = _request_account(request, body)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    with _db() as conn:
        cursor = conn.execute(
            "DELETE FROM friend_requests WHERE requester_peer_id = ? AND target_peer_id = ? AND status = 'pending'",
            (requester_peer_id, account["peer_id"]),
        )
    return {"success": True, "declined": bool(cursor.rowcount)}


@app.post("/api/friends/{peer_id}/block")
def block_friend(peer_id: str, body: dict, request: Request):
    account = _request_account(request, body)
    if not account or peer_id == account["peer_id"]:
        raise HTTPException(status_code=400, detail="invalid_block")
    with _db() as conn:
        if not conn.execute("SELECT 1 FROM accounts WHERE peer_id = ?", (peer_id,)).fetchone():
            raise HTTPException(status_code=404, detail="account_not_found")
        conn.execute("INSERT OR REPLACE INTO friend_blocks (blocker_peer_id, blocked_peer_id, created_at) VALUES (?, ?, ?)", (account["peer_id"], peer_id, time.time()))
        conn.execute("DELETE FROM friendships WHERE (peer_id = ? AND friend_peer_id = ?) OR (peer_id = ? AND friend_peer_id = ?)", (account["peer_id"], peer_id, peer_id, account["peer_id"]))
        conn.execute("DELETE FROM friend_requests WHERE (requester_peer_id = ? AND target_peer_id = ?) OR (requester_peer_id = ? AND target_peer_id = ?)", (account["peer_id"], peer_id, peer_id, account["peer_id"]))
    return {"success": True}


@app.delete("/api/friends/{peer_id}/block")
def unblock_friend(peer_id: str, request: Request):
    account = _request_account(request)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    with _db() as conn:
        conn.execute("DELETE FROM friend_blocks WHERE blocker_peer_id = ? AND blocked_peer_id = ?", (account["peer_id"], peer_id))
    return {"success": True}


@app.post("/api/friends/confirm")
def confirm_friend(body: dict, request: Request):
    account = _request_account(request, body)
    target_id = str(body.get("target_peer_id") or "").strip()
    if not account or not target_id or target_id == account["peer_id"]:
        raise HTTPException(status_code=400, detail="invalid_friend")
    with _db() as conn:
        target = conn.execute("SELECT * FROM accounts WHERE peer_id = ?", (target_id,)).fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="account_not_found")
        now = time.time()
        conn.execute(
            "INSERT OR REPLACE INTO friendships (peer_id, friend_peer_id, status, created_at) VALUES (?, ?, 'accepted', ?)",
            (account["peer_id"], target_id, now),
        )
        conn.execute(
            "INSERT OR REPLACE INTO friendships (peer_id, friend_peer_id, status, created_at) VALUES (?, ?, 'accepted', ?)",
            (target_id, account["peer_id"], now),
        )
    return {"success": True, "friend": _account_public(target)}


@app.post("/api/friends/by-tag")
def add_friend_by_tag(body: dict, request: Request):
    account = _request_account(request, body)
    identifier = str(body.get("identifier") or "").strip()
    match = re.fullmatch(r"(.{2,32})#(\d{4})", identifier)
    if not account or not match:
        raise HTTPException(status_code=400, detail="invalid_friend_tag")
    display_name, discriminator = match.group(1).strip(), match.group(2)
    with _db() as conn:
        target = conn.execute(
            "SELECT * FROM accounts WHERE lower(COALESCE(NULLIF(display_name, ''), username)) = lower(?) AND discriminator = ?",
            (display_name, discriminator),
        ).fetchone()
        if not target:
            raise HTTPException(status_code=404, detail="account_not_found")
        if target["peer_id"] == account["peer_id"]:
            raise HTTPException(status_code=400, detail="cannot_friend_self")
        now = time.time()
        conn.execute(
            "INSERT OR REPLACE INTO friendships (peer_id, friend_peer_id, status, created_at) VALUES (?, ?, 'accepted', ?)",
            (account["peer_id"], target["peer_id"], now),
        )
        conn.execute(
            "INSERT OR REPLACE INTO friendships (peer_id, friend_peer_id, status, created_at) VALUES (?, ?, 'accepted', ?)",
            (target["peer_id"], account["peer_id"], now),
        )
    return {"success": True, "friend": _account_public(target)}


@app.delete("/api/friends/{friend_peer_id}")
def delete_friend(friend_peer_id: str, request: Request):
    account = _request_account(request)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    with _db() as conn:
        conn.execute(
            "DELETE FROM friendships WHERE (peer_id = ? AND friend_peer_id = ?) OR (peer_id = ? AND friend_peer_id = ?)",
            (account["peer_id"], friend_peer_id, friend_peer_id, account["peer_id"]),
        )
    return {"success": True}

@app.get("/api/config")
def get_runtime_config():
    """
    Runtime config for clients (e.g. ICE servers).
    Render users can set env:
      - SCORD_TURN_URLS: comma-separated (e.g. turns:your.turn:443?transport=tcp,turn:your.turn:3478)
      - SCORD_TURN_USERNAME
      - SCORD_TURN_CREDENTIAL
      - SCORD_STUN_URLS (optional): comma-separated
    """
    stun_env = os.environ.get("SCORD_STUN_URLS", "").strip()
    stun_urls = [u.strip() for u in stun_env.split(",") if u.strip()]
    if not stun_urls:
        stun_urls = ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"]

    ice = [{"urls": u} for u in stun_urls]

    turn_urls_env = os.environ.get("SCORD_TURN_URLS", "").strip()
    turn_urls = [u.strip() for u in turn_urls_env.split(",") if u.strip()]
    turn_user = os.environ.get("SCORD_TURN_USERNAME", "").strip()
    turn_cred = os.environ.get("SCORD_TURN_CREDENTIAL", "").strip()
    if turn_urls and turn_user and turn_cred:
        ice.append({"urls": turn_urls, "username": turn_user, "credential": turn_cred})

    return {"iceServers": ice, "hasTurn": bool(turn_urls and turn_user and turn_cred)}


@app.post("/api/rooms")
def create_room(body: dict, request: Request):
    """Create a new P2P room (server). Returns the room_id + secret owner_key."""
    auth = request.headers.get("authorization") or ""
    token = body.get("token") or (auth[7:].strip() if auth.lower().startswith("bearer ") else "")
    account = _account_by_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    room_id = str(uuid.uuid4())
    name = body.get("name", "Unnamed Server")
    owner_id = account["peer_id"]
    room = Room(room_id, name, owner_id, _account_name(account))
    room.owner_key = secrets.token_urlsafe(32)
    rooms[room_id] = room
    save_db()
    _upsert_server_member(room_id, owner_id, _account_name(account), "owner")
    log.info(f"Room created: {name!r} ({room_id}) by {owner_id}")
    return {"room_id": room_id, "invite_code": room.invite_code, "owner_key": room.owner_key}

def _owner_key_ok(room, body: dict | None, query, request: Request | None = None):
    """Owner-op gate: gizli owner_key (body JSON'daki `owner_key` veya ?owner_key=).
    Legacy odalar (owner_key None) geçer — eski davranış korunur. Anahtar yanlış/eksikse 403."""
    if request is not None:
        auth = request.headers.get("authorization") or ""
        token = body.get("token") if body else ""
        if not token and auth.lower().startswith("bearer "):
            token = auth[7:].strip()
        account = _account_by_token(token)
        if account and _is_platform_admin(account["peer_id"]):
            return
    if room.owner_key is None:
        return  # legacy
    supplied = body.get("owner_key") if body else None
    if not supplied:
        supplied = query.get("owner_key")
    if not isinstance(supplied, str) or not secrets.compare_digest(supplied, room.owner_key):
        raise HTTPException(status_code=403, detail="forbidden")


@app.post("/api/rooms/{room_id}/pin")
def toggle_pin(room_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    account = _request_account(request, body)
    if not account or room.role_for(account["peer_id"]) not in ("owner", "admin", "mod"):
        raise HTTPException(status_code=403, detail="forbidden")
    msg = body.get("message")
    if not msg or "id" not in msg:
        return {"error": "Invalid message"}
    
    # Toggle pin
    existing = next((m for m in room.pinned_messages if m["id"] == msg["id"]), None)
    if existing:
        room.pinned_messages = [m for m in room.pinned_messages if m["id"] != msg["id"]]
    else:
        room.pinned_messages.append(msg)
    
    schedule_save_db()
    return {"pinned": room.pinned_messages}

@app.post("/api/rooms/{room_id}/messages")
async def save_history_message(room_id: str, body: dict, request: Request):
    if room_id not in rooms: return {"error": "Not found"}
    room = rooms[room_id]
    msg = body.get("message")
    if not msg: return {"error": "No message"}
    account = _request_account(request, body)
    if not account or msg.get("authorId") != account["peer_id"]:
        raise HTTPException(status_code=403, detail="forbidden")
    if not _is_room_member(room_id, account["peer_id"]):
        raise HTTPException(status_code=403, detail="not_a_member")
    if not room.has_permission(account["peer_id"], "send_messages", msg.get("channelId")):
        raise HTTPException(status_code=403, detail="forbidden")
    if _is_banned(room_id, account["peer_id"]):
        raise HTTPException(status_code=403, detail="banned")
    if _active_timeout(room_id, account["peer_id"]):
        raise HTTPException(status_code=403, detail="timed_out")
    
    ch_id = msg.get("channelId", "general")
    slow_mode_seconds = int(room.channel_slow_modes.get(ch_id, 0) or 0)
    if slow_mode_seconds:
        now = time.time()
        with _db() as conn:
            last = conn.execute(
                "SELECT last_sent_at FROM server_message_throttle WHERE room_id = ? AND channel_id = ? AND peer_id = ?",
                (room_id, ch_id, account["peer_id"]),
            ).fetchone()
            retry_after = slow_mode_seconds - (now - last["last_sent_at"]) if last else 0
            if retry_after > 0:
                raise HTTPException(status_code=429, detail={"error": "slow_mode", "retry_after": round(retry_after, 1)})
            conn.execute(
                "INSERT INTO server_message_throttle (room_id, channel_id, peer_id, last_sent_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(room_id, channel_id, peer_id) DO UPDATE SET last_sent_at = excluded.last_sent_at",
                (room_id, ch_id, account["peer_id"], now),
            )
    if ch_id not in room.messages:
        room.messages[ch_id] = []
    
    # Store last 10000 messages per channel
    room.messages[ch_id].append(msg)
    if len(room.messages[ch_id]) > 10000:
        room.messages[ch_id].pop(0)
    
    # Persist via the debounced combined-write mechanism (same as pin/icon/roles).
    schedule_save_db()
    return {"success": True}

@app.post("/api/rooms/{room_id}/icon")
async def update_icon(room_id: str, body: dict, request: Request):
    if room_id not in rooms: return {"error": "Not found"}
    room = rooms[room_id]
    account = _request_account(request, body)
    if not account or not room.has_permission(account["peer_id"], "manage_server"):
        raise HTTPException(status_code=403, detail="forbidden")
    room.icon_url = body.get("url")
    schedule_save_db()
    # Broadcast icon update to all connected peers
    await broadcast_to_room(room, {
        "type": "server_update",
        "payload": {"id": room_id, "icon_url": room.icon_url}
    })
    return {"success": True}

@app.delete("/api/rooms/{room_id}/messages/{message_id}")
async def delete_message(room_id: str, message_id: str, request: Request, channel_id: str = ""):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    account = _request_account(request)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    candidates = room.messages.get(channel_id, []) if channel_id else [m for values in room.messages.values() for m in values]
    target = next((m for m in candidates if m.get("id") == message_id), None)
    if not target:
        return {"success": True}
    role = room.role_for(account["peer_id"])
    if target.get("authorId") != account["peer_id"] and role not in ("owner", "admin", "mod"):
        raise HTTPException(status_code=403, detail="forbidden")
    target_channel = channel_id or next(
        (ch for ch, values in room.messages.items() if any(m.get("id") == message_id for m in values)),
        "",
    )
    tombstone = {
        "id": message_id,
        "channelId": target_channel or target.get("channelId", ""),
        "author": target.get("author", ""),
        "authorId": target.get("authorId", ""),
        "time": target.get("time", ""),
        "deleted": True,
        "deletedAt": int(time.time() * 1000),
    }
    if target_channel in room.messages:
        room.messages[target_channel] = [tombstone if m.get("id") == message_id else m for m in room.messages[target_channel]]
    room.pinned_messages = [m for m in room.pinned_messages if m.get("id") != message_id]
    schedule_save_db()
    return {"success": True, "message": tombstone}


@app.patch("/api/rooms/{room_id}/messages/{message_id}")
async def edit_message(room_id: str, message_id: str, body: dict, request: Request):
    if room_id not in rooms:
        raise HTTPException(status_code=404, detail="not_found")
    room = rooms[room_id]
    account = _request_account(request, body)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    channel_id = (body.get("channel_id") or "").strip()
    text = (body.get("text") or "").strip()
    if not channel_id or not text or len(text) > 4000:
        raise HTTPException(status_code=400, detail="invalid_message")
    messages = room.messages.get(channel_id, [])
    index = next((i for i, message in enumerate(messages) if message.get("id") == message_id), -1)
    if index < 0:
        raise HTTPException(status_code=404, detail="not_found")
    target = messages[index]
    if target.get("deleted"):
        raise HTTPException(status_code=409, detail="message_deleted")
    role = room.role_for(account["peer_id"])
    if target.get("authorId") != account["peer_id"] and role not in ("owner", "admin", "mod"):
        raise HTTPException(status_code=403, detail="forbidden")
    updated = {
        **target,
        "text": text,
        "edited": True,
        "editedAt": int(time.time() * 1000),
    }
    messages[index] = updated
    schedule_save_db()
    return {"success": True, "message": updated}


@app.post("/api/rooms/{room_id}/channel_background")
async def set_channel_background(room_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    account = _request_account(request, body)
    if not account or not room.has_permission(account["peer_id"], "manage_server"):
        raise HTTPException(status_code=403, detail="forbidden")
    ch = body.get("channel_id")
    url = body.get("url")
    if not ch:
        return {"error": "channel_id required"}
    if not url:
        room.channel_backgrounds.pop(ch, None)
    else:
        room.channel_backgrounds[ch] = url
    schedule_save_db()
    # Broadcast to all peers
    await broadcast_to_room(room, {
        "type": "channel_background_update",
        "channelId": ch,
        "url": url or None,
        "channel_backgrounds": room.channel_backgrounds
    })
    return {"success": True, "channel_backgrounds": room.channel_backgrounds}


@app.post("/api/rooms/{room_id}/settings")
async def update_room_settings(room_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    _owner_key_ok(room, body, request.query_params, request)
    if body.get("name"):
        room.name = body["name"]
    if "icon_url" in body:
        room.icon_url = body["icon_url"]
    if "description" in body:
        room.description = str(body["description"])[:500]
    if "is_public" in body:
        room.is_public = bool(body["is_public"])
    if body.get("roles"):
        room.roles = body["roles"]
    if "peer_roles" in body:
        room.peer_roles = body["peer_roles"]
    if "channel_permissions" in body:
        room.channel_permissions = body["channel_permissions"]
    if "voicePermissionMode" in body:
        pass  # client-side only
    room.normalize_permissions()
    schedule_save_db()
    await broadcast_to_room(room, {
        "type": "server_update",
        "payload": {
            "id": room_id,
            "name": room.name,
            "roles": room.roles,
            "peer_roles": room.peer_roles,
            "channel_permissions": room.channel_permissions,
            "icon_url": room.icon_url,
            "is_public": room.is_public,
        }
    })
    return {"success": True}


@app.post("/api/rooms/{room_id}/invite_rotate")
def rotate_invite(room_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    account = _request_account(request, body)
    if not account or not room.has_permission(account["peer_id"], "manage_invites"):
        raise HTTPException(status_code=403, detail="forbidden")
    old_code = room.invite_code
    room.invite_code = str(uuid.uuid4())[:6].upper()
    with _db() as conn:
        conn.execute("UPDATE server_invites SET revoked_at = ? WHERE invite_code = ?", (time.time(), old_code))
        conn.execute(
            "INSERT INTO server_invites (invite_code, room_id, created_by, created_at) VALUES (?, ?, ?, ?)",
            (room.invite_code, room_id, account["peer_id"], time.time()),
        )
    _record_audit(room_id, account["peer_id"], "invite_rotated", metadata={"previous_code": old_code})
    schedule_save_db(0.4)
    return {"invite_code": room.invite_code}


@app.post("/api/rooms/{room_id}/invites")
def create_room_invite(room_id: str, body: dict, request: Request):
    room, account = _require_room_member(room_id, request, body)
    if not room.has_permission(account["peer_id"], "manage_invites"):
        raise HTTPException(status_code=403, detail="forbidden")
    expires_in = body.get("expires_in_seconds")
    max_uses = body.get("max_uses")
    try:
        expires_in = int(expires_in) if expires_in not in (None, "", 0, "0") else None
        max_uses = int(max_uses) if max_uses not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid_invite_limits")
    if expires_in is not None and not 60 <= expires_in <= 365 * 24 * 3600:
        raise HTTPException(status_code=400, detail="invalid_invite_expiry")
    if max_uses is not None and not 1 <= max_uses <= 10000:
        raise HTTPException(status_code=400, detail="invalid_invite_max_uses")
    code = str(uuid.uuid4())[:8].upper()
    now = time.time()
    with _db() as conn:
        conn.execute(
            "INSERT INTO server_invites (invite_code, room_id, created_by, created_at, expires_at, max_uses) VALUES (?, ?, ?, ?, ?, ?)",
            (code, room_id, account["peer_id"], now, now + expires_in if expires_in else None, max_uses),
        )
        invite = conn.execute("SELECT * FROM server_invites WHERE invite_code = ?", (code,)).fetchone()
    _record_audit(room_id, account["peer_id"], "invite_created", metadata={"invite_code": code, "expires_in_seconds": expires_in, "max_uses": max_uses})
    return {"success": True, "invite": _invite_payload(invite)}


@app.get("/api/rooms/{room_id}/invites")
def list_room_invites(room_id: str, request: Request):
    room, account = _require_room_member(room_id, request)
    if not room.has_permission(account["peer_id"], "manage_invites"):
        raise HTTPException(status_code=403, detail="forbidden")
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM server_invites WHERE room_id = ? ORDER BY created_at DESC LIMIT 100",
            (room_id,),
        ).fetchall()
    return {"invites": [_invite_payload(row) for row in rows]}


@app.delete("/api/rooms/{room_id}/invites/{invite_code}")
def revoke_room_invite(room_id: str, invite_code: str, request: Request):
    room, account = _require_room_member(room_id, request)
    if not room.has_permission(account["peer_id"], "manage_invites"):
        raise HTTPException(status_code=403, detail="forbidden")
    with _db() as conn:
        cursor = conn.execute(
            "UPDATE server_invites SET revoked_at = ? WHERE room_id = ? AND invite_code = ? AND revoked_at IS NULL",
            (time.time(), room_id, invite_code.upper()),
        )
    if not cursor.rowcount:
        raise HTTPException(status_code=404, detail="invite_not_found")
    _record_audit(room_id, account["peer_id"], "invite_revoked", metadata={"invite_code": invite_code.upper()})
    return {"success": True}


def _require_moderator(room_id: str, request: Request, body: dict, permission: str) -> tuple["Room", sqlite3.Row]:
    room, account = _require_room_member(room_id, request, body)
    if not room.has_permission(account["peer_id"], permission):
        raise HTTPException(status_code=403, detail="forbidden")
    return room, account


@app.post("/api/rooms/{room_id}/moderation/ban")
def ban_room_member(room_id: str, body: dict, request: Request):
    room, account = _require_moderator(room_id, request, body, "ban_members")
    target_peer_id = str(body.get("target_peer_id") or "").strip()
    reason = str(body.get("reason") or "").strip()[:500]
    if not target_peer_id or not _can_moderate_target(room, account["peer_id"], target_peer_id):
        raise HTTPException(status_code=403, detail="cannot_moderate_target")
    with _db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO server_bans (room_id, peer_id, actor_peer_id, reason, created_at) VALUES (?, ?, ?, ?, ?)",
            (room_id, target_peer_id, account["peer_id"], reason, time.time()),
        )
        conn.execute("DELETE FROM server_members WHERE room_id = ? AND peer_id = ?", (room_id, target_peer_id))
        conn.execute("DELETE FROM server_timeouts WHERE room_id = ? AND peer_id = ?", (room_id, target_peer_id))
    _remove_peer_from_voice(room, target_peer_id)
    _record_audit(room_id, account["peer_id"], "member_banned", target_peer_id, reason)
    return {"success": True}


@app.delete("/api/rooms/{room_id}/moderation/ban/{target_peer_id}")
def unban_room_member(room_id: str, target_peer_id: str, request: Request):
    room, account = _require_moderator(room_id, request, {}, "ban_members")
    with _db() as conn:
        cursor = conn.execute("DELETE FROM server_bans WHERE room_id = ? AND peer_id = ?", (room_id, target_peer_id))
    if not cursor.rowcount:
        raise HTTPException(status_code=404, detail="ban_not_found")
    _record_audit(room_id, account["peer_id"], "member_unbanned", target_peer_id)
    return {"success": True}


@app.post("/api/rooms/{room_id}/moderation/timeout")
def timeout_room_member(room_id: str, body: dict, request: Request):
    room, account = _require_moderator(room_id, request, body, "timeout_members")
    target_peer_id = str(body.get("target_peer_id") or "").strip()
    reason = str(body.get("reason") or "").strip()[:500]
    try:
        duration_seconds = int(body.get("duration_seconds"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid_timeout_duration")
    if not 60 <= duration_seconds <= 28 * 24 * 3600:
        raise HTTPException(status_code=400, detail="invalid_timeout_duration")
    if not target_peer_id or not _can_moderate_target(room, account["peer_id"], target_peer_id):
        raise HTTPException(status_code=403, detail="cannot_moderate_target")
    now = time.time()
    expires_at = now + duration_seconds
    with _db() as conn:
        conn.execute(
            "INSERT INTO server_timeouts (room_id, peer_id, actor_peer_id, reason, expires_at, created_at) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(room_id, peer_id) DO UPDATE SET actor_peer_id = excluded.actor_peer_id, reason = excluded.reason, expires_at = excluded.expires_at, created_at = excluded.created_at",
            (room_id, target_peer_id, account["peer_id"], reason, expires_at, now),
        )
    _record_audit(room_id, account["peer_id"], "member_timed_out", target_peer_id, reason, {"duration_seconds": duration_seconds})
    return {"success": True, "expires_at": expires_at}


@app.delete("/api/rooms/{room_id}/moderation/timeout/{target_peer_id}")
def clear_room_timeout(room_id: str, target_peer_id: str, request: Request):
    room, account = _require_moderator(room_id, request, {}, "timeout_members")
    with _db() as conn:
        cursor = conn.execute("DELETE FROM server_timeouts WHERE room_id = ? AND peer_id = ?", (room_id, target_peer_id))
    if not cursor.rowcount:
        raise HTTPException(status_code=404, detail="timeout_not_found")
    _record_audit(room_id, account["peer_id"], "member_timeout_cleared", target_peer_id)
    return {"success": True}


@app.post("/api/rooms/{room_id}/moderation/slow-mode")
def set_room_slow_mode(room_id: str, body: dict, request: Request):
    room, account = _require_moderator(room_id, request, body, "manage_channels")
    channel_id = str(body.get("channel_id") or "").strip()
    try:
        seconds = int(body.get("seconds", 0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid_slow_mode")
    if seconds < 0 or seconds > 21600 or not any(channel.get("id") == channel_id and channel.get("type") == "text" for channel in room.channels):
        raise HTTPException(status_code=400, detail="invalid_slow_mode")
    if seconds:
        room.channel_slow_modes[channel_id] = seconds
    else:
        room.channel_slow_modes.pop(channel_id, None)
    schedule_save_db()
    _record_audit(room_id, account["peer_id"], "slow_mode_updated", metadata={"channel_id": channel_id, "seconds": seconds})
    return {"success": True, "channel_id": channel_id, "seconds": seconds}


@app.get("/api/rooms/{room_id}/audit-log")
def get_room_audit_log(room_id: str, request: Request, limit: int = 50):
    room, account = _require_moderator(room_id, request, {}, "view_audit_log")
    limit = max(1, min(int(limit), 100))
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM server_audit_log WHERE room_id = ? ORDER BY audit_id DESC LIMIT ?",
            (room_id, limit),
        ).fetchall()
    return {"entries": [{**dict(row), "metadata": json.loads(row["metadata_json"] or "{}")} for row in rows]}


def _report_payload(row: sqlite3.Row) -> dict:
    return dict(row)


@app.post("/api/rooms/{room_id}/reports")
def create_message_report(room_id: str, body: dict, request: Request):
    room, account = _require_room_member(room_id, request, body)
    channel_id = str(body.get("channelId") or body.get("channel_id") or "").strip()
    message_id = str(body.get("messageId") or body.get("message_id") or "").strip()
    reason = str(body.get("reason") or "other").strip().lower()
    detail = str(body.get("detail") or "").strip()[:500]
    if not channel_id or not message_id or reason not in {"spam", "harassment", "safety", "other"}:
        raise HTTPException(status_code=400, detail="invalid_report")
    message = next((item for item in room.messages.get(channel_id, []) if item.get("id") == message_id), None)
    if not message:
        raise HTTPException(status_code=404, detail="message_not_found")
    report_id = str(body.get("id") or uuid.uuid4())[:100]
    now = time.time()
    with _db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO server_message_reports (report_id, room_id, channel_id, message_id, reporter_peer_id, author_peer_id, reason, detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (report_id, room_id, channel_id, message_id, account["peer_id"], str(message.get("authorId") or ""), reason, detail, now),
        )
        report = conn.execute("SELECT * FROM server_message_reports WHERE report_id = ?", (report_id,)).fetchone()
    _record_audit(room_id, account["peer_id"], "message_reported", str(message.get("authorId") or ""), reason, {"message_id": message_id, "channel_id": channel_id})
    return {"success": True, "report": _report_payload(report)}


@app.get("/api/rooms/{room_id}/reports")
def list_message_reports(room_id: str, request: Request, status: str = "open", limit: int = 50):
    _, _ = _require_moderator(room_id, request, {}, "manage_reports")
    limit = max(1, min(int(limit), 100))
    status = status if status in {"open", "resolved", "dismissed", "all"} else "open"
    with _db() as conn:
        if status == "all":
            rows = conn.execute("SELECT * FROM server_message_reports WHERE room_id = ? ORDER BY created_at DESC LIMIT ?", (room_id, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM server_message_reports WHERE room_id = ? AND status = ? ORDER BY created_at DESC LIMIT ?", (room_id, status, limit)).fetchall()
    return {"reports": [_report_payload(row) for row in rows]}


@app.patch("/api/rooms/{room_id}/reports/{report_id}")
def resolve_message_report(room_id: str, report_id: str, body: dict, request: Request):
    _, account = _require_moderator(room_id, request, body, "manage_reports")
    status = str(body.get("status") or "resolved").lower()
    if status not in {"resolved", "dismissed"}:
        raise HTTPException(status_code=400, detail="invalid_report_status")
    now = time.time()
    with _db() as conn:
        cursor = conn.execute(
            "UPDATE server_message_reports SET status = ?, moderator_peer_id = ?, resolved_at = ? WHERE room_id = ? AND report_id = ?",
            (status, account["peer_id"], now, room_id, report_id),
        )
        if not cursor.rowcount:
            raise HTTPException(status_code=404, detail="report_not_found")
        report = conn.execute("SELECT * FROM server_message_reports WHERE report_id = ?", (report_id,)).fetchone()
    _record_audit(room_id, account["peer_id"], f"report_{status}", metadata={"report_id": report_id})
    return {"success": True, "report": _report_payload(report)}

@app.get("/api/rooms/join/{invite_code}")
def get_room_by_code(invite_code: str, request: Request):
    code = invite_code.upper()
    account = _request_account(request)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    with _db() as conn:
        invite = conn.execute("SELECT * FROM server_invites WHERE invite_code = ?", (code,)).fetchone()
        # A pre-migration room gets a record the first time its existing code is used.
        if not invite:
            legacy_room = next((candidate for candidate in rooms.values() if candidate.invite_code == code), None)
            if legacy_room:
                conn.execute(
                    "INSERT OR IGNORE INTO server_invites (invite_code, room_id, created_by, created_at) VALUES (?, ?, ?, ?)",
                    (code, legacy_room.room_id, legacy_room.owner_id, time.time()),
                )
                invite = conn.execute("SELECT * FROM server_invites WHERE invite_code = ?", (code,)).fetchone()
        if not invite or invite["revoked_at"] or (invite["expires_at"] and invite["expires_at"] <= time.time()):
            raise HTTPException(status_code=404, detail="invite_not_found")
        room = rooms.get(invite["room_id"])
        if not room:
            raise HTTPException(status_code=404, detail="invite_not_found")
        if _is_banned(room.room_id, account["peer_id"]):
            raise HTTPException(status_code=403, detail="banned")
        already_member = _is_room_member(room.room_id, account["peer_id"])
        if not already_member:
            used = conn.execute(
                "UPDATE server_invites SET use_count = use_count + 1 WHERE invite_code = ? AND (max_uses IS NULL OR use_count < max_uses)",
                (code,),
            )
            if used.rowcount != 1:
                raise HTTPException(status_code=410, detail="invite_exhausted")
    _upsert_server_member(room.room_id, account["peer_id"], _account_name(account), room.role_for(account["peer_id"]))
    return room.to_dict()

@app.delete("/api/rooms/{room_id}")
async def delete_room(room_id: str, owner_id: str, request: Request):
    if room_id not in rooms: return {"error": "Not found"}
    room = rooms[room_id]
    # Yeni odalar: owner_key şart. Legacy odalar (owner_key yok): eski owner_id kontrolü.
    if room.owner_key is not None:
        _owner_key_ok(room, None, request.query_params, request)
    auth = request.headers.get("authorization") or ""
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else request.query_params.get("token", "")
    account = _account_by_token(token)
    if room.owner_id != owner_id and not (account and _is_platform_admin(account["peer_id"])):
        return {"error": "Unauthorized"}
    del rooms[room_id]
    deleted_room_ids.add(room_id)
    # Zombie client'lar─▒ temizle: ba─ƒl─▒ herkese silindi─ƒini s├╢yle, sonra soketi kapat.
    # Aksi halde offline/ba┼ƒka odadaki peer'lar sunucunun silindi─ƒini asla ├╢─ƒrenemez
    # ve yerel ├╢nbellekte "hayalet sunucu" olarak sonsuza dek kal─▒r.
    await broadcast_to_room(room, {"type": "room_deleted", "room_id": room_id})
    for ws in list(room.peers.values()):
        try:
            await ws.close(code=4004, reason="room_deleted")
        except Exception:
            pass
    save_db()
    log.info(f"Room deleted: {room_id} by {owner_id}")
    return {"success": True}


@app.post("/api/rooms/sync")
def sync_rooms(body: dict, request: Request):
    """
    ─░stemci a├º─▒l─▒┼ƒta yerel ├╢nbellekteki oda id'lerini buraya postalar.
    D├╢nen s─▒n─▒fland─▒rmaya g├╢re istemci: active -> metadata g├╝nceller,
    deleted -> yerel kopyay─▒ siler, unknown -> restore dener (redeploy sonras─▒
    disk s─▒f─▒rlanm─▒┼ƒsa) ya da bulunamazsa kullan─▒c─▒ya sorar.
    """
    account = _request_account(request, body)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    ids = [str(room_id) for room_id in (body.get("room_ids") or [])[:250]]
    active, deleted, unknown = [], [], []
    for rid in ids:
        if rid in rooms and _is_room_member(rid, account["peer_id"]) and not _is_banned(rid, account["peer_id"]):
            active.append(rooms[rid].to_dict())
        elif rid in deleted_room_ids and _is_platform_admin(account["peer_id"]):
            deleted.append(rid)
        else:
            unknown.append(rid)
    return {"active": active, "deleted": deleted, "unknown": unknown}


@app.post("/api/rooms/{room_id}/restore")
def restore_room(room_id: str, body: dict, request: Request):
    """
    Sunucu (├╢rn. Render redeploy sonras─▒ ephemeral disk s─▒f─▒rlan─▒nca) bir oday─▒
    unutmu┼ƒ ama istemcide h├ól├ó tam kopyas─▒ varsa, istemci bu u├ºla oday─▒ geri
    kay─▒t ettirir. Owner taraf─▒ndan kal─▒c─▒ silinmi┼ƒ (tombstoned) odalar asla
    geri gelmez.
    """
    if room_id in deleted_room_ids:
        return {"error": "deleted"}
    auth = request.headers.get("authorization") or ""
    token = body.get("token") or (auth[7:].strip() if auth.lower().startswith("bearer ") else "")
    account = _account_by_token(token)
    if not account:
        raise HTTPException(status_code=401, detail="unauthorized")
    if room_id in rooms:
        _owner_key_ok(rooms[room_id], body, request.query_params, request)
        return {"success": True, "room": rooms[room_id].to_dict()}
    name = body.get("name", "Unnamed Server")
    owner_id = body.get("owner_id") or account["peer_id"]
    if owner_id != account["peer_id"] and not _is_platform_admin(account["peer_id"]):
        raise HTTPException(status_code=403, detail="forbidden")
    room = Room(room_id, name, owner_id, _account_name(account) if owner_id == account["peer_id"] else body.get("owner_username", ""))
    if body.get("owner_key"):
        room.owner_key = body["owner_key"]  # restore sonrası güvenlik korunur
    if body.get("channels"):
        room.channels = body["channels"]
    if body.get("roles"):
        room.roles = body["roles"]
    if body.get("peer_roles"):
        room.peer_roles = body["peer_roles"]
    if body.get("channel_permissions"):
        room.channel_permissions = body["channel_permissions"]
    if body.get("pinned_messages"):
        room.pinned_messages = body["pinned_messages"]
    if body.get("messages"):
        room.messages = body["messages"]
    if body.get("channel_backgrounds"):
        room.channel_backgrounds = body["channel_backgrounds"]
    if body.get("icon_url"):
        room.icon_url = body["icon_url"]
    if "is_public" in body:
        room.is_public = bool(body["is_public"])
    if body.get("invite_code"):
        room.invite_code = body["invite_code"]
    room.normalize_permissions()
    rooms[room_id] = room
    save_db()
    log.info(f"Room restored from client cache: {name!r} ({room_id})")
    return {"success": True, "room": room.to_dict()}

@app.post("/api/rooms/{room_id}/channels")
def add_channel(room_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    _owner_key_ok(room, body, request.query_params, request)
    new_ch = {
        "id": str(uuid.uuid4())[:8],
        "name": body.get("name", "new-channel"),
        "type": body.get("type", "text")
    }
    room.channels.append(new_ch)
    schedule_save_db()
    return new_ch

@app.delete("/api/rooms/{room_id}/channels/{channel_id}")
async def delete_channel(room_id: str, channel_id: str, request: Request, requester_id: str = ""):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    _owner_key_ok(room, None, request.query_params, request)
    channel = next((c for c in room.channels if c["id"] == channel_id), None)
    if not channel:
        return {"error": "Channel not found"}
    # Remove channel and its messages/background
    room.channels = [c for c in room.channels if c["id"] != channel_id]
    room.messages.pop(channel_id, None)
    room.channel_backgrounds.pop(channel_id, None)
    room.channel_permissions.pop(channel_id, None)
    # Broadcast channel deletion to all connected peers
    await broadcast_to_room(room, {
        "type": "channel_delete",
        "payload": {"serverId": room_id, "channelId": channel_id}
    })
    schedule_save_db()
    log.info(f"Channel {channel_id} deleted from room {room_id}")
    return {"success": True}


@app.patch("/api/rooms/{room_id}/channels/{channel_id}")
async def rename_channel(room_id: str, channel_id: str, body: dict, request: Request, requester_id: str = ""):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    _owner_key_ok(room, body, request.query_params, request)
    channel = next((c for c in room.channels if c["id"] == channel_id), None)
    if not channel:
        return {"error": "Channel not found"}
    
    new_name = body.get("name")
    if new_name:
        channel["name"] = new_name
        schedule_save_db()
        await broadcast_to_room(room, {
            "type": "channel_rename",
            "payload": {"serverId": room_id, "channelId": channel_id, "name": new_name}
        })
        log.info(f"Channel {channel_id} renamed to {new_name} in room {room_id}")
        return {"success": True, "channel": channel}
    return {"error": "Name is required"}


@app.post("/api/rooms/{room_id}/roles")
def add_role(room_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    _owner_key_ok(room, body, request.query_params, request)
    role_id = body.get("name", "new-role").lower()
    room.roles[role_id] = {
        "name": body.get("name", "New Role"),
        "color": body.get("color", "#94a3b8"),
        "hoist": body.get("hoist", False),
        "permissions": body.get("permissions", {})
    }
    schedule_save_db()
    return {"role_id": role_id}

@app.patch("/api/rooms/{room_id}/roles/{role_id}")
def update_role(room_id: str, role_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    _owner_key_ok(room, body, request.query_params, request)
    if role_id not in room.roles:
        return {"error": "Role not found"}
    
    if "name" in body: room.roles[role_id]["name"] = body["name"]
    if "color" in body: room.roles[role_id]["color"] = body["color"]
    if "hoist" in body: room.roles[role_id]["hoist"] = body["hoist"]
    if "permissions" in body: room.roles[role_id]["permissions"] = body["permissions"]
    
    schedule_save_db()
    return {"success": True, "role": room.roles[role_id]}

@app.post("/api/rooms/{room_id}/assign_role")
def assign_role(room_id: str, body: dict, request: Request):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    _owner_key_ok(room, body, request.query_params, request)
    peer_id = body.get("peer_id")
    role_id = body.get("role_id")
    if peer_id and role_id in room.roles:
        room.peer_roles[peer_id] = role_id
        username = room.peer_info.get(peer_id, {}).get("username", "")
        _upsert_server_member(room_id, peer_id, username, role_id)
        schedule_save_db()
        return {"success": True}
    return {"error": "Invalid data"}

@app.get("/api/discord/invite/{code}")
def discord_invite_preview(code: str):
    """
    Discord davet kodu ├╢nizlemesi ΓÇö Discord'un HERKESE A├çIK davet API'si
    (auth gerektirmez). Sadece sunucu ad─▒/ikon/├╝ye say─▒s─▒ d├╢ner; kat─▒l─▒m
    kullan─▒c─▒n─▒n kendi Discord hesab─▒yla discord.gg linki ├╝zerinden yap─▒l─▒r.
    """
    code = re.sub(r"[^A-Za-z0-9-]", "", code)[:32]
    if not code:
        return {"error": "invalid_code"}
    try:
        url = f"https://discord.com/api/v10/invites/{code}?with_counts=true"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=5).read().decode())
        guild = data.get("guild") or {}
        icon = None
        if guild.get("id") and guild.get("icon"):
            icon = f"https://cdn.discordapp.com/icons/{guild['id']}/{guild['icon']}.png?size=128"
        return {
            "code": code,
            "name": guild.get("name") or data.get("channel", {}).get("name") or "Discord Sunucusu",
            "description": guild.get("description") or "",
            "icon": icon,
            "member_count": data.get("approximate_member_count"),
            "online_count": data.get("approximate_presence_count"),
        }
    except Exception as e:
        log.warning(f"Discord invite preview error: {e}")
        return {"error": "not_found"}


@app.get("/api/ytsearch")
def yt_search(q: str):
    """Fallback tiny scraper to get first YT video ID for music bot"""
    try:
        url = "https://www.youtube.com/results?search_query=" + urllib.parse.quote(q)
        # Add basic headers to prevent 403s just in case
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        html = urllib.request.urlopen(req, timeout=3).read().decode()
        video_ids = re.findall(r"watch\?v=(\S{11})", html)
        if (video_ids):
            return {"id": video_ids[0]}
    except Exception as e:
        log.warning(f"YT search error: {e}")
    return {"id": None}

# ΓöÇΓöÇ WebSocket signaling ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

@app.websocket("/ws/{room_id}/{peer_id}")
async def signaling_ws(websocket: WebSocket, room_id: str, peer_id: str):
    await websocket.accept()

    token = websocket.query_params.get("token", "")
    account = _account_by_token(token)
    if not account or account["peer_id"] != peer_id:
        await websocket.send_text(json.dumps({"type": "error", "message": "Unauthorized peer"}))
        await websocket.close(code=4403, reason="unauthorized")
        return

    # Reject unknown rooms
    if room_id not in rooms:
        await websocket.send_text(json.dumps({"type": "error", "message": "Room not found"}))
        await websocket.close()
        return

    room = rooms[room_id]
    if not _is_room_member(room_id, peer_id):
        await websocket.send_text(json.dumps({"type": "error", "message": "Room membership required"}))
        await websocket.close(code=4403, reason="not_a_member")
        return
    if _is_banned(room_id, peer_id):
        await websocket.send_text(json.dumps({"type": "error", "message": "Banned from this server"}))
        await websocket.close(code=4403, reason="banned")
        return
    username = websocket.query_params.get("username", "Anonymous")
    avatar_color = websocket.query_params.get("color", "#7289da")
    # avatar_image is NOT stored server-side (too large); clients share it P2P via identity_announce

    # Register peer
    # Reconnect race: if an older socket is still registered for this peer_id,
    # close it first so its finally block cannot clobber the new registration.
    old_socket = room.peers.get(peer_id)
    if old_socket is not None and old_socket is not websocket:
        try:
            await old_socket.close()
        except Exception:
            pass
    room.peers[peer_id] = websocket
    room.peer_info[peer_id] = {"username": username, "avatar_color": avatar_color}
    room.last_seen[peer_id] = time.time()
    _upsert_server_member(room_id, peer_id, _account_name(account), room.role_for(peer_id))
    log.info(f"Peer {peer_id} ({username}) joined room {room_id}")

    # Tell the new peer who else is here
    await websocket.send_text(json.dumps({
        "type": "room_state",
        "room": room.to_dict(),
        "your_id": peer_id,
    }))
    await websocket.send_text(json.dumps(_voice_snapshot(room)))
    await websocket.send_text(json.dumps(_music_public_state(room)))

    # Tell everyone else the new peer arrived
    await broadcast_to_room(room, {
        "type": "peer_joined",
        "peer_id": peer_id,
        "username": username,
        "avatar_color": avatar_color,
        # avatar_image is sent directly peer-to-peer via identity_announce DataChannel message
    }, exclude=peer_id)

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            msg_type = msg.get("type", "")

            if msg_type in ("offer", "answer", "ice_candidate"):
                # Route signaling messages to a specific peer
                target = msg.get("target")
                if target:
                    msg["from"] = peer_id
                    await send_to_peer(room, target, msg)

            elif msg_type in ("dm_call_offer", "dm_call_answer", "dm_call_end"):
                target = msg.get("target")
                if target:
                    msg["from"] = peer_id
                    await send_to_peer(room, target, msg)

            elif msg_type == "dm_relay":
                target = msg.get("target")
                payload = msg.get("payload")
                if target and payload:
                    # Relay DM to target peer
                    relay_msg = {
                        "type": "dm",
                        "from": peer_id,
                        "payload": payload
                    }
                    await send_to_peer(room, target, relay_msg)

            elif msg_type == "dm":
                # Direct DM between peers (already handled by P2P data channel)
                # This can be used for server-side logging if needed
                pass

            elif msg_type == "broadcast":
                # Generic broadcast (e.g. nick changes)
                msg["from"] = peer_id
                await broadcast_to_room(room, msg, exclude=peer_id)

            elif msg_type == "voice_join":
                ch = msg.get("channelId")
                if not ch:
                    await websocket.send_text(json.dumps({"type": "error", "message": "channelId required"}))
                    continue
                if not room.has_permission(peer_id, "join_voice", ch):
                    await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "join_voice"}))
                    continue
                _remove_peer_from_voice(room, peer_id)
                room.voice_members.setdefault(ch, {})[peer_id] = _member_payload(room, peer_id, username, avatar_color, msg)
                await broadcast_voice_snapshot(room, ch)

            elif msg_type == "voice_leave":
                ch = msg.get("channelId")
                if ch and ch in room.voice_members:
                    room.voice_members[ch].pop(peer_id, None)
                    if not room.voice_members[ch]:
                        room.voice_members.pop(ch, None)
                else:
                    _remove_peer_from_voice(room, peer_id)
                await broadcast_voice_snapshot(room, ch)

            elif msg_type == "voice_status":
                ch = msg.get("channelId")
                member = room.voice_members.get(ch or "", {}).get(peer_id)
                if member:
                    member["isSpeaking"] = bool(msg.get("speaking"))
                    await broadcast_to_room(room, {
                        "type": "voice_state",
                        "channelId": ch,
                        "member": member,
                    })

            elif msg_type == "media_status":
                ch = msg.get("channelId")
                member = room.voice_members.get(ch or "", {}).get(peer_id)
                if not member:
                    continue
                kind = msg.get("kind")
                sharing = bool(msg.get("sharing"))
                if kind == "screen":
                    if sharing and not room.has_permission(peer_id, "screen_share", ch):
                        await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "screen_share"}))
                        continue
                    member["isSharingScreen"] = sharing
                elif kind == "camera":
                    if sharing and not room.has_permission(peer_id, "camera", ch):
                        await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "camera"}))
                        continue
                    member["isSharingCamera"] = sharing
                await broadcast_to_room(room, {
                    "type": "media_status",
                    "channelId": ch,
                    "peer_id": peer_id,
                    "kind": kind,
                    "sharing": sharing,
                    "member": member,
                })

            elif msg_type == "music_command":
                cmd = (msg.get("command") or "").lower()
                ch = msg.get("voiceChannelId")
                if not ch:
                    await websocket.send_text(json.dumps({"type": "error", "message": "voiceChannelId required"}))
                    continue
                if cmd == "play":
                    if not room.has_permission(peer_id, "join_voice", ch):
                        await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "join_voice"}))
                        continue
                    video_id = msg.get("videoId")
                    if not video_id:
                        await websocket.send_text(json.dumps({"type": "error", "message": "videoId required"}))
                        continue
                    now_ms = int(time.time() * 1000)
                    room.music_session = {
                        "active": True,
                        "state": "playing",
                        "videoId": video_id,
                        "voiceChannelId": ch,
                        "controllerId": peer_id,
                        "controllerName": username,
                        "startedAt": msg.get("startedAt") or now_ms,
                        "position": float(msg.get("position", 0)),
                        "updatedAt": now_ms,
                    }
                elif cmd in ("stop", "pause", "resume", "seek", "skip"):
                    if not _can_control_music(room, peer_id):
                        await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "music_control"}))
                        continue
                    if cmd == "stop":
                        room.music_session = None
                    elif room.music_session:
                        now_ms = int(time.time() * 1000)
                        if cmd == "pause":
                            room.music_session["state"] = "paused"
                            room.music_session["position"] = float(msg.get("position", room.music_session.get("position", 0)))
                        elif cmd == "resume":
                            room.music_session["state"] = "playing"
                            room.music_session["startedAt"] = now_ms - int(float(msg.get("position", room.music_session.get("position", 0))) * 1000)
                        elif cmd == "seek":
                            room.music_session["position"] = float(msg.get("position", 0))
                            room.music_session["startedAt"] = now_ms - int(room.music_session["position"] * 1000)
                        elif cmd == "skip" and msg.get("videoId"):
                            room.music_session.update({
                                "state": "playing",
                                "videoId": msg["videoId"],
                                "startedAt": now_ms,
                                "position": 0,
                            })
                        room.music_session["updatedAt"] = now_ms
                await broadcast_to_room(room, _music_public_state(room))

            elif msg_type == "force_disconnect":
                target = msg.get("target")
                ch = msg.get("channelId")
                if not room.has_permission(peer_id, "force_disconnect", ch):
                    await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "force_disconnect"}))
                    continue
                if target == "bot_music":
                    room.music_session = None
                    await broadcast_to_room(room, _music_public_state(room))
                elif target:
                    _remove_peer_from_voice(room, target)
                    await send_to_peer(room, target, {"type": "force_disconnect", "target": target, "channelId": ch})
                    await broadcast_voice_snapshot(room, ch)

            elif msg_type == "role_update":
                if not room.has_permission(peer_id, "manage_roles"):
                    await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "manage_roles"}))
                    continue
                room.roles = msg.get("roles", room.roles)
                room.peer_roles = msg.get("peer_roles", room.peer_roles)
                room.normalize_permissions()
                schedule_save_db()
                await broadcast_to_room(room, {"type": "role_update", "roles": room.roles, "peer_roles": room.peer_roles})

            elif msg_type == "permission_update":
                if not room.has_permission(peer_id, "manage_roles"):
                    await websocket.send_text(json.dumps({"type": "permission_denied", "permission": "manage_roles"}))
                    continue
                room.channel_permissions = msg.get("channel_permissions", room.channel_permissions)
                room.normalize_permissions()
                schedule_save_db()
                await broadcast_to_room(room, {"type": "permission_update", "channel_permissions": room.channel_permissions, "roles": room.roles})

            elif msg_type == "ping":
                room.last_seen[peer_id] = time.time()
                await websocket.send_text(json.dumps({"type": "pong"}))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning(f"Peer {peer_id} error: {e}")
    finally:
        # Conditional cleanup (reconnect race): if this peer_id was re-registered
        # with a NEW socket, only the new socket owns the registration. Popping /
        # broadcasting unconditionally here would delete the live peer's entry
        # (client would stop receiving broadcasts) and announce a bogus peer_left.
        if room.peers.get(peer_id) is websocket:
            room.peers.pop(peer_id, None)
            room.peer_info.pop(peer_id, None)
            _remove_peer_from_voice(room, peer_id)
            log.info(f"Peer {peer_id} left room {room_id}")
            await broadcast_to_room(room, {
                "type": "peer_left",
                "peer_id": peer_id,
                "username": username,
                "avatar_color": avatar_color,
            })
            await broadcast_voice_snapshot(room)
        room.last_seen[peer_id] = time.time()
        if len(room.peers) == 0:
            asyncio.get_event_loop().call_later(30, lambda: _cleanup_room(room_id))


def _cleanup_room(room_id: str):
    room = rooms.get(room_id)
    if room and len(room.peers) == 0:
        log.info(f"Cleaning transient state for empty room {room_id}")
        room.voice_members.clear()
        room.music_session = None
        stale_before = time.time() - 3600
        room.last_seen = {pid: ts for pid, ts in room.last_seen.items() if ts > stale_before}


# ΓöÇΓöÇ Static files / SPA ΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇΓöÇ

PROJECT_ROOT = Path(__file__).parent.parent
STATIC_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def serve_root():
    return FileResponse(str(PROJECT_ROOT / "index.html"))


@app.get("/app.js")
def serve_app_js():
    return FileResponse(str(PROJECT_ROOT / "app.js"))


@app.get("/favicon.ico")
def serve_favicon():
    return Response(status_code=204)


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    return FileResponse(str(PROJECT_ROOT / "index.html"))


# Render / proxies often use HEAD requests for health checks.
@app.head("/", include_in_schema=False)
def health_root():
    return Response(status_code=200)


@app.on_event("startup")
def startup_event():
    global _SUPABASE_SYNC_READY
    init_accounts_db()
    restore_supabase_if_empty(ACCOUNTS_DB_FILE)
    init_accounts_db()
    ensure_bootstrap_platform_admin()
    load_db()
    ensure_template_rooms()
    _SUPABASE_SYNC_READY = True
    schedule_supabase_push(ACCOUNTS_DB_FILE)


@app.on_event("shutdown")
def shutdown_event():
    """Stop deferred persistence work before an ephemeral worker exits."""
    global _db_save_timer
    with _db_save_lock:
        timer = _db_save_timer
        _db_save_timer = None
    if timer:
        timer.cancel()
        timer.join(timeout=1)

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8000))
    print("\n" + "="*55)
    # Avoid UnicodeEncodeError on some Windows consoles (cp1252)
    print("  SCORD Signaling Server")
    print(f"  ->  http://0.0.0.0:{port}")
    print("="*55 + "\n")
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
