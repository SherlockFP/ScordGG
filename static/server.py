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
from typing import Dict, Set, Optional, Any
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi import Response
from starlette.websockets import WebSocketState

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("shercord")

app = FastAPI(title="SCORD Signaling Server")

# ── Global State ──────────────────────────────────────────────────────────────

rooms: Dict[str, "Room"] = {}
DATABASE_FILE = str(Path(__file__).parent.parent / "rooms.json")

DEFAULT_ROLE_PERMISSIONS = {
    "owner": {
        "manage_server", "manage_roles", "manage_channels", "kick_members",
        "move_members", "force_disconnect", "join_voice", "speak",
        "screen_share", "camera", "music_control", "send_messages",
    },
    "admin": {
        "manage_server", "manage_roles", "manage_channels", "kick_members",
        "move_members", "force_disconnect", "join_voice", "speak",
        "screen_share", "camera", "music_control", "send_messages",
    },
    "mod": {
        "kick_members", "move_members", "force_disconnect", "join_voice",
        "speak", "screen_share", "camera", "music_control", "send_messages",
    },
    "member": {"join_voice", "speak", "screen_share", "camera", "send_messages"},
}


def _role_defaults(role_id: str) -> dict:
    return {p: True for p in DEFAULT_ROLE_PERMISSIONS.get(role_id, DEFAULT_ROLE_PERMISSIONS["member"])}

def save_db():
    try:
        data = {rid: r.to_persist_dict() for rid, r in rooms.items()}
        with open(DATABASE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log.info(f"Database saved to {DATABASE_FILE}")
    except Exception as e:
        log.error(f"Failed to save db: {e}")


_db_save_timer: Optional[threading.Timer] = None
_db_save_lock = threading.Lock()


def schedule_save_db(delay_sec: float = 1.2):
    """Birleşik disk yazımı — pin/ikon/rol gibi sık uçları yığında tek save."""
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

def load_db():
    if not os.path.exists(DATABASE_FILE):
        return
    try:
        with open(DATABASE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for rid, rdata in data.items():
            room = Room(rid, rdata["name"], rdata.get("owner_id", "unknown"))
            room.channels = rdata.get("channels", room.channels)
            room.roles = rdata.get("roles", room.roles)
            room.channel_permissions = rdata.get("channel_permissions", room.channel_permissions)
            room.peer_roles = rdata.get("peer_roles", room.peer_roles)
            room.pinned_messages = rdata.get("pinned_messages", [])
            room.messages = rdata.get("messages", {})
            room.channel_backgrounds = rdata.get("channel_backgrounds", {})
            room.icon_url = rdata.get("icon_url", None)
            room.invite_code = rdata.get("invite_code", str(uuid.uuid4())[:6].upper())
            room.normalize_permissions()
            rooms[rid] = room
        log.info(f"Database loaded from {DATABASE_FILE} ({len(rooms)} rooms)")
    except Exception as e:
        log.error(f"Failed to load db: {e}")


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
        room = Room(rid, name, "shercord-bot")
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
    def __init__(self, room_id: str, name: str, owner_id: str):
        self.room_id = room_id
        self.name = name
        self.owner_id = owner_id
        self.created_at = time.time()
        self.last_seen: Dict[str, float] = {}
        self.voice_members: Dict[str, Dict[str, dict]] = {}
        self.music_session: Optional[dict] = None
        self.peers: Dict[str, WebSocket] = {}        # peer_id → ws
        self.peer_info: Dict[str, dict] = {}         # peer_id → {username, avatar_color}
        
        # Channels and Roles — IDs must match client-side constants
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
        self.invite_code = str(uuid.uuid4())[:6].upper()

    def to_persist_dict(self):
        """Data to be saved to disk (metadata only)."""
        self.normalize_permissions()
        return {
            "room_id": self.room_id,
            "name": self.name,
            "owner_id": self.owner_id,
            "channels": self.channels,
            "roles": self.roles,
            "channel_permissions": self.channel_permissions,
            "peer_roles": self.peer_roles,
            "pinned_messages": self.pinned_messages,
            "messages": self.messages,
            "channel_backgrounds": self.channel_backgrounds,
            "icon_url": self.icon_url,
            "invite_code": self.invite_code,
        }

    def to_dict(self):
        """Data to be sent to frontend (includes transient state)."""
        self.normalize_permissions()
        return {
            "room_id": self.room_id,
            "name": self.name,
            "owner_id": self.owner_id,
            "peer_count": max(1, len(self.peers)),
            "channels": self.channels,
            "roles": self.roles,
            "channel_permissions": self.channel_permissions,
            "peer_roles": self.peer_roles,
            "pinned_messages": self.pinned_messages,
            "channel_backgrounds": self.channel_backgrounds,
            "icon_url": self.icon_url,
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
        if peer_id == self.owner_id:
            return "owner"
        return self.peer_roles.get(peer_id, "member")

    def has_permission(self, peer_id: str, permission: str, channel_id: str | None = None) -> bool:
        if peer_id == self.owner_id:
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


# ── Helpers ───────────────────────────────────────────────────────────────────

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

# ── REST endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/rooms")
def list_rooms():
    return [r.to_dict() for r in rooms.values()]

@app.post("/api/auth/verify")
def verify_password(body: dict):
    """Verify password for identity generation."""
    username = body.get("username", "").strip().lower()
    password = body.get("password", "")
    if not username or not password:
        return {"valid": False, "error": "Username and password required"}
    if len(username) < 2 or len(password) < 1:
        return {"valid": False, "error": "Invalid credentials"}
    h = 0
    seed = f"{username}:{password}:scord-v2"
    for ch in seed:
        h = ((h << 5) - h) + ord(ch)
        h &= 0xFFFFFFFF
    identity_id = f"sc_{abs(h) & 0xFFFFFFFF:08x}"
    return {"valid": True, "identity_id": identity_id, "username": username}

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
def create_room(body: dict):
    """Create a new P2P room (server). Returns the room_id."""
    room_id = str(uuid.uuid4())
    name = body.get("name", "Unnamed Server")
    owner_id = body.get("owner_id", "unknown")
    rooms[room_id] = Room(room_id, name, owner_id)
    save_db()
    log.info(f"Room created: {name!r} ({room_id}) by {owner_id}")
    return {"room_id": room_id, "invite_code": rooms[room_id].invite_code}

@app.post("/api/rooms/{room_id}/pin")
def toggle_pin(room_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
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
async def save_history_message(room_id: str, body: dict):
    if room_id not in rooms: return {"error": "Not found"}
    room = rooms[room_id]
    msg = body.get("message")
    if not msg: return {"error": "No message"}
    
    ch_id = msg.get("channelId", "general")
    if ch_id not in room.messages:
        room.messages[ch_id] = []
    
    # Store last 10000 messages per channel
    room.messages[ch_id].append(msg)
    if len(room.messages[ch_id]) > 10000:
        room.messages[ch_id].pop(0)
    
    # Only save to disk periodically or on important changes? 
    # For now, save on every message is safer but heavier.
    # save_db() 
    return {"success": True}

@app.post("/api/rooms/{room_id}/icon")
async def update_icon(room_id: str, body: dict):
    if room_id not in rooms: return {"error": "Not found"}
    room = rooms[room_id]
    room.icon_url = body.get("url")
    schedule_save_db()
    # Broadcast icon update to all connected peers
    await broadcast_to_room(room, {
        "type": "server_update",
        "payload": {"id": room_id, "icon_url": room.icon_url}
    })
    return {"success": True}

@app.delete("/api/rooms/{room_id}/messages/{message_id}")
async def delete_message(room_id: str, message_id: str, channel_id: str = ""):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    if channel_id and channel_id in room.messages:
        room.messages[channel_id] = [m for m in room.messages[channel_id] if m.get("id") != message_id]
    else:
        for ch in list(room.messages.keys()):
            room.messages[ch] = [m for m in room.messages[ch] if m.get("id") != message_id]
    schedule_save_db()
    return {"success": True}


@app.post("/api/rooms/{room_id}/channel_background")
async def set_channel_background(room_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
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
async def update_room_settings(room_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    if body.get("name"):
        room.name = body["name"]
    if "icon_url" in body:
        room.icon_url = body["icon_url"]
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
        }
    })
    return {"success": True}


@app.post("/api/rooms/{room_id}/invite_rotate")
def rotate_invite(room_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    if body.get("owner_id") != room.owner_id:
        return {"error": "Unauthorized"}
    room.invite_code = str(uuid.uuid4())[:6].upper()
    schedule_save_db(0.4)
    return {"invite_code": room.invite_code}

@app.get("/api/rooms/join/{invite_code}")
def get_room_by_code(invite_code: str):
    code = invite_code.upper()
    for room in rooms.values():
        if room.invite_code == code:
            return room.to_dict()
    return {"error": "Not found"}

@app.delete("/api/rooms/{room_id}")
def delete_room(room_id: str, owner_id: str):
    if room_id not in rooms: return {"error": "Not found"}
    room = rooms[room_id]
    if room.owner_id != owner_id:
        return {"error": "Unauthorized"}
    del rooms[room_id]
    save_db()
    return {"success": True}

@app.post("/api/rooms/{room_id}/channels")
def add_channel(room_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    new_ch = {
        "id": str(uuid.uuid4())[:8],
        "name": body.get("name", "new-channel"),
        "type": body.get("type", "text")
    }
    room.channels.append(new_ch)
    schedule_save_db()
    return new_ch

@app.delete("/api/rooms/{room_id}/channels/{channel_id}")
async def delete_channel(room_id: str, channel_id: str, requester_id: str = ""):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
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
async def rename_channel(room_id: str, channel_id: str, body: dict, requester_id: str = ""):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
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
def add_role(room_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
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
def update_role(room_id: str, role_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    if role_id not in room.roles:
        return {"error": "Role not found"}
    
    if "name" in body: room.roles[role_id]["name"] = body["name"]
    if "color" in body: room.roles[role_id]["color"] = body["color"]
    if "hoist" in body: room.roles[role_id]["hoist"] = body["hoist"]
    if "permissions" in body: room.roles[role_id]["permissions"] = body["permissions"]
    
    schedule_save_db()
    return {"success": True, "role": room.roles[role_id]}

@app.post("/api/rooms/{room_id}/assign_role")
def assign_role(room_id: str, body: dict):
    if room_id not in rooms:
        return {"error": "Not found"}
    room = rooms[room_id]
    peer_id = body.get("peer_id")
    role_id = body.get("role_id")
    if peer_id and role_id in room.roles:
        room.peer_roles[peer_id] = role_id
        schedule_save_db()
        return {"success": True}
    return {"error": "Invalid data"}

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

# ── WebSocket signaling ────────────────────────────────────────────────────────

@app.websocket("/ws/{room_id}/{peer_id}")
async def signaling_ws(websocket: WebSocket, room_id: str, peer_id: str):
    await websocket.accept()

    # Reject unknown rooms
    if room_id not in rooms:
        await websocket.send_text(json.dumps({"type": "error", "message": "Room not found"}))
        await websocket.close()
        return

    room = rooms[room_id]
    username = websocket.query_params.get("username", "Anonymous")
    avatar_color = websocket.query_params.get("color", "#7289da")
    # avatar_image is NOT stored server-side (too large); clients share it P2P via identity_announce

    # Register peer
    room.peers[peer_id] = websocket
    room.peer_info[peer_id] = {"username": username, "avatar_color": avatar_color}
    room.last_seen[peer_id] = time.time()
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
        room.peers.pop(peer_id, None)
        room.peer_info.pop(peer_id, None)
        room.last_seen[peer_id] = time.time()
        _remove_peer_from_voice(room, peer_id)
        log.info(f"Peer {peer_id} left room {room_id}")
        await broadcast_to_room(room, {
            "type": "peer_left",
            "peer_id": peer_id,
            "username": username,
            "avatar_color": avatar_color,
        })
        await broadcast_voice_snapshot(room)
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


# ── Static files / SPA ────────────────────────────────────────────────────────

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
    load_db()
    ensure_template_rooms()

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
