import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]


class SQLitePersistenceTests(unittest.TestCase):
    def test_server_channels_messages_and_members_survive_reload(self):
        previous = os.environ.get("SCORD_DATA_DIR")
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                os.environ["SCORD_DATA_DIR"] = temp_dir
                spec = importlib.util.spec_from_file_location(
                    "scord_server_persistence_test",
                    ROOT / "static" / "server.py",
                )
                server = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(server)
                server.init_accounts_db()

                room = server.Room("room-test", "Test Community", "owner-1", "Owner")
                room.description = "Durable test room"
                room.is_public = True
                room.messages = {
                    "ch-genel": [{
                        "id": "msg-1",
                        "channelId": "ch-genel",
                        "authorId": "owner-1",
                        "author": "Owner",
                        "text": "Persist me",
                    }]
                }
                server.rooms[room.room_id] = room
                server.save_db()
                server._upsert_server_member(room.room_id, "member-1", "Member", "member")

                server.rooms.clear()
                server.load_db()

                restored = server.rooms[room.room_id]
                self.assertEqual(restored.channels[0]["id"], "ch-genel")
                self.assertEqual(restored.messages["ch-genel"][0]["text"], "Persist me")
                self.assertTrue(restored.is_public)
                discovery = restored.to_discovery_dict()
                self.assertEqual(discovery["member_count"], 2)
                self.assertNotIn("messages", discovery)

                db_path = Path(temp_dir) / "scord_accounts.db"
                with closing(sqlite3.connect(db_path)) as conn:
                    payload = conn.execute(
                        "SELECT state_json FROM servers WHERE room_id = ?",
                        (room.room_id,),
                    ).fetchone()[0]
                    self.assertEqual(json.loads(payload)["description"], "Durable test room")
                    self.assertEqual(
                        conn.execute("SELECT COUNT(*) FROM server_members WHERE room_id = ?", (room.room_id,)).fetchone()[0],
                        2,
                    )
        finally:
            if previous is None:
                os.environ.pop("SCORD_DATA_DIR", None)
            else:
                os.environ["SCORD_DATA_DIR"] = previous

    def test_email_discriminator_admin_and_message_tombstone_flow(self):
        previous_data = os.environ.get("SCORD_DATA_DIR")
        previous_url = os.environ.pop("SCORD_SUPABASE_URL", None)
        previous_key = os.environ.pop("SCORD_SUPABASE_SERVICE_ROLE_KEY", None)
        previous_admin_password = os.environ.get("SCORD_BOOTSTRAP_ADMIN_PASSWORD")
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                os.environ["SCORD_DATA_DIR"] = temp_dir
                os.environ["SCORD_BOOTSTRAP_ADMIN_PASSWORD"] = "test-admin-password"
                spec = importlib.util.spec_from_file_location(
                    "scord_server_account_flow_test",
                    ROOT / "static" / "server.py",
                )
                server = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(server)

                with TestClient(server.app) as client:
                    admin = client.post(
                        "/api/auth/login",
                        json={"username": "sherlock", "password": "test-admin-password"},
                    ).json()
                    self.assertTrue(admin["success"])
                    self.assertTrue(admin["platform_admin"])
                    self.assertRegex(admin["discriminator"], r"^\d{4}$")

                    first = client.post(
                        "/api/auth/register",
                        json={"username": "Aylin", "email": "aylin1@example.com", "password": "secret1"},
                    ).json()
                    second = client.post(
                        "/api/auth/register",
                        json={"username": "Aylin", "email": "aylin2@example.com", "password": "secret2"},
                    ).json()
                    self.assertTrue(first["success"])
                    self.assertTrue(second["success"])
                    self.assertNotEqual(first["discriminator"], second["discriminator"])
                    self.assertEqual(first["username"], second["username"])
                    self.assertEqual(
                        client.post(
                            "/api/auth/login",
                            json={"username": "Aylin", "password": "secret1"},
                        ).json()["error"],
                        "ambiguous_username",
                    )
                    tagged_login = client.post(
                        "/api/auth/login",
                        json={
                            "username": f"Aylin#{first['discriminator']}",
                            "password": "secret1",
                        },
                    ).json()
                    self.assertTrue(tagged_login["success"])

                    headers = {"Authorization": f"Bearer {first['token']}"}
                    created = client.post("/api/rooms", json={"name": "Tombstone QA"}, headers=headers).json()
                    room_id = created["room_id"]
                    channel_id = server.rooms[room_id].channels[0]["id"]
                    message = {
                        "id": "msg-lifecycle",
                        "channelId": channel_id,
                        "authorId": first["peer_id"],
                        "author": first["username"],
                        "text": "ilk metin",
                        "time": "12:00",
                    }
                    saved = client.post(
                        f"/api/rooms/{room_id}/messages",
                        json={"message": message},
                        headers=headers,
                    )
                    self.assertEqual(saved.status_code, 200)
                    edited = client.patch(
                        f"/api/rooms/{room_id}/messages/{message['id']}",
                        json={"channel_id": channel_id, "text": "düzenlenmiş metin"},
                        headers=headers,
                    ).json()["message"]
                    self.assertTrue(edited["edited"])
                    self.assertEqual(edited["text"], "düzenlenmiş metin")
                    deleted = client.delete(
                        f"/api/rooms/{room_id}/messages/{message['id']}?channel_id={channel_id}",
                        headers=headers,
                    ).json()["message"]
                    self.assertTrue(deleted["deleted"])
                    self.assertNotIn("text", deleted)
        finally:
            if previous_data is None:
                os.environ.pop("SCORD_DATA_DIR", None)
            else:
                os.environ["SCORD_DATA_DIR"] = previous_data
            if previous_url is not None:
                os.environ["SCORD_SUPABASE_URL"] = previous_url
            if previous_key is not None:
                os.environ["SCORD_SUPABASE_SERVICE_ROLE_KEY"] = previous_key
            if previous_admin_password is None:
                os.environ.pop("SCORD_BOOTSTRAP_ADMIN_PASSWORD", None)
            else:
                os.environ["SCORD_BOOTSTRAP_ADMIN_PASSWORD"] = previous_admin_password

    def test_supabase_snapshot_adapter_round_trip(self):
        previous_data = os.environ.get("SCORD_DATA_DIR")
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                os.environ["SCORD_DATA_DIR"] = temp_dir
                server_spec = importlib.util.spec_from_file_location(
                    "scord_server_snapshot_test",
                    ROOT / "static" / "server.py",
                )
                server = importlib.util.module_from_spec(server_spec)
                server_spec.loader.exec_module(server)
                server.init_accounts_db()
                with server._db() as conn:
                    conn.execute(
                        "INSERT INTO accounts (peer_id, username, display_name, discriminator, email, password_hash, salt, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        ("peer-snapshot", "Snapshot", "Snapshot", "4242", "snapshot@example.com", "hash", "salt", 1.0),
                    )
                room = server.Room("room-snapshot", "Snapshot Room", "peer-snapshot", "Snapshot")
                server.rooms[room.room_id] = room
                server.save_db()

                store_spec = importlib.util.spec_from_file_location(
                    "scord_supabase_round_trip_test",
                    ROOT / "static" / "supabase_store.py",
                )
                store = importlib.util.module_from_spec(store_spec)
                store_spec.loader.exec_module(store)
                snapshot = store.build_snapshot(server.ACCOUNTS_DB_FILE)

                with closing(sqlite3.connect(server.ACCOUNTS_DB_FILE)) as conn:
                    conn.execute("DELETE FROM server_members")
                    conn.execute("DELETE FROM servers")
                    conn.execute("DELETE FROM accounts")
                    conn.commit()
                store.SUPABASE_URL = "https://example.supabase.co"
                store.SUPABASE_SERVICE_ROLE_KEY = "test-service-role"
                store._request = lambda method, path, payload=None: [{"payload": snapshot}]
                self.assertTrue(store.restore_if_empty(server.ACCOUNTS_DB_FILE))
                with closing(sqlite3.connect(server.ACCOUNTS_DB_FILE)) as conn:
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 1)
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM servers").fetchone()[0], 1)
        finally:
            if previous_data is None:
                os.environ.pop("SCORD_DATA_DIR", None)
            else:
                os.environ["SCORD_DATA_DIR"] = previous_data


if __name__ == "__main__":
    unittest.main()
