import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
