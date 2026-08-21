import importlib.util
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]


def _load_server(tag: str):
    spec = importlib.util.spec_from_file_location(f"scord_error_propagation_{tag}", ROOT / "static" / "server.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PersistenceFailurePropagationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.previous_data = os.environ.get("SCORD_DATA_DIR")
        os.environ["SCORD_DATA_DIR"] = self.temp_dir.name
        self.server = _load_server(str(id(self)))
        self._client_context = TestClient(self.server.app)
        self.client = self._client_context.__enter__()

    def tearDown(self):
        self._client_context.__exit__(None, None, None)
        timer = getattr(self.server, "_db_save_timer", None)
        if timer:
            timer.cancel()
            timer.join(timeout=1)
        self.temp_dir.cleanup()
        if self.previous_data is None:
            os.environ.pop("SCORD_DATA_DIR", None)
        else:
            os.environ["SCORD_DATA_DIR"] = self.previous_data

    def register(self):
        response = self.client.post(
            "/api/auth/register",
            json={"username": "persist-qa", "email": "persist-qa@example.com", "password": "test-password"},
        )
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_failed_room_persistence_returns_503_and_leaves_no_phantom_room(self):
        account = self.register()
        known_rooms = set(self.server.rooms)

        def broken_save():
            raise sqlite3.OperationalError("disk I/O error")

        self.server.save_db = broken_save
        response = self.client.post(
            "/api/rooms",
            json={"name": "Unsaveable"},
            headers={"Authorization": f"Bearer {account['token']}"},
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "persist_failed")
        self.assertEqual(set(self.server.rooms), known_rooms)

    def test_unreadable_snapshot_aborts_load_instead_of_starting_empty(self):
        def broken_db():
            raise sqlite3.DatabaseError("database disk image is malformed")

        self.server.rooms["ghost"] = self.server.Room("ghost", "Ghost", "owner", "Owner")
        self.server._db = broken_db
        with self.assertRaises(RuntimeError):
            self.server.load_db()
        self.assertEqual(self.server.rooms, {})


class SupabaseSnapshotFailureTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "scord_supabase_error_propagation", ROOT / "static" / "supabase_store.py"
        )
        self.store = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.store)

    def test_missing_table_is_empty_but_broken_table_propagates(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self.assertEqual(self.store._table_rows(conn, "accounts"), [])

        class LockedConnection:
            row_factory = sqlite3.Row

            def execute(self, *_args, **_kwargs):
                raise sqlite3.OperationalError("database is locked")

        with self.assertRaises(sqlite3.OperationalError):
            self.store._table_rows(LockedConnection(), "accounts")


if __name__ == "__main__":
    unittest.main()
