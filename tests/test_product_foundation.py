import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]


class ProductFoundationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.previous_data = os.environ.get("SCORD_DATA_DIR")
        self.previous_admin = os.environ.get("SCORD_BOOTSTRAP_ADMIN_PASSWORD")
        os.environ["SCORD_DATA_DIR"] = self.temp_dir.name
        os.environ["SCORD_BOOTSTRAP_ADMIN_PASSWORD"] = "test-admin-password"
        spec = importlib.util.spec_from_file_location(
            f"scord_product_foundation_{id(self)}", ROOT / "static" / "server.py"
        )
        self.server = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.server)
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
        if self.previous_admin is None:
            os.environ.pop("SCORD_BOOTSTRAP_ADMIN_PASSWORD", None)
        else:
            os.environ["SCORD_BOOTSTRAP_ADMIN_PASSWORD"] = self.previous_admin

    def register(self, name, email):
        response = self.client.post(
            "/api/auth/register",
            json={"username": name, "email": email, "password": "test-password"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        return data

    @staticmethod
    def auth(account):
        return {"Authorization": f"Bearer {account['token']}"}

    def create_room(self, owner):
        response = self.client.post("/api/rooms", json={"name": "Foundation QA"}, headers=self.auth(owner))
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_room_state_and_sync_require_membership(self):
        owner = self.register("Owner", "owner@example.com")
        outsider = self.register("Outsider", "outsider@example.com")
        created = self.create_room(owner)
        room_id = created["room_id"]

        self.assertEqual(self.client.get(f"/api/rooms/{room_id}").status_code, 401)
        self.assertEqual(
            self.client.get(f"/api/rooms/{room_id}", headers=self.auth(outsider)).status_code,
            403,
        )
        allowed = self.client.get(f"/api/rooms/{room_id}", headers=self.auth(owner))
        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(allowed.json()["room_id"], room_id)

        self.assertEqual(self.client.post("/api/rooms/sync", json={"room_ids": [room_id]}).status_code, 401)
        hidden = self.client.post("/api/rooms/sync", json={"room_ids": [room_id]}, headers=self.auth(outsider)).json()
        self.assertEqual(hidden["active"], [])
        visible = self.client.post("/api/rooms/sync", json={"room_ids": [room_id]}, headers=self.auth(owner)).json()
        self.assertEqual(visible["active"][0]["room_id"], room_id)

    def test_friend_requests_acceptance_and_blocking_are_persistent(self):
        first = self.register("First", "first@example.com")
        second = self.register("Second", "second@example.com")
        identifier = f"{second['username']}#{second['discriminator']}"
        request = self.client.post("/api/friends/requests", json={"identifier": identifier}, headers=self.auth(first))
        self.assertEqual(request.status_code, 200)
        incoming = self.client.get("/api/friends", headers=self.auth(second)).json()
        self.assertEqual(incoming["incoming_requests"][0]["peer_id"], first["peer_id"])
        accepted = self.client.post(
            f"/api/friends/requests/{first['peer_id']}/accept", json={}, headers=self.auth(second)
        )
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(self.client.get("/api/friends", headers=self.auth(first)).json()["friends"][0]["peer_id"], second["peer_id"])

        self.assertEqual(
            self.client.post(f"/api/friends/{second['peer_id']}/block", json={}, headers=self.auth(first)).status_code,
            200,
        )
        blocked_request = self.client.post(
            "/api/friends/requests", json={"target_peer_id": first["peer_id"]}, headers=self.auth(second)
        )
        self.assertEqual(blocked_request.status_code, 403)

    def test_invite_limits_moderation_and_slow_mode(self):
        owner = self.register("ModOwner", "modowner@example.com")
        member = self.register("Member", "member@example.com")
        created = self.create_room(owner)
        room_id = created["room_id"]

        invite = self.client.post(
            f"/api/rooms/{room_id}/invites",
            json={"max_uses": 1, "expires_in_seconds": 300},
            headers=self.auth(owner),
        ).json()["invite"]
        joined = self.client.get(f"/api/rooms/join/{invite['invite_code']}", headers=self.auth(member))
        self.assertEqual(joined.status_code, 200)
        self.assertEqual(
            self.client.get(f"/api/rooms/join/{invite['invite_code']}", headers=self.auth(self.register("Third", "third@example.com"))).status_code,
            410,
        )

        channel_id = self.server.rooms[room_id].channels[0]["id"]
        self.assertEqual(
            self.client.post(
                f"/api/rooms/{room_id}/moderation/slow-mode",
                json={"channel_id": channel_id, "seconds": 60},
                headers=self.auth(owner),
            ).status_code,
            200,
        )
        message = {"message": {"id": "slow-1", "channelId": channel_id, "authorId": member["peer_id"], "text": "first"}}
        self.assertEqual(self.client.post(f"/api/rooms/{room_id}/messages", json=message, headers=self.auth(member)).status_code, 200)
        reported = self.client.post(
            f"/api/rooms/{room_id}/reports",
            json={"id": "report-1", "channelId": channel_id, "messageId": "slow-1", "reason": "spam"},
            headers=self.auth(owner),
        )
        self.assertEqual(reported.status_code, 200)
        self.assertEqual(
            self.client.get(f"/api/rooms/{room_id}/reports", headers=self.auth(owner)).json()["reports"][0]["message_id"],
            "slow-1",
        )
        message["message"] = {**message["message"], "id": "slow-2", "text": "second"}
        self.assertEqual(self.client.post(f"/api/rooms/{room_id}/messages", json=message, headers=self.auth(member)).status_code, 429)

        timed_out = self.client.post(
            f"/api/rooms/{room_id}/moderation/timeout",
            json={"target_peer_id": member["peer_id"], "duration_seconds": 60, "reason": "qa"},
            headers=self.auth(owner),
        )
        self.assertEqual(timed_out.status_code, 200)
        audit = self.client.get(f"/api/rooms/{room_id}/audit-log", headers=self.auth(owner)).json()["entries"]
        self.assertIn("member_timed_out", [entry["action"] for entry in audit])

    def test_owner_operations_reject_anonymous_and_unprivileged_callers(self):
        owner = self.register("Keeper", "keeper@example.com")
        outsider = self.register("Stranger", "stranger@example.com")
        created = self.create_room(owner)
        room_id = created["room_id"]
        legacy_id = f"{room_id}-legacy"
        legacy = self.server.Room(legacy_id, "Legacy QA", owner["peer_id"], owner["username"])
        legacy.owner_key = None
        self.server.rooms[legacy_id] = legacy

        for target in (room_id, legacy_id):
            self.assertEqual(
                self.client.post(f"/api/rooms/{target}/channels", json={"name": "pwned"}).status_code,
                403,
            )
            self.assertEqual(
                self.client.post(
                    f"/api/rooms/{target}/channels",
                    json={"name": "pwned"},
                    headers=self.auth(outsider),
                ).status_code,
                403,
            )
            self.assertEqual(
                self.client.delete(f"/api/rooms/{target}?owner_id={owner['peer_id']}").status_code,
                401,
            )
            outsider_delete = self.client.delete(
                f"/api/rooms/{target}?owner_id={owner['peer_id']}",
                headers=self.auth(outsider),
            )
            self.assertNotIn("success", outsider_delete.json())
            self.assertIn(target, self.server.rooms)

        self.assertEqual(
            self.client.post(
                f"/api/rooms/{legacy_id}/channels",
                json={"name": "allowed"},
                headers=self.auth(owner),
            ).status_code,
            200,
        )
        self.assertEqual(
            self.client.post(
                f"/api/rooms/{room_id}/channels",
                json={"name": "allowed", "owner_key": created["owner_key"]},
            ).status_code,
            200,
        )


if __name__ == "__main__":
    unittest.main()
