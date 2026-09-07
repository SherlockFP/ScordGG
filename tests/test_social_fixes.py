"""Regression tests for the social/membership/DM fix pack.

Covers: consent-based friending (by-tag/confirm no longer force-add),
server-side leave, tombstone visibility in sync, restore membership gate,
/api/me/rooms recovery, password rotation + logout-all, and durable DMs.
"""
import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]


class SocialFixesTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self._saved_env = {}
        for key in ("SCORD_DATA_DIR", "SCORD_SUPABASE_URL", "SUPABASE_URL",
                    "SCORD_SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
            self._saved_env[key] = os.environ.pop(key, None)
        os.environ["SCORD_DATA_DIR"] = self.temp_dir.name
        spec = importlib.util.spec_from_file_location(
            f"scord_social_fixes_{id(self)}", ROOT / "static" / "server.py"
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
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def register(self, name, email, password="pw12345"):
        response = self.client.post(
            "/api/auth/register",
            json={"username": name, "email": email, "password": password},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        return data

    @staticmethod
    def auth(account):
        return {"Authorization": f"Bearer {account['token']}"}

    def friends_of(self, account):
        return self.client.get("/api/friends", headers=self.auth(account)).json()

    def test_by_tag_creates_request_not_friendship(self):
        asker = self.register("Asker", "asker@example.com")
        hedef = self.register("Hedef", "hedef@example.com")
        tag = f"{hedef['username']}#{hedef['discriminator']}"
        response = self.client.post(
            "/api/friends/by-tag", json={"identifier": tag}, headers=self.auth(asker)
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertNotIn("friend", data)
        self.assertEqual(data["request"]["status"], "pending")
        # Neither side is friends yet; the target sees an incoming request.
        self.assertEqual(self.friends_of(asker)["friends"], [])
        self.assertEqual(self.friends_of(hedef)["friends"], [])
        incoming = self.friends_of(hedef)["incoming_requests"]
        self.assertEqual(incoming[0]["peer_id"], asker["peer_id"])

    def test_reverse_request_auto_accepts(self):
        first = self.register("Birinci", "birinci@example.com")
        second = self.register("Ikinci", "ikinci@example.com")
        tag = f"{second['username']}#{second['discriminator']}"
        self.client.post("/api/friends/by-tag", json={"identifier": tag}, headers=self.auth(first))
        accepted = self.client.post(
            "/api/friends/requests",
            json={"target_peer_id": first["peer_id"]},
            headers=self.auth(second),
        ).json()
        self.assertTrue(accepted.get("accepted"))
        self.assertEqual(self.friends_of(first)["friends"][0]["peer_id"], second["peer_id"])

    def test_confirm_without_pending_creates_request(self):
        asker = self.register("Onayci", "onayci@example.com")
        hedef = self.register("Onaylanan", "onaylanan@example.com")
        data = self.client.post(
            "/api/friends/confirm",
            json={"target_peer_id": hedef["peer_id"]},
            headers=self.auth(asker),
        ).json()
        self.assertTrue(data["success"])
        self.assertNotIn("friend", data)
        self.assertEqual(self.friends_of(asker)["friends"], [])

    def test_blocked_by_tag_is_rejected(self):
        asker = self.register("Isteyen", "isteyen@example.com")
        hedef = self.register("Engelleyen", "engelleyen@example.com")
        self.client.post(f"/api/friends/{asker['peer_id']}/block", json={}, headers=self.auth(hedef))
        response = self.client.post(
            "/api/friends/by-tag",
            json={"identifier": f"{hedef['username']}#{hedef['discriminator']}"},
            headers=self.auth(asker),
        )
        self.assertEqual(response.status_code, 403)

    def test_leave_removes_membership(self):
        owner = self.register("EvSahibi", "ev@example.com")
        member = self.register("Uye", "uye@example.com")
        created = self.client.post("/api/rooms", json={"name": "Oda"}, headers=self.auth(owner)).json()
        room_id = created["room_id"]
        invite = self.client.post(
            f"/api/rooms/{room_id}/invites", json={}, headers=self.auth(owner)
        ).json()["invite"]["invite_code"]
        self.assertEqual(self.client.get(f"/api/rooms/join/{invite}", headers=self.auth(member)).status_code, 200)
        self.assertEqual(
            self.client.delete(f"/api/rooms/{room_id}/members/me", headers=self.auth(member)).status_code,
            200,
        )
        # No longer authorized, no longer synced as active.
        self.assertEqual(
            self.client.get(f"/api/rooms/{room_id}", headers=self.auth(member)).status_code, 403
        )
        synced = self.client.post(
            "/api/rooms/sync", json={"room_ids": [room_id]}, headers=self.auth(member)
        ).json()
        self.assertEqual(synced["active"], [])
        # Owner cannot abandon the server via leave.
        denied = self.client.delete(f"/api/rooms/{room_id}/members/me", headers=self.auth(owner))
        self.assertEqual(denied.status_code, 400)

    def test_deleted_rooms_visible_to_everyone_in_sync(self):
        owner = self.register("Silici", "silici@example.com")
        member = self.register("Kurban", "kurban@example.com")
        created = self.client.post("/api/rooms", json={"name": "Oda"}, headers=self.auth(owner)).json()
        room_id, owner_key = created["room_id"], created["owner_key"]
        invite = self.client.post(
            f"/api/rooms/{room_id}/invites", json={}, headers=self.auth(owner)
        ).json()["invite"]["invite_code"]
        self.client.get(f"/api/rooms/join/{invite}", headers=self.auth(member))
        self.client.delete(
            f"/api/rooms/{room_id}",
            params={"owner_id": owner["peer_id"], "owner_key": owner_key},
            headers=self.auth(owner),
        )
        synced = self.client.post(
            "/api/rooms/sync", json={"room_ids": [room_id]}, headers=self.auth(member)
        ).json()
        self.assertEqual(synced["deleted"], [room_id])
        self.assertEqual(synced["unknown"], [])

    def test_restore_requires_membership(self):
        owner = self.register("Restorecu", "restorecu@example.com")
        outsider = self.register("Yabanci", "yabanci@example.com")
        created = self.client.post("/api/rooms", json={"name": "Oda"}, headers=self.auth(owner)).json()
        room_id = created["room_id"]
        # Keyed rooms reject outsiders with a machine-readable error so
        # clients purge the ghost copy instead of retrying forever.
        keyed = self.client.post(
            f"/api/rooms/{room_id}/restore",
            json={"token": outsider["token"], "name": "Oda"},
            headers=self.auth(outsider),
        ).json()
        self.assertEqual(keyed.get("error"), "forbidden")
        self.assertNotIn("room", keyed)
        # Legacy rooms (no owner_key) must not leak history to non-members.
        # The hardened owner gate rejects them with 403 before any state is
        # returned; either way no room payload may leak.
        self.server.rooms[room_id].owner_key = None
        denied = self.client.post(
            f"/api/rooms/{room_id}/restore",
            json={"token": outsider["token"], "name": "Oda"},
            headers=self.auth(outsider),
        ).json()
        self.assertEqual(denied.get("error"), "forbidden")
        self.assertNotIn("room", denied)
        self.assertNotIn("messages", denied)

    def test_my_rooms_lists_memberships(self):
        owner = self.register("Listeci", "listeci@example.com")
        member = self.register("Katilimci", "katilimci@example.com")
        stranger = self.register("Eltrafigi", "el@example.com")
        created = self.client.post("/api/rooms", json={"name": "Ozel"}, headers=self.auth(owner)).json()
        room_id = created["room_id"]
        invite = self.client.post(
            f"/api/rooms/{room_id}/invites", json={}, headers=self.auth(owner)
        ).json()["invite"]["invite_code"]
        self.client.get(f"/api/rooms/join/{invite}", headers=self.auth(member))
        mine = self.client.get("/api/me/rooms", headers=self.auth(member)).json()["rooms"]
        self.assertEqual([room["room_id"] for room in mine], [room_id])
        self.assertEqual(self.client.get("/api/me/rooms", headers=self.auth(stranger)).json()["rooms"], [])
        self.assertEqual(self.client.get("/api/me/rooms").status_code, 401)

    def test_password_rotation_and_logout_all(self):
        account = self.register("Sifreci", "sifreci@example.com", password="eski-sifre")
        second_login = self.client.post(
            "/api/auth/login", json={"username": "Sifreci", "password": "eski-sifre"}
        ).json()
        changed = self.client.post(
            "/api/account/change-password",
            json={"token": account["token"], "current_password": "eski-sifre", "new_password": "yeni-sifre"},
        ).json()
        self.assertTrue(changed["success"])
        # Old password dead, new password works, other session killed.
        bad = self.client.post("/api/auth/login", json={"username": "Sifreci", "password": "eski-sifre"}).json()
        self.assertEqual(bad.get("error"), "wrong_password")
        good = self.client.post("/api/auth/login", json={"username": "Sifreci", "password": "yeni-sifre"}).json()
        self.assertTrue(good.get("success"))
        stale = self.client.get("/api/account/me", headers={"Authorization": f"Bearer {second_login['token']}"}).json()
        self.assertEqual(stale.get("error"), "unauthorized")
        # logout-all keeps the caller alive but kills every other session.
        third_login = self.client.post(
            "/api/auth/login", json={"username": "Sifreci", "password": "yeni-sifre"}
        ).json()
        self.client.post("/api/auth/logout-all", json={"token": good["token"]})
        alive = self.client.get("/api/account/me", headers={"Authorization": f"Bearer {good['token']}"}).json()
        self.assertTrue(alive.get("success"))
        gone = self.client.get("/api/account/me", headers={"Authorization": f"Bearer {third_login['token']}"}).json()
        self.assertEqual(gone.get("error"), "unauthorized")

    def test_legacy_rooms_require_owner_for_mutation(self):
        owner = self.register("Legacyci", "legacyci@example.com")
        member = self.register("LegacyUye", "legacyuye@example.com")
        created = self.client.post("/api/rooms", json={"name": "Oda"}, headers=self.auth(owner)).json()
        room_id = created["room_id"]
        invite = self.client.post(
            f"/api/rooms/{room_id}/invites", json={}, headers=self.auth(owner)
        ).json()["invite"]["invite_code"]
        self.client.get(f"/api/rooms/join/{invite}", headers=self.auth(member))
        # Simulate a pre-owner_key room: previously anyone (even anonymous)
        # could inject channels/roles into such rooms.
        self.server.rooms[room_id].owner_key = None
        anon = self.client.post(f"/api/rooms/{room_id}/channels", json={"name": "x", "type": "text"})
        self.assertEqual(anon.status_code, 403)
        stranger = self.client.post(
            f"/api/rooms/{room_id}/channels", json={"name": "x", "type": "text"}, headers=self.auth(member)
        )
        self.assertEqual(stranger.status_code, 403)
        allowed = self.client.post(
            f"/api/rooms/{room_id}/channels", json={"name": "duyuru", "type": "text"}, headers=self.auth(owner)
        )
        self.assertEqual(allowed.status_code, 200)

    def test_message_text_is_capped(self):
        owner = self.register("Kisaltmaci", "kisaltmaci@example.com")
        created = self.client.post("/api/rooms", json={"name": "Oda"}, headers=self.auth(owner)).json()
        room_id = created["room_id"]
        channel_id = self.server.rooms[room_id].channels[0]["id"]
        response = self.client.post(
            f"/api/rooms/{room_id}/messages",
            json={"message": {"id": "big-1", "channelId": channel_id, "authorId": owner["peer_id"], "text": "x" * 10000}},
            headers=self.auth(owner),
        )
        self.assertEqual(response.status_code, 200)
        stored = self.server.rooms[room_id].messages[channel_id][-1]
        self.assertEqual(len(stored["text"]), 4000)

    def test_sessions_are_capped(self):
        account = self.register("Oturumcu", "oturumcu@example.com", password="sifre123")
        tokens = [account["token"]]
        for _ in range(25):
            logged = self.client.post(
                "/api/auth/login", json={"username": "Oturumcu", "password": "sifre123"}
            ).json()
            tokens.append(logged["token"])
        # Oldest sessions are pruned; the newest one still works.
        oldest = self.client.get(
            "/api/account/me", headers={"Authorization": f"Bearer {tokens[0]}"}
        ).json()
        self.assertEqual(oldest.get("error"), "unauthorized")
        newest = self.client.get(
            "/api/account/me", headers={"Authorization": f"Bearer {tokens[-1]}"}
        ).json()
        self.assertTrue(newest.get("success"))

    def test_room_creation_is_throttled(self):
        owner = self.register("OdaSpamci", "odaspamci@example.com")
        codes = []
        for _ in range(11):
            response = self.client.post("/api/rooms", json={"name": "Oda"}, headers=self.auth(owner))
            codes.append(response.status_code)
        self.assertEqual(codes[:10], [200] * 10)
        self.assertEqual(codes[10], 429)

    def test_friend_request_outbox_is_capped(self):
        asker = self.register("Istekci", "istekci@example.com")
        targets = []
        for index in range(31):
            # TestClient shares one IP, so reset the unrelated login/register
            # bucket; this test targets the pending-outbox cap, not that one.
            if index % 8 == 0:
                self.server._login_attempts.clear()
            targets.append(self.register(f"Hedef{index}", f"hedef{index}@example.com"))
        for index, target in enumerate(targets):
            response = self.client.post(
                "/api/friends/requests",
                json={"target_peer_id": target["peer_id"]},
                headers=self.auth(asker),
            )
            if index < 30:
                self.assertEqual(response.status_code, 200)
            else:
                self.assertEqual(response.status_code, 429)

    def test_dm_sending_is_throttled(self):
        first = self.register("DmSpamBir", "dmspir@example.com")
        second = self.register("DmSpamIki", "dmspiki@example.com")
        codes = []
        for index in range(31):
            response = self.client.post(
                f"/api/dm/{second['peer_id']}/messages",
                json={"message": {"id": f"s-{index}", "text": "selam"}},
                headers=self.auth(first),
            )
            codes.append(response.status_code)
        self.assertEqual(codes[:30], [200] * 30)
        self.assertEqual(codes[30], 429)

    def test_durable_dm_roundtrip(self):
        first = self.register("DmBir", "dmbir@example.com")
        second = self.register("DmIki", "dmiki@example.com")
        stored = self.client.post(
            f"/api/dm/{second['peer_id']}/messages",
            json={"message": {"id": "m-1", "text": "selam", "timestamp": 1700000000000}},
            headers=self.auth(first),
        ).json()
        self.assertTrue(stored["success"])
        self.assertEqual(stored["message"]["id"], "m-1")
        history = self.client.get(
            f"/api/dm/{first['peer_id']}/messages", headers=self.auth(second)
        ).json()["messages"]
        self.assertEqual([message["text"] for message in history], ["selam"])
        # Empty and blocked sends are rejected.
        empty = self.client.post(
            f"/api/dm/{second['peer_id']}/messages",
            json={"message": {"id": "m-2", "text": "   "}},
            headers=self.auth(first),
        )
        self.assertEqual(empty.status_code, 400)
        self.client.post(f"/api/friends/{first['peer_id']}/block", json={}, headers=self.auth(second))
        blocked = self.client.post(
            f"/api/dm/{second['peer_id']}/messages",
            json={"message": {"id": "m-3", "text": "spam"}},
            headers=self.auth(first),
        )
        self.assertEqual(blocked.status_code, 403)


if __name__ == "__main__":
    unittest.main()
