import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class FrontendContractTests(unittest.TestCase):
    def test_setup_is_initialized_once(self):
        source = (ROOT / "app.js").read_text(encoding="utf-8")
        self.assertEqual(source.count("    initSetup();"), 1)

    def test_mesh_identity_and_accessibility_hooks_exist(self):
        html = (ROOT / "index.html").read_text(encoding="utf-8")
        self.assertRegex(html, r'href="static/scord-shell\.css(?:\?[^\"]*)?"')
        self.assertIn('id="mesh-pulse"', html)
        self.assertIn('aria-label="Ana sayfa"', html)
        self.assertIn('role="group" aria-label="Hesap işlemi"', html)
        self.assertIn('id="auth-tab-login" aria-pressed="true"', html)

    def test_shell_tokens_and_mobile_drawer_contract(self):
        css = (ROOT / "static" / "scord-shell.css").read_text(encoding="utf-8")
        self.assertIn("--sidebar-width: 284px", css)
        self.assertIn("body.nav-open #app .server-rail", css)
        self.assertIn("grid-template-columns: minmax(0, 1fr) auto", css)
        self.assertNotIn("display: none !important;\n  }\n\n  #app .channel-sidebar", css)

    def test_design_bible_visual_contract(self):
        html = (ROOT / "index.html").read_text(encoding="utf-8")
        app = (ROOT / "app.js").read_text(encoding="utf-8")
        css = (ROOT / "static" / "scord-design.css").read_text(encoding="utf-8")
        bible = (ROOT / "design-system" / "scord" / "MASTER.md").read_text(encoding="utf-8")
        self.assertRegex(html, r'href="static/scord-design\.css\?v=\d+"')
        self.assertIn('id="member-directory-search"', html)
        self.assertIn("--scord-canvas: #050b18", css)
        self.assertIn("html body #app .message.msg-row.msg-row--self .msg-bubble", css)
        self.assertIn("background: transparent !important", css)
        self.assertIn("@media (max-width: 768px)", css)
        self.assertIn("function setupMemberDirectorySearch", app)
        self.assertIn('.member-status-dot, .member-avatar .status-dot', app)
        self.assertIn("function showServerDiscoveryView", app)
        self.assertIn('addQuickAction("Mesajı sil"', app)
        self.assertIn('id="discover-btn"', html)
        self.assertIn('data-password-target="scord-pass-input"', html)
        self.assertIn(".scord-discovery-grid", css)
        self.assertIn("Message backgrounds are transparent", bible)

    def test_message_action_toolbar_has_one_non_overlapping_owner(self):
        html = (ROOT / "index.html").read_text(encoding="utf-8")
        app = (ROOT / "app.js").read_text(encoding="utf-8")
        css = (ROOT / "static" / "scord-design.css").read_text(encoding="utf-8")
        self.assertIn("legacy loop duplicates it", app)
        self.assertIn(".msg-row-inner { padding-right: 156px; }", css)
        self.assertNotIn("top: -14px", css)
        self.assertIn('src="app.js?v=14"', html)

    def test_legacy_identity_store_is_migrated(self):
        source = (ROOT / "app.js").read_text(encoding="utf-8")
        self.assertIn("function migrateLegacyIdentityStore", source)
        self.assertIn("migrateLegacyIdentityStore(nick, pass, data.peer_id);", source)

    def test_auth_bound_transport_contract(self):
        app = (ROOT / "app.js").read_text(encoding="utf-8")
        p2p = (ROOT / "static" / "p2p.js").read_text(encoding="utf-8")
        server = (ROOT / "static" / "server.py").read_text(encoding="utf-8")
        self.assertIn('options.headers.Authorization = `Bearer ${token}`', app)
        self.assertIn('token=${encodeURIComponent(token)}', p2p)
        self.assertIn('account["peer_id"] != peer_id', server)
        self.assertIn("function isSuperAdmin() {\n    return state.isPlatformAdmin === true;", app)
        self.assertNotIn('localStorage.setItem("scord_pass", pass)', app)

    def test_persistent_accounts_friends_and_platform_admin_contract(self):
        server = (ROOT / "static" / "server.py").read_text(encoding="utf-8")
        app = (ROOT / "app.js").read_text(encoding="utf-8")
        self.assertIn("platform_admin", server)
        self.assertIn("CREATE TABLE IF NOT EXISTS friendships", server)
        self.assertIn('@app.get("/api/friends")', server)
        self.assertIn("administrator_snapshot", server)
        self.assertIn("loadPersistentFriends", app)
        self.assertIn("/friends/confirm", app)
        self.assertIn("CREATE TABLE IF NOT EXISTS servers", server)
        self.assertIn("CREATE TABLE IF NOT EXISTS server_members", server)
        self.assertIn("to_discovery_dict", server)

    def test_email_tag_message_lifecycle_and_supabase_contract(self):
        html = (ROOT / "index.html").read_text(encoding="utf-8")
        app = (ROOT / "app.js").read_text(encoding="utf-8")
        server = (ROOT / "static" / "server.py").read_text(encoding="utf-8")
        supabase = (ROOT / "static" / "supabase_store.py").read_text(encoding="utf-8")
        self.assertIn('id="scord-email-input"', html)
        self.assertIn('id="auth-email-group"', html)
        self.assertIn("_allocate_discriminator", server)
        self.assertIn('@app.post("/api/friends/by-tag")', server)
        self.assertIn('@app.patch("/api/rooms/{room_id}/messages/{message_id}")', server)
        self.assertIn('className = "msg-edited-badge"', app)
        self.assertIn('className = "msg-tombstone"', app)
        self.assertNotIn("window.deleteChatMessage = deleteChatMessage", app)
        self.assertIn("SCORD_SUPABASE_SERVICE_ROLE_KEY", supabase)
        self.assertIn("scord_state_snapshots", supabase)

    def test_layout_scroll_owners_and_voice_card_contract(self):
        html = (ROOT / "index.html").read_text(encoding="utf-8")
        css = (ROOT / "static" / "scord-design.css").read_text(encoding="utf-8")
        sidebar_start = html.index('<aside class="channel-sidebar"')
        sidebar_end = html.index("</aside>", sidebar_start)
        voice_index = html.index('id="voice-status-bar"')
        self.assertGreater(voice_index, sidebar_start)
        self.assertLess(voice_index, sidebar_end)
        self.assertIn(".home-view.home-discovery-active .scord-discovery-grid", css)
        self.assertIn("overflow-y: auto", css)
        self.assertIn(".gif-results", css)
        self.assertIn(".server-quick-stats", css)


class ServerTemplateTests(unittest.TestCase):
    def test_default_room_labels_are_valid_utf8(self):
        source = (ROOT / "static" / "server.py").read_text(encoding="utf-8")
        self.assertIn('{"id": "ch-muzik", "name": "müzik", "type": "voice"}', source)
        self.assertIn('"member": {"name": "Üye"', source)
        self.assertNotIn('"name": "m├╝zik"', source)
        self.assertNotIn('"name": "├£ye"', source)


if __name__ == "__main__":
    unittest.main()
