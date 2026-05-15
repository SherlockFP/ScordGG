/**
 * p2p.js — WebRTC P2P Mesh Engine for SCORD
 * ===============================================
 * Manages the full-mesh topology: every peer connects to every other peer
 * via RTCPeerConnection. Text chat goes over RTCDataChannel.
 * Voice goes over audio MediaStreamTracks added to each connection.
 */

"use strict";

function _scordTiming() {
    return typeof window !== "undefined" && window.SCORD_TIMING ? window.SCORD_TIMING : {};
}

function _scordIceServers() {
    if (typeof window !== "undefined" && Array.isArray(window.SCORD_ICE_SERVERS) && window.SCORD_ICE_SERVERS.length) {
        return window.SCORD_ICE_SERVERS;
    }
    return [
        // Default: STUN only (TURN must be configured server-side via /api/config).
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun1.l.google.com:19302" },
    ];
}

class P2PMesh {
    /**
     * @param {string} roomId
     * @param {string} peerId
     * @param {string} signalingUrl  - ws:// URL to signaling server
     * @param {object} callbacks
     *   .onMessage(fromPeerId, data)
     *   .onPeerJoined(peerId, info)
     *   .onPeerLeft(peerId)
     *   .onVoiceStream(peerId, stream)
     *   .onTrackAdded(peerId, track, stream)
     *   .onPeerConnected(peerId)
     *   .onStatusChange(status)
     */
    constructor(roomId, peerId, signalingUrl, callbacks = {}) {
        this.roomId = roomId;
        this.peerId = peerId;
        this.signalingUrl = signalingUrl;
        this.cb = callbacks;

        this.ws = null;                 // Signaling WebSocket
        this.peers = {};                // peerId → { pc, dc, info }
        this.localStream = null;        // MediaStream for voice
        this.screenStream = null;       // MediaStream for screen
        this.voiceActive = false;
        this.micMuted = false;

        this._pendingIce = {};           // peerId → [candidate, ...]
        this._reconnectTimer = null;
        this._dead = false;
        this.cameraStream = null;
    }

    /* ── Connect to signaling server ─────────────────────────── */
    connect(username, avatarColor, avatarImage = null) {
        this._dead = false;
        this.username = username;
        this.avatarColor = avatarColor;
        this.avatarImage = avatarImage;
        const url = `${this.signalingUrl}/${this.roomId}/${this.peerId}?username=${encodeURIComponent(username)}&color=${encodeURIComponent(avatarColor)}`;
        this._setStatus("connecting");
        console.log("[P2P] Signaling URL:", url);
        this.ws = new WebSocket(url);

        this.ws.onopen = () => {
            console.log("[P2P] Signaling connected");
            this._setStatus("connected");
            this._startPing();
        };

        this.ws.onmessage = async (ev) => {
            const msg = JSON.parse(ev.data);
            await this._handleSignal(msg);
        };

        this.ws.onclose = (ev) => {
            if (this._dead) return;
            console.warn("[P2P] Signaling disconnected", { code: ev?.code, reason: ev?.reason, wasClean: ev?.wasClean });
            this._setStatus("disconnected");
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = setTimeout(
                () => this.connect(this.username, this.avatarColor, this.avatarImage),
                _scordTiming().P2P_WS_RECONNECT_MS ?? 3000
            );
        };

        this.ws.onerror = (e) => {
            console.error("[P2P] WS error:", e);
            this._setStatus("ws_error");
        };
    }

    disconnect() {
        this._dead = true;
        clearTimeout(this._reconnectTimer);
        this._pingTimer && clearInterval(this._pingTimer);
        this._reconnectTimer = null;

        // Close all peer connections & data channels
        Object.keys(this.peers).forEach(pid => this._closePeer(pid));
        this.peers = {};
        this._pendingIce = {};

        // Close signaling socket
        if (this.ws) {
            try { this.ws.onmessage = null; } catch { }
            try { this.ws.close(); } catch { }
        }
        this.ws = null;
    }

    /* ── Signaling message dispatcher ────────────────────────── */
    async _handleSignal(msg) {
        switch (msg.type) {
            case "room_state":
                this.cb.onServerEvent?.(msg);
                // We got here — now initiate connections to existing peers
                for (const peer of msg.room.peers) {
                    if (peer.peer_id !== this.peerId) {
                        // V11 Fix: Trigger join callback for existing peers so UI populates early
                        this.cb.onPeerJoined?.(peer.peer_id, {
                            username: peer.username,
                            avatar_color: peer.avatar_color,
                            avatar_image: peer.avatar_image
                        });
                        await this._initiatePeer(peer.peer_id, peer, true);
                    }
                }
                break;

            case "peer_joined":
                // New peer: we are the polite peer, wait for their offer
                if (msg.peer_id !== this.peerId) {
                    this.cb.onPeerJoined?.(msg.peer_id, { username: msg.username, avatar_color: msg.avatar_color });
                    await this._initiatePeer(msg.peer_id, msg, false);
                }
                break;

            case "peer_left":
                this.cb.onPeerLeft?.(msg.peer_id);
                this._closePeer(msg.peer_id);
                break;

            case "offer":
                await this._handleOffer(msg.from, msg.sdp);
                break;

            case "answer":
                await this._handleAnswer(msg.from, msg.sdp);
                break;

            case "ice_candidate":
                await this._handleIce(msg.from, msg.candidate);
                break;

            case "broadcast":
                this.cb.onMessage?.(msg.from, msg.data);
                break;

            case "voice_state_snapshot":
            case "voice_state":
            case "media_status":
            case "music_state":
            case "permission_denied":
            case "permission_update":
            case "role_update":
            case "force_disconnect":
            case "dm_call_offer":
            case "dm_call_answer":
            case "dm_call_end":
                this.cb.onServerEvent?.(msg);
                break;

            case "error":
                console.error("[P2P] Server error:", msg.message);
                if (String(msg.message || "").toLowerCase().includes("room not found")) {
                    this.cb.onStatusChange?.("room_not_found");
                } else {
                    this.cb.onStatusChange?.("server_error");
                }
                break;
        }
    }

    /* ── Create a peer connection ─────────────────────────────── */
    async _initiatePeer(peerId, info, makeOffer) {
        if (this.peers[peerId]) return; // already connected

        const pc = new RTCPeerConnection({ iceServers: _scordIceServers() });
        const peerObj = { pc, dc: null, info };
        this.peers[peerId] = peerObj;
        this._pendingIce[peerId] = [];

        // Add local audio if voice is active
        if (this.localStream) {
            this.localStream.getTracks().forEach(t => pc.addTrack(t, this.localStream));
        }

        // Add local screen stream if active
        if (this.screenStream) {
            this.screenStream.getTracks().forEach(t => pc.addTrack(t, this.screenStream));
        }

        // Add local camera stream if active
        if (this.cameraStream) {
            this.cameraStream.getTracks().forEach(t => pc.addTrack(t, this.cameraStream));
        }

        // If we are the offerer and currently have no local media, still open
        // receive lanes. Otherwise a new user who joins a room while someone is
        // already in voice can create a datachannel-only offer and never receive
        // the existing user's microphone/camera tracks in the answer.
        if (makeOffer && !this.localStream) {
            try { pc.addTransceiver("audio", { direction: "recvonly" }); } catch { }
        }
        if (makeOffer && !this.screenStream && !this.cameraStream) {
            try { pc.addTransceiver("video", { direction: "recvonly" }); } catch { }
            try { pc.addTransceiver("video", { direction: "recvonly" }); } catch { }
        }

        // Receive remote tracks (Voice + Screen)
        pc.ontrack = (ev) => {
            if (!this.remoteStreams) this.remoteStreams = {};
            if (!this.remoteStreams[peerId]) {
                this.remoteStreams[peerId] = new MediaStream();
                this.cb.onVoiceStream?.(peerId, this.remoteStreams[peerId]);
            }
            this.remoteStreams[peerId].addTrack(ev.track);
            this.cb.onTrackAdded?.(peerId, ev.track, this.remoteStreams[peerId]);
        };

        pc.onicecandidate = (ev) => {
            if (ev.candidate) {
                this._send({ type: "ice_candidate", target: peerId, candidate: ev.candidate.toJSON() });
            }
        };

        // Negotiation (glare-safe)
        // Deterministic “polite” side based on peerId ordering.
        // This prevents both sides from creating offers at the same time.
        peerObj._makingOffer = false;
        peerObj._polite = String(this.peerId) > String(peerId);
        peerObj._negTimer = null;

        pc.onnegotiationneeded = () => {
            clearTimeout(peerObj._negTimer);
            peerObj._negTimer = setTimeout(async () => {
                try {
                    if (pc.connectionState === "closed") return;
                    if (pc.signalingState !== "stable") {
                        // Don't create offers when not stable; glare is handled in offer path.
                        return;
                    }

                    if (peerObj._makingOffer) return;
                    peerObj._makingOffer = true;

                    const offer = await pc.createOffer({
                        offerToReceiveAudio: true,
                        offerToReceiveVideo: true,
                    });
                    await pc.setLocalDescription(offer);

                    this._send({ type: "offer", target: peerId, sdp: pc.localDescription });
                } catch (err) {
                    console.error("[P2P] Renegotiation error:", err);
                } finally {
                    peerObj._makingOffer = false;
                }
            }, _scordTiming().P2P_NEGOTIATION_DEBOUNCE_MS ?? 150);
        };

        pc.onconnectionstatechange = () => {
            console.log(`[P2P] ${peerId} → ${pc.connectionState}`);
        };

        pc.oniceconnectionstatechange = () => {
            console.log(`[P2P] ${peerId} ice → ${pc.iceConnectionState}`);
            if (pc.iceConnectionState === "failed") {
                // Common quick recovery: trigger ICE restart by renegotiation
                try { pc.restartIce?.(); } catch { }
            }
        };

        if (makeOffer) {
            // Setup data channel as offerer
            const dc = pc.createDataChannel("chat", { ordered: true });
            this._wireDataChannel(dc, peerId);
            peerObj.dc = dc;

            const offer = await pc.createOffer({
                offerToReceiveAudio: true,
                offerToReceiveVideo: true,
            });
            await pc.setLocalDescription(offer);
            this._send({ type: "offer", target: peerId, sdp: pc.localDescription });
        } else {
            // Answerer: wait for data channel
            pc.ondatachannel = (ev) => {
                this._wireDataChannel(ev.channel, peerId);
                peerObj.dc = ev.channel;
            };
        }
    }

    _wireDataChannel(dc, peerId) {
        dc.onopen = () => {
            console.log(`[P2P] DataChannel open with ${peerId}`);
            this.cb.onPeerConnected?.(peerId);

            dc.send(JSON.stringify({
                type: "identity_announce",
                peerId: this.peerId,
                username: this.username || "Anonim",
                avatarColor: this.avatarColor || "#7c3aed",
                avatarImage: this.avatarImage || null
            }));
        };
        dc.onmessage = (ev) => {
            try {
                const data = JSON.parse(ev.data);
                this.cb.onMessage?.(peerId, data);
            } catch { /* ignore malformed */ }
        };
        dc.onerror = (e) => console.warn(`[P2P] DC error with ${peerId}:`, e);
        dc.onclose = () => {
            console.log(`[P2P] DC closed with ${peerId}`);
            this.cb.onStatusChange?.("p2p_dc_closed");
        };
    }

    async _handleOffer(fromId, sdp) {
        let peerObj = this.peers[fromId];
        if (!peerObj) {
            // Ensure we have a peer entry for answerer
            await this._initiatePeer(fromId, {}, false);
            peerObj = this.peers[fromId];
        }

        const { pc } = peerObj;

        // Glare handling (perfect negotiation style)
        // If we are making an offer and we are NOT polite, ignore this offer.
        // If we are polite, roll over by setting remote description.
        const offerCollision = peerObj._makingOffer || pc.signalingState !== "stable";
        if (offerCollision && !peerObj._polite) {
            console.warn(`[P2P] Offer glare ignored (from ${fromId})`);
            return;
        }

        await pc.setRemoteDescription(new RTCSessionDescription(sdp));


        // Flush pending ICE
        for (const c of (this._pendingIce[fromId] || [])) {
            await pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => { });
        }
        this._pendingIce[fromId] = [];

        const answer = await pc.createAnswer({
            offerToReceiveAudio: true,
            offerToReceiveVideo: true,
        });
        await pc.setLocalDescription(answer);
        this._send({ type: "answer", target: fromId, sdp: pc.localDescription });
    }

    async _handleAnswer(fromId, sdp) {
        const peerObj = this.peers[fromId];
        if (!peerObj) return;
        await peerObj.pc.setRemoteDescription(new RTCSessionDescription(sdp));

        for (const c of (this._pendingIce[fromId] || [])) {
            await peerObj.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => { });
        }
        this._pendingIce[fromId] = [];
    }

    async _handleIce(fromId, candidate) {
        const peerObj = this.peers[fromId];
        if (!peerObj) return;
        const { pc } = peerObj;
        if (pc.remoteDescription) {
            await pc.addIceCandidate(new RTCIceCandidate(candidate)).catch(() => { });
        } else {
            this._pendingIce[fromId] = this._pendingIce[fromId] || [];
            this._pendingIce[fromId].push(candidate);
        }
    }

    _closePeer(peerId) {
        const p = this.peers[peerId];
        if (!p) return;
        p.dc && p.dc.close();
        p.pc.close();
        delete this.peers[peerId];
    }

    /* ── Send text message over DataChannels ──────────────────── */
    broadcast(data) {
        const raw = JSON.stringify(data);
        const maxBuf = _scordTiming().P2P_DC_MAX_BUFFERED_BYTES ?? 262144;
        for (const [, peerObj] of Object.entries(this.peers)) {
            const dc = peerObj.dc;
            if (!dc || dc.readyState !== "open") continue;
            if (dc.bufferedAmount > maxBuf) {
                console.warn("[P2P] DC buffer yüksek, gönderim atlandı:", peerObj);
                continue;
            }
            try {
                dc.send(raw);
            } catch (e) {
                console.warn("[P2P] broadcast send error:", e);
            }
        }
    }

    /**
     * Fallback broadcast over signaling WebSocket.
     * This is NOT for voice/video streams, only small JSON state (chat, presence, etc.).
     */
    broadcastSignal(data) {
        try {
            this._send({ type: "broadcast", data });
        } catch (e) {
            console.warn("[P2P] broadcastSignal failed:", e);
        }
    }

    sendTo(targetPeerId, data) {
        const peerObj = this.peers[targetPeerId];
        const dc = peerObj?.dc;
        if (!dc || dc.readyState !== "open") return;
        const maxBuf = _scordTiming().P2P_DC_MAX_BUFFERED_BYTES ?? 262144;
        if (dc.bufferedAmount > maxBuf) {
            console.warn("[P2P] sendTo buffer yüksek:", targetPeerId);
            return;
        }
        try {
            dc.send(JSON.stringify(data));
        } catch (e) {
            console.warn("[P2P] sendTo error:", e);
        }
    }

    /* ── Voice ───────────────────────────────────────────────── */
    async startVoice(stream = null) {
        try {
            this.localStream = stream || await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
            this.voiceActive = true;
            for (const [, peerObj] of Object.entries(this.peers)) {
                this.localStream.getTracks().forEach(t => {
                    try { peerObj.pc.addTrack(t, this.localStream); } catch { }
                });
            }
            return true;
        } catch (err) {
            console.error("[P2P] Mic error:", err);
            return false;
        }
    }

    stopVoice() {
        if (this.localStream) {
            this.localStream.getTracks().forEach(t => t.stop());
            this.localStream = null;
        }
        this.voiceActive = false;
    }

    toggleMic() {
        if (!this.localStream) return;
        this.micMuted = !this.micMuted;
        this.localStream.getAudioTracks().forEach(t => { t.enabled = !this.micMuted; });
        return this.micMuted;
    }

    /* ── Screen Sharing ──────────────────────────────────────── */
    async startScreenShare() {
        try {
            this.screenStream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });

            const screenTrack = this.screenStream.getVideoTracks()[0];
            screenTrack.onended = () => {
                this.stopScreenShare();
            };

            // Add track to all existing peers -> triggers onnegotiationneeded automatically
            for (const [, peerObj] of Object.entries(this.peers)) {
                try {
                    peerObj.pc.addTrack(screenTrack, this.screenStream);
                } catch (e) {
                    console.warn("[P2P] Failed to add screen track to peer", e);
                }
            }
            return true;
        } catch (err) {
            console.error("[P2P] Screen capture error:", err);
            return false;
        }
    }

    stopScreenShare() {
        if (!this.screenStream) return;

        const screenTrack = this.screenStream.getVideoTracks()[0];

        // Remove track from all existing peers -> triggers onnegotiationneeded automatically
        for (const [, peerObj] of Object.entries(this.peers)) {
            const senders = peerObj.pc.getSenders();
            const sender = senders.find(s => s.track === screenTrack);
            if (sender) {
                try {
                    peerObj.pc.removeTrack(sender);
                } catch (e) { }
            }
        }

        screenTrack.stop();
        this.screenStream = null;

        // Trigger callback if defined globally in app.js
        if (typeof window.onLocalScreenShareEnded === "function") {
            window.onLocalScreenShareEnded();
        }
    }

    /* ── Helpers ─────────────────────────────────────────────── */
    _send(msg) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(msg));
        }
    }

    sendSignal(data) {
        this._send(data);
    }

    _setStatus(status) {
        this.cb.onStatusChange?.(status);
    }

    _startPing() {
        const pingMs = _scordTiming().P2P_SIGNALING_PING_INTERVAL_MS ?? 25000;
        this._pingTimer = setInterval(() => {
            this._send({ type: "ping" });
        }, pingMs);
    }

    get connectedPeerCount() {
        return Object.keys(this.peers).length;
    }
}

// Export globally
window.P2PMesh = P2PMesh;
