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
        this.authToken = "";
    }

    /* ── Connect to signaling server ─────────────────────────── */
    connect(username, avatarColor, avatarImage = null, authToken = "") {
        this._dead = false;
        this.username = username;
        this.avatarColor = avatarColor;
        this.avatarImage = avatarImage;
        this.authToken = authToken || this.authToken || localStorage.getItem("scord_token") || "";
        const token = this.authToken;
        const url = `${this.signalingUrl}/${this.roomId}/${this.peerId}?token=${encodeURIComponent(token)}&username=${encodeURIComponent(username)}&color=${encodeURIComponent(avatarColor)}`;
        this._setStatus("connecting");
        console.log("[P2P] Connecting signaling", {
            roomId: this.roomId,
            peerId: this.peerId,
            tokenPresent: Boolean(token),
        });
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
                () => this.connect(this.username, this.avatarColor, this.avatarImage, this.authToken),
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

        // Stop local capture streams — otherwise mic/screen/camera keep running
        // after disconnect. Existing helpers stop tracks + null the refs.
        this.stopVoice();
        this.stopScreenShare();
        if (this.cameraStream) {
            this.cameraStream.getTracks().forEach(t => t.stop());
            this.cameraStream = null;
        }

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

            case "dm":
                // Sunucu üzerinden hedefli DM relay'i (dm_relay → sadece bize gelir).
                // Bu case yokken WS-fallback DM'ler sessizce düşüyordu.
                this.cb.onMessage?.(msg.from, msg);
                break;

            case "room_deleted":
                // Sunucu tarafında silindi: yeniden bağlanma döngüsüne girmeden
                // app.js'e haber ver, sonra kendimizi kapat.
                this.cb.onServerEvent?.(msg);
                this._terminal = true;
                this.disconnect();
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
                    // Sunucu bu odayı tanımıyor (silinmiş/redeploy sonrası kaybolmuş).
                    // Sonsuz 3sn reconnect döngüsüne girmeyi burada kes; app.js
                    // reconcile akışıyla karar versin (restore dener ya da temizler).
                    this._terminal = true;
                    this.cb.onStatusChange?.("room_not_found");
                    this.disconnect();
                } else {
                    this.cb.onStatusChange?.("server_error");
                }
                break;
        }
    }

    /* ── Create a peer connection ─────────────────────────────── */
    /**
     * Opus SDP ayarı: tarayıcı varsayılanı ~32kbps mono ve FEC kapalı gelir.
     * Burada bitrate/FEC/DTX açıkça istenerek kötü internet'te bile net ses
     * hedeflenir: useinbandfec=1 paket kaybına karşı hata düzeltir, usedtx=1
     * sessizlikte bant genişliği harcamaz (paket optimizasyonu).
     */
    _tuneOpusSdp(sdp) {
        try {
            const lines = sdp.split("\r\n");
            const rtpmapIdx = lines.findIndex(l => /^a=rtpmap:\d+ opus\/48000/i.test(l));
            if (rtpmapIdx === -1) return sdp;
            const pt = lines[rtpmapIdx].match(/^a=rtpmap:(\d+)/)[1];
            // maxaveragebitrate: kullanıcı ayarlarından (varsa) okunur; yoksa 64kbps.
            let abr = 64000;
            try {
                const vs = (typeof window !== "undefined" && window.state && window.state.voiceSettings) || {};
                abr = parseInt(vs.audioBitrate, 10) || 64000;
                if (abr < 24000) abr = 24000;
                if (abr > 128000) abr = 128000;
            } catch { /* keep default */ }
            const tuned = `maxaveragebitrate=${abr};maxplaybackrate=48000;stereo=0;useinbandfec=1;usedtx=1;minptime=20;ptime=20`;
            const fmtpIdx = lines.findIndex(l => l.startsWith(`a=fmtp:${pt}`));
            if (fmtpIdx !== -1) {
                // Mevcut fmtp satırına FEC/DTX/bitrate/ptime ekle; çakışan parametreleri üzerine yaz.
                let line = lines[fmtpIdx];
                line = line.replace(/maxaveragebitrate=\d+/g, `maxaveragebitrate=${abr}`);
                line = line.replace(/useinbandfec=\d/g, "useinbandfec=1");
                line = line.replace(/usedtx=\d/g, "usedtx=1");
                // Önce minptime, sonra ptime normalize edilir (ptime ifadesi
                // minptime içindeki eşleşmeyi yanlışlıkla yakalamasın diye).
                line = line.replace(/minptime=\d+/g, "minptime=20");
                line = line.replace(/(^|[;\s])ptime=\d+/g, "$1ptime=20");
                if (!/useinbandfec/.test(line)) line += ";useinbandfec=1";
                if (!/usedtx/.test(line)) line += ";usedtx=1";
                if (!/minptime/.test(line)) line += ";minptime=20";
                if (!/ptime/.test(line)) line += ";ptime=20";
                lines[fmtpIdx] = line;
            } else {
                lines.splice(rtpmapIdx + 1, 0, `a=fmtp:${pt} ${tuned}`);
            }
            return lines.join("\r\n");
        } catch {
            return sdp;
        }
    }

    /**
     * Gönderim tarafında bitrate tavanı uygular (RTCRtpSender.setParameters).
     * CPU ve bant genişliğini dengeler; kötü ağlarda otomatik düşüşe yer açar.
     */
    _applySenderBitrate(pc, stream, kind, maxKbps = 0) {
        if (!pc || !stream) return;
        const tracks = stream.getTracks().filter(t => (kind ? t.kind === kind : true));
        if (!tracks.length) return;
        const senders = pc.getSenders().filter(s => s.track && tracks.includes(s.track));
        senders.forEach(sender => {
            try {
                const params = sender.getParameters();
                if (!params.encodings || !params.encodings.length) params.encodings = [{}];
                params.encodings.forEach(enc => {
                    if (maxKbps > 0) enc.maxBitrate = maxKbps * 1000;
                    if (kind === "video") {
                        // Ekran paylaşımında metin okunabilirliği için çözünürlük korunur;
                        // düşük bant genişliğinde kare hızı düşer ama görüntü net kalır.
                        enc.degradationPreference = "maintain-resolution";
                    }
                });
                sender.setParameters(params).catch(() => { });
            } catch { /* unsupported — ignore */ }
        });
    }

    async _initiatePeer(peerId, info, makeOffer) {
        if (this.peers[peerId]) return; // already connected

        // bundlePolicy=max-bundle + rtcpMuxPolicy=require: tek port/tek multiplexed
        // akış — paket sayısını ve NAT delik açma yükünü azaltır (kötü ağ dostu).
        const pc = new RTCPeerConnection({ iceServers: _scordIceServers(), bundlePolicy: "max-bundle", rtcpMuxPolicy: "require" });
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
                    offer.sdp = this._tuneOpusSdp(offer.sdp);
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
            offer.sdp = this._tuneOpusSdp(offer.sdp);
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

        // Glare rollback (perfect negotiation): kendi offer'ımızı üretirken
        // rakibin offer'ı geldiyse önce yerel offer'ı geri al — aksi halde
        // setRemoteDescription "have-local-offer" durumunda InvalidStateError
        // fırlatır. Sadece polite taraf buraya ulaşır (impolite yukarıda döndü).
        if (offerCollision && pc.signalingState === "have-local-offer") {
            try {
                await pc.setLocalDescription({ type: "rollback" });
            } catch (e) {
                console.warn("[P2P] rollback failed:", e);
            }
            peerObj._makingOffer = false;
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
        answer.sdp = this._tuneOpusSdp(answer.sdp);
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
        if (this.remoteStreams) delete this.remoteStreams[peerId];
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
            this.localStream = stream || await navigator.mediaDevices.getUserMedia({
                audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true, channelCount: 1 },
                video: false,
            });
            this.voiceActive = true;
            for (const [, peerObj] of Object.entries(this.peers)) {
                this.localStream.getTracks().forEach(t => {
                    try { peerObj.pc.addTrack(t, this.localStream); } catch { }
                });
                // Ses bitrate tavanı: kullanıcı ayarı (24-128k) varsa onu kullan,
                // yoksa opus varsayılanıyla aynı 64k. SDP fmtp + sender tavanı
                // çifte güvence olarak aynı değeri kullanır.
                const vs = (typeof window !== "undefined" && window.state && window.state.voiceSettings) || {};
                let audioKbps = parseInt(vs.audioBitrate, 10) || 64;
                if (audioKbps < 24) audioKbps = 24;
                if (audioKbps > 128) audioKbps = 128;
                this._applySenderBitrate(peerObj.pc, this.localStream, "audio", audioKbps);
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
            // Kalite/FPS kullanıcı ayarlarından gelir (app.js openScreenSharePicker).
            const winState = (typeof window !== "undefined" && window.state) || {};
            const q = winState.screenShareQuality || "720p";
            const fps = winState.screenShareFPS || 30;
            const qMap = {
                "4k": { width: { ideal: 3840 }, height: { ideal: 2160 }, frameRate: { ideal: fps } },
                "1080p": { width: { ideal: 1920 }, height: { ideal: 1080 }, frameRate: { ideal: fps } },
                "720p": { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: fps } },
                "480p": { width: { ideal: 854 }, height: { ideal: 480 }, frameRate: { ideal: fps } },
                "360p": { width: { ideal: 640 }, height: { ideal: 360 }, frameRate: { ideal: fps } },
            };
            const bitrateMap = { "4k": 8000, "1080p": 4000, "720p": 2500, "480p": 1200, "360p": 700 };
            this.screenStream = await navigator.mediaDevices.getDisplayMedia({
                video: { ...(qMap[q] || qMap["720p"]), cursor: "always" },
                audio: winState.screenShareAudio === true,
            });

            const videoTrack = this.screenStream.getVideoTracks()[0];
            if (videoTrack) videoTrack.contentHint = fps >= 60 ? "motion" : "detail";
            if (videoTrack) videoTrack.onended = () => this.stopScreenShare();

            // Her track (video + varsa sistem sesi) için AYRI sender eklenir.
            // Asla replaceTrack kullanılmaz — aksi halde mevcut mic/kamera
            // sender'ının track'i sessizce ekran paylaşımınkiyle değişir.
            for (const [, peerObj] of Object.entries(this.peers)) {
                this.screenStream.getTracks().forEach(track => {
                    try { peerObj.pc.addTrack(track, this.screenStream); } catch (e) {
                        console.warn("[P2P] Failed to add screen track to peer", e);
                    }
                });
            }
            // Video bitrate tavanı — kaliteye göre CPU/ağ dengesi.
            for (const [, peerObj] of Object.entries(this.peers)) {
                this._applySenderBitrate(peerObj.pc, this.screenStream, "video", bitrateMap[q] || 2500);
            }
            return true;
        } catch (err) {
            console.error("[P2P] Screen capture error:", err);
            return false;
        }
    }

    stopScreenShare() {
        if (!this.screenStream) return;

        const tracks = this.screenStream.getTracks();

        // Her peer'da bu stream'e ait TÜM sender'ları kaldır (video + audio).
        for (const [, peerObj] of Object.entries(this.peers)) {
            const senders = peerObj.pc.getSenders();
            senders.filter(s => s.track && tracks.includes(s.track)).forEach(sender => {
                try { peerObj.pc.removeTrack(sender); } catch (e) { }
            });
        }

        tracks.forEach(t => t.stop());
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
        clearInterval(this._pingTimer);
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
