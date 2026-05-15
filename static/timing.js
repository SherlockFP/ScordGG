/**
 * SCORD — gecikme ve zamanlama sabitleri (ms)
 * Tarayıcı konsolunda: SCORD_TIMING
 */
window.SCORD_TIMING = {
    /** WebRTC: birden fazla track eklenince çakışan offer’ları birleştirme */
    P2P_NEGOTIATION_DEBOUNCE_MS: 150,

    /** WebSocket sinyal kopunca yeniden bağlanma */
    P2P_WS_RECONNECT_MS: 3000,

    /** Sunucuya keepalive ping (ICE/signaling) */
    P2P_SIGNALING_PING_INTERVAL_MS: 25000,

    /** Üye listesi yeniden çizimini seyrekleştirme */
    MEMBERS_PANEL_DEBOUNCE_MS: 300,

    /** Konuşma algılama döngüsü (ses seviyesi → konuşuyor göstergesi) */
    VOICE_SPEAKING_POLL_MS: 150,

    /** Konuşma göstergesi histerezisi */
    VOICE_SPEAKING_HOLD_MS: 250,

    /** Uzak video track sonrası UI yenileme */
    VOICE_TRACK_RENDER_DELAY_MS: 400,

    /** Uzak ses akışı ilk bağlantı sonrası UI */
    VOICE_STREAM_RENDER_DELAY_MS: 500,

    /** Ekran paylaşımı overlay video senkron aralığı */
    SCREEN_OVERLAY_SYNC_INTERVAL_MS: 750,

    /** Ana sayfa oda listesi yenileme */
    DISCOVERY_REFRESH_INTERVAL_MS: 15000,

    /** Sohbet: yazıyor göstergesi gönderme minimum aralık */
    TYPING_SEND_MIN_GAP_MS: 2500,

    /** Yazıyor çubuğu otomatik kapanma */
    TYPING_INDICATOR_CLEAR_MS: 4000,

    /** GIF arama debounce */
    GIF_SEARCH_DEBOUNCE_MS: 500,

    /** Toast süresi */
    TOAST_DURATION_MS: 4000,

    /** DataChannel buffer çok doluysa yayını atla (byte) — tıkanmayı önler */
    P2P_DC_MAX_BUFFERED_BYTES: 262144,

    /** Ses kanalı: eşler arası RTT ölçümü (unicast ping/pong) */
    RTT_PING_INTERVAL_MS: 4000,

    /** İstatistik / sohbet üst bilgi yenileme */
    ROOM_STATS_POLL_MS: 10000,

    /** Toast sesleri (playSound) */
    SOUND_JOIN_MS: 300,
    SOUND_LEAVE_MS: 300,

    /** Davet URL otomatik katılım deneme aralığı */
    INVITE_URL_POLL_MS: 500,

    /** Moderasyon: kanala zorla taşından sonra yeniden katılma */
    FORCE_ROUTE_VOICE_JOIN_DELAY_MS: 200,

    /** Aynı katıl/ayrıl toast bildirimini tekrar göstermeme süresi */
    VOICE_NOTIFY_DEDUP_MS: 10000,
};
