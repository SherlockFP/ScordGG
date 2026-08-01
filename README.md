# SCORD

Discord'un P2P versiyonunu yapmaya çalışan bir proje. Metin, sesli sohbet, ekran paylaşımı, roller/izinler, müzik botu, arkadaşlar/DM — Discord'da ne varsa ona benzer bir şey oturtmaya çalıştım, ama mimarisi tamamen farklı: mesajlar ve ses **peer-to-peer** (WebRTC) üzerinden akıyor, merkezi bir sunucu chat trafiğine dokunmuyor.

Canlısı burada: `https://scord.onrender.com` gibi bir Render adresinde host'lanıyor (kendi deploy'unu yaparsan URL değişir).

## Nasıl çalışıyor

Üç katman var:

1. **Hesap sistemi** (SQLite, `scord_accounts.db`) — gerçek üyelik: kayıt ol /
   giriş yap. Şifreler pbkdf2 ile hash'lenip saklanıyor, girişte oturum token'ı
   veriliyor. Avatar, biyografi ve banner hesaba bağlı — başka tarayıcıdan
   girsen de profilin seninle gelir. (Peer ID hâlâ eski deterministik formülle
   üretiliyor ki mevcut sunucu sahiplikleri kırılmasın.)

2. **Sinyal / kayıt sunucusu** (`static/server.py`, FastAPI) — Render'da 7/24 açık duran tek sunucu. İşi şu:
   - Hangi sunucuların (server/guild) var olduğunu tutar (`/api/rooms`), oluşturma/silme/davet kodu gibi işlemleri yönetir.
   - WebRTC bağlantısını kurmak için gereken signaling'i (offer/answer/ICE candidate mesajlarını iki peer arasında forward etmek) yapar — kendisi asla ses/video/mesaj içeriğini görmez, sadece "şu paketi şu peer'a ilet" der.
   - Kanal listesi, roller, mesaj geçmişi gibi *sunucu metadata'sını* diskte (`rooms.json`) tutar. Bunun sebebi basit: Render'ın ücretsiz planında disk ephemeral, yani container her redeploy'da sıfırlanabiliyor. Bir client'ın elinde hâlâ eski bir sunucunun tam kopyası varsa, açılışta bunu backend'e geri "restore" edip kaybı telafi ediyor. Sahiden silinmiş bir sunucu ise (owner sildiyse) tombstone listesinde tutuluyor ki geri dirilmesin.

3. **P2P mesh** (`static/p2p.js`) — chat, ses, ekran paylaşımı, kamera burada. Odaya giren her peer, odadaki diğer herkesle ayrı ayrı `RTCPeerConnection` açar (full mesh). Metin `RTCDataChannel` üzerinden, ses/görüntü track olarak gider. Sinyalleşme WebSocket üzerinden yukarıdaki sunucuya gidip geliyor, ama trafiğin kendisi doğrudan iki tarayıcı arasında.

Yani kısaca: "kim kimdir, kim nerede, hangi sunucu hâlâ var" gibi sorular için tek bir otorite var (backend), ama "ne konuşuluyor / kim konuşuyor" tamamen uçtan uca.

## Kurulum

```bash
pip install -r requirements.txt
python app.py        # ya da: uvicorn app:app --reload --port 8000
```

Windows'ta `run.bat` aynısını yapıyor. `http://localhost:8000` açılınca giriş ekranı geliyor — hesabın yoksa "Kayıt Ol" sekmesinden bir tane oluştur.

### Render'a deploy

`Procfile` ve `requirements.txt` zaten hazır, Render'da yeni bir Web Service açıp bu repoyu bağlaman yeterli. `SCORD_TURN_URLS` / `SCORD_TURN_USERNAME` / `SCORD_TURN_CREDENTIAL` env değişkenlerini set edersen kendi TURN sunucunu da devreye sokabilirsin (simetrik olmayan NAT'ların arkasındaki kullanıcılar için STUN yetmeyebiliyor).

## Bilinen sınırlar

- Full-mesh P2P olduğu için sesli kanaldaki kişi sayısı arttıkça her istemcinin CPU/bant genişliği yükü doğrusal artıyor. Büyük sunucular için SFU'ya geçmek gerekir ama bu proje o ölçek için tasarlanmadı.
- TURN sunucusu ayarlamazsan bazı ağlarda (simetrik NAT, kurumsal firewall) bağlantı kurulamayabilir.
- `app.js` tek dosya, epeyce büyüdü. Yeni özellik eklerken önce mevcut fonksiyonu ara, üstüne yeni bir patch yığma — kod tabanı zaten bunun izlerini taşıyor.

## Katkı

Repo hâlâ aktif geliştiriliyor, issue/PR açabilirsin.
