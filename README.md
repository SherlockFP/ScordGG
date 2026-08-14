# SCORD

P2P-öncelikli bir topluluk ve sohbet uygulaması. Metin, sesli sohbet, ekran paylaşımı, roller/izinler, müzik botu, arkadaşlar ve DM içeriyor. Medya ve mesajlar mümkün olduğunda **peer-to-peer** (WebRTC) akar; merkezi servis kimlik, alan metadata'sı, sinyalleşme, mesaj geçmişi ve bağlantı kurulamadığında WebSocket fallback'i sağlar.

Canlısı burada: `https://scord.onrender.com` gibi bir Render adresinde host'lanıyor (kendi deploy'unu yaparsan URL değişir).

## Nasıl çalışıyor

Üç katman var:

1. **Kalıcı SCORD veritabanı** (SQLite cache + opsiyonel Supabase mirror) — hesaplar, oturumlar,
   arkadaşlıklar, sunucular, üyelikler, roller, kanallar ve mesaj geçmişi aynı
   transactional veritabanında tutulur. Supabase ayarlandığında her yazma atomik
   bir uzak snapshot'a aktarılır ve boş Render instance'ı açılışta buradan geri
   yüklenir. Gerçek üyelik: e-posta ile kayıt ol / giriş yap. Görünen adlar aynı
   olabilir; hesaplar `kullanici#1234` etiketiyle ayrılır. Şifreler pbkdf2 ile hash'lenip saklanıyor, girişte oturum token'ı
   veriliyor. Avatar, biyografi ve banner hesaba bağlı — başka tarayıcıdan
   girsen de profilin seninle gelir. (Peer ID hâlâ eski deterministik formülle
   üretiliyor ki mevcut sunucu sahiplikleri kırılmasın.)

2. **Sinyal / kayıt sunucusu** (`static/server.py`, FastAPI) — Render'da 7/24 açık duran tek sunucu. İşi şu:
   - Hangi sunucuların (server/guild) var olduğunu tutar (`/api/rooms`), oluşturma/silme/davet kodu gibi işlemleri yönetir.
   - WebRTC bağlantısını kurmak için gereken signaling'i (offer/answer/ICE candidate mesajlarını iki peer arasında forward etmek) yapar. Metin mesajları deduplikasyon için kimlikli olarak hem açık DataChannel'lara hem WebSocket hattına gönderilir; WebSocket yoksa açık DataChannel tek başına çalışabilir.
   - Kanal listesi, roller, üyelikler ve mesaj geçmişi gibi *sunucu metadata'sını*
     SQLite'a yazar. Eski kurulumdaki `rooms.json` varsa ilk açılışta otomatik
     içeri aktarılır. Silinen sunucuların tombstone kayıtları da aynı veritabanında
     tutulur; istemci kopyaları silinmiş bir sunucuyu geri diriltemez.

3. **P2P mesh** (`static/p2p.js`) — chat tesliminin P2P kolu, ses, ekran paylaşımı ve kamera burada. Odaya giren her peer, odadaki diğer herkesle ayrı ayrı `RTCPeerConnection` açar (full mesh). Ses/görüntü track'leri mümkün olduğunda doğrudan akar; metin ise DataChannel + WebSocket çift teslim ve merkezi geçmiş kullanır.

Yani kısaca: kimlik ve alan durumu için backend otoritesi, gerçek zamanlı medya için doğrudan WebRTC mesh, metin içinse P2P + merkezi geçmiş/fallback birlikte kullanılır. Bu sürüm uçtan uca şifreli mesajlaşma iddiasında bulunmaz.

## Kurulum

```bash
pip install -r requirements.txt
python -m uvicorn app:app --reload --port 8000
```

Windows'ta `run.bat` aynısını yapıyor. `http://localhost:8000` açılınca giriş ekranı geliyor — hesabın yoksa "Kayıt Ol" sekmesinden bir tane oluştur.

### Render'a deploy

`Procfile` ve `requirements.txt` hazır. Render ayarları:

- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Persistent Disk mount path: `/var/data`
- Environment: `SCORD_DATA_DIR=/var/data`
- Environment: `SCORD_BOOTSTRAP_ADMIN_USERNAME=sherlock`
- Secret: `SCORD_BOOTSTRAP_ADMIN_PASSWORD=<yalnız Render'a yazacağın parola>`
- Environment: `SCORD_BOOTSTRAP_ADMIN_EMAIL=<kendi e-posta adresin>`

`SCORD_DATA_DIR` yerelde zorunlu değildir; verilmezse veritabanı proje kökünde
oluşur. Render'da yeniden deploy sonrası hesap/sunucu verisinin kalması için
persistent disk ve yukarıdaki environment değeri gereklidir. Ayrıca
`SCORD_TURN_URLS` / `SCORD_TURN_USERNAME` / `SCORD_TURN_CREDENTIAL` değerlerini
set edersen kendi TURN sunucunu devreye sokabilirsin.

### Supabase kalıcı yedek

Supabase projesinde `create_scord_persistence_v2` ve
`add_scord_atomic_snapshot` migration'ları uygulanmıştır. Render'da şu iki
değeri eklediğinde uzak kalıcılık otomatik açılır:

- `SCORD_SUPABASE_URL=https://wmvahbyjyahpqkffisbt.supabase.co`
- `SCORD_SUPABASE_SERVICE_ROLE_KEY=<Supabase service_role secret>`

`service_role` anahtarını frontend'e, GitHub'a veya normal environment çıktısına
yazma; Render'da **Secret** olarak ekle. Anahtar yoksa uygulama güvenli biçimde
yalnız SQLite ile çalışmaya devam eder. Supabase tablolarında RLS açık ve public
policy yoktur; hesap hash/salt verisine yalnız backend servis anahtarı erişir.

## Bilinen sınırlar

- Full-mesh P2P olduğu için sesli kanaldaki kişi sayısı arttıkça her istemcinin CPU/bant genişliği yükü doğrusal artıyor. Büyük sunucular için SFU'ya geçmek gerekir ama bu proje o ölçek için tasarlanmadı.
- TURN sunucusu ayarlamazsan bazı ağlarda (simetrik NAT, kurumsal firewall) bağlantı kurulamayabilir.
- `app.js` tek dosya, epeyce büyüdü. Yeni özellik eklerken önce mevcut fonksiyonu ara, üstüne yeni bir patch yığma — kod tabanı zaten bunun izlerini taşıyor.

## Katkı

Repo hâlâ aktif geliştiriliyor, issue/PR açabilirsin.
