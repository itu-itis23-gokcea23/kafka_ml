# Apache Kafka ile Gerçek Zamanlı Fraud Tespiti (Stream Processing Demo)

Bu demo, Apache Kafka üzerinden **sürekli akan kart işlemlerini** geldikleri anda bir
makine öğrenmesi modeliyle skorlar ve sonucu yeni bir Kafka topic'ine yazar.

## Ne yapıyor?

```
train.py  ->  model.pkl   (offline eğitim, bir kez)

producer.py --> Kafka: "transactions" --> consumer.py --> Kafka: "predictions"
 (ham işlem)      (veri burada durur)     (stream processing:    (sonuç akışı)
                                           state + canlı özellik
                                           + anlık model tahmini)
```

Producer **ham** işlem gönderir (kullanıcı, tutar, saat, yurt dışı mı). Modelin kullandığı
`amount_ratio` özelliği akışta gelmez; consumer her kullanıcının ortalama harcamasını
(**state**) tutarak bunu canlı üretir. Eğitimde de aynı hesap kullanıldığı için eğitim ve
canlı sonuç tutarlıdır.

| Dosya | Görevi |
|---|---|
| `docker-compose.yml` | Tek düğümlü Kafka'yı (KRaft modu) ayağa kaldırır |
| `common.py` | Sentetik işlem üretici ve `UserStats` (kullanıcı başına ortalama harcama state'i); eğitim, producer ve consumer ortak kullanır |
| `train.py` | İşlem akışını simüle edip Logistic Regression ile fraud modeli eğitir, `model.pkl` üretir |
| `producer.py` | Sürekli ham sahte işlem üretip `transactions` topic'ine yazar |
| `consumer.py` | **Stream processing:** her işlemi okur, state'ten `amount_ratio` üretir, modelle skorlar, `predictions` topic'ine yazar, son 10 sn'lik pencere istatistiği tutar |

Model özellikleri: `amount`, `hour`, `is_foreign`, `amount_ratio` (tutar / kullanıcının ortalama harcaması,
**consumer'da canlı hesaplanır**; bir kullanıcının ilk 3 işleminde geçmiş yetersiz olduğu için 1.0 kabul edilir).

## Kurulum

Gereksinimler: Docker, Python 3.9+

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Çalıştırma

1. Kafka'yı başlat (ilk seferde imaj iner, birkaç saniye bekle):
   ```bash
   docker compose up -d
   ```
2. Modeli eğit (bir kez):
   ```bash
   python train.py
   ```
3. **Terminal A** – stream processing uygulaması:
   ```bash
   python consumer.py
   ```
4. **Terminal B** – veri akışı:
   ```bash
   python producer.py
   ```

Terminal A'da şuna benzer bir çıktı akar:

```
[p1] tx   41 u7  tutar=   62.10 oran= 0.98 -> NORMAL (0.01)  | son 10 sn: toplam=14 fraud=2
[p2] tx   42 u12 tutar= 1480.55 oran= 9.40 -> FRAUD  (1.00)  | son 10 sn: toplam=15 fraud=3
```

Durdurmak için Ctrl+C. Kafka'yı kapatmak için: `docker compose down`.

## Demoda gösterilebilecekler

- **Veri Kafka'da durur:** Consumer'ı kapat, producer çalışmaya devam etsin, sonra consumer'ı
  yeniden aç. Kapalıyken gelen işlemleri kaldığı yerden (offset) işler.
- **Ölçeklenme:** Üçüncü bir terminalde ikinci bir `python consumer.py` başlat. Topic 3
  partition'a sahip olduğu için Kafka partition'ları iki consumer arasında paylaştırır
  (çıktıdaki `[p0]`, `[p1]`, `[p2]` partition numarasıdır).
- **Çıktı topic'ini izlemek:**
  ```bash
  docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic predictions --from-beginning
  ```

## Notlar

- Stream processing burada Kafka consumer'ı ile elle yapılıyor (framework yok). Büyük ölçek ve
  state yönetimi için Kafka Streams, Flink veya Quix Streams gibi araçlar kullanılabilir.
- Kullanıcı ortalamaları (state) consumer belleğinde tutulur; consumer yeniden başlarsa sıfırlanır
  ve kullanıcılar tekrar "geçmiş yetersiz" aşamasından başlar. Producer mesajı `user_id` anahtarıyla
  gönderdiği için bir kullanıcının tüm işlemleri aynı partition'a, dolayısıyla aynı consumer'a düşer.
  Üretimde state'i Kafka'ya yedekleyen Kafka Streams / Flink gibi araçlar bu sorunu çözer.
- Offset, işlem bittikten sonra commit edilir (at-least-once): çökme durumunda mesaj kaybolmaz,
  en kötü ihtimalle bir mesaj iki kez işlenir.
- Veriler sentetiktir; amaç model başarısı değil, akış mimarisini göstermektir.

## Sorun giderme

- `Connection refused` / `Broker transport failure`: Kafka henüz hazır değil, 10-15 sn bekleyip tekrar dene (`docker compose logs kafka`).
- `model.pkl bulunamadı`: önce `python train.py` çalıştır.
- 9092 portu doluysa `docker-compose.yml` ve scriptlerdeki portu değiştir.
