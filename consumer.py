"""STREAM PROCESSING UYGULAMASI.

'transactions' topic'inden gelen her ham işlemi geldiği anda okur ve:
  1) STATE: kullanıcının şimdiye kadarki ortalama harcamasını tutar,
  2) FEATURE: bundan 'amount_ratio' özelliğini canlı üretir,
  3) INFERENCE: eğitilmiş modelle fraud skoru çıkarır,
  4) sonucu 'predictions' topic'ine yazar.
Ek olarak son 10 saniyelik kayan pencerede fraud istatistiği tutar.

Not: State bellekte tutulur; consumer çökerse sıfırlanır. Üretimde bunu state store'u
Kafka'ya yedekleyen Kafka Streams / Flink gibi araçlar çözer.
"""
import json
import sys
import time
from collections import deque

import joblib
from confluent_kafka import Consumer, Producer

from common import FEATURES, UserStats

BOOTSTRAP = "localhost:9092"
IN_TOPIC = "transactions"
OUT_TOPIC = "predictions"
THRESHOLD = 0.5
WINDOW_SEC = 10

model = joblib.load("model.pkl")
stats = UserStats()   # kullanıcı başına ortalama harcama (state)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "fraud-scorer",        # aynı group'taki consumer'lar partition'ları paylaşır
    "auto.offset.reset": "earliest",   # ilk açılışta baştan oku
    "enable.auto.commit": False,       # işlemden SONRA elle commit: mesaj kaybolmaz
})
producer = Producer({"bootstrap.servers": BOOTSTRAP})
consumer.subscribe([IN_TOPIC])

window = deque()  # (zaman, fraud_mu) çiftleri


def update_window(is_fraud: bool):
    now = time.time()
    window.append((now, is_fraud))
    while window and now - window[0][0] > WINDOW_SEC:
        window.popleft()
    total = len(window)
    frauds = sum(1 for _, f in window if f)
    return total, frauds


print(f"Consumer başladı <- '{IN_TOPIC}' (durdurmak için Ctrl+C)")
try:
    while True:
        msg = consumer.poll(1.0)           # yeni işlem var mı?
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka hatası: {msg.error()}", file=sys.stderr)
            continue

        tx = json.loads(msg.value())                       # 1) ham event'i al
        user, amount = tx["user_id"], tx["amount"]

        tx["amount_ratio"] = stats.ratio(user, amount)     # 2) state'ten canlı özellik üret
        x = [[tx[f] for f in FEATURES]]
        score = float(model.predict_proba(x)[0][1])        # 3) anlık ML tahmini
        is_fraud = score >= THRESHOLD
        label = "FRAUD " if is_fraud else "NORMAL"

        if not is_fraud:                                   # fraud, kullanıcı ortalamasını bozmasın
            stats.update(user, amount)

        result = {                                         # 4) sonucu yeni akışa yaz
            "tx_id": tx["tx_id"],
            "user_id": user,
            "amount_ratio": tx["amount_ratio"],
            "fraud_score": round(score, 3),
            "prediction": "FRAUD" if is_fraud else "NORMAL",
        }
        producer.produce(OUT_TOPIC, key=str(user), value=json.dumps(result))
        producer.poll(0)

        total, frauds = update_window(is_fraud)
        print(
            f"[p{msg.partition()}] tx {tx['tx_id']:>4} u{user:<2} tutar={amount:>8.2f} "
            f"oran={tx['amount_ratio']:>5.2f} -> {label} ({score:.2f})"
            f"  | son {WINDOW_SEC} sn: toplam={total} fraud={frauds}"
        )

        consumer.commit(message=msg, asynchronous=False)   # işlendi, offset'i kaydet
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    consumer.close()
    print("Consumer kapandı.")
