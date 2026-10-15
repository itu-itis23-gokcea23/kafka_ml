"""Sürekli sahte kart işlemi üretir ve Kafka'daki 'transactions' topic'ine yazar.

Gerçek hayatta bu rolü bankanın POS/ATM sistemleri oynar. Olaylar ham veridir:
amount_ratio gibi türetilmiş özellikler YOKTUR, onu consumer hesaplar.
"""
import json
import random
import time

from confluent_kafka import Producer

from common import make_tx

BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"
FRAUD_PROB = 0.12   # demoda fraud sık görünsün diye gerçekçi olmayan yüksek oran
INTERVAL = 0.7      # saniye
USERS = 20

# Her kullanıcının normal harcama düzeyi (sadece simülasyon için; consumer bunu bilmez)
baselines = {u: random.uniform(20, 200) for u in range(1, USERS + 1)}

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def on_delivery(err, msg):
    if err is not None:
        print(f"Gönderim hatası: {err}")


tx_id = 0
print(f"Producer başladı -> topic '{TOPIC}' (durdurmak için Ctrl+C)")
try:
    while True:
        tx_id += 1
        user = random.randint(1, USERS)
        tx = make_tx(random.random() < FRAUD_PROB, baselines[user])
        tx["tx_id"] = tx_id
        tx["user_id"] = user

        # key = user_id: aynı kullanıcının işlemleri hep aynı partition'a düşer,
        # böylece o kullanıcının state'i tek bir consumer'da tutulur
        producer.produce(
            TOPIC,
            key=str(user),
            value=json.dumps(tx),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        print(f"gönderildi: tx {tx_id}  kullanıcı={user}  tutar={tx['amount']}  saat={tx['hour']}")
        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Producer kapandı.")
