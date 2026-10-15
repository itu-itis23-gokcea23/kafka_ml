"""Eğitim, producer ve consumer'ın ortak kullandığı kod.

Önemli: `amount_ratio` özelliği akışta GELMEZ, consumer tarafından UserStats ile
canlı hesaplanır. Eğitimde de aynı sınıf kullanıldığı için eğitim ve canlı
hesap birebir aynıdır (train/serve skew olmaz).
"""
import random

FEATURES = ["amount", "hour", "is_foreign", "amount_ratio"]
MIN_HISTORY = 3  # bir kullanıcı için oran hesaplamadan önce gereken geçmiş işlem sayısı


def make_tx(fraud: bool, baseline: float) -> dict:
    """Tek bir sahte kart işlemi üretir (amount_ratio YOK).

    baseline: kullanıcının normal harcama düzeyi. Normal işlemler buna yakın,
    fraud işlemler kat kat yüksektir.
    """
    if fraud:
        amount = round(baseline * random.uniform(4, 15), 2)
        hour = random.choice([0, 1, 2, 3, 4, 5, 23])
        is_foreign = int(random.random() < 0.8)
    else:
        amount = round(baseline * random.uniform(0.4, 1.8), 2)
        hour = random.randint(8, 22)
        is_foreign = int(random.random() < 0.05)
    return {
        "amount": amount,
        "hour": hour,
        "is_foreign": is_foreign,
        "true_label": int(fraud),  # sadece demoda karşılaştırma için
    }


class UserStats:
    """STATE: her kullanıcının şimdiye kadarki ortalama harcaması (artımlı ortalama)."""

    def __init__(self):
        self.count = {}
        self.avg = {}

    def ratio(self, user_id, amount: float) -> float:
        """Tutarın kullanıcının ortalamasına oranı. Yeterli geçmiş yoksa 1.0 (nötr)."""
        if self.count.get(user_id, 0) < MIN_HISTORY:
            return 1.0
        return round(amount / self.avg[user_id], 2)

    def update(self, user_id, amount: float):
        n = self.count.get(user_id, 0) + 1
        old = self.avg.get(user_id, 0.0)
        self.avg[user_id] = old + (amount - old) / n
        self.count[user_id] = n
