"""Offline eğitim: sentetik bir işlem AKIŞI simüle eder, özellikleri canlıdaki gibi
(UserStats ile) hesaplar ve fraud modelini model.pkl olarak kaydeder."""
import random

import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from common import FEATURES, UserStats, make_tx

random.seed(42)

N = 8000
FRAUD_RATE = 0.05
USERS = 20

baselines = {u: random.uniform(20, 200) for u in range(1, USERS + 1)}
stats = UserStats()

X, y = [], []
for _ in range(N):
    user = random.randint(1, USERS)
    fraud = random.random() < FRAUD_RATE
    tx = make_tx(fraud, baselines[user])

    # consumer ile AYNI mantık: önce oranı hesapla, sonra (fraud değilse) state'i güncelle
    tx["amount_ratio"] = stats.ratio(user, tx["amount"])
    if not fraud:
        stats.update(user, tx["amount"])

    X.append([tx[f] for f in FEATURES])
    y.append(tx["true_label"])

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

model = make_pipeline(
    StandardScaler(),
    LogisticRegression(class_weight="balanced", max_iter=1000),
)
model.fit(X_train, y_train)

print(classification_report(y_test, model.predict(X_test), target_names=["normal", "fraud"]))

joblib.dump(model, "model.pkl")
print("Model kaydedildi: model.pkl")
