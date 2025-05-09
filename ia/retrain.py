import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="xgboost.core")

import json
import pandas as pd
import numpy as np
import xgboost as xgb
from kafka import KafkaConsumer
from threading import Lock
from utils import load_pickle, save_pickle

MODEL_F = 'xgb_top5_model.pkl'
LE_F    = 'xgb_le_top5.pkl'
OE_F    = 'xgb_oe_top5.pkl'
COLS_F  = 'xgb_top5_cols.pkl'
TOPIC   = 'train-data'
BROKER  = 'kafka:9092'

model = load_pickle(MODEL_F)
le    = load_pickle(LE_F)
oe    = load_pickle(OE_F)
cols  = load_pickle(COLS_F)
lock  = Lock()

label_map = {lab: idx for idx, lab in enumerate(le.classes_)}
params = model.get_xgb_params()
for bad_key in ("use_label_encoder", "n_jobs", "objective", "eval_metric"):
    params.pop(bad_key, None)

def listen_retrain():
    print("🎧  En attente de messages sur Kafka...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='retrain-group'
    )

    try:
        for msg in consumer:
            records = msg.value
            print(f"📩  Reçu : {records}")

            if not isinstance(records, list):
                records = [records]

            expected_fields = ['addr_pct_cd', 'boro_nm', 'cmplnt_fr_dt', 'cmplnt_fr_tm', 'ofns_desc', 'latitude', 'longitude']

            for rec in records:
                data = {key: rec.get(key) for key in expected_fields}
                df = pd.DataFrame([data]).dropna(subset=expected_fields)
                if df.empty:
                    print("⚠️  Données incomplètes, passage au record suivant.")
                    continue

                off = df.at[0, 'ofns_desc']
                if off not in label_map:
                    print(f"⚠️  Offense '{off}' non vue à l'entraînement, skip.")
                    continue

                try:
                    df['cmplnt_fr_dt'] = pd.to_datetime(df['cmplnt_fr_dt'], errors='coerce')
                    df['hour'] = pd.to_datetime(df['cmplnt_fr_tm'], format='%H:%M:%S', errors='coerce').dt.hour
                    df = df.dropna(subset=['cmplnt_fr_dt', 'hour'])
                    df['hour'] = df['hour'].astype(int)
                    df['month'], df['wday'] = df['cmplnt_fr_dt'].dt.month, df['cmplnt_fr_dt'].dt.dayofweek
                    df['h_sin'], df['h_cos'] = np.sin(2 * np.pi * df['hour'] / 24), np.cos(2 * np.pi * df['hour'] / 24)
                    df['w_sin'], df['w_cos'] = np.sin(2 * np.pi * df['wday'] / 7), np.cos(2 * np.pi * df['wday'] / 7)
                    df['lat'], df['lon'] = df['latitude'].astype(float), df['longitude'].astype(float)

                    X_num = df[['month', 'h_sin', 'h_cos', 'w_sin', 'w_cos', 'lat', 'lon']]
                    pct_map = {v: i for i, v in enumerate(oe.categories_[0])}
                    boro_map = {v: i for i, v in enumerate(oe.categories_[1])}
                    pct_code = df['addr_pct_cd'].map(pct_map).fillna(-1).astype(int)
                    boro_code = df['boro_nm'].map(boro_map).fillna(-1).astype(int)
                    X_cat = pd.DataFrame({'pct_enc': pct_code, 'bor_enc': boro_code})

                    X_new = pd.concat([X_num.reset_index(drop=True), X_cat.reset_index(drop=True)], axis=1)[cols]
                    y_new = np.array([label_map[off]])
                    dtrain = xgb.DMatrix(X_new, label=y_new)

                    with lock:
                        booster = xgb.train(
                            params,
                            dtrain,
                            num_boost_round=1,
                            xgb_model=model.get_booster()
                        )
                        model._Booster = booster
                        print(f"📄  Sauvegarde du modèle dans : {MODEL_F}")
                        save_pickle(model, MODEL_F)
                        print(f"✅  Modèle mis à jour pour '{off}' (1 itération)")

                except Exception as e:
                    print(f"❌ Erreur lors de la mise à jour du modèle : {e}")

    except Exception as e:
        print(f"❌ Erreur dans la boucle Kafka : {e}")

if __name__ == "__main__":
    listen_retrain()
