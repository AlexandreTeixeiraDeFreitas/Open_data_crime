import warnings
# on ignore tous les UserWarning venant de xgboost.core
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

# chargement initial
model = load_pickle(MODEL_F)     # XGBClassifier
le    = load_pickle(LE_F)        # LabelEncoder
oe    = load_pickle(OE_F)        # OrdinalEncoder (pour categories_)
cols  = load_pickle(COLS_F)      # liste des colonnes à conserver
lock  = Lock()

# mapping manuel label → code
label_map = {lab: idx for idx, lab in enumerate(le.classes_)}

# récupérer les paramètres XGBoost et nettoyer les clés obsolètes
params = model.get_xgb_params()
for bad_key in ("use_label_encoder", "n_jobs", "objective", "eval_metric"):
    params.pop(bad_key, None)

def listen_retrain():
    print("Début du train")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='retrain-group'
    )

    for msg in consumer:
        rec = msg.value
        df = pd.DataFrame([rec]).dropna(subset=[
            'addr_pct_cd','boro_nm','cmplnt_fr_dt','cmplnt_fr_tm',
            'ofns_desc','latitude','longitude'
        ])
        if df.empty:
            continue

        off = df.at[0, 'ofns_desc']
        if off not in label_map:
            print(f"⚠️  Offense '{off}' non vue à l'entraînement, skip.")
            continue

        # Prétraitement temporel et géographique
        df['cmplnt_fr_dt'] = pd.to_datetime(df['cmplnt_fr_dt'], errors='coerce')
        df['hour'] = pd.to_datetime(df['cmplnt_fr_tm'],
                                     format='%H:%M:%S',
                                     errors='coerce').dt.hour
        df = df.dropna(subset=['cmplnt_fr_dt','hour'])
        df['hour']  = df['hour'].astype(int)
        df['month'], df['wday'] = df['cmplnt_fr_dt'].dt.month, df['cmplnt_fr_dt'].dt.dayofweek
        df['h_sin'], df['h_cos'] = np.sin(2*np.pi*df['hour']/24), np.cos(2*np.pi*df['hour']/24)
        df['w_sin'], df['w_cos'] = np.sin(2*np.pi*df['wday']/7),   np.cos(2*np.pi*df['wday']/7)
        df['lat'], df['lon'] = df['latitude'].astype(float), df['longitude'].astype(float)

        X_num = df[['month','h_sin','h_cos','w_sin','w_cos','lat','lon']]

        # Encodage manuel des catégories
        pct_map  = {v:i for i, v in enumerate(oe.categories_[0])}
        boro_map = {v:i for i, v in enumerate(oe.categories_[1])}
        pct_code  = df['addr_pct_cd'].map(pct_map).fillna(-1).astype(int)
        boro_code = df['boro_nm'].map(boro_map).fillna(-1).astype(int)
        X_cat = pd.DataFrame({'pct_enc': pct_code, 'bor_enc': boro_code})

        # Assemblage des features finales
        X_new = pd.concat([X_num.reset_index(drop=True),
                           X_cat.reset_index(drop=True)], axis=1)[cols]

        # encodage manuel du label
        y_new = np.array([label_map[off]])

        # transformer en DMatrix pour xgb.train
        dtrain = xgb.DMatrix(X_new, label=y_new)

        # ajout d'une seule itération de boosting
        with lock:
            booster = xgb.train(
                params,
                dtrain,
                num_boost_round=1,
                xgb_model=model.get_booster()
            )
            # mise à jour du booster interne du wrapper sklearn
            model._Booster = booster
            save_pickle(model, MODEL_F)
            print(f"✅  Modèle mis à jour pour '{off}' (1 itération)")

if __name__ == "__main__":
    listen_retrain()
