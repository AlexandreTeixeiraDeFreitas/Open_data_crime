import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from threading import Lock
from utils import preprocess, load_pickle, save_pickle


MODEL_F = 'xgb_top5_model.pkl'
LE_F    = 'xgb_le_top5.pkl'
OE_F    = 'xgb_oe_top5.pkl'
COLS_F  = 'xgb_top5_cols.pkl'
TOPIC   = 'train-data'
BROKER  = 'kafka:9092'

# chargement initial
model = load_pickle(MODEL_F)
le    = load_pickle(LE_F)
oe    = load_pickle(OE_F)
cols  = load_pickle(COLS_F)
lock  = Lock()

def listen_retrain():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda m: json.loads(m.decode()),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='retrain-group'
    )
    for msg in consumer:
        rec = msg.value
        df = (pd.DataFrame([rec])
              .dropna(subset=['addr_pct_cd','boro_nm','cmplnt_fr_dt','cmplnt_fr_tm','ofns_desc','latitude','longitude'])
        )
        if df.empty or not df['ofns_desc'].isin(le.classes_).any():
            continue

        # même preproc que utils mais en-ligne pour 1 enregistrement
        df['cmplnt_fr_dt'] = pd.to_datetime(df['cmplnt_fr_dt'], errors='coerce')
        df['hour'] = pd.to_datetime(df['cmplnt_fr_tm'], format='%H:%M:%S', errors='coerce').dt.hour
        df = df.dropna(subset=['cmplnt_fr_dt','hour'])
        df['hour'], df['month'], df['wday'] = df['hour'].astype(int), df['cmplnt_fr_dt'].dt.month, df['cmplnt_fr_dt'].dt.dayofweek
        df['h_sin'] = np.sin(2*np.pi*df['hour']/24)
        df['h_cos'] = np.cos(2*np.pi*df['hour']/24)
        df['w_sin'] = np.sin(2*np.pi*df['wday']/7)
        df['w_cos'] = np.cos(2*np.pi*df['wday']/7)
        df['lat'], df['lon'] = df['latitude'].astype(float), df['longitude'].astype(float)

        X_num = df[['month','h_sin','h_cos','w_sin','w_cos','lat','lon']].reset_index(drop=True)
        pct_cats  = oe.categories_[0]   # ex. array([1,2,3, …], dtype=object)
        boro_cats = oe.categories_[1]   # ex. array(['BRONX','BROOKLYN',…], dtype=object)
        pct_map   = {v:i for i,v in enumerate(pct_cats)}
        boro_map  = {v:i for i,v in enumerate(boro_cats)}
        # map → si valeur inconnue, on met -1 (comme avec handle_unknown)
        pct_code  = df['addr_pct_cd'].map(pct_map).fillna(-1).astype(int)
        boro_code = df['boro_nm'].map(boro_map).fillna(-1).astype(int)
        X_cat = pd.DataFrame({
            'pct_enc': pct_code,
            'bor_enc': boro_code
        })
        X_new = pd.concat([X_num, X_cat], axis=1)[cols]
        y_new = le.transform(df['ofns_desc'])

        with lock:
            model.fit(X_new, y_new, xgb_model=model)
            save_pickle(model, MODEL_F)
            print("Modèle mis à jour !")

if __name__ == "__main__":
    listen_retrain()
