# ia/utils.py
import os
import requests
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder

API_URL = 'https://data.cityofnewyork.us/resource/5uac-w243.json'
LIMIT = 1000
TOP_K = 5

def fetch_all() -> pd.DataFrame:
    offset, rows = 0, []
    while True:
        r = requests.get(API_URL, params={'$limit': LIMIT, '$offset': offset})
        r.raise_for_status()
        page = r.json()
        if not page: break
        rows.extend(page)
        offset += LIMIT
    return pd.DataFrame(rows)

def preprocess(df: pd.DataFrame):
    df = (
        df.drop_duplicates('cmplnt_num')
          .dropna(subset=['addr_pct_cd','boro_nm','cmplnt_fr_dt','cmplnt_fr_tm','ofns_desc','latitude','longitude'])
          .copy()
    )
    df['cmplnt_fr_dt'] = pd.to_datetime(df['cmplnt_fr_dt'], errors='coerce')
    df['hour']       = pd.to_datetime(df['cmplnt_fr_tm'], format='%H:%M:%S', errors='coerce').dt.hour
    df = df.dropna(subset=['cmplnt_fr_dt','hour'])
    df['hour'] = df['hour'].astype(int)
    df['month'], df['wday'] = df['cmplnt_fr_dt'].dt.month, df['cmplnt_fr_dt'].dt.dayofweek
    # cyclic features
    df['h_sin'] = np.sin(2*np.pi*df['hour']/24)
    df['h_cos'] = np.cos(2*np.pi*df['hour']/24)
    df['w_sin'] = np.sin(2*np.pi*df['wday']/7)
    df['w_cos'] = np.cos(2*np.pi*df['wday']/7)
    df['lat'], df['lon'] = df['latitude'].astype(float), df['longitude'].astype(float)

    # ne garder que TOP_K ofns
    top = df['ofns_desc'].value_counts().nlargest(TOP_K).index
    df = df[df['ofns_desc'].isin(top)].copy()

    # encodings
    le = LabelEncoder().fit(df['ofns_desc'])
    y  = le.transform(df['ofns_desc'])
    oe = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)
    oe.fit(df[['addr_pct_cd','boro_nm']])
    cat_enc = oe.transform(df[['addr_pct_cd','boro_nm']])

    X_num = df[['month','h_sin','h_cos','w_sin','w_cos','lat','lon']].reset_index(drop=True)
    X_cat = pd.DataFrame(cat_enc, columns=['pct_enc','bor_enc'])
    X = pd.concat([X_num, X_cat], axis=1)

    return X, y, le, oe

def save_pickle(obj, path):
    joblib.dump(obj, path)

def load_pickle(path):
    return joblib.load(path)
