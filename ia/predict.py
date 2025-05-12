# ia/predict.py
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS
from threading import Lock
from utils import load_pickle

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})

# chargement global
model = load_pickle('xgb_top5_model.pkl')
le    = load_pickle('xgb_le_top5.pkl')
oe    = load_pickle('xgb_oe_top5.pkl')
cols  = load_pickle('xgb_top5_cols.pkl')
lock  = Lock()


@app.route('/predict', methods=['POST'])
def predict():
    if not request.is_json:
        return jsonify({'error': 'Invalid content type. Expected application/json'}), 415
    data = request.get_json()
    if not all(key in data for key in ['addr_pct_cd', 'boro_nm', 'cmplnt_fr_dt', 'cmplnt_fr_tm', 'latitude', 'longitude']):
        return jsonify({'error': 'Missing required fields in the input data'}), 400
    df = pd.DataFrame([data])
    df['cmplnt_fr_dt'] = pd.to_datetime(df['cmplnt_fr_dt'], errors='coerce')
    df['hour'] = pd.to_datetime(df['cmplnt_fr_tm'], format='%H:%M:%S', errors='coerce').dt.hour
    df = df.dropna(subset=['cmplnt_fr_dt', 'hour'])
    df['hour'] = df['hour'].astype(int)
    df['month'], df['wday'] = df['cmplnt_fr_dt'].dt.month, df['cmplnt_fr_dt'].dt.dayofweek
    df['h_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['h_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['w_sin'] = np.sin(2 * np.pi * df['wday'] / 7)
    df['w_cos'] = np.cos(2 * np.pi * df['wday'] / 7)
    df['lat'], df['lon'] = df['latitude'].astype(float), df['longitude'].astype(float)

    # Conversion en str avant transformation
    df['addr_pct_cd'] = df['addr_pct_cd'].astype(str)
    df['boro_nm'] = df['boro_nm'].astype(str)

    X_num = df[['month', 'h_sin', 'h_cos', 'w_sin', 'w_cos', 'lat', 'lon']].reset_index(drop=True)
    X_cat = pd.DataFrame(oe.transform(df[['addr_pct_cd', 'boro_nm']]), columns=['pct_enc', 'bor_enc'])
    X = pd.concat([X_num, X_cat], axis=1)[cols]

    with lock:
        pred = model.predict(X)
        prob = model.predict_proba(X).max(axis=1)[0].item()

    label = le.inverse_transform(pred)[0]
    return jsonify({'prediction': label, 'probability': prob})


@app.route('/', methods=['GET'])
def home():
    return jsonify({'message': 'IA service ready'})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
