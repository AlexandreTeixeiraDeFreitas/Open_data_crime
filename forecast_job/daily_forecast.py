import pandas as pd
import requests
from datetime import datetime, timedelta
import psycopg2
import os
from psycopg2.extras import RealDictCursor

def connect_db():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'crimenyc'),
        user=os.getenv('POSTGRES_USER', 'crimenyc'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin'),
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432')
    )

def ensure_table_exists(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crime_forecasts (
                id SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                addr_pct_cd VARCHAR(10) NOT NULL,
                boro_nm VARCHAR(50) NOT NULL,
                prediction_date DATE NOT NULL,
                prediction_time TIME NOT NULL,
                prediction VARCHAR(100) NOT NULL,
                probability DOUBLE PRECISION NOT NULL
            );
        """)
        conn.commit()
        print("✅ Table crime_forecasts vérifiée ou créée.", flush=True)

def fetch_positions(cursor):
    cursor.execute("""
        SELECT DISTINCT latitude, longitude, addr_pct_cd, boro_nm
        FROM Crimes
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND addr_pct_cd IS NOT NULL
    """)
    return cursor.fetchall()

def forecast_and_store():
    conn = connect_db()
    ensure_table_exists(conn)

    print("Lancement du script", flush=True)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    print("Sélection des positions déjà existantes", flush=True)
    records = fetch_positions(cursor)
    print("Sélection terminée", flush=True)

    if not records:
        print("❌ Aucune donnée à prédire.", flush=True)
        conn.close()
        return

    print("Filtrage des données", flush=True)
    df_base = pd.DataFrame(records)
    df_base = df_base.dropna(subset=['latitude', 'longitude', 'addr_pct_cd'])
    df_base = df_base[(df_base['latitude'] != 0) & (df_base['longitude'] != 0) & (df_base['addr_pct_cd'] != 0)]

    if df_base.empty:
        print("❌ Aucune donnée valide après filtrage.", flush=True)
        conn.close()
        return

    df_base = df_base.groupby(['addr_pct_cd', 'boro_nm']).agg({
        'latitude': 'mean',
        'longitude': 'mean'
    }).reset_index()
    print("Fin du filtrage avec position moyenne par secteur", flush=True)

    date_today = datetime.now().date()
    times = pd.date_range('00:00', '23:50', freq='10min').time
    time_strings = [t.strftime('%H:%M:%S') for t in times]

    total_positions = len(df_base)
    total_times = len(time_strings)
    total_steps = total_positions * total_times

    print("Préparer toutes les combinaisons à prédire", flush=True)

    prediction_requests = []
    step_count = 0

    for _, row in df_base.iterrows():
        for prediction_time_str in time_strings:
            prediction_requests.append({
                "payload": {
                    "addr_pct_cd": str(row["addr_pct_cd"]),
                    "boro_nm": row["boro_nm"],
                    "cmplnt_fr_dt": str(date_today),
                    "cmplnt_fr_tm": prediction_time_str,
                    "latitude": row["latitude"],
                    "longitude": row["longitude"]
                }
            })
            step_count += 1
            if step_count % 50 == 0 or step_count == total_steps:
                progress_message = f"🔄 Préparation {step_count}/{total_steps} combinaisons"
                print(progress_message.ljust(80), end='\r', flush=True)

    print("\n✅ Fin de la préparation", flush=True)

    print(f"📝 {len(prediction_requests)} prédictions à traiter.", flush=True)

    rows_to_insert = []
    total = len(prediction_requests)

    for idx, req in enumerate(prediction_requests, start=1):
        progress = f"🔄 Traitement {idx}/{total} : {req['payload']}"
        print(progress.ljust(120), end='\r', flush=True)
        try:
            response = requests.post("http://flask-ia:5001/predict", json=req["payload"], timeout=10)
            response.raise_for_status()
            result = response.json()
            payload = req["payload"]

            # Conversion en objet time
            prediction_time_obj = datetime.strptime(payload['cmplnt_fr_tm'], '%H:%M:%S').time()

            rows_to_insert.append((
                payload['latitude'], payload['longitude'], payload['addr_pct_cd'], payload['boro_nm'],
                payload['cmplnt_fr_dt'], prediction_time_obj,
                result['prediction'], result['probability']
            ))
        except Exception as e:
            print(f"\n❌ Erreur sur {req['payload']}: {e}", flush=True)
            continue

    print("\n🚀 Début de l'insertion des nouvelles prévisions en base PostgreSQL...", flush=True)

    if rows_to_insert:
        insert_query = """
            INSERT INTO crime_forecasts (latitude, longitude, addr_pct_cd, boro_nm, prediction_date, prediction_time, prediction, probability)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.executemany(insert_query, rows_to_insert)
            conn.commit()
            print(f"✅ {len(rows_to_insert)} lignes insérées.", flush=True)
        except Exception as e:
            print(f"❌ Erreur lors de l'insertion : {e}", flush=True)
            conn.rollback()
    else:
        print("ℹ️ Aucune nouvelle ligne à insérer.", flush=True)

    cursor.close()
    conn.close()
    print("✅ Processus terminé.", flush=True)

if __name__ == "__main__":
    forecast_and_store()
