#!/usr/bin/env python3
import json
import requests
import time
from kafka import KafkaProducer

# Configuration Kafka
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'send-data'

# API NYC
BASE_URL = "https://data.cityofnewyork.us/resource/5uac-w243.json"
LIMIT = 1000  # Nombre d'enregistrements par page

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_page(offset):
    params = {"$limit": LIMIT, "$offset": offset}
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        if response.status_code == 500:
            print(f"❌ Erreur 500 détectée à l'offset {offset}. Arrêt.")
            return None
        return response.json()
    except Exception as e:
        print(f"❌ Erreur HTTP à l'offset {offset}: {e}")
        return None

def main():
    offset = 0
    page_count = 0
    total_sent = 0

    while True:
        print(f"🔄 Récupération de la page offset {offset}...")
        data = fetch_page(offset)

        if data is None or not data:
            print("✅ Fin des données à envoyer.")
            break

        print(f"📄 Page {page_count + 1} : {len(data)} enregistrements à envoyer.")
        page_count += 1

        for record in data:
            producer.send(TOPIC_NAME, value=record)
            total_sent += 1

        producer.flush()
        print(f"✅ Page {page_count} envoyée ({len(data)} enregistrements).")

        offset += LIMIT
        time.sleep(0.5)

    print(f"✅ Terminé : {page_count} pages, {total_sent} enregistrements envoyés au topic '{TOPIC_NAME}'.")
    producer.close()    

if __name__ == "__main__":
    main()
