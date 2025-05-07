import json
import requests
from kafka import KafkaProducer

# Configuration Kafka
KAFKA_BROKER = 'kafka:9092'  # Change si nécessaire
TOPIC_NAME = 'send-data'

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Appel de l'API publique NYC
url = "https://data.cityofnewyork.us/resource/5uac-w243.json"

try:
    response = requests.get(url)
    response.raise_for_status()  # Lève une exception en cas d'erreur HTTP

    data = response.json()
    
    print(f"Envoi de {len(data)} enregistrements au topic Kafka '{TOPIC_NAME}'...")

    # Envoi de chaque objet individuellement dans le topic
    for record in data:
        producer.send(TOPIC_NAME, value=record)

    producer.flush()
    print("✅ Données envoyées avec succès.")

except requests.RequestException as e:
    print(f"Erreur lors de l'appel à l'API : {e}")
except Exception as e:
    print(f"Erreur inattendue : {e}")
finally:
    producer.close()
