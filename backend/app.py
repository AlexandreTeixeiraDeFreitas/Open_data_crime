from flask import Flask, request, Response
import requests
from kafka import KafkaProducer
import json
import os
from flask_cors import CORS

from logging.config import dictConfig

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

app = Flask(__name__)
CORS(app)
API_URL = 'https://data.cityofnewyork.us/resource/5uac-w243.json'
KAFKA_TOPIC = 'send-data'
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/crimes', methods=['GET'])
def crimes():
    headers = {'Accept': request.headers.get('Accept', 'application/json')}
    resp = requests.get(API_URL, params=request.args, headers=headers)

    if resp.status_code == 200:
        try:
            data = resp.json()
            for record in data:
                producer.send(KAFKA_TOPIC, record)
        except Exception:
            pass

    return Response(resp.content, status=resp.status_code, content_type=resp.headers.get('Content-Type'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
