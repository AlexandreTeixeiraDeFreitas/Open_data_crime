# Utilise une image Python légère
FROM python:3.9-slim

# Création du dossier de travail
WORKDIR /app

# Installer les dépendances système nécessaires (build-essential pour xgboost, etc.)
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

# Copier tout le code IA (train.py, retrain.py, predict.py, utils.py, 
# flask_kafka_api.py, entrypoint.sh, requirements.txt…) dans /app
COPY . /app

# Installer les dépendances Python et rendre l’entrypoint exécutable
RUN pip install --no-cache-dir -r requirements.txt \
 && chmod +x entrypoint.sh

# Exposer le port de l’API Flask
EXPOSE 5001

# Au démarrage, ./entrypoint.sh vérifiera et entraînera si besoin,
# puis lancera flask_kafka_api.py (ou predict.py selon votre script)
ENTRYPOINT ["./entrypoint.sh"]
