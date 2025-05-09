FROM python:3.13-slim

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential bash \
 && rm -rf /var/lib/apt/lists/*

# Copier tout le projet
COPY . /app

# Copier aussi une version propre d'entrypoint.sh à la racine pour qu'il ne soit pas écrasé par le volume
COPY ./entrypoint.sh /entrypoint_clean.sh

# Nettoyer les CRLF sur la version propre
RUN sed -i 's/\r$//' /entrypoint_clean.sh && chmod +x /entrypoint_clean.sh

RUN pip install --no-cache-dir -r requirements.txt \
 && pip install pandas

EXPOSE 5001

# Lancer la version propre
CMD ["bash", "/entrypoint_clean.sh"]
