FROM python:3.11.5-slim

WORKDIR /app

RUN apt-get update && apt-get install -y cron build-essential libpq-dev

COPY requirements.txt ./
COPY daily_forecast.py ./

RUN pip install --no-cache-dir -r requirements.txt

# Ajout Cron Job
RUN echo "0 1 * * * python /app/daily_forecast.py >> /var/log/cron.log 2>&1" > /etc/cron.d/forecast-cron \
    && chmod 0644 /etc/cron.d/forecast-cron \
    && crontab /etc/cron.d/forecast-cron

# Commande qui exécute immédiatement puis démarre cron
CMD python /app/daily_forecast.py && cron -f
