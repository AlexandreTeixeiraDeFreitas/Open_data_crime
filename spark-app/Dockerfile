FROM bde2020/spark-master:3.3.0-hadoop3.3

WORKDIR /app

RUN pip install kafka-python

# Ajoute le driver PostgreSQL en plus du connecteur Kafka
CMD /spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.24 \
    /app/spark-stream.py
