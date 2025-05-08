#!/usr/bin/env python3
from spark_crime import start_flux_crime
from spark_train import start_confirm_send_ia
from spark_select import handle_sql_topic
from pyspark.sql import SparkSession

# SparkSession obligatoire ici
spark = SparkSession.builder.appName("MasterStreamingJob").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Liste des couples topic entrée/sortie
topic_pairs = [
    # ("sql-queries-user1", "sql-results-user1"),
    # ("sql-queries-admin", "sql-results-admin"),
]

# Liste des StreamingQuery à suivre
all_streams = []

# Lancer tous les flux SQL
for input_topic, output_topic in topic_pairs:
    print(f"🚀 Démarrage du flux SQL : {input_topic} → {output_topic}")
    stream = handle_sql_topic(spark, input_topic, output_topic)
    all_streams.append(stream)

# Ajouter le flux d'accusé de réception IA
print("🚀 Démarrage du flux lecture du topic Kafka train-data")
train_confirm_stream = start_confirm_send_ia(spark)
all_streams.append(train_confirm_stream)

# Ajouter le flux d'insertion de crimes + publication IA
print("🚀 Démarrage du flux Kafka vers PostgreSQL + IA : send-data → crimes/train-data")
crime_stream = start_flux_crime(spark)
all_streams.append(crime_stream)

# Afficher état global
print(f"✅ {len(all_streams)} flux lancés. En attente...")

# ✅ Tous les flux sont lancés
print("✅ Tous les flux sont lancés. En attente globale...")

# 🔄 Attente globale de tous les flux Spark
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"❌ Un des flux s’est terminé avec une erreur : {e}")