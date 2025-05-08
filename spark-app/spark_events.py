#!/usr/bin/env python3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from schema import schema_event, cast_columns_to_schema
from functools import reduce
import json
import logging

# Logger configuration
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logger = logging.getLogger("events")

jdbc_url = "jdbc:postgresql://postgres:5432/crimenyc"
db_props = {
    "user": "crimenyc",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder.appName("InsertOnlyNewEventAndSendToKafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def ensure_table_exists():
    try:
        spark.read.jdbc(jdbc_url, "events", properties=db_props).limit(1).collect()
    except:
        spark.createDataFrame([], schema_event).write.jdbc(jdbc_url, "events", mode="overwrite", properties=db_props)


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"📥 Batch {batch_id} reçu avec {batch_df.count()} ligne(s)")
    ensure_table_exists()

    # Lecture des messages bruts Kafka
    raw_rows = batch_df.selectExpr("CAST(value AS STRING) AS raw").collect()
    unique_jsons = {}
    for row in raw_rows:
        try:
            crime = json.loads(row["raw"])
            event_id = crime.get("event_id")
            if event_id:
                unique_jsons[event_id] = json.dumps(crime)
        except Exception as e:
            logger.error(f"Erreur de parsing JSON : {e}")

    if not unique_jsons:
        print("⚠️ Aucun message Kafka exploitable.")
        return

    # Lecture des event_id déjà présents en base
    try:
        id_string = ",".join(f"'{id}'" for id in unique_jsons.keys())
        ids_query = f"SELECT event_id FROM \"events\" WHERE event_id IN ({id_string})"
        existing_df = spark.read.jdbc(jdbc_url, f"({ids_query}) as existing", properties=db_props)
        existing_ids = set(row["event_id"] for row in existing_df.collect())
        print(f"🔎 {len(existing_ids)} lignes déjà en base, table events.")
    except Exception as e:
        logger.error(f"Erreur lecture events existants : {e}")
        existing_ids = set()

    # Filtrage des JSON à insérer
    jsons_to_insert = [json_str for event_id, json_str in unique_jsons.items() if event_id not in existing_ids]

    if not jsons_to_insert:
        print("📭 Aucune nouvelle ligne à insérer dans events.")
    else:
        df_json = spark.read.json(spark.sparkContext.parallelize(jsons_to_insert))
        casted_df = cast_columns_to_schema(df_json, schema_event)
        casted_df.write.jdbc(jdbc_url, "events", mode="append", properties=db_props)
        print(f"🗃️ {casted_df.count()} lignes insérées dans la table events.")

# Streaming depuis Kafka
kafka_read_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "send-event") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

query_event = kafka_read_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/events_final") \
    .start()

