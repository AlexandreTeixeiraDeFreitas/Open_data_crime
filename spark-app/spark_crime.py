#!/usr/bin/env python3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from schema import crime_schema, train_schema, cast_columns_to_schema
from functools import reduce
import json
import logging

# Logger configuration
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logger = logging.getLogger("crimes")

jdbc_url = "jdbc:postgresql://postgres:5432/crimenyc"
db_props = {
    "user": "crimenyc",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder.appName("InsertOnlyNewCrimesAndSendToKafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def ensure_table_exists():
    try:
        spark.read.jdbc(jdbc_url, "crimes", properties=db_props).limit(1).collect()
    except:
        spark.createDataFrame([], crime_schema).write.jdbc(jdbc_url, "crimes", mode="overwrite", properties=db_props)

def ensure_train_table_exists():
    try:
        spark.read.jdbc(jdbc_url, "crime_train", properties=db_props).limit(1).collect()
    except:
        spark.createDataFrame([], train_schema).write.jdbc(jdbc_url, "crime_train", mode="overwrite", properties=db_props)

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"📥 Batch {batch_id} reçu avec {batch_df.count()} ligne(s)")
    ensure_table_exists()
    ensure_train_table_exists()

    # Lecture des messages bruts Kafka
    raw_rows = batch_df.selectExpr("CAST(value AS STRING) AS raw").collect()
    unique_jsons = {}
    for row in raw_rows:
        try:
            crime = json.loads(row["raw"])
            cmplnt_num = crime.get("cmplnt_num")
            if cmplnt_num:
                unique_jsons[cmplnt_num] = json.dumps(crime)
        except Exception as e:
            logger.error(f"Erreur de parsing JSON : {e}")

    if not unique_jsons:
        print("⚠️ Aucun message Kafka exploitable.")
        return

    # Lecture des cmplnt_num déjà présents en base
    try:
        id_string = ",".join(f"'{id}'" for id in unique_jsons.keys())
        ids_query = f"SELECT cmplnt_num FROM \"crimes\" WHERE cmplnt_num IN ({id_string})"
        existing_df = spark.read.jdbc(jdbc_url, f"({ids_query}) as existing", properties=db_props)
        existing_ids = set(row["cmplnt_num"] for row in existing_df.collect())
        print(f"🔎 {len(existing_ids)} lignes déjà en base.")
    except Exception as e:
        logger.error(f"Erreur lecture crimes existants : {e}")
        existing_ids = set()

    # Filtrage des JSON à insérer
    jsons_to_insert = [json_str for cmplnt_num, json_str in unique_jsons.items() if cmplnt_num not in existing_ids]

    if not jsons_to_insert:
        print("📭 Aucune nouvelle ligne à insérer dans crimes.")
    else:
        df_json = spark.read.json(spark.sparkContext.parallelize(jsons_to_insert))
        casted_df = cast_columns_to_schema(df_json, crime_schema)
        casted_df.write.jdbc(jdbc_url, "crimes", mode="append", properties=db_props)
        print(f"🗃️ {casted_df.count()} lignes insérées dans la table crimes.")

    # Récupération des lignes à envoyer à l’IA
    query = """
        SELECT DISTINCT ON (c.cmplnt_num) c.*
        FROM "crimes" c
        WHERE NOT EXISTS (
        SELECT 1 FROM "crime_train" t WHERE t.id = c.cmplnt_num
        )
        ORDER BY c.cmplnt_num, c.rpt_dt DESC
    """
    joined_df = spark.read.jdbc(jdbc_url, f"({query}) AS sub", properties=db_props)

    if joined_df.count() >= 900:
        batch_to_send = joined_df
        kafka_df = batch_to_send.selectExpr("to_json(struct(*)) AS value")

        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "train-data") \
            .save()

        sent_ids = batch_to_send.select("cmplnt_num").withColumnRenamed("cmplnt_num", "id").withColumn("status", lit("sent"))
        sent_count = sent_ids.count()
        sent_ids.write.jdbc(jdbc_url, "crime_train", mode="append", properties=db_props)
        print(f"📤 {sent_count} lignes envoyées à l'IA.")
    else:
        print(f"⏳ Moins de 900 lignes disponibles à envoyer à l'IA ({joined_df.count()})")

# Streaming depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "send-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

query_crime = kafka_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/crimes_final") \
    .start()

