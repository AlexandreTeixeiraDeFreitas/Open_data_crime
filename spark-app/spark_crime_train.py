#!/usr/bin/env python3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from schema import train_schema, cast_columns_to_schema
from functools import reduce
import json
import logging

# Logger configuration
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logger = logging.getLogger("crime_train")

jdbc_url = "jdbc:postgresql://postgres:5432/crimenyc"
db_props = {
    "user": "crimenyc",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder.appName("InsertOnlyNewCrimeTrain").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def ensure_train_table_exists():
    try:
        spark.read.jdbc(jdbc_url, "crime_train", properties=db_props).limit(1).collect()
    except:
        spark.createDataFrame([], train_schema).write.jdbc(jdbc_url, "crime_train", mode="overwrite", properties=db_props)
        
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    print(f"📥 Batch {batch_id} reçu avec {batch_df.count()} ligne(s)")
    ensure_train_table_exists()

    # Lecture des messages bruts Kafka
    raw_rows = batch_df.selectExpr("CAST(value AS STRING) AS raw").collect()
    unique_jsons = {}
    for row in raw_rows:
        try:
            parsed = json.loads(row["raw"])

            # Cas 1 : objet JSON unique
            if isinstance(parsed, dict):
                id = parsed.get("id")
                if id:
                    unique_jsons[id] = json.dumps(parsed)

            # Cas 2 : liste d’objets JSON
            elif isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        id = item.get("id")
                        if id:
                            unique_jsons[id] = json.dumps(item)
                    else:
                        logger.error(f"Ignoré : élément non-dict dans liste JSON : {item}")

            else:
                logger.error(f"Ignoré : contenu JSON non pris en charge : {parsed}")

        except Exception as e:
            logger.error(f"Erreur de parsing JSON : {e}")

    if not unique_jsons:
        print("⚠️ Aucun message Kafka exploitable.")
        return

    # Lecture des id déjà présents en base
    try:
        id_string = ",".join(f"'{id}'" for id in unique_jsons.keys())
        ids_query = f"SELECT id FROM \"crime_train\" WHERE id IN ({id_string})"
        existing_df = spark.read.jdbc(jdbc_url, f"({ids_query}) as existing", properties=db_props)
        existing_ids = set(row["id"] for row in existing_df.collect())
        print(f"🔎 {len(existing_ids)} lignes déjà en base crime_train.")
    except Exception as e:
        logger.error(f"Erreur lecture crime_train existants : {e}")
        existing_ids = set()

    # Filtrage des JSON à insérer
    jsons_to_insert = [json_str for id, json_str in unique_jsons.items() if id not in existing_ids]

    if not jsons_to_insert:
        print("📭 Aucune nouvelle ligne à insérer dans crime_train.")
    else:
        df_json = spark.read.json(spark.sparkContext.parallelize(jsons_to_insert))
        casted_df = cast_columns_to_schema(df_json, train_schema)
        casted_df.write.jdbc(jdbc_url, "crime_train", mode="append", properties=db_props)
        print(f"🗃️ {casted_df.count()} lignes insérées dans la table crime_train.")

# Streaming depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "send-train") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

query_crime_train = kafka_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/crimes_train") \
    .start()